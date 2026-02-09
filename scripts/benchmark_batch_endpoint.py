#!/usr/bin/env python3
"""
Benchmark simulation for Runqy batch endpoint.
Simulates the server-side processing of POST /queue/add-batch

This measures what the endpoint would achieve with pipelining.
"""

import time
import json
import uuid
import msgpack
from dataclasses import dataclass
import redis

REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
QUEUE_NAME = "benchmark.default"


@dataclass
class BenchmarkResult:
    system: str
    job_count: int
    total_time_seconds: float
    throughput_per_second: float
    batch_size: int
    optimization: str
    
    def to_dict(self):
        return {
            "system": self.system,
            "job_count": self.job_count,
            "total_time_seconds": self.total_time_seconds,
            "throughput_per_second": self.throughput_per_second,
            "batch_size": self.batch_size,
            "optimization": self.optimization
        }


def create_asynq_task(task_id: str, queue: str, payload_bytes: bytes) -> bytes:
    """Create asynq task in msgpack format."""
    task = {
        "type": f"task:{queue}",
        "payload": payload_bytes,
        "id": task_id,
        "queue": queue,
        "retry": 3,
        "timeout": 30,
        "deadline": int(time.time()) + 3600,
    }
    return msgpack.packb(task)


def simulate_batch_endpoint(jobs: list, queue: str = QUEUE_NAME) -> dict:
    """
    Simulates what POST /queue/add-batch does server-side.
    
    This is what the Go endpoint will do:
    1. Parse JSON request
    2. Create tasks
    3. Pipeline insert to Redis
    4. Return response
    """
    rdb = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    ctx_start = time.perf_counter()
    
    pending_key = f"asynq:{{{queue}}}:pending"
    pipe = rdb.pipeline()
    
    task_ids = []
    errors = []
    
    # Process jobs (simulating Go's processing)
    for job in jobs:
        try:
            task_id = str(uuid.uuid4())
            payload_bytes = msgpack.packb(job)
            task_msg = create_asynq_task(task_id, queue, payload_bytes)
            
            # Queue Redis commands
            pipe.lpush(pending_key, task_msg)
            pipe.hset(f"asynq:t:{task_id}", mapping={
                "queue": queue,
                "state": "pending",
                "created": str(int(time.time()))
            })
            
            task_ids.append(task_id)
        except Exception as e:
            errors.append(str(e))
    
    # Execute pipeline
    pipe.sadd("asynq:queues", queue)
    pipe.execute()
    
    processing_time = time.perf_counter() - ctx_start
    
    rdb.close()
    
    return {
        "enqueued": len(task_ids),
        "failed": len(errors),
        "task_ids": task_ids,
        "processing_time_ms": processing_time * 1000
    }


def benchmark_batch_endpoint(total_jobs: int, batch_size: int = 100) -> BenchmarkResult:
    """
    Benchmark the batch endpoint simulation.
    
    Simulates multiple HTTP requests with batch_size jobs each.
    """
    print(f"\n🚀 Benchmarking Batch Endpoint Simulation")
    print(f"   Total jobs: {total_jobs:,}")
    print(f"   Batch size: {batch_size}")
    print(f"   Requests: {total_jobs // batch_size}")
    
    # Pre-generate jobs
    jobs = [{"id": i, "scenario": "simple", "ts": time.time()} for i in range(total_jobs)]
    
    start_time = time.perf_counter()
    
    total_enqueued = 0
    request_times = []
    
    # Simulate HTTP requests with batches
    for i in range(0, total_jobs, batch_size):
        batch = jobs[i:i+batch_size]
        
        req_start = time.perf_counter()
        result = simulate_batch_endpoint(batch)
        req_time = time.perf_counter() - req_start
        
        request_times.append(req_time)
        total_enqueued += result["enqueued"]
    
    end_time = time.perf_counter()
    total_time = end_time - start_time
    
    avg_request_time = sum(request_times) / len(request_times) * 1000
    
    result = BenchmarkResult(
        system="runqy",
        job_count=total_jobs,
        total_time_seconds=total_time,
        throughput_per_second=total_jobs / total_time,
        batch_size=batch_size,
        optimization="batch_endpoint"
    )
    
    print(f"\nResults:")
    print(f"  Total throughput: {result.throughput_per_second:.2f} jobs/s")
    print(f"  Avg request time: {avg_request_time:.2f}ms")
    print(f"  Jobs per request: {batch_size}")
    print(f"  Requests/s: {len(request_times) / total_time:.2f}")
    
    return result


def compare_single_vs_batch():
    """Compare single-job API vs batch endpoint."""
    print("\n" + "="*60)
    print("COMPARISON: Single Job API vs Batch Endpoint")
    print("="*60)
    
    # Simulate current single-job HTTP API overhead
    # Assuming ~1ms network + ~1ms parsing + ~0.5ms Redis = ~2.5ms per job
    single_job_overhead_ms = 2.5
    jobs_per_second_single = 1000 / single_job_overhead_ms
    
    print(f"\n📍 Single Job API (POST /queue/add):")
    print(f"   Overhead per job: ~{single_job_overhead_ms}ms")
    print(f"   Theoretical max: ~{jobs_per_second_single:.0f} jobs/s")
    
    # Batch endpoint with pipelining
    print(f"\n📦 Batch Endpoint (POST /queue/add-batch):")
    
    batch_sizes = [10, 50, 100, 500, 1000]
    for bs in batch_sizes:
        result = benchmark_batch_endpoint(10000, batch_size=bs)
        print(f"   Batch={bs}: {result.throughput_per_second:.0f} jobs/s")
    
    print("\n" + "="*60)


if __name__ == "__main__":
    import argparse
    from pathlib import Path
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=10000)
    parser.add_argument("--batch", type=int, default=100)
    parser.add_argument("--compare", action="store_true")
    args = parser.parse_args()
    
    # Check Redis
    try:
        rdb = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        rdb.ping()
        print("✅ Redis is running")
    except:
        print("❌ Redis not running")
        exit(1)
    
    if args.compare:
        compare_single_vs_batch()
    else:
        result = benchmark_batch_endpoint(args.jobs, args.batch)
        
        results_dir = Path(__file__).parent.parent / "results"
        results_dir.mkdir(exist_ok=True)
        
        output_file = results_dir / f"runqy_batch_endpoint_{args.jobs}.json"
        with open(output_file, "w") as f:
            json.dump(result.to_dict(), f, indent=2)
        print(f"Saved: {output_file}")
