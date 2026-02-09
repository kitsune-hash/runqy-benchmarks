#!/usr/bin/env python3
"""
Benchmark script for Runqy - Direct Redis insertion (asynq format).
Bypasses HTTP API for fair comparison with BullMQ/Celery.

Requires: pip install redis msgpack
"""

import time
import json
import uuid
import msgpack
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import redis

# Configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
QUEUE_NAME = "benchmark.default"  # Runqy format: queue.subqueue


@dataclass
class BenchmarkResult:
    """Result of a benchmark run."""
    system: str
    job_count: int
    total_time_seconds: float
    throughput_per_second: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    errors: int
    
    def to_dict(self):
        return {
            "system": self.system,
            "job_count": self.job_count,
            "total_time_seconds": self.total_time_seconds,
            "throughput_per_second": self.throughput_per_second,
            "latency_p50_ms": self.latency_p50_ms,
            "latency_p95_ms": self.latency_p95_ms,
            "latency_p99_ms": self.latency_p99_ms,
            "errors": self.errors
        }


def create_asynq_task(task_id: str, queue: str, payload: dict) -> bytes:
    """
    Create an asynq task message in msgpack format.
    
    Asynq task structure (simplified):
    {
        "type": str,       # task type name
        "payload": bytes,  # msgpack encoded payload
        "id": str,         # unique task ID  
        "queue": str,      # queue name
        "retry": int,      # max retries
        "timeout": int,    # timeout in seconds
        "deadline": int,   # unix timestamp
    }
    """
    task = {
        "type": f"task:{queue}",
        "payload": msgpack.packb(payload),
        "id": task_id,
        "queue": queue,
        "retry": 3,
        "timeout": 30,
        "deadline": int(time.time()) + 3600,  # 1 hour from now
    }
    return msgpack.packb(task)


def submit_task_direct(rdb: redis.Redis, job_id: int, queue: str = QUEUE_NAME) -> tuple:
    """Submit a task directly to Redis in asynq format."""
    task_id = str(uuid.uuid4())
    payload = {
        "id": job_id,
        "scenario": "simple",
        "timestamp": time.time()
    }
    
    start = time.perf_counter()
    try:
        # Create task message
        task_msg = create_asynq_task(task_id, queue, payload)
        
        # Push to pending list (asynq format)
        pending_key = f"asynq:{{{queue}}}:pending"
        
        # Use pipeline for atomic operations
        pipe = rdb.pipeline()
        pipe.lpush(pending_key, task_msg)
        # Store task metadata
        pipe.hset(f"asynq:t:{task_id}", mapping={
            "queue": queue,
            "state": "pending",
            "created": str(int(time.time()))
        })
        # Register queue
        pipe.sadd("asynq:queues", queue)
        pipe.execute()
        
        end = time.perf_counter()
        return (end - start, None)
    except Exception as e:
        end = time.perf_counter()
        return (end - start, str(e))


def benchmark_runqy_direct(jobs_count: int, concurrency: int = 10) -> BenchmarkResult:
    """
    Run benchmark against Runqy via direct Redis insertion.
    """
    print(f"\n🚀 Benchmarking Runqy (Direct Redis) - {jobs_count:,} jobs, {concurrency} concurrent")
    
    # Create Redis connection pool
    pool = redis.ConnectionPool(
        host=REDIS_HOST, 
        port=REDIS_PORT, 
        db=REDIS_DB,
        max_connections=concurrency + 10
    )
    
    latencies = []
    errors = 0
    
    start_time = time.perf_counter()
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for i in range(jobs_count):
            rdb = redis.Redis(connection_pool=pool)
            futures.append(executor.submit(submit_task_direct, rdb, i))
        
        for future in as_completed(futures):
            latency, error = future.result()
            latencies.append(latency)
            if error:
                errors += 1
    
    end_time = time.perf_counter()
    total_time = end_time - start_time
    
    # Calculate percentiles
    latencies.sort()
    latencies_ms = [l * 1000 for l in latencies]
    
    p50_idx = int(len(latencies_ms) * 0.50)
    p95_idx = int(len(latencies_ms) * 0.95)
    p99_idx = min(int(len(latencies_ms) * 0.99), len(latencies_ms) - 1)
    
    result = BenchmarkResult(
        system="runqy",
        job_count=jobs_count,
        total_time_seconds=total_time,
        throughput_per_second=jobs_count / total_time,
        latency_p50_ms=latencies_ms[p50_idx],
        latency_p95_ms=latencies_ms[p95_idx],
        latency_p99_ms=latencies_ms[p99_idx],
        errors=errors
    )
    
    print(f"\nResults:")
    print(f"  Throughput: {result.throughput_per_second:.2f} jobs/s")
    print(f"  Latency P50: {result.latency_p50_ms:.2f}ms")
    print(f"  Latency P95: {result.latency_p95_ms:.2f}ms")
    print(f"  Latency P99: {result.latency_p99_ms:.2f}ms")
    print(f"  Errors: {result.errors}")
    
    return result


def check_redis():
    """Check if Redis is running."""
    try:
        rdb = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        rdb.ping()
        return True
    except:
        return False


if __name__ == "__main__":
    import argparse
    from pathlib import Path
    
    parser = argparse.ArgumentParser(description="Benchmark Runqy (direct Redis)")
    parser.add_argument("--jobs", type=int, default=1000, help="Number of jobs")
    parser.add_argument("--concurrency", type=int, default=50, help="Concurrent submissions")
    parser.add_argument("--all", action="store_true", help="Run all benchmarks (1K, 10K, 50K)")
    args = parser.parse_args()
    
    # Check Redis
    if not check_redis():
        print("❌ Redis not running at", f"{REDIS_HOST}:{REDIS_PORT}")
        exit(1)
    
    print("✅ Redis is running")
    
    results_dir = Path(__file__).parent.parent / "results"
    results_dir.mkdir(exist_ok=True)
    
    if args.all:
        job_counts = [1000, 10000, 50000]
    else:
        job_counts = [args.jobs]
    
    for count in job_counts:
        result = benchmark_runqy_direct(count, args.concurrency)
        
        # Save result
        output_file = results_dir / f"runqy_simple_{count}.json"
        with open(output_file, "w") as f:
            json.dump(result.to_dict(), f, indent=2)
        print(f"Saved: {output_file}")
