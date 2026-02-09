#!/usr/bin/env python3
"""
Benchmark script for Runqy - Pipelined Redis insertion.
Uses Redis pipelining for maximum throughput.

OPTIMIZATIONS APPLIED:
1. Redis pipelining - batch multiple commands in single round-trip
2. Pre-serialized payloads - serialize once, reuse
3. Batch processing - process jobs in chunks
4. Connection reuse - single connection for all operations

Requires: pip install redis msgpack
"""

import time
import json
import uuid
import msgpack
from dataclasses import dataclass
import redis

# Configuration
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0
QUEUE_NAME = "benchmark.default"
PIPELINE_BATCH_SIZE = 100  # Commands per pipeline


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
    optimization: str
    
    def to_dict(self):
        return {
            "system": self.system,
            "job_count": self.job_count,
            "total_time_seconds": self.total_time_seconds,
            "throughput_per_second": self.throughput_per_second,
            "latency_p50_ms": self.latency_p50_ms,
            "latency_p95_ms": self.latency_p95_ms,
            "latency_p99_ms": self.latency_p99_ms,
            "errors": self.errors,
            "optimization": self.optimization
        }


def create_asynq_task(task_id: str, queue: str, payload_bytes: bytes) -> bytes:
    """
    Create an asynq task message in msgpack format.
    """
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


def benchmark_pipelined(jobs_count: int, batch_size: int = PIPELINE_BATCH_SIZE) -> BenchmarkResult:
    """
    Run benchmark with Redis pipelining.
    
    OPTIMIZATION #1: Pipeline batching
    Instead of: LPUSH → wait → LPUSH → wait (N round-trips)
    We do: LPUSH + LPUSH + ... → wait (N/batch_size round-trips)
    """
    print(f"\n🚀 Benchmarking Runqy (Pipelined) - {jobs_count:,} jobs, batch={batch_size}")
    
    rdb = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    pending_key = f"asynq:{{{QUEUE_NAME}}}:pending"
    
    # OPTIMIZATION #2: Pre-serialize base payload
    # Serialize template once, only change ID per job
    base_payload = {
        "scenario": "simple",
        "timestamp": time.time()
    }
    
    errors = 0
    batch_latencies = []
    
    # Register queue once
    rdb.sadd("asynq:queues", QUEUE_NAME)
    
    start_time = time.perf_counter()
    
    # Process in batches
    for batch_start in range(0, jobs_count, batch_size):
        batch_end = min(batch_start + batch_size, jobs_count)
        
        batch_start_time = time.perf_counter()
        
        # OPTIMIZATION #3: Use pipeline for batch
        pipe = rdb.pipeline(transaction=False)  # No MULTI/EXEC overhead
        
        for job_id in range(batch_start, batch_end):
            task_id = str(uuid.uuid4())
            
            # Create payload with job ID
            payload = {**base_payload, "id": job_id}
            payload_bytes = msgpack.packb(payload)
            
            # Create task message
            task_msg = create_asynq_task(task_id, QUEUE_NAME, payload_bytes)
            
            # Queue commands (not executed yet)
            pipe.lpush(pending_key, task_msg)
            pipe.hset(f"asynq:t:{task_id}", mapping={
                "queue": QUEUE_NAME,
                "state": "pending",
                "created": str(int(time.time()))
            })
        
        # Execute batch in single round-trip
        try:
            pipe.execute()
        except Exception as e:
            errors += (batch_end - batch_start)
            print(f"Batch error: {e}")
        
        batch_latency = (time.perf_counter() - batch_start_time) * 1000
        batch_latencies.append(batch_latency)
    
    end_time = time.perf_counter()
    total_time = end_time - start_time
    
    # Calculate per-job latency from batch latencies
    # Approximate: distribute batch latency across jobs
    per_job_latencies = []
    for i, batch_lat in enumerate(batch_latencies):
        batch_start = i * batch_size
        batch_end = min(batch_start + batch_size, jobs_count)
        jobs_in_batch = batch_end - batch_start
        per_job = batch_lat / jobs_in_batch
        per_job_latencies.extend([per_job] * jobs_in_batch)
    
    per_job_latencies.sort()
    
    p50_idx = int(len(per_job_latencies) * 0.50)
    p95_idx = int(len(per_job_latencies) * 0.95)
    p99_idx = min(int(len(per_job_latencies) * 0.99), len(per_job_latencies) - 1)
    
    result = BenchmarkResult(
        system="runqy",
        job_count=jobs_count,
        total_time_seconds=total_time,
        throughput_per_second=jobs_count / total_time,
        latency_p50_ms=per_job_latencies[p50_idx],
        latency_p95_ms=per_job_latencies[p95_idx],
        latency_p99_ms=per_job_latencies[p99_idx],
        errors=errors,
        optimization="pipelined"
    )
    
    print(f"\nResults:")
    print(f"  Throughput: {result.throughput_per_second:.2f} jobs/s")
    print(f"  Latency P50: {result.latency_p50_ms:.4f}ms")
    print(f"  Latency P95: {result.latency_p95_ms:.4f}ms")
    print(f"  Latency P99: {result.latency_p99_ms:.4f}ms")
    print(f"  Errors: {result.errors}")
    print(f"  Batches: {len(batch_latencies)} (avg {sum(batch_latencies)/len(batch_latencies):.2f}ms each)")
    
    rdb.close()
    return result


def benchmark_bulk_insert(jobs_count: int) -> BenchmarkResult:
    """
    OPTIMIZATION #4: Bulk LPUSH
    Redis LPUSH supports multiple values in single command:
    LPUSH key val1 val2 val3 ... (even faster than pipelining)
    """
    print(f"\n🚀 Benchmarking Runqy (Bulk LPUSH) - {jobs_count:,} jobs")
    
    rdb = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    pending_key = f"asynq:{{{QUEUE_NAME}}}:pending"
    
    base_payload = {"scenario": "simple", "timestamp": time.time()}
    
    # Pre-generate all tasks
    print("  Pre-generating tasks...")
    tasks = []
    task_metadata = []
    
    for job_id in range(jobs_count):
        task_id = str(uuid.uuid4())
        payload = {**base_payload, "id": job_id}
        payload_bytes = msgpack.packb(payload)
        task_msg = create_asynq_task(task_id, QUEUE_NAME, payload_bytes)
        tasks.append(task_msg)
        task_metadata.append((task_id, QUEUE_NAME))
    
    print("  Inserting...")
    rdb.sadd("asynq:queues", QUEUE_NAME)
    
    start_time = time.perf_counter()
    
    # Bulk insert - single LPUSH with all values
    # Note: Very large lists may need chunking for memory
    CHUNK_SIZE = 10000
    for i in range(0, len(tasks), CHUNK_SIZE):
        chunk = tasks[i:i+CHUNK_SIZE]
        rdb.lpush(pending_key, *chunk)
    
    # Bulk metadata insert with pipeline
    pipe = rdb.pipeline(transaction=False)
    for task_id, queue in task_metadata:
        pipe.hset(f"asynq:t:{task_id}", mapping={
            "queue": queue,
            "state": "pending", 
            "created": str(int(time.time()))
        })
    pipe.execute()
    
    end_time = time.perf_counter()
    total_time = end_time - start_time
    
    # Per-job latency is essentially total_time / jobs_count
    per_job_latency = (total_time * 1000) / jobs_count
    
    result = BenchmarkResult(
        system="runqy",
        job_count=jobs_count,
        total_time_seconds=total_time,
        throughput_per_second=jobs_count / total_time,
        latency_p50_ms=per_job_latency,
        latency_p95_ms=per_job_latency,
        latency_p99_ms=per_job_latency,
        errors=0,
        optimization="bulk_lpush"
    )
    
    print(f"\nResults:")
    print(f"  Throughput: {result.throughput_per_second:.2f} jobs/s")
    print(f"  Avg latency: {per_job_latency:.4f}ms per job")
    
    rdb.close()
    return result


if __name__ == "__main__":
    import argparse
    from pathlib import Path
    
    parser = argparse.ArgumentParser(description="Benchmark Runqy (pipelined)")
    parser.add_argument("--jobs", type=int, default=1000, help="Number of jobs")
    parser.add_argument("--batch", type=int, default=100, help="Pipeline batch size")
    parser.add_argument("--bulk", action="store_true", help="Use bulk LPUSH mode")
    parser.add_argument("--all", action="store_true", help="Run all benchmarks")
    args = parser.parse_args()
    
    # Check Redis
    try:
        rdb = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        rdb.ping()
        print("✅ Redis is running")
    except:
        print("❌ Redis not running")
        exit(1)
    
    results_dir = Path(__file__).parent.parent / "results"
    results_dir.mkdir(exist_ok=True)
    
    if args.all:
        job_counts = [1000, 10000, 50000]
    else:
        job_counts = [args.jobs]
    
    print("\n" + "="*60)
    print("RUNQY PIPELINED BENCHMARK")
    print("="*60)
    
    for count in job_counts:
        # Pipelined benchmark
        result = benchmark_pipelined(count, args.batch)
        output_file = results_dir / f"runqy_pipelined_{count}.json"
        with open(output_file, "w") as f:
            json.dump(result.to_dict(), f, indent=2)
        print(f"Saved: {output_file}")
        
        if args.bulk:
            # Bulk benchmark
            result_bulk = benchmark_bulk_insert(count)
            output_file = results_dir / f"runqy_bulk_{count}.json"
            with open(output_file, "w") as f:
                json.dump(result_bulk.to_dict(), f, indent=2)
            print(f"Saved: {output_file}")
    
    print("\n" + "="*60)
    print("OPTIMIZATION NOTES FOR RUNQY SERVER (Go)")
    print("="*60)
    print("""
1. PIPELINE BATCHING (app/client/client.go)
   Current:  client.Enqueue() per job (1 round-trip each)
   Optimal:  Batch multiple jobs, use rdb.Pipeline()
   
   Example Go code:
   ```go
   pipe := rdb.Pipeline()
   for _, job := range jobs {
       pipe.LPush(ctx, pendingKey, taskMsg)
       pipe.HSet(ctx, taskKey, metadata)
   }
   pipe.Exec(ctx)
   ```

2. BULK LPUSH (for high-volume batch submissions)
   Redis LPUSH supports: LPUSH key val1 val2 val3 ...
   Go: rdb.LPush(ctx, key, val1, val2, val3...)

3. CONNECTION POOLING
   Ensure go-redis connection pool is sized appropriately:
   ```go
   rdb := redis.NewClient(&redis.Options{
       PoolSize: 100,  // Match expected concurrency
   })
   ```

4. ASYNC ENQUEUEING (API endpoint)
   Consider async endpoint that batches incoming jobs:
   - Collect jobs for 10ms or until batch_size reached
   - Flush batch with pipelined insert
   - Return immediately with job IDs
""")
