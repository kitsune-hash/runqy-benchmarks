#!/usr/bin/env python3
"""
Benchmark script for Celery task queue.
Requires: pip install celery[redis]
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from celery import Celery
from benchmark_common import calculate_metrics, print_results, BenchmarkResult

# Configuration
REDIS_URL = "redis://localhost:6379/1"

# Create Celery app
app = Celery('benchmark')
app.config_from_object({
    'broker_url': REDIS_URL,
    'result_backend': REDIS_URL,
    'task_serializer': 'json',
    'result_serializer': 'json',
    'accept_content': ['json'],
    'task_acks_late': False,
    'worker_prefetch_multiplier': 1,
})

def submit_job(job_id: int, scenario: str = "simple") -> tuple:
    """Submit a job and measure submission time"""
    payload = {
        "id": job_id,
        "scenario": scenario,
        "timestamp": time.time()
    }
    
    start = time.perf_counter()
    try:
        # Send task (async, don't wait for result)
        task_name = f"benchmark.{scenario}"
        result = app.send_task(task_name, args=[payload])
        end = time.perf_counter()
        return (end - start, None)
    except Exception as e:
        end = time.perf_counter()
        return (end - start, str(e))

def benchmark_celery(jobs_count: int, scenario: str = "simple", concurrency: int = 10) -> BenchmarkResult:
    """
    Run benchmark against Celery.
    """
    print(f"\n🥬 Benchmarking Celery - {scenario} ({jobs_count:,} jobs, {concurrency} concurrent)")
    
    latencies = []
    errors = 0
    
    start_time = time.perf_counter()
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [
            executor.submit(submit_job, i, scenario) 
            for i in range(jobs_count)
        ]
        
        for future in as_completed(futures):
            latency, error = future.result()
            latencies.append(latency)
            if error:
                errors += 1
    
    end_time = time.perf_counter()
    
    result = calculate_metrics(
        system="Celery",
        scenario=scenario,
        jobs_count=jobs_count,
        start_time=start_time,
        end_time=end_time,
        latencies=latencies,
        errors=errors
    )
    
    print_results(result)
    return result

def check_redis():
    """Check if Redis is running"""
    try:
        import redis
        r = redis.from_url(REDIS_URL)
        r.ping()
        return True
    except:
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Benchmark Celery task queue")
    parser.add_argument("--jobs", type=int, default=1000, help="Number of jobs")
    parser.add_argument("--scenario", choices=["simple", "cpu", "io"], default="simple")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrent submissions")
    parser.add_argument("--redis", default=REDIS_URL, help="Redis URL")
    args = parser.parse_args()
    
    REDIS_URL = args.redis
    
    # Check Redis
    if not check_redis():
        print("❌ Redis not running at", REDIS_URL)
        exit(1)
    
    print("✅ Redis is running")
    
    # Run benchmark
    result = benchmark_celery(args.jobs, args.scenario, args.concurrency)
    
    # Save result
    import json
    with open(f"../results/celery_{args.scenario}_{args.jobs}.json", "w") as f:
        json.dump(result.to_dict(), f, indent=2)
