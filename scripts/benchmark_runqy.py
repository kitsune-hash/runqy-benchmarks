#!/usr/bin/env python3
"""
Benchmark script for Runqy task queue.
Requires: pip install runqy-python requests
"""

import time
import asyncio
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from benchmark_common import calculate_metrics, print_results, BenchmarkResult

# Configuration
RUNQY_URL = "http://localhost:3000"
RUNQY_API_KEY = "test-api-key-456"  # Default test key
QUEUE_NAME = "benchmark"

def submit_job(job_id: int, scenario: str = "simple") -> tuple:
    """Submit a job and measure round-trip time"""
    payload = {
        "queue": QUEUE_NAME,
        "payload": {
            "id": job_id,
            "scenario": scenario,
            "timestamp": time.time()
        }
    }
    
    start = time.perf_counter()
    try:
        response = requests.post(
            f"{RUNQY_URL}/queue/add",
            json=payload,
            headers={
                "Authorization": f"Bearer {RUNQY_API_KEY}",
                "Content-Type": "application/json"
            },
            timeout=30
        )
        end = time.perf_counter()
        
        if response.status_code == 200:
            return (end - start, None)
        else:
            return (end - start, f"HTTP {response.status_code}")
    except Exception as e:
        end = time.perf_counter()
        return (end - start, str(e))

def benchmark_runqy(jobs_count: int, scenario: str = "simple", concurrency: int = 10) -> BenchmarkResult:
    """
    Run benchmark against Runqy.
    
    Args:
        jobs_count: Number of jobs to submit
        scenario: Type of job (simple, cpu, io)
        concurrency: Number of concurrent submissions
    """
    print(f"\n🚀 Benchmarking Runqy - {scenario} ({jobs_count:,} jobs, {concurrency} concurrent)")
    
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
        system="Runqy",
        scenario=scenario,
        jobs_count=jobs_count,
        start_time=start_time,
        end_time=end_time,
        latencies=latencies,
        errors=errors
    )
    
    print_results(result)
    return result

def check_runqy_server():
    """Check if Runqy server is running"""
    try:
        response = requests.get(
            f"{RUNQY_URL}/workers/queues",
            headers={"Authorization": f"Bearer {RUNQY_API_KEY}"},
            timeout=5
        )
        return response.status_code == 200
    except:
        return False

def check_worker_connected():
    """Check if at least one worker is connected"""
    try:
        response = requests.get(
            f"{RUNQY_URL}/workers",
            headers={"Authorization": f"Bearer {RUNQY_API_KEY}"},
            timeout=5
        )
        if response.status_code == 200:
            data = response.json()
            return data.get("count", 0) > 0
        return False
    except:
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Benchmark Runqy task queue")
    parser.add_argument("--jobs", type=int, default=1000, help="Number of jobs")
    parser.add_argument("--scenario", choices=["simple", "cpu", "io"], default="simple")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrent submissions")
    parser.add_argument("--url", default=RUNQY_URL, help="Runqy server URL")
    parser.add_argument("--api-key", default=RUNQY_API_KEY, help="API key")
    args = parser.parse_args()
    
    RUNQY_URL = args.url
    RUNQY_API_KEY = args.api_key
    
    # Check server
    if not check_runqy_server():
        print("❌ Runqy server not running at", RUNQY_URL)
        exit(1)
    
    print("✅ Runqy server is running")
    
    # Check worker
    if not check_worker_connected():
        print("⚠️  No workers connected - jobs will queue but not process")
    else:
        print("✅ Worker(s) connected")
    
    # Run benchmark
    result = benchmark_runqy(args.jobs, args.scenario, args.concurrency)
    
    # Save result
    import json
    with open(f"../results/runqy_{args.scenario}_{args.jobs}.json", "w") as f:
        json.dump(result.to_dict(), f, indent=2)
