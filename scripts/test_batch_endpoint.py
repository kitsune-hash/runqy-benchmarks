#!/usr/bin/env python3
"""
Test the real /queue/add-batch endpoint.
"""

import time
import json
import requests
from dataclasses import dataclass

RUNQY_URL = "http://localhost:3000"
API_KEY = "test-api-key-456"  # Default test key


def test_batch_endpoint(jobs_count: int = 100):
    """Test the batch endpoint with N jobs."""
    print(f"\n🧪 Testing POST /queue/add-batch with {jobs_count} jobs...")
    
    # Create batch request
    jobs = [{"id": i, "scenario": "simple", "ts": time.time()} for i in range(jobs_count)]
    
    payload = {
        "queue": "benchmark",
        "timeout": 30,
        "jobs": jobs
    }
    
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    start = time.perf_counter()
    
    try:
        response = requests.post(
            f"{RUNQY_URL}/queue/add-batch",
            json=payload,
            headers=headers,
            timeout=60
        )
        
        end = time.perf_counter()
        duration = end - start
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Success!")
            print(f"   Enqueued: {result.get('enqueued', 0)}")
            print(f"   Failed: {result.get('failed', 0)}")
            print(f"   Duration: {duration*1000:.2f}ms")
            print(f"   Throughput: {jobs_count/duration:.0f} jobs/s")
            return result
        else:
            print(f"❌ Error: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
            
    except requests.exceptions.ConnectionError:
        print(f"❌ Cannot connect to {RUNQY_URL}")
        print("   Is the Runqy server running?")
        return None


def benchmark_batch_sizes():
    """Benchmark different batch sizes."""
    print("\n" + "="*60)
    print("BATCH ENDPOINT BENCHMARK")
    print("="*60)
    
    batch_sizes = [10, 50, 100, 500, 1000]
    total_jobs_per_test = 10000
    
    results = []
    
    for batch_size in batch_sizes:
        num_requests = total_jobs_per_test // batch_size
        
        print(f"\n📦 Batch size: {batch_size} ({num_requests} requests)")
        
        jobs = [{"id": i, "scenario": "simple"} for i in range(batch_size)]
        payload = {
            "queue": "benchmark",
            "timeout": 30,
            "jobs": jobs
        }
        headers = {
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json"
        }
        
        start = time.perf_counter()
        total_enqueued = 0
        errors = 0
        
        for _ in range(num_requests):
            try:
                response = requests.post(
                    f"{RUNQY_URL}/queue/add-batch",
                    json=payload,
                    headers=headers,
                    timeout=60
                )
                if response.status_code == 200:
                    result = response.json()
                    total_enqueued += result.get("enqueued", 0)
                else:
                    errors += 1
            except:
                errors += 1
        
        end = time.perf_counter()
        duration = end - start
        throughput = total_enqueued / duration if duration > 0 else 0
        
        print(f"   Enqueued: {total_enqueued}")
        print(f"   Errors: {errors}")
        print(f"   Duration: {duration:.2f}s")
        print(f"   Throughput: {throughput:.0f} jobs/s")
        
        results.append({
            "batch_size": batch_size,
            "throughput": throughput,
            "duration": duration
        })
    
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    for r in results:
        print(f"  Batch {r['batch_size']:4d}: {r['throughput']:,.0f} jobs/s")
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", type=int, default=100, help="Quick test with N jobs")
    parser.add_argument("--benchmark", action="store_true", help="Full benchmark")
    parser.add_argument("--url", default=RUNQY_URL)
    parser.add_argument("--key", default=API_KEY)
    args = parser.parse_args()
    
    RUNQY_URL = args.url
    API_KEY = args.key
    
    if args.benchmark:
        benchmark_batch_sizes()
    else:
        test_batch_endpoint(args.test)
