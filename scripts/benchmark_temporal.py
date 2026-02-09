#!/usr/bin/env python3
"""
Temporal Benchmark Script
Measures workflow submission throughput and latency.
"""
import asyncio
import json
import time
import statistics
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent))
from benchmark_common import BenchmarkResult, save_results

try:
    from temporalio.client import Client
except ImportError:
    print("temporalio not installed. Run: pip install temporalio")
    sys.exit(1)


@dataclass
class TemporalBenchmarkResult:
    """Result of a Temporal benchmark run."""
    system: str = "temporal"
    job_count: int = 0
    total_time_seconds: float = 0.0
    throughput_per_second: float = 0.0
    latency_p50_ms: float = 0.0
    latency_p95_ms: float = 0.0
    latency_p99_ms: float = 0.0
    errors: int = 0


async def submit_workflow(client: Client, payload: str) -> tuple[float, bool]:
    """Submit a workflow and measure latency."""
    start = time.perf_counter()
    try:
        handle = await client.start_workflow(
            "SimpleWorkflow",
            payload,
            id=f"bench-{time.time_ns()}",
            task_queue="benchmark-queue",
        )
        latency = (time.perf_counter() - start) * 1000  # ms
        return latency, True
    except Exception as e:
        print(f"Error: {e}")
        return 0.0, False


async def run_benchmark(
    host: str = "localhost",
    port: int = 7233,
    job_count: int = 1000,
    concurrency: int = 10,
) -> TemporalBenchmarkResult:
    """Run the Temporal benchmark."""
    print(f"\n{'='*50}")
    print(f"Temporal Benchmark: {job_count} workflows")
    print(f"{'='*50}")
    
    # Connect to Temporal
    client = await Client.connect(f"{host}:{port}")
    
    latencies: list[float] = []
    errors = 0
    
    # Warmup
    print("Warming up...")
    for i in range(min(100, job_count // 10)):
        await submit_workflow(client, f"warmup-{i}")
    
    print(f"Running benchmark with concurrency={concurrency}...")
    start_time = time.perf_counter()
    
    # Use semaphore for concurrency control
    semaphore = asyncio.Semaphore(concurrency)
    
    async def bounded_submit(i: int) -> tuple[float, bool]:
        async with semaphore:
            return await submit_workflow(client, f"bench-payload-{i}")
    
    # Submit all jobs
    tasks = [bounded_submit(i) for i in range(job_count)]
    results = await asyncio.gather(*tasks)
    
    total_time = time.perf_counter() - start_time
    
    # Collect results
    for latency, success in results:
        if success:
            latencies.append(latency)
        else:
            errors += 1
    
    # Calculate statistics
    if latencies:
        latencies.sort()
        p50_idx = int(len(latencies) * 0.50)
        p95_idx = int(len(latencies) * 0.95)
        p99_idx = int(len(latencies) * 0.99)
        
        result = TemporalBenchmarkResult(
            job_count=job_count,
            total_time_seconds=total_time,
            throughput_per_second=job_count / total_time,
            latency_p50_ms=latencies[p50_idx],
            latency_p95_ms=latencies[p95_idx],
            latency_p99_ms=latencies[min(p99_idx, len(latencies)-1)],
            errors=errors,
        )
    else:
        result = TemporalBenchmarkResult(job_count=job_count, errors=errors)
    
    print(f"\nResults:")
    print(f"  Throughput: {result.throughput_per_second:.2f} workflows/s")
    print(f"  Latency P50: {result.latency_p50_ms:.2f}ms")
    print(f"  Latency P95: {result.latency_p95_ms:.2f}ms")
    print(f"  Latency P99: {result.latency_p99_ms:.2f}ms")
    print(f"  Errors: {result.errors}")
    
    return result


async def main():
    """Run benchmarks for different job counts."""
    results_dir = Path(__file__).parent.parent / "results"
    results_dir.mkdir(exist_ok=True)
    
    job_counts = [1000, 10000, 50000]
    
    for count in job_counts:
        result = await run_benchmark(
            host="localhost",
            port=7233,
            job_count=count,
            concurrency=50,
        )
        
        # Save individual result
        output_file = results_dir / f"temporal_simple_{count}.json"
        with open(output_file, "w") as f:
            json.dump(asdict(result), f, indent=2)
        print(f"Saved: {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
