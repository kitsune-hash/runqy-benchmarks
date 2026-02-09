#!/usr/bin/env python3
"""
Temporal Benchmark Script
Measures workflow submission throughput and latency.
"""
import asyncio
import json
import time
import uuid
from pathlib import Path
from dataclasses import dataclass, asdict
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.worker import Worker


@activity.defn
async def simple_task(payload: str) -> str:
    """Minimal task - just return the payload."""
    return f"processed: {payload}"


@workflow.defn
class SimpleWorkflow:
    """Simple workflow that runs one activity."""
    
    @workflow.run
    async def run(self, payload: str) -> str:
        return await workflow.execute_activity(
            simple_task,
            payload,
            start_to_close_timeout=timedelta(seconds=60),
        )


@dataclass
class BenchmarkResult:
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
            SimpleWorkflow.run,
            payload,
            id=f"bench-{uuid.uuid4()}",
            task_queue="benchmark-queue",
        )
        latency = (time.perf_counter() - start) * 1000  # ms
        return latency, True
    except Exception as e:
        print(f"Error: {e}")
        return 0.0, False


async def run_benchmark(
    client: Client,
    job_count: int = 1000,
    concurrency: int = 50,
) -> BenchmarkResult:
    """Run the Temporal benchmark."""
    print(f"\n{'='*50}")
    print(f"Temporal Benchmark: {job_count} workflows")
    print(f"{'='*50}")
    
    latencies: list[float] = []
    errors = 0
    
    # Warmup
    print("Warming up...")
    warmup_count = min(50, job_count // 20)
    for i in range(warmup_count):
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
        
        result = BenchmarkResult(
            job_count=job_count,
            total_time_seconds=total_time,
            throughput_per_second=job_count / total_time,
            latency_p50_ms=latencies[p50_idx],
            latency_p95_ms=latencies[p95_idx],
            latency_p99_ms=latencies[min(p99_idx, len(latencies)-1)],
            errors=errors,
        )
    else:
        result = BenchmarkResult(job_count=job_count, errors=errors)
    
    print(f"\nResults:")
    print(f"  Throughput: {result.throughput_per_second:.2f} workflows/s")
    print(f"  Latency P50: {result.latency_p50_ms:.2f}ms")
    print(f"  Latency P95: {result.latency_p95_ms:.2f}ms")
    print(f"  Latency P99: {result.latency_p99_ms:.2f}ms")
    print(f"  Errors: {result.errors}")
    
    return result


async def run_worker(client: Client, stop_event: asyncio.Event):
    """Run the Temporal worker until stop_event is set."""
    worker = Worker(
        client,
        task_queue="benchmark-queue",
        workflows=[SimpleWorkflow],
        activities=[simple_task],
    )
    
    async def worker_task():
        await worker.run()
    
    task = asyncio.create_task(worker_task())
    await stop_event.wait()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def main():
    """Run benchmarks for different job counts."""
    results_dir = Path(__file__).parent.parent / "results"
    results_dir.mkdir(exist_ok=True)
    
    # Connect to Temporal
    print("Connecting to Temporal server...")
    client = await Client.connect("localhost:7233")
    
    # Start worker in background
    stop_event = asyncio.Event()
    worker_task = asyncio.create_task(run_worker(client, stop_event))
    
    # Wait for worker to be ready
    await asyncio.sleep(2)
    
    job_counts = [1000, 10000, 50000]
    all_results = {}
    
    try:
        for count in job_counts:
            result = await run_benchmark(
                client,
                job_count=count,
                concurrency=50,
            )
            
            # Save individual result
            output_file = results_dir / f"temporal_simple_{count}.json"
            with open(output_file, "w") as f:
                json.dump(asdict(result), f, indent=2)
            print(f"Saved: {output_file}")
            
            all_results[f"{count}_jobs"] = asdict(result)
            
            # Brief pause between tests
            await asyncio.sleep(2)
    finally:
        # Stop worker
        stop_event.set()
        await worker_task
    
    print("\n" + "="*50)
    print("All benchmarks complete!")
    print("="*50)
    
    return all_results


if __name__ == "__main__":
    asyncio.run(main())
