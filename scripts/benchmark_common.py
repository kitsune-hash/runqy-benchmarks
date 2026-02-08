#!/usr/bin/env python3
"""
Common utilities for benchmarking task queue systems.
"""

import time
import json
import statistics
from dataclasses import dataclass, asdict
from typing import List, Optional
from datetime import datetime

@dataclass
class BenchmarkResult:
    """Results from a benchmark run"""
    system: str
    scenario: str
    jobs_count: int
    duration_seconds: float
    throughput_per_second: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    latency_avg_ms: float
    latency_min_ms: float
    latency_max_ms: float
    errors: int
    timestamp: str
    
    def to_dict(self):
        return asdict(self)

def calculate_percentile(latencies: List[float], percentile: float) -> float:
    """Calculate percentile from a list of latencies"""
    if not latencies:
        return 0.0
    sorted_latencies = sorted(latencies)
    index = int(len(sorted_latencies) * percentile / 100)
    return sorted_latencies[min(index, len(sorted_latencies) - 1)]

def calculate_metrics(
    system: str,
    scenario: str,
    jobs_count: int,
    start_time: float,
    end_time: float,
    latencies: List[float],
    errors: int = 0
) -> BenchmarkResult:
    """Calculate benchmark metrics from raw data"""
    
    duration = end_time - start_time
    throughput = jobs_count / duration if duration > 0 else 0
    
    # Convert latencies to milliseconds
    latencies_ms = [l * 1000 for l in latencies]
    
    return BenchmarkResult(
        system=system,
        scenario=scenario,
        jobs_count=jobs_count,
        duration_seconds=round(duration, 3),
        throughput_per_second=round(throughput, 2),
        latency_p50_ms=round(calculate_percentile(latencies_ms, 50), 3),
        latency_p95_ms=round(calculate_percentile(latencies_ms, 95), 3),
        latency_p99_ms=round(calculate_percentile(latencies_ms, 99), 3),
        latency_avg_ms=round(statistics.mean(latencies_ms), 3) if latencies_ms else 0,
        latency_min_ms=round(min(latencies_ms), 3) if latencies_ms else 0,
        latency_max_ms=round(max(latencies_ms), 3) if latencies_ms else 0,
        errors=errors,
        timestamp=datetime.utcnow().isoformat() + "Z"
    )

def save_results(results: List[BenchmarkResult], filename: str):
    """Save benchmark results to JSON file"""
    with open(filename, 'w') as f:
        json.dump([r.to_dict() for r in results], f, indent=2)
    print(f"Results saved to {filename}")

def print_results(result: BenchmarkResult):
    """Print benchmark results in a nice format"""
    print(f"\n{'='*50}")
    print(f"  {result.system} - {result.scenario}")
    print(f"{'='*50}")
    print(f"  Jobs:        {result.jobs_count:,}")
    print(f"  Duration:    {result.duration_seconds:.2f}s")
    print(f"  Throughput:  {result.throughput_per_second:,.2f} jobs/sec")
    print(f"  Latency P50: {result.latency_p50_ms:.2f}ms")
    print(f"  Latency P95: {result.latency_p95_ms:.2f}ms")
    print(f"  Latency P99: {result.latency_p99_ms:.2f}ms")
    print(f"  Errors:      {result.errors}")
    print(f"{'='*50}")
