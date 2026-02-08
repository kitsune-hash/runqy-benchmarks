#!/usr/bin/env python3
"""
Master benchmark runner - runs all benchmarks and generates comparison results.
"""

import subprocess
import json
import os
import sys
from datetime import datetime
from pathlib import Path

RESULTS_DIR = Path(__file__).parent.parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

# Benchmark configurations
CONFIGS = [
    {"jobs": 1000, "scenario": "simple", "concurrency": 10},
    {"jobs": 5000, "scenario": "simple", "concurrency": 50},
    {"jobs": 10000, "scenario": "simple", "concurrency": 100},
]

def run_command(cmd, cwd=None):
    """Run a command and return output"""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True,
            cwd=cwd,
            timeout=300
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Timeout"
    except Exception as e:
        return False, "", str(e)

def check_prerequisites():
    """Check if all systems are ready"""
    print("\n🔍 Checking prerequisites...\n")
    
    checks = {
        "Redis": "redis-cli ping",
        "Python packages": "python3 -c 'import celery, redis'",
        "Node packages": "node -e \"require('bullmq')\"",
    }
    
    all_ok = True
    for name, cmd in checks.items():
        ok, _, _ = run_command(cmd)
        status = "✅" if ok else "❌"
        print(f"  {status} {name}")
        if not ok:
            all_ok = False
    
    return all_ok

def run_runqy_benchmark(jobs, scenario, concurrency):
    """Run Runqy benchmark"""
    cmd = f"python3 benchmark_runqy.py --jobs {jobs} --scenario {scenario} --concurrency {concurrency}"
    ok, stdout, stderr = run_command(cmd, cwd=Path(__file__).parent)
    
    if not ok:
        print(f"  ❌ Runqy benchmark failed: {stderr}")
        return None
    
    # Load result
    result_file = RESULTS_DIR / f"runqy_{scenario}_{jobs}.json"
    if result_file.exists():
        with open(result_file) as f:
            return json.load(f)
    return None

def run_celery_benchmark(jobs, scenario, concurrency):
    """Run Celery benchmark"""
    cmd = f"python3 benchmark_celery.py --jobs {jobs} --scenario {scenario} --concurrency {concurrency}"
    ok, stdout, stderr = run_command(cmd, cwd=Path(__file__).parent)
    
    if not ok:
        print(f"  ❌ Celery benchmark failed: {stderr}")
        return None
    
    result_file = RESULTS_DIR / f"celery_{scenario}_{jobs}.json"
    if result_file.exists():
        with open(result_file) as f:
            return json.load(f)
    return None

def run_bullmq_benchmark(jobs, scenario, concurrency):
    """Run BullMQ benchmark"""
    cmd = f"node benchmark_bullmq.js {jobs} {scenario} {concurrency}"
    ok, stdout, stderr = run_command(cmd, cwd=Path(__file__).parent)
    
    if not ok:
        print(f"  ❌ BullMQ benchmark failed: {stderr}")
        return None
    
    result_file = RESULTS_DIR / f"bullmq_{scenario}_{jobs}.json"
    if result_file.exists():
        with open(result_file) as f:
            return json.load(f)
    return None

def generate_comparison(all_results):
    """Generate comparison JSON"""
    comparison = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "benchmarks": all_results,
        "summary": {}
    }
    
    # Group by scenario and jobs
    for result in all_results:
        key = f"{result['scenario']}_{result['jobs_count']}"
        if key not in comparison["summary"]:
            comparison["summary"][key] = {}
        comparison["summary"][key][result["system"]] = {
            "throughput": result["throughput_per_second"],
            "latency_p99": result["latency_p99_ms"]
        }
    
    return comparison

def main():
    print("=" * 60)
    print("  RUNQY BENCHMARK SUITE")
    print("=" * 60)
    
    # Check prerequisites
    if not check_prerequisites():
        print("\n⚠️  Some prerequisites are missing. Continue anyway? (y/n)")
        if input().lower() != 'y':
            sys.exit(1)
    
    all_results = []
    
    for config in CONFIGS:
        jobs = config["jobs"]
        scenario = config["scenario"]
        concurrency = config["concurrency"]
        
        print(f"\n{'='*60}")
        print(f"  Running {scenario} benchmark with {jobs:,} jobs")
        print(f"{'='*60}")
        
        # Run each system
        print("\n  📊 Runqy...")
        result = run_runqy_benchmark(jobs, scenario, concurrency)
        if result:
            all_results.append(result)
        
        print("\n  📊 Celery...")
        result = run_celery_benchmark(jobs, scenario, concurrency)
        if result:
            all_results.append(result)
        
        print("\n  📊 BullMQ...")
        result = run_bullmq_benchmark(jobs, scenario, concurrency)
        if result:
            all_results.append(result)
    
    # Generate comparison
    comparison = generate_comparison(all_results)
    
    comparison_file = RESULTS_DIR / "comparison.json"
    with open(comparison_file, "w") as f:
        json.dump(comparison, f, indent=2)
    
    print(f"\n✅ All benchmarks complete!")
    print(f"   Results saved to {RESULTS_DIR}")
    print(f"   Comparison: {comparison_file}")

if __name__ == "__main__":
    main()
