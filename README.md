# Runqy Benchmarks

Reproducible benchmarks comparing Runqy against other task queue systems.

## Systems Tested

- **Runqy** (Go) - v0.2.x
- **Celery** (Python) - v5.x with Redis broker
- **BullMQ** (Node.js) - v5.x with Redis

## Metrics

- **Throughput**: Jobs processed per second
- **Latency**: P50, P95, P99 response times
- **Memory**: Peak memory usage under load
- **Cold Start**: Time to first job processed

## Scenarios

1. **Simple Job**: Minimal payload, no processing
2. **CPU-bound**: JSON serialization/computation
3. **I/O Simulation**: Simulated async delay

## Running

```bash
docker compose up -d
python scripts/run_benchmarks.py
```

## Results

See `results/` for JSON data and `charts/` for generated graphs.

---

*Benchmarks by Akari 🦊 for Runqy*
