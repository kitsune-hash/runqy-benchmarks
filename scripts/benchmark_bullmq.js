#!/usr/bin/env node
/**
 * Benchmark script for BullMQ task queue.
 * Requires: npm install bullmq ioredis
 */

const { Queue } = require('bullmq');

// Configuration
const REDIS_HOST = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT = parseInt(process.env.REDIS_PORT) || 6379;

const connection = { host: REDIS_HOST, port: REDIS_PORT };

// Percentile calculation
function percentile(arr, p) {
  const sorted = [...arr].sort((a, b) => a - b);
  const index = Math.floor(sorted.length * p / 100);
  return sorted[Math.min(index, sorted.length - 1)];
}

// Mean calculation
function mean(arr) {
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

async function submitJob(queue, jobId, scenario) {
  const payload = {
    id: jobId,
    scenario,
    timestamp: Date.now()
  };
  
  const start = process.hrtime.bigint();
  try {
    await queue.add(scenario, payload);
    const end = process.hrtime.bigint();
    return { latency: Number(end - start) / 1e6, error: null }; // Convert to ms
  } catch (e) {
    const end = process.hrtime.bigint();
    return { latency: Number(end - start) / 1e6, error: e.message };
  }
}

async function benchmarkBullMQ(jobsCount, scenario, concurrency) {
  console.log(`\n🐂 Benchmarking BullMQ - ${scenario} (${jobsCount.toLocaleString()} jobs, ${concurrency} concurrent)`);
  
  const queueName = `benchmark-${scenario}`;
  const queue = new Queue(queueName, { connection });
  
  const latencies = [];
  let errors = 0;
  
  const startTime = Date.now();
  
  // Submit jobs in batches
  const batchSize = concurrency;
  for (let i = 0; i < jobsCount; i += batchSize) {
    const batch = [];
    for (let j = i; j < Math.min(i + batchSize, jobsCount); j++) {
      batch.push(submitJob(queue, j, scenario));
    }
    
    const results = await Promise.all(batch);
    for (const { latency, error } of results) {
      latencies.push(latency);
      if (error) errors++;
    }
  }
  
  const endTime = Date.now();
  const duration = (endTime - startTime) / 1000;
  const throughput = jobsCount / duration;
  
  const result = {
    system: 'BullMQ',
    scenario,
    jobs_count: jobsCount,
    duration_seconds: Math.round(duration * 1000) / 1000,
    throughput_per_second: Math.round(throughput * 100) / 100,
    latency_p50_ms: Math.round(percentile(latencies, 50) * 1000) / 1000,
    latency_p95_ms: Math.round(percentile(latencies, 95) * 1000) / 1000,
    latency_p99_ms: Math.round(percentile(latencies, 99) * 1000) / 1000,
    latency_avg_ms: Math.round(mean(latencies) * 1000) / 1000,
    latency_min_ms: Math.round(Math.min(...latencies) * 1000) / 1000,
    latency_max_ms: Math.round(Math.max(...latencies) * 1000) / 1000,
    errors,
    timestamp: new Date().toISOString()
  };
  
  console.log('\n' + '='.repeat(50));
  console.log(`  ${result.system} - ${result.scenario}`);
  console.log('='.repeat(50));
  console.log(`  Jobs:        ${result.jobs_count.toLocaleString()}`);
  console.log(`  Duration:    ${result.duration_seconds.toFixed(2)}s`);
  console.log(`  Throughput:  ${result.throughput_per_second.toLocaleString()} jobs/sec`);
  console.log(`  Latency P50: ${result.latency_p50_ms.toFixed(2)}ms`);
  console.log(`  Latency P95: ${result.latency_p95_ms.toFixed(2)}ms`);
  console.log(`  Latency P99: ${result.latency_p99_ms.toFixed(2)}ms`);
  console.log(`  Errors:      ${result.errors}`);
  console.log('='.repeat(50));
  
  await queue.close();
  
  return result;
}

async function main() {
  const args = process.argv.slice(2);
  const jobsCount = parseInt(args[0]) || 1000;
  const scenario = args[1] || 'simple';
  const concurrency = parseInt(args[2]) || 10;
  
  try {
    const result = await benchmarkBullMQ(jobsCount, scenario, concurrency);
    
    // Save result
    const fs = require('fs');
    fs.writeFileSync(
      `../results/bullmq_${scenario}_${jobsCount}.json`,
      JSON.stringify(result, null, 2)
    );
    
    process.exit(0);
  } catch (e) {
    console.error('Error:', e.message);
    process.exit(1);
  }
}

main();
