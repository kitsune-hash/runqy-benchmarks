const { Worker } = require('bullmq');

const connection = {
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT) || 6379,
};

// Simple task
const simpleWorker = new Worker('benchmark-simple', async (job) => {
  return { status: 'ok', received: job.data.id };
}, { connection, concurrency: 1 });

// CPU-bound task
const cpuWorker = new Worker('benchmark-cpu', async (job) => {
  const data = { items: Array.from({ length: 100 }, (_, i) => ({ id: i, value: `item_${i}` })) };
  const serialized = JSON.stringify(data);
  const parsed = JSON.parse(serialized);
  return { status: 'ok', items: parsed.items.length };
}, { connection, concurrency: 1 });

// I/O simulation task
const ioWorker = new Worker('benchmark-io', async (job) => {
  await new Promise(resolve => setTimeout(resolve, 10)); // 10ms simulated I/O
  return { status: 'ok' };
}, { connection, concurrency: 1 });

console.log('BullMQ workers started');

// Graceful shutdown
process.on('SIGTERM', async () => {
  await simpleWorker.close();
  await cpuWorker.close();
  await ioWorker.close();
  process.exit(0);
});
