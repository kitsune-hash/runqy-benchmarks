FROM node:20-alpine

WORKDIR /app

RUN npm init -y && npm install bullmq@5 ioredis@5

COPY worker.js .

CMD ["node", "worker.js"]
