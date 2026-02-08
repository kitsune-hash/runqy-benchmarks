FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir celery[redis]==5.4.0 redis

COPY tasks.py .

CMD ["celery", "-A", "tasks", "worker", "--loglevel=warning"]
