from celery import Celery
import json
import time

app = Celery('tasks')
app.config_from_object({
    'broker_url': 'redis://localhost:6379/1',
    'result_backend': 'redis://localhost:6379/1',
    'task_serializer': 'json',
    'result_serializer': 'json',
    'accept_content': ['json'],
    'task_acks_late': False,
    'worker_prefetch_multiplier': 1,
})

@app.task(name='benchmark.simple')
def simple_task(payload):
    return {"status": "ok", "received": payload.get("id")}

@app.task(name='benchmark.cpu')
def cpu_task(payload):
    data = {"items": [{"id": i, "value": f"item_{i}"} for i in range(100)]}
    serialized = json.dumps(data)
    parsed = json.loads(serialized)
    return {"status": "ok", "items": len(parsed["items"])}

@app.task(name='benchmark.io')
def io_task(payload):
    time.sleep(0.01)
    return {"status": "ok"}
