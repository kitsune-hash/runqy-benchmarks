"""Temporal worker for benchmark testing."""
import asyncio
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
            start_to_close_timeout=60,
        )


async def main():
    """Start the Temporal worker."""
    client = await Client.connect("temporal:7233")
    
    worker = Worker(
        client,
        task_queue="benchmark-queue",
        workflows=[SimpleWorkflow],
        activities=[simple_task],
    )
    
    print("Temporal worker started...")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
