#!/usr/bin/env python3
"""
Simple NatsPushPull Example

A minimal example demonstrating the core NatsPushPull functionality:
- Job production and consumption
- Worker registration
- Job status tracking
- Error handling
"""

import asyncio
import time
from hooklet.pilot.nats_pilot import NatsPilot
from hooklet.base.types import Job
from hooklet.utils.id_generator import generate_id


class SimpleJobProducer:
    """Simple job producer that creates and pushes jobs"""
    
    def __init__(self, pushpull):
        self.pushpull = pushpull
        self.job_id = 0
    
    async def create_and_push_job(self, task: str, data: dict) -> bool:
        """Create a job and push it to the queue"""
        self.job_id += 1
        
        job: Job = {
            "_id": generate_id(),
            "type": "simple_task",
            "data": {
                "task": task,
                "job_id": self.job_id,
                "data": data,
                "created_at": time.time()
            },
            "error": None,
            "recv_ms": 0,
            "start_ms": 0,
            "end_ms": 0,
            "status": "new",
            "retry_count": 0
        }
        
        success = await self.pushpull.push("demo.jobs", job)
        if success:
            print(f"📤 Pushed job {self.job_id}: {task}")
        else:
            print(f"❌ Failed to push job {self.job_id}")
        
        return success


class SimpleWorker:
    """Simple worker that processes jobs"""
    
    def __init__(self, name: str, pushpull):
        self.name = name
        self.pushpull = pushpull
        self.processed_count = 0
    
    async def process_job(self, job: Job) -> None:
        """Process a job"""
        self.processed_count += 1
        data = job.get('data', {})
        task = data.get('task')
        job_id = data.get('job_id')
        
        print(f"🔧 {self.name} processing job {job_id}: {task}")
        
        # Simulate work
        await asyncio.sleep(0.5)
        
        # Simulate some jobs failing
        if job_id == 3:
            raise Exception("Simulated failure for job 3")
        
        print(f"✅ {self.name} completed job {job_id}: {task}")
    
    async def start(self):
        """Start the worker"""
        print(f"🚀 {self.name} starting...")
        await self.pushpull.register_worker("demo.jobs", self.process_job, n_workers=2)
        print(f"✅ {self.name} started with 2 workers")


class JobStatusMonitor:
    """Monitor job status changes"""
    
    def __init__(self, pushpull):
        self.pushpull = pushpull
        self.job_statuses = {}
    
    async def on_job_update(self, job: Job) -> None:
        """Handle job status updates"""
        job_id = job['_id']
        status = job['status']
        data = job.get('data', {})
        task = data.get('task')
        
        self.job_statuses[job_id] = status
        
        print(f"📊 Job {data.get('job_id')} ({task}) -> {status}")
        
        # Show processing time for completed jobs
        if status in ['finished', 'failed', 'cancelled']:
            if job.get('start_ms') and job.get('end_ms'):
                duration = (job['end_ms'] - job['start_ms']) / 1000.0
                print(f"   ⏱️  Duration: {duration:.3f}s")
    
    async def start(self):
        """Start monitoring"""
        print("👁️  Starting job monitor...")
        await self.pushpull.subscribe("demo.jobs", self.on_job_update)
        print("✅ Job monitor started")


async def run_simple_demo():
    """Run the simple NatsPushPull demo"""
    print("🚀 Simple NatsPushPull Demo")
    print("=" * 50)
    
    # Connect to NATS
    pilot = NatsPilot(nats_url="nats://localhost:4222")
    await pilot.connect()
    print("✅ Connected to NATS")
    
    pushpull = pilot.pushpull()
    
    # Create components
    producer = SimpleJobProducer(pushpull)
    worker = SimpleWorker("demo-worker", pushpull)
    monitor = JobStatusMonitor(pushpull)
    
    # Start worker and monitor
    await worker.start()
    await monitor.start()
    
    print("\n" + "=" * 50)
    print("🎯 Producing jobs...")
    print("=" * 50)
    
    # Produce some jobs
    tasks = [
        "Calculate fibonacci(10)",
        "Process user data",
        "Generate report",  # This will fail
        "Send email notification",
        "Update database"
    ]
    
    for task in tasks:
        await producer.create_and_push_job(task, {"priority": "normal"})
        await asyncio.sleep(0.2)  # Small delay between jobs
    
    print("\n" + "=" * 50)
    print("⏳ Waiting for job completion...")
    print("=" * 50)
    
    # Wait for processing
    await asyncio.sleep(5)
    
    # Print summary
    print("\n" + "=" * 50)
    print("📈 Summary:")
    print("=" * 50)
    print(f"Jobs processed: {worker.processed_count}")
    print(f"Jobs monitored: {len(monitor.job_statuses)}")
    
    # Status breakdown
    status_counts = {}
    for status in monitor.job_statuses.values():
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print("\nJob Status Breakdown:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")
    
    # Cleanup
    await pilot.disconnect()
    print("\n✅ Demo completed!")


if __name__ == "__main__":
    try:
        asyncio.run(run_simple_demo())
    except KeyboardInterrupt:
        print("\n⏹️  Demo interrupted")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc() 