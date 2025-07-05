#!/usr/bin/env python3
"""
NatsPushPull Example - Job Processing with JetStream

This example demonstrates the refactored NatsPushPull functionality using NATS JetStream
for reliable job processing with work queue pattern. It shows:

1. Job producers that push jobs to the queue
2. Workers that process jobs from the queue
3. Subscribers that monitor job status updates
4. Automatic stream and consumer management
5. Job status tracking and error handling
"""

import asyncio
import json
import random
import time
from typing import Any, Dict

from hooklet.pilot.nats_pilot import NatsPilot
from hooklet.base.types import Job
from hooklet.utils.id_generator import generate_id


class JobProducer:
    """Produces various types of jobs and pushes them to the queue"""
    
    def __init__(self, name: str, pushpull):
        self.name = name
        self.pushpull = pushpull
        self.job_count = 0
    
    async def create_job(self, job_type: str, data: Any) -> Job:
        """Create a new job with the given type and data"""
        self.job_count += 1
        return {
            "_id": generate_id(),
            "type": job_type,
            "data": data,
            "error": None,
            "recv_ms": 0,  # Will be set by pushpull
            "start_ms": 0,  # Will be set by worker
            "end_ms": 0,    # Will be set by worker
            "status": "new", # Will be set by pushpull
            "retry_count": 0
        }
    
    async def produce_math_job(self, operation: str, a: float, b: float) -> bool:
        """Produce a mathematical operation job"""
        job = await self.create_job("math_operation", {
            "operation": operation,
            "operands": [a, b],
            "producer": self.name,
            "timestamp": time.time()
        })
        
        success = await self.pushpull.push("math.jobs", job)
        if success:
            print(f"📤 {self.name} pushed math job {job['_id']}: {operation}({a}, {b})")
        else:
            print(f"❌ {self.name} failed to push math job {job['_id']}")
        return success
    
    async def produce_image_job(self, image_url: str, operation: str) -> bool:
        """Produce an image processing job"""
        job = await self.create_job("image_processing", {
            "image_url": image_url,
            "operation": operation,
            "producer": self.name,
            "timestamp": time.time()
        })
        
        success = await self.pushpull.push("image.jobs", job)
        if success:
            print(f"📤 {self.name} pushed image job {job['_id']}: {operation} on {image_url}")
        else:
            print(f"❌ {self.name} failed to push image job {job['_id']}")
        return success
    
    async def produce_data_job(self, dataset: str, analysis_type: str) -> bool:
        """Produce a data analysis job"""
        job = await self.create_job("data_analysis", {
            "dataset": dataset,
            "analysis_type": analysis_type,
            "producer": self.name,
            "timestamp": time.time()
        })
        
        success = await self.pushpull.push("data.jobs", job)
        if success:
            print(f"📤 {self.name} pushed data job {job['_id']}: {analysis_type} on {dataset}")
        else:
            print(f"❌ {self.name} failed to push data job {job['_id']}")
        return success


class MathWorker:
    """Worker that processes mathematical operation jobs"""
    
    def __init__(self, name: str, pushpull):
        self.name = name
        self.pushpull = pushpull
        self.processed_count = 0
    
    async def process_math_job(self, job: Job) -> None:
        """Process a mathematical operation job"""
        self.processed_count += 1
        data = job.get('data', {})
        operation = data.get('operation')
        operands = data.get('operands', [])
        
        print(f"🧮 {self.name} processing math job {job['_id']}: {operation}({operands})")
        
        # Simulate processing time
        await asyncio.sleep(random.uniform(0.1, 0.5))
        
        # Perform the calculation
        try:
            if operation == "add":
                result = operands[0] + operands[1]
            elif operation == "subtract":
                result = operands[0] - operands[1]
            elif operation == "multiply":
                result = operands[0] * operands[1]
            elif operation == "divide":
                if operands[1] == 0:
                    raise ValueError("Division by zero")
                result = operands[0] / operands[1]
            else:
                raise ValueError(f"Unknown operation: {operation}")
            
            print(f"✅ {self.name} completed math job {job['_id']}: {operation}({operands}) = {result}")
            
        except Exception as e:
            print(f"❌ {self.name} failed math job {job['_id']}: {e}")
            raise
    
    async def start(self):
        """Start the math worker"""
        print(f"🔧 {self.name} starting math worker...")
        await self.pushpull.register_worker("math.jobs", self.process_math_job, n_workers=2)
        print(f"✅ {self.name} math worker started with 2 workers")


class ImageWorker:
    """Worker that processes image processing jobs"""
    
    def __init__(self, name: str, pushpull):
        self.name = name
        self.pushpull = pushpull
        self.processed_count = 0
    
    async def process_image_job(self, job: Job) -> None:
        """Process an image processing job"""
        self.processed_count += 1
        data = job.get('data', {})
        image_url = data.get('image_url')
        operation = data.get('operation')
        
        print(f"🖼️  {self.name} processing image job {job['_id']}: {operation} on {image_url}")
        
        # Simulate processing time
        await asyncio.sleep(random.uniform(0.5, 1.5))
        
        # Simulate image processing
        try:
            if operation == "resize":
                result = f"resized_{image_url}"
            elif operation == "filter":
                result = f"filtered_{image_url}"
            elif operation == "compress":
                result = f"compressed_{image_url}"
            else:
                raise ValueError(f"Unknown image operation: {operation}")
            
            print(f"✅ {self.name} completed image job {job['_id']}: {operation} -> {result}")
            
        except Exception as e:
            print(f"❌ {self.name} failed image job {job['_id']}: {e}")
            raise
    
    async def start(self):
        """Start the image worker"""
        print(f"🔧 {self.name} starting image worker...")
        await self.pushpull.register_worker("image.jobs", self.process_image_job, n_workers=1)
        print(f"✅ {self.name} image worker started with 1 worker")


class DataWorker:
    """Worker that processes data analysis jobs"""
    
    def __init__(self, name: str, pushpull):
        self.name = name
        self.pushpull = pushpull
        self.processed_count = 0
    
    async def process_data_job(self, job: Job) -> None:
        """Process a data analysis job"""
        self.processed_count += 1
        data = job.get('data', {})
        dataset = data.get('dataset')
        analysis_type = data.get('analysis_type')
        
        print(f"📊 {self.name} processing data job {job['_id']}: {analysis_type} on {dataset}")
        
        # Simulate processing time
        await asyncio.sleep(random.uniform(1.0, 2.0))
        
        # Simulate data analysis
        try:
            if analysis_type == "statistics":
                result = {"mean": 42.5, "std": 15.2, "count": 1000}
            elif analysis_type == "clustering":
                result = {"clusters": 5, "silhouette_score": 0.85}
            elif analysis_type == "regression":
                result = {"r2_score": 0.92, "rmse": 0.15}
            else:
                raise ValueError(f"Unknown analysis type: {analysis_type}")
            
            print(f"✅ {self.name} completed data job {job['_id']}: {analysis_type} -> {result}")
            
        except Exception as e:
            print(f"❌ {self.name} failed data job {job['_id']}: {e}")
            raise
    
    async def start(self):
        """Start the data worker"""
        print(f"🔧 {self.name} starting data worker...")
        await self.pushpull.register_worker("data.jobs", self.process_data_job, n_workers=3)
        print(f"✅ {self.name} data worker started with 3 workers")


class JobMonitor:
    """Monitors job status updates from all queues"""
    
    def __init__(self, name: str, pushpull):
        self.name = name
        self.pushpull = pushpull
        self.monitored_jobs = {}
    
    async def monitor_job_updates(self, job: Job) -> None:
        """Monitor job status updates"""
        job_id = job['_id']
        status = job['status']
        job_type = job['type']
        
        if job_id not in self.monitored_jobs:
            self.monitored_jobs[job_id] = {
                'type': job_type,
                'status_history': []
            }
        
        self.monitored_jobs[job_id]['status_history'].append({
            'status': status,
            'timestamp': time.time(),
            'start_ms': job.get('start_ms', 0),
            'end_ms': job.get('end_ms', 0)
        })
        
        # Calculate processing time if job is finished
        processing_time = None
        if status in ['finished', 'failed', 'cancelled'] and job.get('start_ms') and job.get('end_ms'):
            processing_time = (job['end_ms'] - job['start_ms']) / 1000.0
        
        print(f"📈 {self.name} job update: {job_id} ({job_type}) -> {status}")
        if processing_time:
            print(f"   ⏱️  Processing time: {processing_time:.3f}s")
        
        # Show status summary
        if status in ['finished', 'failed', 'cancelled']:
            total_jobs = len(self.monitored_jobs)
            completed_jobs = sum(1 for j in self.monitored_jobs.values() 
                               if j['status_history'][-1]['status'] in ['finished', 'failed', 'cancelled'])
            print(f"   📊 Progress: {completed_jobs}/{total_jobs} jobs completed")
    
    async def start(self):
        """Start monitoring all job types"""
        print(f"👁️  {self.name} starting job monitor...")
        
        # Subscribe to all job types
        await self.pushpull.subscribe("math.jobs", self.monitor_job_updates)
        await self.pushpull.subscribe("image.jobs", self.monitor_job_updates)
        await self.pushpull.subscribe("data.jobs", self.monitor_job_updates)
        
        print(f"✅ {self.name} job monitor started")


async def run_pushpull_demo():
    """Run the NatsPushPull demo"""
    print("🚀 Starting NatsPushPull Demo with JetStream")
    print("=" * 70)
    
    # Create NATS pilot and connect
    pilot = NatsPilot(nats_url="nats://localhost:4222")
    await pilot.connect()
    print("✅ Connected to NATS server")
    
    pushpull = pilot.pushpull()
    
    # Create workers
    math_worker = MathWorker("math-worker-1", pushpull)
    image_worker = ImageWorker("image-worker-1", pushpull)
    data_worker = DataWorker("data-worker-1", pushpull)
    
    # Create job monitor
    monitor = JobMonitor("job-monitor", pushpull)
    
    # Start workers and monitor
    await math_worker.start()
    await image_worker.start()
    await data_worker.start()
    await monitor.start()
    
    print("\n" + "=" * 70)
    print("🎯 Starting job production...")
    print("=" * 70)
    
    # Create producers
    producer1 = JobProducer("producer-1", pushpull)
    producer2 = JobProducer("producer-2", pushpull)
    
    # Produce various jobs
    jobs_to_produce = [
        # Math jobs
        lambda: producer1.produce_math_job("add", 10, 20),
        lambda: producer1.produce_math_job("multiply", 5, 7),
        lambda: producer2.produce_math_job("divide", 100, 4),
        lambda: producer2.produce_math_job("subtract", 50, 15),
        lambda: producer1.produce_math_job("divide", 10, 0),  # This will fail
        
        # Image jobs
        lambda: producer1.produce_image_job("https://example.com/image1.jpg", "resize"),
        lambda: producer2.produce_image_job("https://example.com/image2.jpg", "filter"),
        lambda: producer1.produce_image_job("https://example.com/image3.jpg", "compress"),
        
        # Data jobs
        lambda: producer2.produce_data_job("sales_data.csv", "statistics"),
        lambda: producer1.produce_data_job("customer_data.csv", "clustering"),
        lambda: producer2.produce_data_job("product_data.csv", "regression"),
    ]
    
    # Produce jobs with delays
    for i, job_func in enumerate(jobs_to_produce):
        await job_func()
        await asyncio.sleep(0.1)  # Small delay between jobs
    
    print("\n" + "=" * 70)
    print("⏳ Waiting for all jobs to complete...")
    print("=" * 70)
    
    # Wait for all jobs to be processed
    await asyncio.sleep(5)
    
    # Print final statistics
    print("\n" + "=" * 70)
    print("📊 Final Statistics:")
    print("=" * 70)
    print(f"Math jobs processed: {math_worker.processed_count}")
    print(f"Image jobs processed: {image_worker.processed_count}")
    print(f"Data jobs processed: {data_worker.processed_count}")
    print(f"Total jobs monitored: {len(monitor.monitored_jobs)}")
    
    # Show job status breakdown
    status_counts = {}
    for job_info in monitor.monitored_jobs.values():
        final_status = job_info['status_history'][-1]['status']
        status_counts[final_status] = status_counts.get(final_status, 0) + 1
    
    print("\nJob Status Breakdown:")
    for status, count in status_counts.items():
        print(f"  {status}: {count}")
    
    # Cleanup
    await pilot.disconnect()
    print("\n✅ Demo completed successfully!")


if __name__ == "__main__":
    try:
        asyncio.run(run_pushpull_demo())
    except KeyboardInterrupt:
        print("\n⏹️  Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc() 