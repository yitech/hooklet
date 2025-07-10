#!/usr/bin/env python3
"""
Worker Demo Example - Job Processing System

This example demonstrates a modular job processing system using the PushPull pattern
with different worker types for text, image, and data processing.
"""

import asyncio
import time
import json
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum

from hooklet.node.worker import Worker, Dispatcher
from hooklet.pilot.inproc_pilot import InprocPilot
from hooklet.pilot.nats_pilot import NatsPilot
from hooklet.base.types import Job
from hooklet.utils.id_generator import generate_id


class JobType(Enum):
    """Enumeration of supported job types"""
    # Text processing
    TEXT_UPPERCASE = "text_uppercase"
    TEXT_LOWERCASE = "text_lowercase"
    TEXT_WORD_COUNT = "text_word_count"
    TEXT_REVERSE = "text_reverse"
    
    # Image processing
    IMAGE_RESIZE = "image_resize"
    IMAGE_CROP = "image_crop"
    IMAGE_FILTER = "image_filter"
    IMAGE_CONVERT = "image_convert"
    
    # Data analysis
    DATA_MEAN = "data_mean"
    DATA_SUM = "data_sum"
    DATA_MAX = "data_max"
    DATA_MIN = "data_min"


class JobStatus(Enum):
    """Job status enumeration"""
    NEW = "new"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class JobResult:
    """Represents the result of a job processing"""
    success: bool
    result: Any = None
    error: Optional[str] = None
    processing_time: float = 0.0


class BaseWorker(Worker):
    """Base worker class with common functionality"""
    
    def __init__(self, name: str, pushpull):
        super().__init__(name, pushpull)
        self.processed_jobs = 0
        self.failed_jobs = 0
        
    async def on_job(self, job: Job) -> int:
        """Process job with monitoring and error handling - required by Worker class"""
        job_id = job.id if job.id else 'unknown'
        job_type = job.type if job.type else 'unknown'
        
        start_time = time.time()
        print(f"🔄 {self.name} processing job {job_id[:8]}: {job_type}")
        
        try:
            result = await self.process_job(job)
            
            if result.success:
                self.processed_jobs += 1
                processing_time = time.time() - start_time
                print(f"✅ {self.name} completed job {job_id[:8]} in {processing_time:.2f}s")
                return 0
            else:
                self.failed_jobs += 1
                print(f"❌ {self.name} failed job {job_id[:8]}: {result.error}")
                return 1
                
        except Exception as e:
            self.failed_jobs += 1
            processing_time = time.time() - start_time
            print(f"💥 {self.name} crashed on job {job_id[:8]} after {processing_time:.2f}s: {str(e)}")
            return 1
    
    async def process_job(self, job: Job) -> JobResult:
        """Process a job - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement process_job")
    
    def get_stats(self) -> Dict[str, int]:
        """Get worker statistics"""
        return {
            'processed': self.processed_jobs,
            'failed': self.failed_jobs,
            'total': self.processed_jobs + self.failed_jobs
        }


class TextWorker(BaseWorker):
    """Worker specialized in text processing jobs"""
    
    def __init__(self, name: str, pushpull):
        super().__init__(name, pushpull)
    
    async def process_job(self, job: Job) -> JobResult:
        """Process text-related jobs"""
        job_type = job.type
        data = job.data or {}
        text = data.get('text', '')
        
        if not text:
            return JobResult(success=False, error="No text provided")
        
        # Simulate processing delay
        await asyncio.sleep(0.1)
        
        try:
            if job_type == JobType.TEXT_UPPERCASE.value:
                result = text.upper()
                print(f"   📝 Converted to uppercase: '{text}' → '{result}'")
                
            elif job_type == JobType.TEXT_LOWERCASE.value:
                result = text.lower()
                print(f"   📝 Converted to lowercase: '{text}' → '{result}'")
                
            elif job_type == JobType.TEXT_WORD_COUNT.value:
                result = len(text.split())
                print(f"   📝 Word count: '{text}' → {result} words")
                
            elif job_type == JobType.TEXT_REVERSE.value:
                result = text[::-1]
                print(f"   📝 Reversed: '{text}' → '{result}'")
                
            else:
                return JobResult(success=False, error=f"Unknown text job type: {job_type}")
            
            return JobResult(success=True, result=result)
            
        except Exception as e:
            return JobResult(success=False, error=f"Text processing error: {str(e)}")


class ImageWorker(BaseWorker):
    """Worker specialized in image processing jobs"""
    
    def __init__(self, name: str, pushpull):
        super().__init__(name, pushpull)
    
    async def process_job(self, job: Job) -> JobResult:
        """Process image-related jobs"""
        job_type = job.type
        data = job.data or {}
        
        # Simulate processing delay
        await asyncio.sleep(0.3)
        
        try:
            if job_type == JobType.IMAGE_RESIZE.value:
                width = data.get('width', 0)
                height = data.get('height', 0)
                if width <= 0 or height <= 0:
                    return JobResult(success=False, error="Invalid dimensions")
                result = f"Resized to {width}x{height}"
                print(f"   🖼️ {result}")
                
            elif job_type == JobType.IMAGE_CROP.value:
                x, y = data.get('x', 0), data.get('y', 0)
                w, h = data.get('width', 0), data.get('height', 0)
                if w <= 0 or h <= 0:
                    return JobResult(success=False, error="Invalid crop dimensions")
                result = f"Cropped at ({x},{y}) with size {w}x{h}"
                print(f"   🖼️ {result}")
                
            elif job_type == JobType.IMAGE_FILTER.value:
                filter_type = data.get('filter', 'unknown')
                result = f"Applied {filter_type} filter"
                print(f"   🖼️ {result}")
                
            elif job_type == JobType.IMAGE_CONVERT.value:
                format_to = data.get('format', 'unknown')
                result = f"Converted to {format_to} format"
                print(f"   🖼️ {result}")
                
            else:
                return JobResult(success=False, error=f"Unknown image job type: {job_type}")
            
            return JobResult(success=True, result=result)
            
        except Exception as e:
            return JobResult(success=False, error=f"Image processing error: {str(e)}")


class DataWorker(BaseWorker):
    """Worker specialized in data analysis jobs"""
    
    def __init__(self, name: str, pushpull):
        super().__init__(name, pushpull)
    
    async def process_job(self, job: Job) -> JobResult:
        """Process data analysis jobs"""
        job_type = job.type
        data = job.data or {}
        numbers = data.get('numbers', [])
        
        if not numbers:
            return JobResult(success=False, error="No numbers provided")
        
        if not all(isinstance(n, (int, float)) for n in numbers):
            return JobResult(success=False, error="All values must be numbers")
        
        # Simulate processing delay
        await asyncio.sleep(0.2)
        
        try:
            if job_type == JobType.DATA_MEAN.value:
                result = sum(numbers) / len(numbers)
                print(f"   📊 Mean of {numbers}: {result:.2f}")
                
            elif job_type == JobType.DATA_SUM.value:
                result = sum(numbers)
                print(f"   📊 Sum of {numbers}: {result}")
                
            elif job_type == JobType.DATA_MAX.value:
                result = max(numbers)
                print(f"   📊 Max of {numbers}: {result}")
                
            elif job_type == JobType.DATA_MIN.value:
                result = min(numbers)
                print(f"   📊 Min of {numbers}: {result}")
                
            else:
                return JobResult(success=False, error=f"Unknown data job type: {job_type}")
            
            return JobResult(success=True, result=result)
            
        except Exception as e:
            return JobResult(success=False, error=f"Data analysis error: {str(e)}")


class JobMonitor:
    """Monitors job status changes and provides statistics"""
    
    def __init__(self, name: str, dispatcher: Dispatcher):
        self.name = name
        self.dispatcher = dispatcher
        self.job_history: Dict[str, Dict] = {}
        self.status_counts: Dict[str, int] = {}
    
    async def start(self, subjects: List[str]):
        """Start monitoring job status changes for given subjects"""
        print(f"📊 {self.name} started monitoring subjects: {subjects}")
        
        for subject in subjects:
            await self.dispatcher.subscribe(subject, self._handle_job_status)
    
    async def _handle_job_status(self, job: Job):
        """Handle job status updates"""
        job_id = job.id if job.id else 'unknown'
        status = job.status if job.status else 'unknown'
        job_type = job.type if job.type else 'unknown'
        
        # Update job history
        if job_id not in self.job_history:
            self.job_history[job_id] = {
                'type': job_type,
                'created_at': time.time(),
                'statuses': []
            }
        
        self.job_history[job_id]['statuses'].append({
            'status': status,
            'timestamp': time.time()
        })
        
        # Update status counts
        self.status_counts[status] = self.status_counts.get(status, 0) + 1
        
        # Print status update
        emoji = self._get_status_emoji(status)
        print(f"{emoji} Job {job_id[:8]} [{job_type}]: {status}")
        
        if status == "failed" and job.error:
            print(f"   ❌ Error: {job.error}")
    
    def _get_status_emoji(self, status: str) -> str:
        """Get emoji for job status"""
        emojis = {
            "new": "🆕",
            "running": "⚡",
            "finished": "✅",
            "failed": "❌",
            "cancelled": "🚫"
        }
        return emojis.get(status, "❓")
    
    def print_summary(self):
        """Print monitoring summary"""
        print("\n📈 Job Monitoring Summary:")
        print(f"   Total jobs tracked: {len(self.job_history)}")
        
        for status, count in self.status_counts.items():
            emoji = self._get_status_emoji(status)
            print(f"   {emoji} {status}: {count}")


class JobFactory:
    """Factory for creating different types of jobs"""
    
    @staticmethod
    def create_job(job_type: JobType, data: Dict[str, Any]) -> Job:
        """Create a job with proper structure"""
        return Job(
            _id=generate_id(),
            type=job_type.value,
            data=data,
            error=None,
            recv_ms=int(time.time() * 1000),
            start_ms=0,
            end_ms=0,
            status=JobStatus.NEW.value,
            retry_count=0
        )
    
    @staticmethod
    def create_text_job(job_type: JobType, text: str) -> Job:
        """Create a text processing job"""
        return JobFactory.create_job(job_type, {"text": text})
    
    @staticmethod
    def create_image_job(job_type: JobType, **kwargs) -> Job:
        """Create an image processing job"""
        return JobFactory.create_job(job_type, kwargs)
    
    @staticmethod
    def create_data_job(job_type: JobType, numbers: List[float]) -> Job:
        """Create a data analysis job"""
        return JobFactory.create_job(job_type, {"numbers": numbers})


class WorkerSystem:
    """Main system that manages workers and job processing"""
    
    def __init__(self, pilot):
        self.pilot = pilot
        self.workers: List[BaseWorker] = []
        self.dispatcher = Dispatcher(pilot.pushpull())
        self.monitor = JobMonitor("job-monitor", self.dispatcher)
        
    async def setup_workers(self):
        """Setup and start all workers"""
        # Create workers with appropriate names that will be used as subjects
        text_worker = TextWorker("text-processing", self.pilot.pushpull())
        image_worker = ImageWorker("image-processing", self.pilot.pushpull())
        data_worker = DataWorker("data-analysis", self.pilot.pushpull())
        
        self.workers = [text_worker, image_worker, data_worker]
        
        # Start monitoring
        await self.monitor.start(["text-processing", "image-processing", "data-analysis"])
        
        # Start workers
        for worker in self.workers:
            await worker.start()
            print(f"✅ {worker.__class__.__name__} '{worker.name}' started")
        
        print(f"✅ Worker system ready with {len(self.workers)} workers")
    
    async def dispatch_sample_jobs(self):
        """Dispatch a variety of sample jobs for demonstration"""
        print("\n📤 Dispatching sample jobs...")
        
        # Text processing jobs
        text_jobs = [
            JobFactory.create_text_job(JobType.TEXT_UPPERCASE, "hello world"),
            JobFactory.create_text_job(JobType.TEXT_LOWERCASE, "HELLO WORLD"),
            JobFactory.create_text_job(JobType.TEXT_WORD_COUNT, "This is a sample text for counting"),
            JobFactory.create_text_job(JobType.TEXT_REVERSE, "python"),
        ]
        
        # Image processing jobs
        image_jobs = [
            JobFactory.create_image_job(JobType.IMAGE_RESIZE, width=1920, height=1080),
            JobFactory.create_image_job(JobType.IMAGE_CROP, x=100, y=100, width=800, height=600),
            JobFactory.create_image_job(JobType.IMAGE_FILTER, filter="blur"),
            JobFactory.create_image_job(JobType.IMAGE_CONVERT, format="PNG"),
        ]
        
        # Data analysis jobs
        data_jobs = [
            JobFactory.create_data_job(JobType.DATA_MEAN, [1, 2, 3, 4, 5]),
            JobFactory.create_data_job(JobType.DATA_SUM, [10, 20, 30, 40]),
            JobFactory.create_data_job(JobType.DATA_MAX, [3, 7, 2, 9, 1]),
            JobFactory.create_data_job(JobType.DATA_MIN, [5, 8, 2, 10, 3]),
        ]
        
        # Dispatch all jobs
        all_jobs = text_jobs + image_jobs + data_jobs
        for job in all_jobs:
            subject = self._get_subject_for_job(job.type)
            await self.dispatcher.dispatch(subject, job)
        
        print(f"📤 Dispatched {len(all_jobs)} jobs")
        
        # Dispatch some jobs that will fail for testing error handling
        print("\n📤 Dispatching error test jobs...")
        error_jobs = [
            JobFactory.create_text_job(JobType.TEXT_UPPERCASE, ""),  # Empty text
            JobFactory.create_data_job(JobType.DATA_MEAN, []),  # Empty numbers
            JobFactory.create_image_job(JobType.IMAGE_RESIZE, width=0, height=0),  # Invalid dimensions
        ]
        
        for job in error_jobs:
            subject = self._get_subject_for_job(job.type)
            await self.dispatcher.dispatch(subject, job)
        
        print(f"📤 Dispatched {len(error_jobs)} error test jobs")
    
    def _get_subject_for_job(self, job_type: str) -> str:
        """Get the appropriate subject for a job type"""
        if job_type.startswith('text_'):
            return "text-processing"
        elif job_type.startswith('image_'):
            return "image-processing"
        elif job_type.startswith('data_'):
            return "data-analysis"
        else:
            return "unknown"
    
    async def shutdown(self):
        """Shutdown the worker system"""
        print("\n🛑 Shutting down worker system...")
        
        # Stop workers
        for worker in self.workers:
            await worker.close()
            stats = worker.get_stats()
            print(f"📊 {worker.name}: {stats['processed']} processed, {stats['failed']} failed")
        
        # Print monitoring summary
        self.monitor.print_summary()
        
        print("✅ Worker system shutdown complete")


async def run_demo():
    """Run the complete worker system demo"""
    print("🚀 Starting Advanced Worker System Demo")
    print("=" * 60)
    
    # Create and connect pilot
    pilot = InprocPilot()
    # Uncomment to use NATS instead:
    # pilot = NatsPilot(nats_url="nats://localhost:4222")
    
    await pilot.connect()
    print("✅ Pilot connected")
    
    # Create and setup worker system
    system = WorkerSystem(pilot)
    await system.setup_workers()
    
    # Dispatch sample jobs
    await system.dispatch_sample_jobs()
    
    # Wait for processing
    print("\n⏳ Waiting for job processing to complete...")
    await asyncio.sleep(5)
    
    # Shutdown system
    await system.shutdown()
    
    print("\n" + "=" * 60)
    print("✅ Advanced Worker System Demo completed!")


if __name__ == "__main__":
    try:
        asyncio.run(run_demo())
    except KeyboardInterrupt:
        print("\n👋 Demo stopped by user")
    except Exception as e:
        print(f"\n💥 Demo failed: {e}")
        raise 