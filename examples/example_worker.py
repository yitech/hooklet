#!/usr/bin/env python3
"""
Worker Demo Example - Job Processing

This example demonstrates how to create workers that process different types
of jobs (text processing, image processing, data analysis) using the PushPull pattern.
"""

import asyncio
import time
import uuid
from typing import Any, Dict

from hooklet.node.worker import Worker, Dispatcher
from hooklet.pilot.inproc_pilot import InprocPilot
from hooklet.pilot.nats_pilot import NatsPilot
from hooklet.base.types import Job
from hooklet.utils.id_generator import generate_id


class TextProcessorWorker(Worker):
    """Worker that processes text-related jobs"""
    
    def __init__(self, name: str, pushpull, subject: str = "text-processing"):
        super().__init__(name, pushpull)
        self.subject = subject
    
    async def start(self):
        await super().start()
        await self.pushpull.register_worker(self.subject, self.process)
    
    async def process(self, job: Job) -> int:
        """Process text processing jobs"""
        job_type = job.get('type')
        data = job.get('data', {})
        
        print(f"📝 TextProcessor processing job {job.get('_id')}: {job_type}")
        
        try:
            if job_type == "uppercase":
                text = data.get('text', '')
                result = text.upper()
                print(f"   ✅ Converted to uppercase: '{text}' -> '{result}'")
                
            elif job_type == "lowercase":
                text = data.get('text', '')
                result = text.lower()
                print(f"   ✅ Converted to lowercase: '{text}' -> '{result}'")
                
            elif job_type == "word_count":
                text = data.get('text', '')
                result = len(text.split())
                print(f"   ✅ Word count: '{text}' -> {result} words")
                
            elif job_type == "reverse":
                text = data.get('text', '')
                result = text[::-1]
                print(f"   ✅ Reversed: '{text}' -> '{result}'")
                
            else:
                print(f"   ❌ Unknown text job type: {job_type}")
                return 1
            
            # Simulate processing time
            await asyncio.sleep(0.5)
            return 0
            
        except Exception as e:
            print(f"   ❌ Text processing error: {e}")
            return 1


class ImageProcessorWorker(Worker):
    """Worker that processes image-related jobs"""
    
    def __init__(self, name: str, pushpull, subject: str = "image-processing"):
        super().__init__(name, pushpull)
        self.subject = subject
    
    async def start(self):
        await super().start()
        await self.pushpull.register_worker(self.subject, self.process)
    
    async def process(self, job: Job) -> int:
        """Process image processing jobs"""
        job_type = job.get('type')
        data = job.get('data', {})
        
        print(f"🖼️  ImageProcessor processing job {job.get('_id')}: {job_type}")
        
        try:
            if job_type == "resize":
                width = data.get('width', 0)
                height = data.get('height', 0)
                print(f"   ✅ Resized image to {width}x{height}")
                
            elif job_type == "crop":
                x = data.get('x', 0)
                y = data.get('y', 0)
                w = data.get('width', 0)
                h = data.get('height', 0)
                print(f"   ✅ Cropped image at ({x},{y}) with size {w}x{h}")
                
            elif job_type == "filter":
                filter_type = data.get('filter', 'unknown')
                print(f"   ✅ Applied {filter_type} filter to image")
                
            elif job_type == "convert":
                format_to = data.get('format', 'unknown')
                print(f"   ✅ Converted image to {format_to} format")
                
            else:
                print(f"   ❌ Unknown image job type: {job_type}")
                return 1
            
            # Simulate processing time
            await asyncio.sleep(1.0)
            return 0
            
        except Exception as e:
            print(f"   ❌ Image processing error: {e}")
            return 1


class DataAnalyzerWorker(Worker):
    """Worker that processes data analysis jobs"""
    
    def __init__(self, name: str, pushpull, subject: str = "data-analysis"):
        super().__init__(name, pushpull)
        self.subject = subject
    
    async def start(self):
        await super().start()
        await self.pushpull.register_worker(self.subject, self.process)
    
    async def process(self, job: Job) -> int:
        """Process data analysis jobs"""
        job_type = job.get('type')
        data = job.get('data', {})
        
        print(f"📊 DataAnalyzer processing job {job.get('_id')}: {job_type}")
        
        try:
            if job_type == "calculate_mean":
                numbers = data.get('numbers', [])
                if numbers:
                    result = sum(numbers) / len(numbers)
                    print(f"   ✅ Mean of {numbers}: {result:.2f}")
                else:
                    print(f"   ❌ No numbers provided for mean calculation")
                    return 1
                    
            elif job_type == "calculate_sum":
                numbers = data.get('numbers', [])
                result = sum(numbers)
                print(f"   ✅ Sum of {numbers}: {result}")
                
            elif job_type == "find_max":
                numbers = data.get('numbers', [])
                if numbers:
                    result = max(numbers)
                    print(f"   ✅ Max of {numbers}: {result}")
                else:
                    print(f"   ❌ No numbers provided for max calculation")
                    return 1
                    
            elif job_type == "find_min":
                numbers = data.get('numbers', [])
                if numbers:
                    result = min(numbers)
                    print(f"   ✅ Min of {numbers}: {result}")
                else:
                    print(f"   ❌ No numbers provided for min calculation")
                    return 1
                    
            else:
                print(f"   ❌ Unknown data analysis job type: {job_type}")
                return 1
            
            # Simulate processing time
            await asyncio.sleep(0.8)
            return 0
            
        except Exception as e:
            print(f"   ❌ Data analysis error: {e}")
            return 1


class JobDispatcher:
    """Helper class to dispatch jobs to different workers"""
    
    def __init__(self, dispatcher: Dispatcher):
        self.dispatcher = dispatcher
    
    async def dispatch_text_job(self, job_type: str, text: str) -> bool:
        """Dispatch a text processing job"""
        job: Job = {
            "_id": generate_id(),
            "type": job_type,
            "data": {"text": text},
            "error": None,
            "recv_ms": int(time.time() * 1000),
            "start_ms": 0,
            "end_ms": 0,
            "status": "new",
            "retry_count": 0
        }
        return await self.dispatcher.dispatch("text-processing", job)
    
    async def dispatch_image_job(self, job_type: str, **kwargs) -> bool:
        """Dispatch an image processing job"""
        job: Job = {
            "_id": generate_id(),
            "type": job_type,
            "data": kwargs,
            "error": None,
            "recv_ms": int(time.time() * 1000),
            "start_ms": 0,
            "end_ms": 0,
            "status": "new",
            "retry_count": 0
        }
        return await self.dispatcher.dispatch("image-processing", job)
    
    async def dispatch_data_job(self, job_type: str, numbers: list) -> bool:
        """Dispatch a data analysis job"""
        job: Job = {
            "_id": generate_id(),
            "type": job_type,
            "data": {"numbers": numbers},
            "error": None,
            "recv_ms": int(time.time() * 1000),
            "start_ms": 0,
            "end_ms": 0,
            "status": "new",
            "retry_count": 0
        }
        return await self.dispatcher.dispatch("data-analysis", job)


async def run_demo():
    """Run the worker job processing demo"""
    print("🚀 Starting Worker Job Processing Demo")
    print("=" * 60)
    
    # Create pilot and connect
    pilot = InprocPilot()
    # pilot = NatsPilot(
    #     nats_url="nats://localhost:4222"
    # )
    await pilot.connect()
    
    # Create workers
    text_worker = TextProcessorWorker("text-worker", pilot.pushpull())
    image_worker = ImageProcessorWorker("image-worker", pilot.pushpull())
    data_worker = DataAnalyzerWorker("data-worker", pilot.pushpull())
    
    # Create dispatcher
    dispatcher = Dispatcher(pilot.pushpull())
    job_dispatcher = JobDispatcher(dispatcher)
    
    # Start workers
    await text_worker.start()
    await image_worker.start()
    await data_worker.start()
    
    print("✅ All workers started and ready to process jobs")
    print("=" * 60)
    
    # Dispatch various jobs
    print("\n📤 Dispatching text processing jobs...")
    await job_dispatcher.dispatch_text_job("uppercase", "hello world")
    await job_dispatcher.dispatch_text_job("lowercase", "HELLO WORLD")
    await job_dispatcher.dispatch_text_job("word_count", "This is a sample text")
    await job_dispatcher.dispatch_text_job("reverse", "python")
    
    print("\n📤 Dispatching image processing jobs...")
    await job_dispatcher.dispatch_image_job("resize", width=1920, height=1080)
    await job_dispatcher.dispatch_image_job("crop", x=100, y=100, width=800, height=600)
    await job_dispatcher.dispatch_image_job("filter", filter="blur")
    await job_dispatcher.dispatch_image_job("convert", format="PNG")
    
    print("\n📤 Dispatching data analysis jobs...")
    await job_dispatcher.dispatch_data_job("calculate_mean", [1, 2, 3, 4, 5])
    await job_dispatcher.dispatch_data_job("calculate_sum", [10, 20, 30, 40])
    await job_dispatcher.dispatch_data_job("find_max", [3, 7, 2, 9, 1])
    await job_dispatcher.dispatch_data_job("find_min", [5, 8, 2, 10, 3])
    
    # Let workers process the jobs
    print("\n⏳ Waiting for workers to process all jobs...")
    await asyncio.sleep(8)
    
    # Stop everything
    await text_worker.close()
    await image_worker.close()
    await data_worker.close()
    
    print("\n" + "=" * 60)
    print("✅ Worker Job Processing Demo completed!")


if __name__ == "__main__":
    try:
        asyncio.run(run_demo())
    except KeyboardInterrupt:
        print("\n👋 Demo stopped by user") 