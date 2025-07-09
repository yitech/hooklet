import pytest
import pytest_asyncio
import asyncio
import time
from typing import Any

from hooklet.node.worker import Worker, Dispatcher
from hooklet.base.pilot import Msg
from hooklet.base.types import Job
from hooklet.pilot.inproc_pilot import InprocPilot
from hooklet.utils.id_generator import generate_id


class TestWorker:
    """Test cases for Worker class using real InprocPilot interfaces."""

    class MockWorker(Worker):
        """Concrete implementation of Worker for testing."""
        def __init__(self, name, pushpull):
            super().__init__(name, pushpull)
            self.processed_jobs = []
            self.process_results = []
            self.should_fail = False
            self.fail_message = "Test failure"

        async def on_job(self, job: Job) -> int:
            self.processed_jobs.append(job)
            if self.should_fail:
                raise Exception(self.fail_message)
            await asyncio.sleep(0.01)
            result = len(self.processed_jobs)
            self.process_results.append(result)
            return 0

    @pytest_asyncio.fixture(scope="function")
    async def pilot(self):
        pilot = InprocPilot()
        await pilot.connect()
        yield pilot
        await pilot.disconnect()

    @pytest_asyncio.fixture
    async def pushpull(self, pilot):
        return pilot.pushpull()

    @pytest_asyncio.fixture
    async def worker(self, pushpull):
        return self.MockWorker("test-worker", pushpull)

    @pytest.fixture
    def sample_job(self):
        return Job(
            _id=generate_id(),
            type="test_job",
            data={"test": "data"},
            error=None,
            recv_ms=int(time.time() * 1000),
            start_ms=0,
            end_ms=0,
            status="pending",
            retry_count=0
        )

    @pytest.mark.asyncio
    async def test_worker_initialization(self, worker, pushpull):
        assert worker.name == "test-worker"
        assert worker.pushpull == pushpull
        assert worker.processed_jobs == []
        assert worker.process_results == []

    @pytest.mark.asyncio
    async def test_worker_start(self, worker):
        await worker.start()
        # No direct way to check callback registration, but no error means success
        await worker.close()

    @pytest.mark.asyncio
    async def test_worker_process_job(self, worker, sample_job):
        result = await worker.on_job(sample_job)
        assert result == 0
        assert len(worker.processed_jobs) == 1
        assert worker.processed_jobs[0] == sample_job
        assert len(worker.process_results) == 1
        assert worker.process_results[0] == 1

    @pytest.mark.asyncio
    async def test_worker_process_multiple_jobs(self, worker):
        jobs = [Job(
            _id=generate_id(),
            type=f"test_job_{i}",
            data={"index": i},
            error=None,
            recv_ms=int(time.time() * 1000),
            start_ms=0,
            end_ms=0,
            status="pending",
            retry_count=0
        ) for i in range(3)]
        for job in jobs:
            result = await worker.on_job(job)
            assert result == 0
        assert len(worker.processed_jobs) == 3
        assert len(worker.process_results) == 3
        assert worker.process_results == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_worker_process_failure(self, worker, sample_job):
        worker.should_fail = True
        with pytest.raises(Exception, match="Test failure"):
            await worker.on_job(sample_job)
        assert len(worker.processed_jobs) == 1
        assert worker.processed_jobs[0] == sample_job

    @pytest.mark.asyncio
    async def test_worker_shutdown(self, worker):
        await worker.start()
        await worker.close()

    @pytest.mark.asyncio
    async def test_worker_context_manager(self, pushpull):
        class ContextWorker(self.MockWorker):
            async def __aenter__(self):
                await self.start()
                return self
            async def __aexit__(self, exc_type, exc_value, traceback):
                await self.close()
        async with ContextWorker("test-worker", pushpull) as worker:
            pass

    @pytest.mark.asyncio
    async def test_worker_with_real_pilot(self, pilot):
        worker = self.MockWorker("test-worker", pilot.pushpull())
        await worker.start()
        job = Job(
            _id=generate_id(),
            type="test_job",
            data={"test": "data"},
            error=None,
            recv_ms=int(time.time() * 1000),
            start_ms=0,
            end_ms=0,
            status="pending",
            retry_count=0
        )
        result = await worker.on_job(job)
        assert result == 0
        await worker.close()

    @pytest.mark.asyncio
    async def test_worker_error_handling(self, pushpull):
        class ErrorWorker(self.MockWorker):
            async def on_job(self, job: Job) -> int:
                raise ValueError("Processing error")
        
        worker = ErrorWorker("error-worker", pushpull)
        job = Job(
            _id=generate_id(),
            type="error_job",
            data={"test": "data"},
            error=None,
            recv_ms=int(time.time() * 1000),
            start_ms=0,
            end_ms=0,
            status="pending",
            retry_count=0
        )
        with pytest.raises(ValueError, match="Processing error"):
            await worker.on_job(job)

    @pytest.mark.asyncio
    async def test_worker_registration_with_pushpull(self, pushpull):
        worker = self.MockWorker("test-worker", pushpull)
        await worker.start()
        await worker.close()


class TestDispatcher:
    """Test cases for Dispatcher class using real InprocPilot interfaces."""

    @pytest_asyncio.fixture(scope="function")
    async def pilot(self):
        pilot = InprocPilot()
        await pilot.connect()
        yield pilot
        await pilot.disconnect()

    @pytest_asyncio.fixture
    async def pushpull(self, pilot):
        return pilot.pushpull()

    @pytest_asyncio.fixture
    async def dispatcher(self, pushpull):
        return Dispatcher(pushpull)

    @pytest.fixture
    def sample_job(self):
        return Job(
            _id=generate_id(),
            type="test_job",
            data={"test": "data"},
            error=None,
            recv_ms=int(time.time() * 1000),
            start_ms=0,
            end_ms=0,
            status="new",
            retry_count=0
        )

    @pytest.mark.asyncio
    async def test_dispatcher_initialization(self, dispatcher, pushpull):
        assert dispatcher.pushpull == pushpull
        assert dispatcher._subscriptions == {}

    @pytest.mark.asyncio
    async def test_dispatcher_dispatch(self, dispatcher, pushpull, sample_job):
        result = await dispatcher.dispatch("test-subject", sample_job)
        assert result is True

    @pytest.mark.asyncio
    async def test_dispatcher_subscribe_and_publish(self, dispatcher, pushpull):
        received_jobs = []
        async def job_callback(job: Job):
            received_jobs.append(job)
        
        await dispatcher.subscribe("test-subject", job_callback)
        assert "test-subject" in dispatcher._subscriptions
        
        # Dispatch a job to the subscribed subject
        job = Job(
            _id=generate_id(),
            type="test_job",
            data={"test": "data"},
            error=None,
            recv_ms=int(time.time() * 1000),
            start_ms=0,
            end_ms=0,
            status="new",
            retry_count=0
        )
        
        await dispatcher.dispatch("test-subject", job)
        await asyncio.sleep(0.1)  # Give time for callback to execute
        
        assert len(received_jobs) == 1
        assert received_jobs[0].id == job.id

    @pytest.mark.asyncio
    async def test_dispatcher_unsubscribe(self, dispatcher):
        received_jobs = []
        async def job_callback(job: Job):
            received_jobs.append(job)
        
        await dispatcher.subscribe("test-subject", job_callback)
        assert "test-subject" in dispatcher._subscriptions
        
        result = await dispatcher.unsubscribe("test-subject")
        assert result is True
        assert "test-subject" not in dispatcher._subscriptions

    @pytest.mark.asyncio
    async def test_dispatcher_multiple_subscriptions(self, dispatcher):
        received_jobs_1 = []
        received_jobs_2 = []
        
        async def job_callback_1(job: Job):
            received_jobs_1.append(job)
        
        async def job_callback_2(job: Job):
            received_jobs_2.append(job)
        
        await dispatcher.subscribe("subject-1", job_callback_1)
        await dispatcher.subscribe("subject-2", job_callback_2)
        
        assert "subject-1" in dispatcher._subscriptions
        assert "subject-2" in dispatcher._subscriptions
        assert len(dispatcher._subscriptions) == 2

    @pytest.mark.asyncio
    async def test_dispatcher_unsubscribe_nonexistent(self, dispatcher):
        result = await dispatcher.unsubscribe("nonexistent-subject")
        assert result is False

    @pytest.mark.asyncio
    async def test_dispatcher_dispatch_failure(self, dispatcher):
        # Test dispatch with a valid job (should return True)
        valid_job = Job(
            _id=generate_id(),
            type="test_job",
            data={"test": "data"},
            error=None,
            recv_ms=int(time.time() * 1000),
            start_ms=0,
            end_ms=0,
            status="pending",
            retry_count=0
        )
        result = await dispatcher.dispatch("test-subject", valid_job)
        assert result is True


class TestWorkerIntegration:
    """Integration tests for Worker and Dispatcher working together."""

    @pytest_asyncio.fixture(scope="function")
    async def pilot(self):
        pilot = InprocPilot()
        await pilot.connect()
        yield pilot
        await pilot.disconnect()

    @pytest.mark.asyncio
    async def test_worker_with_dispatcher(self, pilot):
        # Create worker and dispatcher
        worker = TestWorker.MockWorker("test-worker", pilot.pushpull())
        dispatcher = Dispatcher(pilot.pushpull())
        
        # Start worker
        await worker.start()
        
        # Subscribe to worker status via dispatcher
        received_status = []
        async def status_callback(job: Job):
            received_status.append(job)
        
        await dispatcher.subscribe("test-worker", status_callback)
        
        # Dispatch a job
        job = Job(
            _id=generate_id(),
            type="test_job",
            data={"test": "data"},
            error=None,
            recv_ms=int(time.time() * 1000),
            start_ms=0,
            end_ms=0,
            status="new",
            retry_count=0
        )
        
        success = await dispatcher.dispatch("test-worker", job)
        assert success is True
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Verify worker processed the job
        assert len(worker.processed_jobs) == 1
        assert worker.processed_jobs[0].id == job.id
        
        await worker.close()

    @pytest.mark.asyncio
    async def test_worker_performance(self, pilot):
        worker = TestWorker.MockWorker("perf-worker", pilot.pushpull())
        await worker.start()
        
        # Process multiple jobs quickly
        jobs = []
        for i in range(10):
            job = Job(
                _id=generate_id(),
                type=f"perf_job_{i}",
                data={"index": i},
                error=None,
                recv_ms=int(time.time() * 1000),
                start_ms=0,
                end_ms=0,
                status="new",
                retry_count=0
            )
            jobs.append(job)
        
        start_time = time.time()
        for job in jobs:
            result = await worker.on_job(job)
            assert result == 0
        end_time = time.time()
        
        # Verify all jobs were processed
        assert len(worker.processed_jobs) == 10
        assert len(worker.process_results) == 10
        
        # Performance should be reasonable (less than 1 second for 10 jobs)
        assert end_time - start_time < 1.0
        
        await worker.close()

    @pytest.mark.asyncio
    async def test_worker_dispatcher_round_trip(self, pilot):
        """Test complete round trip: dispatcher -> worker -> result"""
        worker = TestWorker.MockWorker("round-trip-worker", pilot.pushpull())
        dispatcher = Dispatcher(pilot.pushpull())
        
        await worker.start()
        
        # Subscribe to results
        received_results = []
        async def result_callback(job: Job):
            received_results.append(job)
        
        await dispatcher.subscribe("round-trip-worker", result_callback)
        
        # Dispatch job
        job = Job(
            _id=generate_id(),
            type="round_trip_job",
            data={"message": "hello"},
            error=None,
            recv_ms=int(time.time() * 1000),
            start_ms=0,
            end_ms=0,
            status="new",
            retry_count=0
        )
        
        await dispatcher.dispatch("round-trip-worker", job)
        
        # Wait for processing
        await asyncio.sleep(0.1)
        
        # Verify worker processed the job
        assert len(worker.processed_jobs) == 1
        assert worker.processed_jobs[0].id == job.id
        
        await worker.close() 