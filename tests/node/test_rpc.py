import pytest
import pytest_asyncio
import asyncio
import time
from unittest.mock import AsyncMock

from hooklet.node.rpc import RPCServer, RPCClient
from hooklet.base.pilot import ReqReply
from hooklet.base.types import Reply, Req
from hooklet.pilot.inproc_pilot import InprocPilot
from hooklet.utils.id_generator import generate_id


class TestRPCServer:
    """Test cases for RPCServer class."""

    class MockServer(RPCServer):
        """Concrete implementation of RPCServer for testing."""
        async def callback(self, req: Req) -> Reply:
            params = req.get("params", {})
            operation = params.get("operation", "unknown")
            data = params.get("data", {})
            
            start_ms = int(time.time() * 1000)
            end_ms = int(time.time() * 1000)
            
            if operation == "add":
                result = {"sum": data.get("a", 0) + data.get("b", 0)}
            elif operation == "multiply":
                result = {"product": data.get("a", 0) * data.get("b", 0)}
            else:
                result = {"error": "Unknown operation"}
            
            return {
                "_id": generate_id(),
                "type": "reply",
                "result": result,
                "error": None,
                "start_ms": start_ms,
                "end_ms": end_ms
            }

    @pytest_asyncio.fixture(scope="function")
    async def pilot(self):
        pilot = InprocPilot()
        await pilot.connect()
        yield pilot
        await pilot.disconnect()

    @pytest_asyncio.fixture
    async def reqreply(self, pilot):
        return pilot.reqreply()

    @pytest_asyncio.fixture
    async def server(self, reqreply):
        return self.MockServer("test-server", reqreply)

    @pytest.mark.asyncio
    async def test_server_initialization(self, server, reqreply):
        """Test RPCServer initialization."""
        assert server.name == "test-server"
        assert server.reqreply == reqreply
        assert not server.is_running

    @pytest.mark.asyncio
    async def test_server_start(self, server):
        """Test RPCServer start method."""
        await server.start()
        assert server.is_running
        await server.close()

    @pytest.mark.asyncio
    async def test_server_callback_registration(self, server, reqreply):
        """Test that server callback is registered with reqreply."""
        await server.start()
        # The callback should be registered with the reqreply interface
        # This is tested indirectly by checking that start() doesn't raise errors
        assert server.is_running
        await server.close()

    @pytest.mark.asyncio
    async def test_server_callback_execution(self, server, reqreply):
        """Test server callback execution."""
        await server.start()
        
        # Create a test request
        test_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {
                "operation": "add",
                "data": {"a": 5, "b": 3}
            },
            "error": None
        }
        
        # Call the callback directly
        response = await server.callback(test_req)
        
        assert response["type"] == "reply"
        assert response["result"]["sum"] == 8
        assert response["error"] is None
        
        await server.close()

    @pytest.mark.asyncio
    async def test_server_multiple_requests(self, server):
        """Test server handling multiple requests."""
        await server.start()
        
        requests = [
            {
                "_id": generate_id(),
                "type": "request",
                "params": {"operation": "add", "data": {"a": 1, "b": 2}},
                "error": None
            },
            {
                "_id": generate_id(),
                "type": "request",
                "params": {"operation": "multiply", "data": {"a": 3, "b": 4}},
                "error": None
            }
        ]
        
        responses = []
        for req in requests:
            response = await server.callback(req)
            responses.append(response)
        
        assert len(responses) == 2
        assert responses[0]["result"]["sum"] == 3
        assert responses[1]["result"]["product"] == 12
        
        await server.close()

    @pytest.mark.asyncio
    async def test_server_shutdown(self, server):
        """Test RPCServer shutdown."""
        await server.start()
        assert server.is_running
        await server.close()
        assert not server.is_running

    @pytest.mark.asyncio
    async def test_server_with_mock_reqreply(self):
        """Test server with mocked reqreply interface."""
        mock_reqreply = AsyncMock()
        server = self.MockServer("mock-server", mock_reqreply)
        
        await server.start()
        
        # Verify that register_callback was called
        mock_reqreply.register_callback.assert_called_once_with("mock-server", server.callback)
        
        await server.close()


class TestRPCClient:
    """Test cases for RPCClient class."""

    @pytest_asyncio.fixture(scope="function")
    async def pilot(self):
        pilot = InprocPilot()
        await pilot.connect()
        yield pilot
        await pilot.disconnect()

    @pytest_asyncio.fixture
    async def reqreply(self, pilot):
        return pilot.reqreply()

    @pytest_asyncio.fixture
    async def client(self, reqreply):
        """Create a test RPCClient instance."""
        return RPCClient(reqreply)

    @pytest.fixture
    def server(self, reqreply):
        """Create a test server for client tests."""
        class MockServer(RPCServer):
            async def callback(self, req: Req) -> Reply:
                params = req.get("params", {})
                operation = params.get("operation", "unknown")
                data = params.get("data", {})
                
                start_ms = int(time.time() * 1000)
                end_ms = int(time.time() * 1000)
                
                if operation == "add":
                    result = {"sum": data.get("a", 0) + data.get("b", 0)}
                else:
                    result = {"error": "Unknown operation"}
                
                return {
                    "_id": generate_id(),
                    "type": "reply",
                    "result": result,
                    "error": None,
                    "start_ms": start_ms,
                    "end_ms": end_ms
                }
        
        return MockServer("test-server", reqreply)

    @pytest.mark.asyncio
    async def test_client_initialization(self, client, reqreply):
        """Test RPCClient initialization."""
        assert client.reqreply == reqreply

    @pytest.mark.asyncio
    async def test_client_request(self, client, server, pilot):
        """Test RPCClient request method."""
        await pilot.connect()
        await server.start()
        
        # Create a test request
        test_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {
                "operation": "add",
                "data": {"a": 10, "b": 20}
            },
            "error": None
        }
        
        # Make the request
        response = await client.request("test-server", test_req)
        
        assert response["type"] == "reply"
        assert response["result"]["sum"] == 30
        assert response["error"] is None
        
        await server.close()

    @pytest.mark.asyncio
    async def test_client_multiple_requests(self, client, server, pilot):
        """Test client sending multiple requests."""
        await pilot.connect()
        await server.start()
        
        requests = [
            {
                "_id": generate_id(),
                "type": "request",
                "params": {"operation": "add", "data": {"a": 1, "b": 2}},
                "error": None
            },
            {
                "_id": generate_id(),
                "type": "request",
                "params": {"operation": "add", "data": {"a": 5, "b": 5}},
                "error": None
            }
        ]
        
        responses = []
        for req in requests:
            response = await client.request("test-server", req)
            responses.append(response)
        
        assert len(responses) == 2
        assert responses[0]["result"]["sum"] == 3
        assert responses[1]["result"]["sum"] == 10
        
        await server.close()

    @pytest.mark.asyncio
    async def test_client_request_to_nonexistent_server(self, client, pilot):
        """Test client request to non-existent server."""
        await pilot.connect()
        
        test_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {"operation": "add", "data": {"a": 1, "b": 2}},
            "error": None
        }
        
        with pytest.raises(ValueError, match="No callback registered for test-server"):
            await client.request("test-server", test_req)

    @pytest.mark.asyncio
    async def test_client_with_mock_reqreply(self):
        """Test client with mocked reqreply interface."""
        mock_reqreply = AsyncMock()
        client = RPCClient(mock_reqreply)
        
        test_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {"operation": "test"},
            "error": None
        }
        
        mock_response: Reply = {
            "_id": generate_id(),
            "type": "reply",
            "result": {"status": "success"},
            "error": None,
            "start_ms": int(time.time() * 1000),
            "end_ms": int(time.time() * 1000)
        }
        
        mock_reqreply.request.return_value = mock_response
        
        response = await client.request("test-subject", test_req)
        
        assert response == mock_response
        mock_reqreply.request.assert_called_once_with("test-subject", test_req, 10.0)


class TestRPCIntegration:
    """Integration tests for RPC server and client."""

    @pytest_asyncio.fixture(scope="function")
    async def pilot(self):
        pilot = InprocPilot()
        await pilot.connect()
        yield pilot
        await pilot.disconnect()

    @pytest.mark.asyncio
    async def test_server_client_integration(self, pilot):
        """Test full integration between server and client."""
        await pilot.connect()
        reqreply = pilot.reqreply()

        # Create a server with custom callback
        class IntegrationServer(RPCServer):
            async def callback(self, req: Req) -> Reply:
                params = req.get("params", {})
                operation = params.get("operation", "unknown")
                data = params.get("data", "")

                start_ms = int(time.time() * 1000)
                end_ms = int(time.time() * 1000)

                if operation == "add":
                    result = {"sum": data.get("a", 0) + data.get("b", 0)}
                elif operation == "multiply":
                    result = {"product": data.get("a", 0) * data.get("b", 0)}
                else:
                    result = {"error": "Unknown operation"}

                return {
                    "_id": generate_id(),
                    "type": "reply",
                    "result": result,
                    "error": None,
                    "start_ms": start_ms,
                    "end_ms": end_ms
                }

        server = IntegrationServer("math-server", reqreply)
        client = RPCClient(reqreply)

        await server.start()

        # Test add operation
        add_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {
                "operation": "add",
                "data": {"a": 15, "b": 25}
            },
            "error": None
        }

        add_response = await client.request("math-server", add_req)
        assert add_response["result"]["sum"] == 40

        # Test multiply operation
        multiply_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {
                "operation": "multiply",
                "data": {"a": 6, "b": 7}
            },
            "error": None
        }

        multiply_response = await client.request("math-server", multiply_req)
        assert multiply_response["result"]["product"] == 42

        await server.close()

    @pytest.mark.asyncio
    async def test_concurrent_requests(self, pilot):
        """Test handling concurrent requests."""
        await pilot.connect()
        reqreply = pilot.reqreply()

        class ConcurrentServer(RPCServer):
            def __init__(self, name: str, reqreply):
                super().__init__(name, reqreply)
                self.request_count = 0
                self._lock = asyncio.Lock()

            async def callback(self, req: Req) -> Reply:
                async with self._lock:
                    self.request_count += 1
                    current_count = self.request_count
                
                # Simulate some processing time
                await asyncio.sleep(0.01)

                start_ms = int(time.time() * 1000)
                end_ms = int(time.time() * 1000)

                return {
                    "_id": generate_id(),
                    "type": "reply",
                    "result": {"processed": True, "count": current_count},
                    "error": None,
                    "start_ms": start_ms,
                    "end_ms": end_ms
                }

        server = ConcurrentServer("concurrent-server", reqreply)
        client = RPCClient(reqreply)

        await server.start()

        # Send multiple concurrent requests
        requests = []
        for i in range(5):
            req: Req = {
                "_id": generate_id(),
                "type": "request",
                "params": {"operation": "test", "data": {"index": i}},
                "error": None
            }
            requests.append(client.request("concurrent-server", req))

        responses = await asyncio.gather(*requests)

        assert len(responses) == 5
        for i, response in enumerate(responses):
            assert response["result"]["processed"] is True
            assert response["result"]["count"] == i + 1

        await server.close()

    @pytest.mark.asyncio
    async def test_error_handling_in_callback(self, pilot):
        """Test error handling in server callback."""
        await pilot.connect()
        reqreply = pilot.reqreply()

        class ErrorServer(RPCServer):
            async def callback(self, req: Req) -> Reply:
                params = req.get("params", {})
                should_error = params.get("should_error", False)

                start_ms = int(time.time() * 1000)
                end_ms = int(time.time() * 1000)

                if should_error:
                    return {
                        "_id": generate_id(),
                        "type": "reply",
                        "result": None,
                        "error": "Simulated error occurred",
                        "start_ms": start_ms,
                        "end_ms": end_ms
                    }
                else:
                    return {
                        "_id": generate_id(),
                        "type": "reply",
                        "result": {"status": "success"},
                        "error": None,
                        "start_ms": start_ms,
                        "end_ms": end_ms
                    }

        server = ErrorServer("error-server", reqreply)
        client = RPCClient(reqreply)

        await server.start()

        # Test successful request
        success_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {"should_error": False},
            "error": None
        }

        success_response = await client.request("error-server", success_req)
        assert success_response["result"]["status"] == "success"
        assert success_response["error"] is None

        # Test error request
        error_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {"should_error": True},
            "error": None
        }

        error_response = await client.request("error-server", error_req)
        assert error_response["result"] is None
        assert error_response["error"] == "Simulated error occurred"

        await server.close()


if __name__ == "__main__":
    pytest.main([__file__])
