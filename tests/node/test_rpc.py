import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock
from hooklet.node.rpc import RPCServer, RPCClient
from hooklet.pilot.inproc_pilot import InprocPilot
from hooklet.base.types import Req, Reply
from hooklet.utils.id_generator import generate_id


class TestRPCServer:
    """Test cases for RPCServer class."""
    
    class MockRPCServer(RPCServer):
        """Concrete implementation of RPCServer for testing."""
        
        def __init__(self, name: str, reqreply):
            super().__init__(name, reqreply)
            self.received_requests = []
        
        async def callback(self, req: Req) -> Reply:
            """Simple echo callback for testing."""
            self.received_requests.append(req)
            start_ms = int(time.time() * 1000)
            end_ms = int(time.time() * 1000)
            return {
                "_id": generate_id(),
                "type": "reply",
                "result": {"response": "echo", "data": req.get("params", {}).get("data", "")},
                "error": None,
                "start_ms": start_ms,
                "end_ms": end_ms
            }
    
    @pytest.fixture
    def pilot(self):
        """Create an InprocPilot instance for testing."""
        return InprocPilot()
    
    @pytest.fixture
    def reqreply(self, pilot):
        """Get the reqreply interface from the pilot."""
        return pilot.reqreply()
    
    @pytest.fixture
    def server(self, reqreply):
        """Create a test RPCServer instance."""
        return self.MockRPCServer("test-server", reqreply)
    
    @pytest.mark.asyncio
    async def test_server_initialization(self, server, reqreply):
        """Test RPCServer initialization."""
        assert server.name == "test-server"
        assert server.reqreply == reqreply
        assert not server.is_running
    
    @pytest.mark.asyncio
    async def test_server_start(self, server, pilot):
        """Test RPCServer start method."""
        await pilot.connect()
        
        # Start the server
        await server.start()
        
        # Check that server is running
        assert server.is_running
        
        # Check that callback is registered
        assert "test-server" in pilot.reqreply()._callbacks
        
        # Cleanup
        await server.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_server_callback_registration(self, server, pilot):
        """Test that server callback is properly registered."""
        await pilot.connect()
        await server.start()
        
        # Verify callback is registered
        assert "test-server" in pilot.reqreply()._callbacks
        assert pilot.reqreply()._callbacks["test-server"] == server.callback
        
        await server.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_server_callback_execution(self, server, pilot):
        """Test that server callback executes correctly."""
        await pilot.connect()
        await server.start()
        
        # Send a test request
        test_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {"data": "hello world", "id": 123},
            "error": None
        }
        response: Reply = await pilot.reqreply().request("test-server", test_req)
        
        # Verify response structure
        assert response["type"] == "reply"
        assert response["error"] is None
        assert "result" in response
        assert response["result"]["response"] == "echo"
        assert response["result"]["data"] == "hello world"
        assert "start_ms" in response
        assert "end_ms" in response
        
        # Verify request was received
        assert len(server.received_requests) == 1
        assert server.received_requests[0] == test_req
        
        await server.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_server_multiple_requests(self, server, pilot):
        """Test handling multiple requests."""
        await pilot.connect()
        await server.start()
        
        # Send multiple test requests
        requests = [
            {
                "_id": generate_id(),
                "type": "request",
                "params": {"data": "msg1", "id": 1},
                "error": None
            },
            {
                "_id": generate_id(),
                "type": "request",
                "params": {"data": "msg2", "id": 2},
                "error": None
            },
            {
                "_id": generate_id(),
                "type": "request",
                "params": {"data": "msg3", "id": 3},
                "error": None
            }
        ]
        
        responses = []
        for req in requests:
            response = await pilot.reqreply().request("test-server", req)
            responses.append(response)
        
        # Verify all responses
        for i, response in enumerate(responses):
            assert response["type"] == "reply"
            assert response["error"] is None
            assert response["result"]["response"] == "echo"
            assert response["result"]["data"] == f"msg{i+1}"
        
        # Verify all requests were received
        assert len(server.received_requests) == 3
        assert server.received_requests == requests
        
        await server.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_server_shutdown(self, server, pilot):
        """Test server shutdown behavior."""
        await pilot.connect()
        await server.start()
        
        assert server.is_running
        
        # Shutdown the server
        await server.close()
        
        # Verify server is no longer running
        assert not server.is_running
        
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_server_with_mock_reqreply(self):
        """Test server with mocked reqreply interface."""
        mock_reqreply = AsyncMock()
        server = self.MockRPCServer("test-server", mock_reqreply)
        
        await server.start()
        
        # Verify register_callback was called
        mock_reqreply.register_callback.assert_called_once_with("test-server", server.callback)
        
        await server.close()


class TestRPCClient:
    """Test cases for RPCClient class."""
    
    @pytest.fixture
    def pilot(self):
        """Create an InprocPilot instance for testing."""
        return InprocPilot()
    
    @pytest.fixture
    def reqreply(self, pilot):
        """Get the reqreply interface from the pilot."""
        return pilot.reqreply()
    
    @pytest.fixture
    def client(self, reqreply):
        """Create a test RPCClient instance."""
        return RPCClient("test-client", reqreply)
    
    @pytest.fixture
    def server(self, reqreply):
        """Create a test server for client testing."""
        class TestServer(TestRPCServer.MockRPCServer):
            pass
        
        return TestServer("test-server", reqreply)
    
    @pytest.mark.asyncio
    async def test_client_initialization(self, client, reqreply):
        """Test RPCClient initialization."""
        assert client.name == "test-client"
        assert client.reqreply == reqreply
        assert not client.is_running
    
    @pytest.mark.asyncio
    async def test_client_request(self, client, server, pilot):
        """Test RPCClient request method."""
        await pilot.connect()
        await server.start()
        await client.start()
        
        # Send a request through the client
        test_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {"data": "client request", "id": 456},
            "error": None
        }
        response: Reply = await client.request("test-server", test_req)
        
        # Verify response
        assert response["type"] == "reply"
        assert response["error"] is None
        assert response["result"]["response"] == "echo"
        assert response["result"]["data"] == "client request"
        
        # Verify request was received by server
        assert len(server.received_requests) == 1
        assert server.received_requests[0] == test_req
        
        await client.close()
        await server.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_client_multiple_requests(self, client, server, pilot):
        """Test client sending multiple requests."""
        await pilot.connect()
        await server.start()
        await client.start()
        
        # Send multiple requests
        requests = [
            {
                "_id": generate_id(),
                "type": "request",
                "params": {"data": "req1", "id": 1},
                "error": None
            },
            {
                "_id": generate_id(),
                "type": "request",
                "params": {"data": "req2", "id": 2},
                "error": None
            },
            {
                "_id": generate_id(),
                "type": "request",
                "params": {"data": "req3", "id": 3},
                "error": None
            }
        ]
        
        responses = []
        for req in requests:
            response = await client.request("test-server", req)
            responses.append(response)
        
        # Verify all responses
        for i, response in enumerate(responses):
            assert response["type"] == "reply"
            assert response["error"] is None
            assert response["result"]["response"] == "echo"
            assert response["result"]["data"] == f"req{i+1}"
        
        # Verify all requests were received
        assert len(server.received_requests) == 3
        
        await client.close()
        await server.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_client_request_to_nonexistent_server(self, client, pilot):
        """Test client request to non-existent server."""
        await pilot.connect()
        await client.start()
        
        test_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {"data": "test"},
            "error": None
        }
        
        # Should raise an error for non-existent server
        with pytest.raises(ValueError, match="No callback registered for nonexistent-server"):
            await client.request("nonexistent-server", test_req)
        
        await client.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_client_with_mock_reqreply(self):
        """Test client with mocked reqreply interface."""
        mock_reqreply = AsyncMock()
        client = RPCClient("test-client", mock_reqreply)
        
        test_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {"data": "test"},
            "error": None
        }
        
        mock_response: Reply = {
            "_id": generate_id(),
            "type": "reply",
            "result": {"status": "ok"},
            "error": None,
            "start_ms": int(time.time() * 1000),
            "end_ms": int(time.time() * 1000)
        }
        mock_reqreply.request.return_value = mock_response
        
        response = await client.request("test-server", test_req)
        
        # Verify request was made
        mock_reqreply.request.assert_called_once_with("test-server", test_req)
        assert response == mock_response
    
    @pytest.mark.asyncio
    async def test_client_shutdown(self, client, pilot):
        """Test client shutdown behavior."""
        await pilot.connect()
        await client.start()
        
        assert client.is_running
        
        # Shutdown the client
        await client.close()
        
        # Verify client is no longer running
        assert not client.is_running
        
        await pilot.disconnect()


class TestRPCIntegration:
    """Integration tests for RPC functionality."""
    
    @pytest.fixture
    def pilot(self):
        """Create an InprocPilot instance for testing."""
        return InprocPilot()
    
    @pytest.fixture
    def reqreply(self, pilot):
        """Get the reqreply interface from the pilot."""
        return pilot.reqreply()
    
    @pytest.mark.asyncio
    async def test_server_client_integration(self, pilot, reqreply):
        """Test full integration between server and client."""
        await pilot.connect()
        
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
        client = RPCClient("math-client", reqreply)
        
        await server.start()
        await client.start()
        
        # Test addition
        add_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {
                "operation": "add",
                "data": {"a": 5, "b": 3}
            },
            "error": None
        }
        add_response = await client.request("math-server", add_req)
        assert add_response["result"]["sum"] == 8
        
        # Test multiplication
        mul_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {
                "operation": "multiply",
                "data": {"a": 4, "b": 7}
            },
            "error": None
        }
        mul_response = await client.request("math-server", mul_req)
        assert mul_response["result"]["product"] == 28
        
        await client.close()
        await server.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_concurrent_requests(self, pilot, reqreply):
        """Test handling concurrent requests."""
        await pilot.connect()
        
        class ConcurrentServer(RPCServer):
            def __init__(self, name: str, reqreply):
                super().__init__(name, reqreply)
                self.request_count = 0
            
            async def callback(self, req: Req) -> Reply:
                self.request_count += 1
                # Simulate some processing time
                await asyncio.sleep(0.01)
                
                start_ms = int(time.time() * 1000)
                end_ms = int(time.time() * 1000)
                
                return {
                    "_id": generate_id(),
                    "type": "reply",
                    "result": {"processed": True, "count": self.request_count},
                    "error": None,
                    "start_ms": start_ms,
                    "end_ms": end_ms
                }
        
        server = ConcurrentServer("concurrent-server", reqreply)
        client = RPCClient("concurrent-client", reqreply)
        
        await server.start()
        await client.start()
        
        # Send concurrent requests
        async def send_request(i):
            req: Req = {
                "_id": generate_id(),
                "type": "request",
                "params": {"index": i},
                "error": None
            }
            return await client.request("concurrent-server", req)
        
        # Send 10 concurrent requests
        tasks = [send_request(i) for i in range(10)]
        responses = await asyncio.gather(*tasks)
        
        # Verify all responses
        for i, response in enumerate(responses):
            assert response["type"] == "reply"
            assert response["error"] is None
            assert response["result"]["processed"] is True
        
        # Verify all requests were processed
        assert server.request_count == 10
        
        await client.close()
        await server.close()
        await pilot.disconnect()
    
    @pytest.mark.asyncio
    async def test_error_handling_in_callback(self, pilot, reqreply):
        """Test error handling in server callback."""
        await pilot.connect()
        
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
        client = RPCClient("error-client", reqreply)
        
        await server.start()
        await client.start()
        
        # Test successful request
        success_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {"should_error": False},
            "error": None
        }
        success_response = await client.request("error-server", success_req)
        assert success_response["error"] is None
        assert success_response["result"]["status"] == "success"
        
        # Test error request
        error_req: Req = {
            "_id": generate_id(),
            "type": "request",
            "params": {"should_error": True},
            "error": None
        }
        error_response = await client.request("error-server", error_req)
        assert error_response["error"] == "Simulated error occurred"
        assert error_response["result"] is None
        
        await client.close()
        await server.close()
        await pilot.disconnect()


if __name__ == "__main__":
    pytest.main([__file__])
