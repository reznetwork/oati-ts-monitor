import socket
import threading
import time
import unittest

from daemon_runtime import SocketIpcServer
from ipc_protocol import PROTOCOL_VERSION, decode_message, encode_message


class FakeState:
    def __init__(self):
        self.update_event = threading.Event()
        self._snap = {"last_update": time.time(), "vehicle": "test"}

    def snapshot(self):
        return dict(self._snap)

    def bump(self):
        self._snap["last_update"] = time.time()
        self.update_event.set()


class FakePoller:
    def __init__(self):
        self.reconnect_requested = False

    def request_reconnect(self):
        self.reconnect_requested = True


class TestIpcLifecycle(unittest.TestCase):
    def test_protocol_roundtrip(self):
        raw = encode_message("ping", {"x": 1}).decode("utf-8").strip()
        parsed = decode_message(raw)
        self.assertEqual(parsed["v"], PROTOCOL_VERSION)
        self.assertEqual(parsed["type"], "ping")
        self.assertEqual(parsed["payload"]["x"], 1)

    def test_server_command_and_stream(self):
        state = FakeState()
        poller = FakePoller()
        server = SocketIpcServer(state=state, poller=poller, host="127.0.0.1", port=9312)
        server.start()
        try:
            deadline = time.time() + 2.0
            while server.sock is None and time.time() < deadline:
                time.sleep(0.05)
            self.assertIsNotNone(server.sock)

            with socket.create_connection(("127.0.0.1", 9312), timeout=2.0) as sock:
                fh = sock.makefile("r", encoding="utf-8", newline="\n")
                hello = decode_message(fh.readline().strip())
                self.assertEqual(hello["type"], "hello")

                sock.sendall(encode_message("ping", {}))
                pong = decode_message(fh.readline().strip())
                self.assertEqual(pong["type"], "pong")

                sock.sendall(encode_message("request_reconnect", {}))
                ack = decode_message(fh.readline().strip())
                self.assertEqual(ack["type"], "ack")
                self.assertTrue(poller.reconnect_requested)

                state.bump()
                msg = decode_message(fh.readline().strip())
                self.assertEqual(msg["type"], "state_update")
                self.assertIn("state", msg["payload"])
        finally:
            server.stop()


if __name__ == "__main__":
    unittest.main()
