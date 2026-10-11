"""JSON-line request and failure contracts without an OMNeT++ sidecar."""

import io
import json
import unittest
from unittest.mock import Mock

from clients.VeinsClient import VeinsClient, VeinsConnectionError, VeinsProtocolError


class VeinsRequestTests(unittest.TestCase):
    def client(self, response):
        client = VeinsClient(j2735_codec="aligned")
        client.socket = Mock()
        client.reader = io.StringIO(response)
        client.process = Mock()
        self.addCleanup(client.close)
        return client

    def test_requests_preserve_unicode_framing_and_increment_ids(self):
        responses = [{"request_id": 1, "data": ["道路"]}, {"request_id": 2, "status": "OK"}]
        client = self.client("\n".join(json.dumps(item) for item in responses) + "\n")
        for index, response in enumerate(responses, start=1):
            self.assertEqual(client.request("update_mobility", road="道路"), response)
            payload = client.socket.sendall.call_args.args[0]
            self.assertTrue(payload.endswith(b"\n"))
            self.assertEqual(json.loads(payload), {
                "type": "update_mobility", "request_id": index, "road": "道路",
            })
        self.assertEqual(client.request_id, 2)

    def test_transport_failures_close_connection_but_leave_owned_process_running(self):
        for failure in ("write", "read", "eof"):
            with self.subTest(failure=failure):
                client = self.client("")
                socket = client.socket
                if failure == "write":
                    socket.sendall.side_effect = OSError("broken pipe")
                elif failure == "read":
                    client.reader.close()
                    client.reader = Mock()
                    client.reader.readline.side_effect = OSError("connection reset")
                reader = client.reader
                with self.assertRaises(VeinsConnectionError):
                    client.ping()
                self.assertIsNone(client.socket)
                self.assertIsNone(client.reader)
                socket.close.assert_called_once_with()
                client.process.terminate.assert_not_called()
                if failure == "read":
                    reader.close.assert_called_once_with()
                else:
                    self.assertTrue(reader.closed)

    def test_protocol_errors_do_not_silently_accept_or_disconnect(self):
        for response, error in (
            ("invalid\n", "invalid JSON"),
            ("[]\n", "must be a JSON object"),
            ('{"request_id": 99}\n', "expected 1"),
            ('{"status": "error", "message": "denied"}\n', "denied"),
        ):
            with self.subTest(response=response):
                client = self.client(response)
                with self.assertRaisesRegex(VeinsProtocolError, error):
                    client.ping()
                self.assertTrue(client.connected)
                client.socket.close.assert_not_called()


if __name__ == "__main__":
    unittest.main()
