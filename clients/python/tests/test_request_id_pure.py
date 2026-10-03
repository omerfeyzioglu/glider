"""Request-ID API tests that do not open a listening socket."""
import unittest

import support  # noqa: F401 (sets up sys.path)
from glider_client import Client, GliderError


class ExplicitRequestIdTests(unittest.TestCase):
    def test_write_resends_identical_id_after_busy(self):
        client = Client(max_retries=1)
        client._sleep = lambda _: None
        calls = []
        rid = {"boundary": 4, "nonce": "a" * 32}

        def request(method, path, body=None):
            calls.append((method, path, body))
            if len(calls) == 1:
                raise GliderError("busy", 429)
            return {"sequence": 5}

        client._request = request
        self.assertEqual(client.upsert([{"id": 1, "vector": [1]}], request_id=rid), 5)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0][2], calls[1][2])
        self.assertEqual(calls[0][2]["request_id"], rid)

    def test_bad_request_id_rejected_before_network(self):
        client = Client()
        client._request = lambda *_args, **_kwargs: self.fail("network call")
        with self.assertRaises(ValueError):
            client.write(delete=[1], request_id={"boundary": 0, "nonce": "invalid"})


if __name__ == "__main__":
    unittest.main()
