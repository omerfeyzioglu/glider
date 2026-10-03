"""Integration tests against a real glider-server.

Run only when GLIDER_SERVER_BIN points to a glider-server binary. Tests that
need the newer endpoints (`exact`, `/v1/scan`, `/v1/points/get`) skip
themselves when the binary does not have them.
"""

import secrets
import unittest

import support
from support import RealServer, requires_server, supports

from glider_client import Client, GliderError

NEW_SCAN = ("POST", "/v1/scan", {"limit": 1})
NEW_GET = ("POST", "/v1/points/get", {"ids": [1]})
NEW_EXACT = ("POST", "/v1/query", {"vector": [0, 0, 1], "k": 1, "exact": True})


@requires_server
class IntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = RealServer(dimensions=3)
        cls.client = Client(cls.server.url, timeout=10)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def need(self, probe):
        if not supports(self.client, *probe):
            self.skipTest(f"server lacks {probe[1]} (needs the release with the new endpoints)")

    def test_health_and_status(self):
        self.assertTrue(self.client.health())
        self.assertIn("sequence", self.client.status())

    def test_write_query_get_delete(self):
        before = self.client.status()["sequence"]
        seq = self.client.upsert(
            [
                {"id": 1001, "vector": [-50, -50, -50], "metadata": {"color": "red"}},
                {"id": 1002, "vector": [-49, -49, -49], "metadata": {"color": "blue"}},
                {"id": 1003, "vector": [-45, -45, -45]},
            ]
        )
        self.assertGreater(seq, before)
        hits = self.client.query([-49, -49, -49.1], k=2, include_metadata=True, include_vector=True)
        self.assertEqual([h.id for h in hits], [1002, 1001])
        self.assertEqual(hits[1].metadata, {"color": "red"})
        self.assertEqual(hits[0].vector, [-49.0, -49.0, -49.0])
        self.assertLess(hits[0].distance, hits[1].distance)
        point = self.client.get(1001)
        self.assertEqual((point.vector, point.metadata), ([-50.0, -50.0, -50.0], {"color": "red"}))
        self.assertIsNone(self.client.get(999_999))
        self.client.write(upsert=[{"id": 1003, "vector": [9, 9, 9]}], delete=[1002, 424242])
        self.assertIsNone(self.client.get(1002))
        self.assertEqual(self.client.get(1003).vector, [9.0, 9.0, 9.0])

    def test_server_errors_surface(self):
        with self.assertRaises(GliderError) as ctx:
            self.client.upsert([{"id": 1, "vector": [1, 2]}])  # wrong dimension
        self.assertEqual(ctx.exception.status, 400)
        with self.assertRaises(GliderError) as ctx:
            self.client.query([1, 2, 3], k=0)
        self.assertEqual(ctx.exception.status, 400)

    def test_resending_a_request_id_applies_the_write_once(self):
        boundary = self.client.status()["sequence"]
        rid = {"boundary": boundary, "nonce": secrets.token_hex(16)}
        body = {"upsert": [{"id": 2001, "vector": [2, 2, 2], "metadata": {}}], "delete": [], "request_id": rid}
        first = self.client._request("POST", "/v1/write", body)
        after_first = self.client.status()["sequence"]
        second = self.client._request("POST", "/v1/write", body)
        self.assertEqual(first["sequence"], second["sequence"])
        self.assertEqual(self.client.status()["sequence"], after_first)
        state = self.client._request("GET", f"/v1/requests/{boundary}/{rid['nonce']}")
        self.assertEqual(state["state"], "retained")
        self.assertEqual(state["outcome"]["sequence"], first["sequence"])
        # A different body under the same ID is a conflict, not retried.
        body2 = dict(body, delete=[1])
        with self.assertRaises(GliderError) as ctx:
            self.client._request("POST", "/v1/write", body2)
        self.assertEqual(ctx.exception.status, 409)

    def test_upsert_many(self):
        points = [
            {"id": 10_000 + i, "vector": [100 + i, 100, 100], "metadata": {"batch": "many"}}
            for i in range(250)
        ]
        before = self.client.status()["sequence"]
        self.assertEqual(self.client.upsert_many(points), 250)
        self.assertEqual(self.client.status()["sequence"], before + 3)
        self.assertEqual(self.client.get(10_249).vector, [349.0, 100.0, 100.0])

    def test_get_many(self):
        self.need(NEW_GET)
        self.client.upsert(
            [{"id": 3001, "vector": [1, 0, 0], "metadata": {"a": "b"}}, {"id": 3003, "vector": [0, 1, 0]}]
        )
        found = self.client.get_many([3003, 3002, 3001])
        self.assertEqual([p.id if p else None for p in found], [3003, None, 3001])
        self.assertEqual(found[2].metadata, {"a": "b"})
        self.assertEqual(found[2].vector, [1.0, 0.0, 0.0])
        light = self.client.get_many([3001], include_vector=False)
        self.assertIsNone(light[0].vector)

    def test_scan_count_and_delete_by_filter(self):
        self.need(NEW_SCAN)
        group = secrets.token_hex(4)
        ids = [20_000 + i for i in range(230)]
        self.client.upsert_many(
            [{"id": i, "vector": [1, 2, 3], "metadata": {"group": group, "parity": str(i % 2)}} for i in ids]
        )
        self.client.upsert([{"id": 19_999, "vector": [1, 2, 3], "metadata": {"group": "other-" + group}}])
        self.assertEqual(self.client.count({"group": group}), 230)
        self.assertEqual(self.client.count({"group": group, "parity": "0"}), 115)
        self.assertEqual(list(self.client.scan({"group": group}, page_size=50)), ids)
        with_meta = list(self.client.scan({"group": group}, include_metadata=True, page_size=100))
        self.assertEqual([p.id for p in with_meta], ids)
        self.assertEqual(with_meta[0].metadata["group"], group)
        self.assertEqual(self.client.delete_by_filter({"group": group, "parity": "1"}), 115)
        self.assertEqual(self.client.count({"group": group}), 115)
        self.assertEqual(self.client.delete_by_filter({"group": group}), 115)
        self.assertEqual(self.client.count({"group": group}), 0)
        self.assertEqual(self.client.count({"group": "other-" + group}), 1)
        self.assertEqual(self.client.delete_by_filter({"group": group}), 0)

    def test_exact_filtered_query_returns_all_matches(self):
        self.need(NEW_EXACT)
        group = secrets.token_hex(4)
        self.client.upsert_many(
            [
                {"id": 40_000 + i, "vector": [i, i % 7, 1], "metadata": {"group": group}}
                for i in range(150)
            ]
        )
        hits = self.client.query([3, 3, 1], k=200, filter={"group": group}, exact=True)
        self.assertEqual(len(hits), 150)
        self.assertEqual(hits[0].id, 40_003)
        distances = [h.distance for h in hits]
        self.assertEqual(distances, sorted(distances))
        self.assertEqual(len(self.client.query([3, 3, 1], k=10, filter={"group": group}, exact=True)), 10)


@requires_server
class CollectionIntegrationTests(unittest.TestCase):
    """A server started without GLIDER_DIMENSIONS serves many collections."""

    @classmethod
    def setUpClass(cls):
        cls.server = RealServer()
        cls.client = Client(cls.server.url, timeout=10)
        try:
            cls.client.list_collections()
        except GliderError:
            cls.server.stop()
            raise unittest.SkipTest("server lacks collections")

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def test_lifecycle_isolation_and_data_calls(self):
        c = self.client
        self.assertEqual(c.list_collections(), [])
        self.assertIsNone(c.get_collection("docs"))
        created = c.create_collection("docs", 3)
        self.assertEqual((created["name"], created["dimensions"], created["metric"]),
                         ("docs", 3, "squared_euclidean"))
        self.assertEqual(c.create_collection("docs", 3)["name"], "docs")  # idempotent
        with self.assertRaises(GliderError) as ctx:
            c.create_collection("docs", 4)
        self.assertEqual(ctx.exception.status, 409)
        c.create_collection("notes", 2, metric="cosine")
        self.assertEqual([d["name"] for d in c.list_collections()], ["docs", "notes"])
        self.assertEqual(c.get_collection("notes")["metric"], "cosine")
        self.assertIn("sequence", c.get_collection("docs")["status"])

        docs = c.collection("docs")
        notes = Client(self.server.url, timeout=10, collection="notes")
        before = docs.status()["sequence"]
        seq = docs.upsert_many(
            [{"id": i, "vector": [i, 0, 0], "metadata": {"parity": str(i % 2)}} for i in range(1, 251)]
        )
        self.assertEqual(seq, 250)
        self.assertGreater(docs.status()["sequence"], before)
        notes.upsert([{"id": 1, "vector": [1, 0]}])
        self.assertEqual(docs.count(), 250)
        self.assertEqual(notes.count(), 1)  # isolated from "docs"

        hits = docs.query([10.2, 0, 0], k=2, include_metadata=True)
        self.assertEqual([h.id for h in hits], [10, 11])
        self.assertEqual(hits[0].metadata, {"parity": "0"})
        self.assertEqual(docs.get(7).vector, [7.0, 0.0, 0.0])
        self.assertIsNone(docs.get(9999))
        self.assertEqual([p.id for p in docs.get_many([3, 9999, 1])[:1]], [3])
        self.assertEqual(len(docs.query([1, 0, 0], k=300, filter={"parity": "1"}, exact=True)), 125)
        self.assertEqual(list(docs.scan({"parity": "0"}, page_size=40))[:3], [2, 4, 6])
        self.assertEqual(docs.delete_by_filter({"parity": "1"}), 125)
        self.assertEqual(docs.count(), 125)
        self.assertEqual(docs.delete([2, 123456]) > 0, True)
        self.assertEqual(docs.count(), 124)

        # Wrong dimension is rejected per collection.
        with self.assertRaises(GliderError) as ctx:
            notes.upsert([{"id": 2, "vector": [1, 2, 3]}])
        self.assertEqual(ctx.exception.status, 400)

        self.assertTrue(c.delete_collection("docs"))
        self.assertFalse(c.delete_collection("docs"))
        self.assertIsNone(c.get_collection("docs"))
        with self.assertRaises(GliderError) as ctx:
            docs.status()
        self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(notes.count(), 1)  # other collections unaffected
        # Re-creating the name starts empty.
        c.create_collection("docs", 3)
        self.assertEqual(docs.count(), 0)
        self.assertTrue(c.delete_collection("docs"))
        self.assertTrue(c.delete_collection("notes"))
        self.assertEqual(c.list_collections(), [])

    def test_unprefixed_data_routes_are_404(self):
        with self.assertRaises(GliderError) as ctx:
            self.client.status()
        self.assertEqual(ctx.exception.status, 404)
        self.assertTrue(self.client.health())  # /healthz stays global


@requires_server
class AuthTests(unittest.TestCase):
    def test_bearer_token(self):
        token = secrets.token_hex(16)
        server = RealServer(dimensions=3, token=token)
        self.addCleanup(server.stop)
        with self.assertRaises(GliderError) as ctx:
            Client(server.url).status()
        self.assertEqual(ctx.exception.status, 401)
        self.assertTrue(Client(server.url).health())  # /healthz needs no token
        client = Client(server.url, token=token)
        client.upsert([{"id": 1, "vector": [1, 2, 3]}])
        self.assertEqual(client.get(1).vector, [1.0, 2.0, 3.0])


if __name__ == "__main__":
    unittest.main()
