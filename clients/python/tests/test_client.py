import json
import unittest

import support  # noqa: F401  (sets up sys.path)
from support import DROP, FakeServer, fast_client

from glider_client import Client, GliderError, Point


class ClientTestCase(unittest.TestCase):
    def setUp(self):
        self.server = FakeServer()
        self.addCleanup(self.server.close)
        self.server.script("GET", "/v1/status", (200, {"sequence": 7}))
        self.client = fast_client(self.server.url, max_retries=3)

    def writes(self):
        return self.server.calls("POST", "/v1/write")

    def resolves(self):
        return [r for r in self.server.requests if r[1].startswith("/v1/requests/")]


class WriteRetryTests(ClientTestCase):
    def test_success_sends_request_id_from_status_boundary(self):
        self.server.script("POST", "/v1/write", (200, {"sequence": 8}))
        self.assertEqual(self.client.upsert([{"id": 1, "vector": [0, 1, 2]}]), 8)
        (_, _, _, body), = self.writes()
        self.assertEqual(body["request_id"]["boundary"], 7)
        self.assertRegex(body["request_id"]["nonce"], r"^[0-9a-f]{32}$")
        self.assertEqual(body["upsert"], [{"id": 1, "vector": [0.0, 1.0, 2.0], "metadata": {}}])
        self.assertEqual(body["delete"], [])

    def test_accepts_point_dataclass(self):
        self.server.script("POST", "/v1/write", (200, {"sequence": 8}))
        self.client.upsert([Point(id=3, vector=[1, 2, 3], metadata={"a": "b"})])
        self.assertEqual(self.writes()[0][3]["upsert"][0]["metadata"], {"a": "b"})

    def test_explicit_request_id_is_reused_without_status_lookup(self):
        rid = {"boundary": 3, "nonce": "a" * 32}
        self.server.script("POST", "/v1/write", (429, {"error": "busy"}), (200, {"sequence": 4}))
        self.assertEqual(self.client.upsert([{"id": 1, "vector": [1]}], request_id=rid), 4)
        self.assertEqual([w[3]["request_id"] for w in self.writes()], [rid, rid])
        self.assertEqual(self.server.calls("GET", "/v1/status"), [])

    def test_explicit_request_id_is_validated(self):
        for rid in ({"boundary": -1, "nonce": "a" * 32},
                    {"boundary": 1, "nonce": "A" * 32},
                    {"boundary": 1, "nonce": "a" * 32, "extra": 1}):
            with self.subTest(rid=rid), self.assertRaises(ValueError):
                self.client.write(delete=[1], request_id=rid)
        self.assertEqual(self.server.requests, [])

    def test_429_retries_with_same_request_id(self):
        self.server.script(
            "POST", "/v1/write",
            (429, {"error": "queue full"}), (429, {"error": "queue full"}), (200, {"sequence": 9}),
        )
        self.assertEqual(self.client.delete([5]), 9)
        bodies = [w[3] for w in self.writes()]
        self.assertEqual(len(bodies), 3)
        self.assertEqual(bodies[0], bodies[1])
        self.assertEqual(bodies[1], bodies[2])
        self.assertEqual(self.resolves(), [])  # 429 means not committed: no lookup needed

    def test_429_gives_up_after_max_retries(self):
        self.server.script("POST", "/v1/write", (429, {"error": "queue full"}))
        with self.assertRaises(GliderError) as ctx:
            self.client.delete([5])
        self.assertEqual(len(self.writes()), 4)  # first try + 3 retries
        self.assertIsNotNone(ctx.exception.request_id)

    def test_lost_response_retained_does_not_resend(self):
        self.server.script("POST", "/v1/write", DROP)
        self._script_resolve(
            lambda rid: (200, {"state": "retained", "outcome": {"request_id": rid, "sequence": 11}})
        )
        self.assertEqual(self.client.delete([5]), 11)
        self.assertEqual(len(self.writes()), 1)
        rid = self.writes()[0][3]["request_id"]
        self.assertEqual(self.resolves()[0][1], f"/v1/requests/{rid['boundary']}/{rid['nonce']}")

    def _script_resolve(self, make_response):
        """Answer GET /v1/requests/... using the request_id of the first write."""
        server = self.server
        original = server._scripts

        class Dict(dict):
            def get(self_, key, default=None):
                if key[0] == "GET" and key[1].startswith("/v1/requests/"):
                    def respond(body):
                        _, boundary, nonce = key[1].rsplit("/", 2)
                        return make_response({"boundary": int(boundary), "nonce": nonce})
                    return [respond]
                return dict.get(self_, key, default)

        server._scripts = Dict(original)

    def test_lost_response_unknown_resends_same_request_id(self):
        self.server.script("POST", "/v1/write", DROP, (200, {"sequence": 12}))
        self._script_resolve(lambda rid: (200, {"state": "unknown"}))
        self.assertEqual(self.client.delete([5]), 12)
        writes = self.writes()
        self.assertEqual(len(writes), 2)
        self.assertEqual(writes[0][3], writes[1][3])
        self.assertEqual(len(self.resolves()), 1)

    def test_503_resolves_before_resending(self):
        self.server.script(
            "POST", "/v1/write", (503, {"error": "worker stopped"}), (200, {"sequence": 13})
        )
        self._script_resolve(lambda rid: (200, {"state": "unknown"}))
        self.assertEqual(self.client.delete([5]), 13)
        order = [r[1] for r in self.server.requests if r[1] != "/v1/status"]
        self.assertEqual([p.split("/")[2] for p in order], ["write", "requests", "write"])

    def test_503_retained_returns_outcome(self):
        self.server.script("POST", "/v1/write", (503, {"error": "storage error"}))
        self._script_resolve(lambda rid: (200, {"state": "retained", "outcome": {"sequence": 20}}))
        self.assertEqual(self.client.delete([5]), 20)
        self.assertEqual(len(self.writes()), 1)

    def test_resolve_is_retried_when_it_fails(self):
        self.server.script("POST", "/v1/write", DROP)
        answers = iter([DROP, (503, {"error": "starting"}), (200, {"state": "retained", "outcome": {"sequence": 21}})])
        server = self.server
        original = server._scripts

        class Dict(dict):
            def get(self_, key, default=None):
                if key[1].startswith("/v1/requests/"):
                    item = next(answers)
                    return [item]
                return dict.get(self_, key, default)

        server._scripts = Dict(original)
        self.assertEqual(self.client.delete([5]), 21)
        self.assertEqual(len(self.writes()), 1)

    def test_expired_request_raises_with_request_id(self):
        self.server.script("POST", "/v1/write", DROP)
        self._script_resolve(lambda rid: (200, {"state": "expired"}))
        with self.assertRaises(GliderError) as ctx:
            self.client.delete([5])
        self.assertEqual(ctx.exception.request_id["boundary"], 7)
        self.assertEqual(len(self.writes()), 1)

    def test_uncertain_outcome_after_exhausted_retries(self):
        self.server.script("POST", "/v1/write", DROP)
        self._script_resolve(lambda rid: (503, {"error": "down"}))
        with self.assertRaises(GliderError) as ctx:
            self.client.delete([5])
        self.assertIn("uncertain", ctx.exception.message)
        self.assertIsNotNone(ctx.exception.request_id)

    def test_400_raises_without_retry(self):
        self.server.script("POST", "/v1/write", (400, {"error": "wrong dimension"}))
        with self.assertRaises(GliderError) as ctx:
            self.client.upsert([{"id": 1, "vector": [1, 2]}])
        self.assertEqual(ctx.exception.status, 400)
        self.assertEqual(ctx.exception.message, "wrong dimension")
        self.assertEqual(len(self.writes()), 1)
        self.assertEqual(self.resolves(), [])

    def test_other_non_retryable_statuses_raise(self):
        for status in (401, 404, 409, 422):
            with self.subTest(status=status):
                self.server.requests.clear()
                self.server.script("POST", "/v1/write", (status, {"error": "nope"}))
                with self.assertRaises(GliderError) as ctx:
                    self.client.delete([5])
                self.assertEqual(ctx.exception.status, status)
                self.assertEqual(len(self.writes()), 1)

    def test_batch_limits_are_checked_locally(self):
        with self.assertRaises(ValueError):
            self.client.write()
        with self.assertRaises(ValueError):
            self.client.delete(range(101))
        with self.assertRaises(ValueError):
            self.client.upsert([{"id": 1, "vector": [float("nan")]}])
        self.assertEqual(self.server.requests, [])

    def test_bearer_token_and_prefix(self):
        server = FakeServer()
        self.addCleanup(server.close)
        server.script("GET", "/api/v1/status", (200, {"sequence": 1}))
        client = Client(server.url + "/api/", token="secret-token")
        client.status()
        self.assertEqual(server.requests[0][2]["Authorization"], "Bearer secret-token")


class UpsertManyTests(ClientTestCase):
    def test_batches_of_batch_size(self):
        self.server.script("POST", "/v1/write", (200, {"sequence": 8}))
        points = [{"id": i, "vector": [0, 0, 0]} for i in range(250)]
        self.assertEqual(self.client.upsert_many(points, batch_size=100), 250)
        self.assertEqual([len(w[3]["upsert"]) for w in self.writes()], [100, 100, 50])
        ids = [p["id"] for w in self.writes() for p in w[3]["upsert"]]
        self.assertEqual(ids, list(range(250)))
        nonces = {w[3]["request_id"]["nonce"] for w in self.writes()}
        self.assertEqual(len(nonces), 3)

    def test_batches_are_cut_below_request_size_limit(self):
        self.server.script("POST", "/v1/write", (200, {"sequence": 8}))
        big = {"id": 0, "vector": [0.123456789] * 1, "metadata": {"k": "x" * 400_000}}
        points = [dict(big, id=i) for i in range(5)]
        self.client.upsert_many(points)
        for _, _, _, body in self.writes():
            self.assertLess(len(json.dumps(body)), 1024 * 1024)
        self.assertEqual(sum(len(w[3]["upsert"]) for w in self.writes()), 5)
        self.assertGreater(len(self.writes()), 1)

    def test_empty_input_writes_nothing(self):
        self.assertEqual(self.client.upsert_many([]), 0)
        self.assertEqual(self.writes(), [])

    def test_failure_stops_and_reports(self):
        self.server.script("POST", "/v1/write", (200, {"sequence": 8}), (400, {"error": "bad"}))
        points = [{"id": i, "vector": [0, 0, 0]} for i in range(150)]
        with self.assertRaises(GliderError):
            self.client.upsert_many(points)
        self.assertEqual(len(self.writes()), 2)


class ReadTests(ClientTestCase):
    def test_query_body_only_sends_new_fields_when_used(self):
        self.server.script(
            "POST", "/v1/query",
            (200, {"results": [{"id": 4, "distance": 0.5, "metadata": {"a": "b"}}], "sequence": 7}),
        )
        hits = self.client.query([1, 2, 3], k=2, include_metadata=True)
        self.assertEqual(hits[0].id, 4)
        self.assertEqual(hits[0].distance, 0.5)
        self.assertEqual(hits[0].metadata, {"a": "b"})
        self.assertIsNone(hits[0].vector)
        body = self.server.calls("POST", "/v1/query")[0][3]
        self.assertEqual(
            body, {"vector": [1.0, 2.0, 3.0], "k": 2, "include_metadata": True, "include_vector": False}
        )
        self.client.query([1, 2, 3], filter={"c": "d"}, exact=True)
        body = self.server.calls("POST", "/v1/query")[1][3]
        self.assertEqual(body["filter"], {"c": "d"})
        self.assertIs(body["exact"], True)

    def test_reads_retry_transient_errors(self):
        self.server.script(
            "POST", "/v1/query", (429, {"error": "busy"}), DROP, (503, {"error": "x"}),
            (200, {"results": [], "sequence": 7}),
        )
        self.assertEqual(self.client.query([1, 2, 3]), [])
        self.assertEqual(len(self.server.calls("POST", "/v1/query")), 4)

    def test_query_error_not_retried(self):
        self.server.script("POST", "/v1/query", (400, {"error": "k must be between 1 and 1000"}))
        with self.assertRaises(GliderError):
            self.client.query([1, 2, 3], k=0)
        self.assertEqual(len(self.server.calls("POST", "/v1/query")), 1)

    def test_get_returns_none_on_404(self):
        self.server.script("GET", "/v1/points/1", (200, {"id": 1, "vector": [0.0], "metadata": {"a": "b"}}))
        self.server.script("GET", "/v1/points/2", (404, {"error": "no point 2"}))
        self.assertEqual(self.client.get(1), Point(id=1, vector=[0.0], metadata={"a": "b"}))
        self.assertIsNone(self.client.get(2))

    def test_get_many_chunks_and_aligns(self):
        def respond(body):
            ids = body["ids"]
            present = [i for i in ids if i % 2 == 0]
            return 200, {
                "points": [{"id": i, "metadata": {"i": str(i)}} for i in present],
                "missing": [i for i in ids if i % 2],
                "sequence": 7,
            }

        self.server.script("POST", "/v1/points/get", respond)
        ids = list(range(2500))
        result = self.client.get_many(ids, include_vector=False)
        calls = self.server.calls("POST", "/v1/points/get")
        self.assertEqual([len(c[3]["ids"]) for c in calls], [1000, 1000, 500])
        self.assertIs(calls[0][3]["include_vector"], False)
        self.assertEqual(len(result), 2500)
        self.assertEqual(result[4].metadata, {"i": "4"})
        self.assertIsNone(result[5])


class ScanTests(ClientTestCase):
    def scripted_scan(self, total):
        def respond(body):
            after = body.get("after")
            start = 0 if after is None else after + 1
            ids = list(range(start, min(start + body["limit"], total)))
            nxt = ids[-1] if ids and ids[-1] + 1 < total else None
            page = {"next": nxt, "matched": total, "sequence": 7}
            if body["include_metadata"]:
                page["points"] = [{"id": i, "metadata": {"n": str(i)}} for i in ids]
            else:
                page["ids"] = ids
            return 200, page

        self.server.script("POST", "/v1/scan", respond)

    def test_scan_follows_next_cursor(self):
        self.scripted_scan(2500)
        self.assertEqual(list(self.client.scan({"a": "b"})), list(range(2500)))
        calls = self.server.calls("POST", "/v1/scan")
        self.assertEqual([c[3].get("after") for c in calls], [None, 999, 1999])
        self.assertEqual(calls[0][3]["filter"], {"a": "b"})
        self.assertEqual(calls[0][3]["limit"], 1000)

    def test_scan_exact_page_boundary_terminates(self):
        self.scripted_scan(2000)
        self.assertEqual(len(list(self.client.scan())), 2000)
        self.assertEqual(len(self.server.calls("POST", "/v1/scan")), 2)

    def test_scan_with_metadata_and_small_pages(self):
        self.scripted_scan(5)
        points = list(self.client.scan(include_metadata=True, page_size=2))
        self.assertEqual([p.id for p in points], [0, 1, 2, 3, 4])
        self.assertEqual(points[3].metadata, {"n": "3"})
        self.assertEqual(len(self.server.calls("POST", "/v1/scan")), 3)

    def test_scan_empty(self):
        self.server.script("POST", "/v1/scan", (200, {"ids": [], "next": None, "matched": 0, "sequence": 7}))
        self.assertEqual(list(self.client.scan()), [])

    def test_scan_is_lazy(self):
        self.scripted_scan(2500)
        iterator = self.client.scan()
        self.assertEqual(self.server.calls("POST", "/v1/scan"), [])
        next(iterator)
        self.assertEqual(len(self.server.calls("POST", "/v1/scan")), 1)

    def test_page_size_validated(self):
        with self.assertRaises(ValueError):
            list(self.client.scan(page_size=0))
        with self.assertRaises(ValueError):
            list(self.client.scan(page_size=10001))

    def test_count_uses_matched_with_limit_one(self):
        self.scripted_scan(42)
        self.assertEqual(self.client.count({"a": "b"}), 42)
        (_, _, _, body), = self.server.calls("POST", "/v1/scan")
        self.assertEqual(body["limit"], 1)
        self.assertEqual(body["filter"], {"a": "b"})

    def test_delete_by_filter_batches_of_100(self):
        self.scripted_scan(250)
        self.server.script("POST", "/v1/write", (200, {"sequence": 8}))
        self.assertEqual(self.client.delete_by_filter({"a": "b"}), 250)
        sizes = [len(w[3]["delete"]) for w in self.writes()]
        self.assertEqual(sizes, [100, 100, 50])
        deleted = [i for w in self.writes() for i in w[3]["delete"]]
        self.assertEqual(deleted, list(range(250)))
        self.assertTrue(all(w[3]["upsert"] == [] for w in self.writes()))

    def test_delete_by_filter_no_matches_writes_nothing(self):
        self.server.script("POST", "/v1/scan", (200, {"ids": [], "next": None, "matched": 0, "sequence": 7}))
        self.assertEqual(self.client.delete_by_filter({"a": "b"}), 0)
        self.assertEqual(self.writes(), [])

    def test_delete_by_filter_refuses_empty_filter(self):
        for flt in (None, {}):
            with self.assertRaises(ValueError):
                self.client.delete_by_filter(flt)
        self.assertEqual(self.server.requests, [])


class StatusTests(ClientTestCase):
    def test_health(self):
        self.server.script("GET", "/healthz", (200, None))
        self.assertTrue(self.client.health())
        self.server.script("GET", "/healthz", (503, None))
        self.assertFalse(self.client.health())
        self.assertFalse(Client("http://127.0.0.1:1", timeout=1).health())

    def test_rejects_bad_url(self):
        with self.assertRaises(ValueError):
            Client("localhost:8080")


if __name__ == "__main__":
    unittest.main()
