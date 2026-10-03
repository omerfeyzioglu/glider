import unittest

import support  # noqa: F401  (sets up sys.path)
from support import DROP, FakeServer, fast_client

from glider_client import Client, GliderError

P = "/v1/collections/mem"


class CollectionClientTests(unittest.TestCase):
    def setUp(self):
        self.server = FakeServer()
        self.addCleanup(self.server.close)
        self.client = fast_client(self.server.url, max_retries=2, collection="mem")

    def paths(self):
        return [(r[0], r[1]) for r in self.server.requests]

    def test_every_data_call_uses_the_collection_prefix(self):
        s = self.server
        s.script("GET", f"{P}/status", (200, {"sequence": 7}))
        s.script("POST", f"{P}/write", (200, {"sequence": 8}))
        s.script("POST", f"{P}/query", (200, {"results": [], "sequence": 8}))
        s.script("GET", f"{P}/points/5", (200, {"id": 5, "vector": [1.0], "metadata": {}}))
        s.script("POST", f"{P}/points/get", (200, {"points": [], "sequence": 8}))
        s.script("POST", f"{P}/scan", (200, {"ids": [], "next": None, "matched": 0, "sequence": 8}))
        c = self.client
        self.assertEqual(c.status()["sequence"], 7)
        self.assertEqual(c.upsert([{"id": 5, "vector": [1.0]}]), 8)
        c.query([1.0])
        c.get(5)
        c.get_many([5])
        list(c.scan())
        c.count()
        self.assertEqual(
            sorted(set(self.paths())),
            sorted(
                {
                    ("GET", f"{P}/status"),
                    ("POST", f"{P}/write"),
                    ("POST", f"{P}/query"),
                    ("GET", f"{P}/points/5"),
                    ("POST", f"{P}/points/get"),
                    ("POST", f"{P}/scan"),
                }
            ),
        )

    def test_write_boundary_and_request_resolution_use_the_collection(self):
        s = self.server
        s.script("GET", f"{P}/status", (200, {"sequence": 7}))
        s.script("POST", f"{P}/write", DROP)
        original = s._scripts

        class Dict(dict):
            def get(self_, key, default=None):
                if key[0] == "GET" and key[1].startswith(f"{P}/requests/"):
                    return [(200, {"state": "retained", "outcome": {"sequence": 11}})]
                return dict.get(self_, key, default)

        s._scripts = Dict(original)
        self.assertEqual(self.client.delete([5]), 11)
        (write,) = s.calls("POST", f"{P}/write")
        rid = write[3]["request_id"]
        self.assertEqual(rid["boundary"], 7)
        self.assertIn(("GET", f"{P}/requests/7/{rid['nonce']}"), self.paths())
        self.assertTrue(all(path.startswith(P) for _, path in self.paths()))

    def test_unbound_client_keeps_single_collection_paths(self):
        self.server.script("GET", "/v1/status", (200, {"sequence": 1}))
        self.assertEqual(fast_client(self.server.url).status()["sequence"], 1)
        self.assertIsNone(fast_client(self.server.url).collection_name)

    def test_collection_returns_bound_copy_sharing_settings(self):
        self.server.script("GET", "/v1/collections/other/status", (200, {"sequence": 2}))
        base = Client(self.server.url, token="t", timeout=3, max_retries=9)
        bound = base.collection("other")
        self.assertIsNot(bound, base)
        self.assertEqual(bound.collection_name, "other")
        self.assertIsNone(base.collection_name)
        self.assertEqual((bound._token, bound._timeout, bound.max_retries), ("t", 3, 9))
        bound._sleep = lambda seconds: None
        self.assertEqual(bound.status()["sequence"], 2)
        self.assertEqual(
            self.server.requests[-1][2].get("Authorization"), "Bearer t"
        )
        # Rebinding a bound client works too.
        self.assertEqual(bound.collection("third").collection_name, "third")
        self.assertEqual(bound.collection_name, "other")

    def test_base_path_prefix_is_kept(self):
        self.server.script("GET", "/proxy/v1/collections/mem/status", (200, {"sequence": 4}))
        client = fast_client(self.server.url + "/proxy/", collection="mem")
        self.assertEqual(client.status()["sequence"], 4)

    def test_name_validation(self):
        bad = ["", "Upper", "-lead", "has space", "has/slash", "under_score", "a" * 64, "ünï", None, 5]
        for name in bad:
            with self.subTest(name=name):
                if name is not None:  # collection=None means "not bound"
                    with self.assertRaises(ValueError):
                        Client(self.server.url, collection=name)
                with self.assertRaises(ValueError):
                    self.client.collection(name)
                with self.assertRaises(ValueError):
                    self.client.create_collection(name, 3)
                with self.assertRaises(ValueError):
                    self.client.get_collection(name)
                with self.assertRaises(ValueError):
                    self.client.delete_collection(name)
        for name in ("a", "0", "memory", "a-b-c", "a" * 63):
            Client(self.server.url, collection=name)
        self.assertEqual(self.server.requests, [])


class CollectionManagementTests(unittest.TestCase):
    def setUp(self):
        self.server = FakeServer()
        self.addCleanup(self.server.close)
        self.client = fast_client(self.server.url, max_retries=2)
        self.description = {
            "name": "mem",
            "dimensions": 3,
            "metric": "cosine",
            "resident_filter": None,
            "routed_keys": [],
            "open": False,
        }

    def test_create_sends_defaults_and_returns_description(self):
        self.server.script("POST", "/v1/collections", (201, self.description))
        self.assertEqual(self.client.create_collection("mem", 3), self.description)
        (body,) = [r[3] for r in self.server.calls("POST", "/v1/collections")]
        self.assertEqual(body, {"name": "mem", "dimensions": 3, "metric": "squared_euclidean"})

    def test_create_with_options_and_existing_collection(self):
        self.server.script("POST", "/v1/collections", (200, self.description))
        got = self.client.create_collection(
            "mem", 3, metric="cosine", resident_filter={"k": "v"}, routed_keys=("tenant",)
        )
        self.assertEqual(got, self.description)
        body = self.server.requests[-1][3]
        self.assertEqual(
            body,
            {
                "name": "mem",
                "dimensions": 3,
                "metric": "cosine",
                "resident_filter": {"k": "v"},
                "routed_keys": ["tenant"],
            },
        )

    def test_create_conflict_raises_409_without_retry(self):
        self.server.script("POST", "/v1/collections", (409, {"error": "collection exists with other config"}))
        with self.assertRaises(GliderError) as ctx:
            self.client.create_collection("mem", 4)
        self.assertEqual(ctx.exception.status, 409)
        self.assertEqual(len(self.server.calls("POST", "/v1/collections")), 1)

    def test_create_validates_dimensions(self):
        for dims in (0, -1, 1.5, "3", True, None):
            with self.subTest(dims=dims), self.assertRaises(ValueError):
                self.client.create_collection("mem", dims)
        self.assertEqual(self.server.requests, [])

    def test_create_retries_when_busy(self):
        self.server.script(
            "POST", "/v1/collections", (429, {"error": "too many open"}), (201, self.description)
        )
        self.assertEqual(self.client.create_collection("mem", 3)["name"], "mem")
        self.assertEqual(len(self.server.calls("POST", "/v1/collections")), 2)

    def test_list_collections(self):
        self.server.script("GET", "/v1/collections", (200, {"collections": [self.description]}))
        self.assertEqual(self.client.list_collections(), [self.description])
        self.server.script("GET", "/v1/collections", (200, {"collections": []}))
        self.assertEqual(self.client.list_collections(), [])

    def test_get_collection_returns_none_when_absent(self):
        self.server.script("GET", "/v1/collections/mem", (200, dict(self.description, status={"sequence": 0})))
        self.server.script("GET", "/v1/collections/gone", (404, {"error": "no collection gone"}))
        self.assertEqual(self.client.get_collection("mem")["status"], {"sequence": 0})
        self.assertIsNone(self.client.get_collection("gone"))
        self.server.script("GET", "/v1/collections/bad", (401, {"error": "unauthorized"}))
        with self.assertRaises(GliderError) as ctx:
            self.client.get_collection("bad")
        self.assertEqual(ctx.exception.status, 401)

    def test_delete_collection(self):
        self.server.script("DELETE", "/v1/collections/mem", (204, None))
        self.server.script("DELETE", "/v1/collections/gone", (404, {"error": "no collection gone"}))
        self.assertIs(self.client.delete_collection("mem"), True)
        self.assertIs(self.client.delete_collection("gone"), False)
        self.server.script("DELETE", "/v1/collections/bad", (500, {"error": "corrupt"}))
        with self.assertRaises(GliderError):
            self.client.delete_collection("bad")

    def test_management_works_on_a_bound_client(self):
        self.server.script("GET", "/v1/collections", (200, {"collections": []}))
        bound = fast_client(self.server.url, collection="mem")
        self.assertEqual(bound.list_collections(), [])


if __name__ == "__main__":
    unittest.main()
