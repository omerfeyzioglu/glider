import unittest
import support  # noqa: F401
from support import FakeServer, fast_client
from glider_client import GliderError


class EmbeddingTests(unittest.TestCase):
    def setUp(self):
        self.server = FakeServer()
        self.addCleanup(self.server.close)
        self.client = fast_client(self.server.url, token="server-token")

    def test_embed_is_global_even_on_bound_client(self):
        result = {"model": "fake", "dimensions": 2, "vectors": [[1, 0], [0, 1]]}
        self.server.script("POST", "/v1/embed", (200, result))
        self.assertEqual(self.client.collection("docs").embed(["cat", "dog"]), result)
        call, = self.server.calls("POST", "/v1/embed")
        self.assertEqual(call[3], {"input": ["cat", "dog"], "kind": "document"})
        self.assertEqual(call[2]["Authorization"], "Bearer server-token")

    def test_query_kind_and_disabled_error(self):
        self.server.script("POST", "/v1/embed", (400, {"error": "embedding disabled"}))
        with self.assertRaises(GliderError) as ctx:
            self.client.embed(["cat"], kind="query")
        self.assertEqual(ctx.exception.status, 400)
        self.assertEqual(self.server.calls("POST", "/v1/embed")[0][3]["kind"], "query")

    def test_text_query_preserves_options(self):
        self.server.script("POST", "/v1/collections/docs/query", (200, {"results": [{"id": 1, "distance": 0, "metadata": {"text": "cat"}}]}))
        hits = self.client.collection("docs").query(text="cat", k=2, filter={"lang": "en"}, exact=True, include_metadata=True)
        self.assertEqual(hits[0].metadata["text"], "cat")
        self.assertEqual(self.server.calls("POST", "/v1/collections/docs/query")[0][3], {
            "text": "cat", "k": 2, "filter": {"lang": "en"}, "exact": True,
            "include_metadata": True, "include_vector": False,
        })

    def test_validation_before_network(self):
        for kwargs in ({}, {"vector": [1, 0], "text": "cat"}, {"text": " "}, {"text": 3}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.client.query(**kwargs)
        for texts, kind in [("cat", "document"), ([], "document"), (["cat"] * 65, "document"), ([" "], "document"), ([3], "document"), (["é" * 16385], "document"), (["a" * 32768] * 9, "document"), (["cat"], "other")]:
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                self.client.embed(texts, kind)
        self.assertEqual(self.server.requests, [])


class EmbeddingPayloadTests(unittest.TestCase):
    """Payload and validation checks without a listening socket."""
    def setUp(self):
        from unittest.mock import Mock
        from glider_client import Client
        self.client = Client().collection("docs")
        self.client._read = Mock(return_value={"results": [], "vectors": [[1, 0]]})

    def test_server_wide_embed(self):
        self.client.embed(["cat"], "query")
        self.client._read.assert_called_once_with("POST", "/v1/embed", {"input": ["cat"], "kind": "query"})

    def test_collection_text_query(self):
        self.assertEqual(self.client.query(text="cat", exact=True), [])
        self.client._read.assert_called_once_with("POST", "/v1/collections/docs/query", {
            "text": "cat", "k": 10, "exact": True, "include_metadata": False, "include_vector": False,
        })

    def test_invalid_input_never_sends(self):
        for kwargs in ({}, {"vector": [1, 0], "text": "cat"}, {"text": " "}, {"text": 3}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.client.query(**kwargs)
        for texts, kind in [("cat", "document"), ([], "document"), (["cat"] * 65, "document"), ([" "], "document"), ([3], "document"), (["é" * 16385], "document"), (["a" * 32768] * 9, "document"), (["cat"], "other")]:
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                self.client.embed(texts, kind)
        self.client._read.assert_not_called()
