import asyncio
import hashlib
import os
import sys
import unittest

import support
from support import DIMS, DROP, FakeServer, RealServer, fake_embed, fast_client, requires_server, supports

from glider_client import Client, GliderError, Point
from glider_client.mcp_server import ConfigError, Memory, memory_id

try:
    import mcp  # noqa: F401

    HAVE_MCP = True
except ImportError:
    HAVE_MCP = False


class MemoryUnitTests(unittest.TestCase):
    def setUp(self):
        self.server = FakeServer()
        self.addCleanup(self.server.close)
        self.server.script("GET", "/v1/status", (200, {"sequence": 3}))
        self.server.script("POST", "/v1/query", (200, {"results": [], "sequence": 3}))
        self.server.script("POST", "/v1/write", (200, {"sequence": 4}))
        self.memory = Memory(fast_client(self.server.url), fake_embed)

    def writes(self):
        return [r[3] for r in self.server.calls("POST", "/v1/write")]

    def test_id_is_deterministic_63_bit_hash(self):
        digest = hashlib.sha256(b"work\0hello").digest()
        expected = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
        self.assertEqual(memory_id("work", "hello"), expected)
        self.assertLess(memory_id("work", "hello"), 2**63)
        self.assertNotEqual(memory_id("work", "hello"), memory_id("home", "hello"))
        # NUL separator: scope/text boundaries cannot be shifted.
        self.assertNotEqual(memory_id("ab", "c"), memory_id("a", "bc"))

    def test_remember_upserts_embedding_and_metadata(self):
        result = self.memory.remember("the sky is blue", tags={"topic": "color"}, scope="work")
        self.assertEqual(result, str(memory_id("work", "the sky is blue")))
        (body,) = self.writes()
        (point,) = body["upsert"]
        self.assertEqual(point["id"], int(result))
        self.assertEqual(point["vector"], fake_embed(["the sky is blue"])[0])
        self.assertEqual(
            point["metadata"], {"text": "the sky is blue", "scope": "work", "topic": "color"}
        )

    def test_remember_same_text_is_idempotent(self):
        a = self.memory.remember("same", scope="s")
        b = self.memory.remember("same", scope="s")
        self.assertEqual(a, b)
        ids = {w["upsert"][0]["id"] for w in self.writes()}
        self.assertEqual(len(ids), 1)

    def test_remember_validation(self):
        for tags in ({"text": "x"}, {"scope": "x"}, {"k": 1}, {"": "v"}):
            with self.subTest(tags=tags), self.assertRaises(ValueError):
                self.memory.remember("hello", tags=tags)
        for text in ("", "   "):
            with self.assertRaises(ValueError):
                self.memory.remember(text)
        with self.assertRaises(ValueError):
            self.memory.remember("hello", scope="")
        self.assertEqual(self.writes(), [])

    def test_dimension_mismatch_gives_clear_message(self):
        self.server.script("POST", "/v1/query", (400, {"error": "vector has 3 dimensions, expected 384"}))
        with self.assertRaises(ConfigError) as ctx:
            self.memory.remember("hello")
        message = str(ctx.exception)
        self.assertIn(f"GLIDER_DIMENSIONS={DIMS}", message)
        self.assertIn("GLIDER_METRIC=cosine", message)
        self.assertEqual(self.writes(), [])

    def test_check_runs_once_and_is_read_only(self):
        self.memory.remember("a")
        self.memory.remember("b")
        self.assertEqual(len(self.server.calls("POST", "/v1/query")), 1)
        probe = self.server.calls("POST", "/v1/query")[0][3]
        self.assertEqual(len(probe["vector"]), DIMS)

    def test_recall_filters_on_scope_and_is_exact(self):
        self.server.script(
            "POST", "/v1/query",
            (200, {"results": [], "sequence": 3}),
            (
                200,
                {
                    "results": [
                        {"id": 7, "distance": 0.25, "metadata": {"text": "hi", "scope": "work", "t": "1"}},
                        {"id": 9, "distance": 0.5, "metadata": {"text": "yo", "scope": "work"}},
                    ],
                    "sequence": 3,
                },
            ),
        )
        results = self.memory.recall("hi there", k=3, scope="work", tags={"t": "1"})
        self.assertEqual(
            results,
            [
                {"id": "7", "text": "hi", "score": 0.75, "tags": {"t": "1"}},
                {"id": "9", "text": "yo", "score": 0.5, "tags": {}},
            ],
        )
        body = self.server.calls("POST", "/v1/query")[-1][3]
        self.assertEqual(body["filter"], {"scope": "work", "t": "1"})
        self.assertIs(body["exact"], True)
        self.assertEqual(body["k"], 3)
        self.assertIs(body["include_metadata"], True)
        self.assertEqual(body["vector"], fake_embed(["hi there"])[0])

    def test_recall_validation(self):
        for kwargs in ({"query": " "}, {"query": "x", "k": 0}, {"query": "x", "k": 1001},
                       {"query": "x", "tags": {"scope": "other"}}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.memory.recall(**kwargs)

    def test_forget_refuses_without_any_filter(self):
        with self.assertRaises(ValueError):
            self.memory.forget()
        with self.assertRaises(ValueError):
            self.memory.forget(id=None, scope=None, tags={})
        self.assertEqual(self.server.requests, [])

    def test_forget_by_id(self):
        self.server.script(
            "GET", "/v1/points/5", (200, {"id": 5, "vector": [0.0], "metadata": {"text": "x", "scope": "s"}})
        )
        self.server.script("GET", "/v1/points/6", (404, {"error": "no point 6"}))
        self.assertEqual(self.memory.forget(id="5"), 1)
        self.assertEqual(self.writes()[-1]["delete"], [5])
        self.assertEqual(self.memory.forget(id=6), 0)
        self.assertEqual(self.memory.forget(id=5, scope="other"), 0)  # wrong scope: untouched
        self.assertEqual(len(self.writes()), 1)
        with self.assertRaises(ValueError):
            self.memory.forget(id="not a number")

    def test_forget_by_scope_and_tags_uses_delete_by_filter(self):
        self.server.script(
            "POST", "/v1/scan", (200, {"ids": [1, 2, 3], "next": None, "matched": 3, "sequence": 3})
        )
        self.assertEqual(self.memory.forget(scope="s", tags={"k": "v"}), 3)
        self.assertEqual(self.server.calls("POST", "/v1/scan")[0][3]["filter"], {"k": "v", "scope": "s"})
        self.assertEqual(self.writes()[-1]["delete"], [1, 2, 3])

    def test_memory_count(self):
        self.server.script("POST", "/v1/scan", (200, {"ids": [1], "next": 1, "matched": 17, "sequence": 3}))
        self.assertEqual(self.memory.memory_count("work"), 17)
        self.assertEqual(self.server.calls("POST", "/v1/scan")[0][3]["filter"], {"scope": "work"})


class MemoryCollectionTests(unittest.TestCase):
    """GLIDER_COLLECTION mode: the collection is created on first use."""

    P = "/v1/collections/memory"

    def setUp(self):
        self.server = FakeServer()
        self.addCleanup(self.server.close)
        s, p = self.server, self.P
        s.script("GET", f"{p}/status", (200, {"sequence": 3}))
        s.script("POST", f"{p}/query", (200, {"results": [], "sequence": 3}))
        s.script("POST", f"{p}/write", (200, {"sequence": 4}))
        s.script("POST", f"{p}/scan", (200, {"ids": [], "next": None, "matched": 2, "sequence": 3}))
        self.memory = Memory(fast_client(self.server.url), fake_embed, collection="memory")

    def creates(self):
        return [r[3] for r in self.server.calls("POST", "/v1/collections")]

    def test_created_on_first_use_with_embedder_dimension_and_cosine(self):
        self.server.script("POST", "/v1/collections", (201, {"name": "memory"}))
        self.memory.remember("hello world", scope="s")
        self.assertEqual(self.creates(), [{"name": "memory", "dimensions": DIMS, "metric": "cosine"}])
        (write,) = [r[3] for r in self.server.calls("POST", f"{self.P}/write")]
        self.assertEqual(write["upsert"][0]["vector"], fake_embed(["hello world"])[0])
        # Data calls go to the collection only; nothing on the un-prefixed routes.
        self.assertEqual(self.server.calls("POST", "/v1/write"), [])
        self.assertEqual(self.server.calls("POST", "/v1/query"), [])

    def test_existing_collection_with_same_config_is_reused(self):
        self.server.script("POST", "/v1/collections", (200, {"name": "memory"}))
        self.assertEqual(self.memory.memory_count("s"), 2)
        self.assertEqual(len(self.creates()), 1)

    def test_create_runs_once_per_memory(self):
        self.server.script("POST", "/v1/collections", (201, {"name": "memory"}))
        self.memory.remember("a")
        self.memory.remember("b")
        self.memory.recall("a")
        self.assertEqual(len(self.creates()), 1)

    def test_other_config_gives_clear_config_error(self):
        self.server.script("POST", "/v1/collections", (409, {"error": "collection memory exists with dimensions 3"}))
        for call in (
            lambda: self.memory.remember("hello"),
            lambda: self.memory.recall("hello"),
            lambda: self.memory.forget(scope="s"),
            lambda: self.memory.memory_count(),
        ):
            with self.assertRaises(ConfigError) as ctx:
                call()
            message = str(ctx.exception)
            self.assertIn("'memory'", message)
            self.assertIn("different configuration", message)
            self.assertIn(f"{DIMS} dimensions", message)
            self.assertIn("cosine", message)
        self.assertEqual(self.server.calls("POST", f"{self.P}/write"), [])

    def test_single_collection_server_gives_clear_config_error(self):
        self.server.script("POST", "/v1/collections", (404, {"error": "not found"}))
        with self.assertRaises(ConfigError) as ctx:
            self.memory.remember("hello")
        self.assertIn("multi-collection", str(ctx.exception))

    def test_other_errors_propagate_and_are_retried_on_next_use(self):
        from glider_client import GliderError

        self.server.script("POST", "/v1/collections", (500, {"error": "boom"}), (201, {"name": "memory"}))
        with self.assertRaises(GliderError):
            self.memory.remember("hello")
        self.memory.remember("hello")
        self.assertEqual(len(self.creates()), 2)

    def test_without_collection_nothing_is_created(self):
        self.server.script("GET", "/v1/status", (200, {"sequence": 3}))
        self.server.script("POST", "/v1/query", (200, {"results": [], "sequence": 3}))
        self.server.script("POST", "/v1/write", (200, {"sequence": 4}))
        Memory(fast_client(self.server.url), fake_embed).remember("x")
        self.assertEqual(self.creates(), [])


@requires_server
class MemoryIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = RealServer(dimensions=DIMS, metric="cosine")
        cls.client = Client(cls.server.url, timeout=10)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def setUp(self):
        if not supports(self.client, "POST", "/v1/scan", {"limit": 1}) or not supports(
            self.client, "POST", "/v1/query", {"vector": [1] + [0] * (DIMS - 1), "k": 1, "exact": True}
        ):
            self.skipTest("server lacks exact/scan (needs the release with the new endpoints)")
        self.memory = Memory(self.client, fake_embed)

    def test_roundtrip_scopes_tags_and_forget(self):
        m = self.memory
        a = m.remember("alpha beta gamma", tags={"kind": "greek"}, scope="it-1")
        m.remember("alpha beta gamma", tags={"kind": "greek"}, scope="it-1")  # idempotent
        m.remember("delta epsilon", tags={"kind": "greek"}, scope="it-1")
        m.remember("alpha beta gamma", scope="it-2")
        m.remember("zeta eta", tags={"kind": "other"}, scope="it-1")
        self.assertEqual(m.memory_count("it-1"), 3)
        self.assertEqual(m.memory_count("it-2"), 1)
        self.assertEqual(m.memory_count("nobody"), 0)

        hits = m.recall("alpha beta", k=5, scope="it-1")
        self.assertEqual(len(hits), 3)  # exact: filtered recall is complete
        self.assertEqual(hits[0]["id"], a)
        self.assertEqual(hits[0]["text"], "alpha beta gamma")
        self.assertEqual(hits[0]["tags"], {"kind": "greek"})
        self.assertGreater(hits[0]["score"], hits[-1]["score"])
        self.assertEqual(len(m.recall("alpha", k=5, scope="it-1", tags={"kind": "other"})), 1)
        self.assertEqual(len(m.recall("alpha", k=5, scope="it-2")), 1)

        self.assertEqual(m.forget(id=a, scope="it-2"), 0)
        self.assertEqual(m.forget(id=a), 1)
        self.assertEqual(m.forget(id=a), 0)
        self.assertEqual(m.forget(scope="it-1", tags={"kind": "greek"}), 1)
        self.assertEqual(m.memory_count("it-1"), 1)
        self.assertEqual(m.forget(scope="it-1"), 1)
        self.assertEqual(m.memory_count("it-1"), 0)
        self.assertEqual(m.memory_count("it-2"), 1)

    def test_wrong_dimension_reports_config_error(self):
        wrong = Memory(self.client, lambda texts: [[1.0, 2.0, 3.0] for _ in texts])
        with self.assertRaises(ConfigError) as ctx:
            wrong.remember("hello")
        self.assertIn("GLIDER_DIMENSIONS=3", str(ctx.exception))


@requires_server
class MemoryCollectionIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = RealServer()  # multi-collection mode
        cls.client = Client(cls.server.url, timeout=10)

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()

    def setUp(self):
        try:
            self.client.list_collections()
        except GliderError:
            self.skipTest("server lacks collections")

    def test_auto_created_cosine_collection_roundtrip(self):
        memory = Memory(self.client, fake_embed, collection="mcp-memory")
        a = memory.remember("alpha beta gamma", scope="s1")
        memory.remember("delta epsilon", scope="s1")
        memory.remember("alpha beta gamma", scope="s2")
        self.assertEqual(memory.memory_count("s1"), 2)
        hits = memory.recall("alpha beta", k=5, scope="s1")
        self.assertEqual(hits[0]["id"], a)
        self.assertGreater(hits[0]["score"], hits[1]["score"])
        description = self.client.get_collection("mcp-memory")
        self.assertEqual((description["dimensions"], description["metric"]), (DIMS, "cosine"))
        # A second Memory (a restarted MCP server) reuses it and sees the data.
        again = Memory(self.client, fake_embed, collection="mcp-memory")
        self.assertEqual(again.memory_count("s1"), 2)
        self.assertEqual(again.forget(scope="s1"), 2)
        self.assertEqual(again.memory_count("s1"), 0)
        self.assertEqual(again.memory_count("s2"), 1)

    def test_mismatched_existing_collection_is_a_config_error(self):
        self.client.create_collection("mcp-other", 3, metric="cosine")
        with self.assertRaises(ConfigError) as ctx:
            Memory(self.client, fake_embed, collection="mcp-other").remember("hello")
        self.assertIn("different configuration", str(ctx.exception))
        self.client.create_collection("mcp-euclid", DIMS)  # right dimension, wrong metric
        with self.assertRaises(ConfigError):
            Memory(self.client, fake_embed, collection="mcp-euclid").memory_count()


@unittest.skipUnless(HAVE_MCP, "the 'mcp' package is not installed")
class McpServerTests(unittest.TestCase):
    def setUp(self):
        self.server = FakeServer()
        self.addCleanup(self.server.close)
        self.server.script("GET", "/v1/status", (200, {"sequence": 3}))
        self.server.script("POST", "/v1/query", (200, {"results": [], "sequence": 3}))
        self.server.script("POST", "/v1/write", (200, {"sequence": 4}))
        self.server.script("POST", "/v1/scan", (200, {"ids": [1], "next": None, "matched": 5, "sequence": 3}))

    def test_tools_over_stdio_with_collection(self):
        server = self.server
        server.script("POST", "/v1/collections", (201, {"name": "memory"}))
        for method, path, item in (
            ("GET", "/v1/collections/memory/status", (200, {"sequence": 3})),
            ("POST", "/v1/collections/memory/write", (200, {"sequence": 4})),
            ("POST", "/v1/collections/memory/scan", (200, {"ids": [1], "next": None, "matched": 5, "sequence": 3})),
        ):
            server.script(method, path, item)
        self.run_tools({"GLIDER_COLLECTION": "memory"})
        self.assertEqual(
            [r[3] for r in server.calls("POST", "/v1/collections")],
            [{"name": "memory", "dimensions": DIMS, "metric": "cosine"}],
        )
        self.assertEqual(len(server.calls("POST", "/v1/collections/memory/write")), 1)
        self.assertEqual(server.calls("POST", "/v1/write"), [])

    def test_tools_over_stdio(self):
        self.run_tools({})

    def run_tools(self, extra_env):
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        here = os.path.dirname(os.path.abspath(__file__))
        params = StdioServerParameters(
            command=sys.executable,
            args=[os.path.join(here, "stdio_server.py")],
            env={**os.environ, "GLIDER_URL": self.server.url, **extra_env},
        )

        async def run():
            async with stdio_client(params) as streams:
                async with ClientSession(*streams) as session:
                    await session.initialize()
                    tools = await session.list_tools()
                    names = sorted(t.name for t in tools.tools)
                    remembered = await session.call_tool("remember", {"text": "hello world", "scope": "s"})
                    counted = await session.call_tool("memory_count", {"scope": "s"})
                    refused = await session.call_tool("forget", {})
                    reserved = await session.call_tool("remember", {"text": "x", "tags": {"scope": "y"}})
                    return names, remembered, counted, refused, reserved

        names, remembered, counted, refused, reserved = asyncio.run(run())

        def failed(result):  # mcp 1.x spells it isError, 2.x is_error
            return getattr(result, "isError", getattr(result, "is_error", None))

        self.assertEqual(names, ["forget", "memory_count", "recall", "remember"])
        self.assertFalse(failed(remembered))
        self.assertIn(str(memory_id("s", "hello world")), remembered.content[0].text)
        self.assertFalse(failed(counted))
        self.assertIn("5", counted.content[0].text)
        self.assertTrue(failed(refused))
        self.assertIn("refusing", refused.content[0].text)
        self.assertTrue(failed(reserved))
        self.assertIn("reserved", reserved.content[0].text)


if __name__ == "__main__":
    unittest.main()
