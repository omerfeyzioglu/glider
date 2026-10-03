"""MCP server giving AI agents durable memory backed by Glider.

Run with ``glider-mcp`` (stdio transport). Configuration by environment:

* ``GLIDER_URL``: server address, default ``http://localhost:8080``
* ``GLIDER_API_TOKEN``: bearer token, if the server requires one
* ``GLIDER_EMBED_MODEL``: fastembed model, default ``BAAI/bge-small-en-v1.5``
  (384 dimensions)
* ``GLIDER_COLLECTION``: collection name. Set it when the server runs in
  multi-collection mode (started without ``GLIDER_DIMENSIONS``): the collection
  is created on first use with the model's dimension and the cosine metric.
  Unset, the server must be a single-collection server started with the
  model's dimension and, for sensible scores, the cosine metric:
  ``GLIDER_DIMENSIONS=384 GLIDER_METRIC=cosine``.

``mcp`` and ``fastembed`` are imported lazily, so the :class:`Memory` logic is
usable (and testable) without them.
"""

from __future__ import annotations

import functools
import hashlib
import math
import os
import sys
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Union

from .client import Client, GliderError

DEFAULT_MODEL = "BAAI/bge-small-en-v1.5"
DEFAULT_SCOPE = "default"
RESERVED_KEYS = ("text", "scope")
MAX_RECALL = 1000

Embedder = Callable[[Sequence[str]], List[List[float]]]


class ConfigError(RuntimeError):
    """The Glider server is not configured to match the embedding model."""


class FastEmbedder:
    """Embeds text with fastembed; the model is loaded on first use."""

    def __init__(self, model_name: str = DEFAULT_MODEL) -> None:
        self.model_name = model_name
        self._model: Any = None

    def __call__(self, texts: Sequence[str]) -> List[List[float]]:
        if self._model is None:
            from fastembed import TextEmbedding

            self._model = TextEmbedding(model_name=self.model_name)
        return [vector.tolist() for vector in self._model.embed(list(texts))]


def memory_id(scope: str, text: str) -> int:
    """Deterministic point ID: first 8 bytes of sha256(scope NUL text), top bit cleared."""
    digest = hashlib.sha256(f"{scope}\0{text}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFF_FFFF_FFFF_FFFF


def _check_scope(scope: Any) -> str:
    if not isinstance(scope, str) or not scope:
        raise ValueError("scope must be a non-empty string")
    return scope


def _check_tags(tags: Optional[Mapping[str, Any]]) -> Dict[str, str]:
    checked: Dict[str, str] = {}
    for key, value in (tags or {}).items():
        if not isinstance(key, str) or not key:
            raise ValueError("tag keys must be non-empty strings")
        if key in RESERVED_KEYS:
            raise ValueError(f"tag key {key!r} is reserved")
        if not isinstance(value, str):
            raise ValueError(f"tag {key!r} must have a string value")
        checked[key] = value
    return checked


def _parse_id(value: Union[int, str]) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"id must be an unsigned integer, got {value!r}") from None
    if isinstance(value, bool) or not 0 <= parsed < 2**64:
        raise ValueError(f"id must be an unsigned 64-bit integer, got {value!r}")
    return parsed


class Memory:
    """Memory operations over a Glider collection. Independent of MCP.

    With ``collection``, memories live in that collection of a multi-collection
    server, created on first use if missing; otherwise ``client`` is used as it
    is (a single-collection server, or a client already bound to a collection).
    """

    def __init__(
        self, client: Client, embedder: Embedder, collection: Optional[str] = None
    ) -> None:
        self._collection = collection
        self._admin = client
        self._client = client.collection(collection) if collection else client
        self._embedder = embedder
        self._ready = False

    def _embed(self, text: str) -> List[float]:
        vectors = self._embedder([text])
        if len(vectors) != 1:
            raise RuntimeError("embedder must return one vector per text")
        vector = [float(x) for x in vectors[0]]
        if not vector or not all(math.isfinite(x) for x in vector):
            raise RuntimeError("embedder returned an empty or non-finite vector")
        return vector

    def check(self) -> None:
        """Verify the server accepts vectors of the embedder's dimension.

        Single-collection mode uses a read-only query, so nothing is written.
        With a ``collection`` it is created if missing (cosine metric, the
        embedder's dimension). Called before the first operation; raises
        :class:`ConfigError` on a mismatch.
        """
        if self._ready:
            return
        dimensions = len(self._embed("dimension probe"))
        if self._collection:
            self._ensure_collection(dimensions)
            self._ready = True
            return
        probe = [1.0] + [0.0] * (dimensions - 1)
        try:
            self._client.query(probe, k=1)
        except GliderError as exc:
            if exc.status == 400:
                raise ConfigError(
                    f"glider-server rejected a {dimensions}-dimensional vector "
                    f"({exc.message}). Start it with GLIDER_DIMENSIONS={dimensions} "
                    "GLIDER_METRIC=cosine (dimension and metric are fixed when "
                    "the collection is created)."
                ) from exc
            raise
        self._ready = True

    def _ensure_collection(self, dimensions: int) -> None:
        name = self._collection
        try:
            self._admin.create_collection(name, dimensions, metric="cosine")
        except GliderError as exc:
            if exc.status == 409:
                raise ConfigError(
                    f"collection {name!r} already exists with a different "
                    f"configuration ({exc.message}). Memory needs {dimensions} "
                    "dimensions and the cosine metric (dimension and metric are "
                    "fixed at creation): set GLIDER_COLLECTION to another name, "
                    "or delete the collection if it is not needed."
                ) from exc
            if exc.status == 404:
                raise ConfigError(
                    f"glider-server has no /v1/collections ({exc.message}). "
                    "GLIDER_COLLECTION needs a server in multi-collection mode "
                    "(started without GLIDER_DIMENSIONS); unset GLIDER_COLLECTION "
                    "for a single-collection server."
                ) from exc
            raise

    def remember(
        self,
        text: str,
        tags: Optional[Mapping[str, str]] = None,
        scope: str = DEFAULT_SCOPE,
    ) -> str:
        """Store ``text``; return its ID. The same scope and text always map to
        the same ID, so remembering again replaces the earlier entry (and its tags)."""
        if not isinstance(text, str) or not text.strip():
            raise ValueError("text must be a non-empty string")
        _check_scope(scope)
        extra = _check_tags(tags)
        self.check()
        point_id = memory_id(scope, text)
        metadata = {**extra, "text": text, "scope": scope}
        self._client.upsert(
            [{"id": point_id, "vector": self._embed(text), "metadata": metadata}]
        )
        return str(point_id)

    def recall(
        self,
        query: str,
        k: int = 5,
        scope: str = DEFAULT_SCOPE,
        tags: Optional[Mapping[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """Return up to ``k`` memories in ``scope`` nearest to ``query``.

        ``score`` is ``1 - distance``: cosine similarity on a cosine collection.
        """
        if not isinstance(query, str) or not query.strip():
            raise ValueError("query must be a non-empty string")
        if not isinstance(k, int) or isinstance(k, bool) or not 1 <= k <= MAX_RECALL:
            raise ValueError(f"k must be an integer between 1 and {MAX_RECALL}")
        _check_scope(scope)
        flt = {**_check_tags(tags), "scope": scope}
        self.check()
        hits = self._client.query(
            self._embed(query), k=k, filter=flt, exact=True, include_metadata=True
        )
        results = []
        for hit in hits:
            metadata = dict(hit.metadata or {})
            text = metadata.pop("text", "")
            metadata.pop("scope", None)
            results.append(
                {
                    "id": str(hit.id),
                    "text": text,
                    "score": 1.0 - hit.distance,
                    "tags": metadata,
                }
            )
        return results

    def forget(
        self,
        id: Union[int, str, None] = None,
        scope: Optional[str] = None,
        tags: Optional[Mapping[str, str]] = None,
    ) -> int:
        """Delete memories; return how many were deleted.

        With ``id``, delete that memory (only if it also matches ``scope`` and
        ``tags`` when given). Without ``id``, delete every memory matching
        ``scope`` and ``tags``. At least one of the three is required.
        """
        if scope is not None:
            _check_scope(scope)
        extra = _check_tags(tags)
        if id is None and scope is None and not extra:
            raise ValueError("refusing to forget everything: pass id, scope or tags")
        self.check()
        if id is not None:
            point_id = _parse_id(id)
            point = self._client.get(point_id)
            if point is None:
                return 0
            metadata = point.metadata or {}
            wanted = dict(extra)
            if scope is not None:
                wanted["scope"] = scope
            if any(metadata.get(key) != value for key, value in wanted.items()):
                return 0
            self._client.delete([point_id])
            return 1
        flt = dict(extra)
        if scope is not None:
            flt["scope"] = scope
        return self._client.delete_by_filter(flt)

    def memory_count(self, scope: str = DEFAULT_SCOPE) -> int:
        """Number of memories stored in ``scope``."""
        _check_scope(scope)
        self.check()
        return self._client.count({"scope": scope})


def build_server(memory: Memory) -> Any:
    """Create the FastMCP server exposing ``memory`` as tools."""
    try:  # mcp >= 2 renamed FastMCP to MCPServer; the API used here is the same
        from mcp.server.mcpserver import MCPServer as FastMCP
        from mcp.server.mcpserver.exceptions import ToolError
    except ImportError:
        from mcp.server.fastmcp import FastMCP
        from mcp.server.fastmcp.exceptions import ToolError

    def reported(function: Callable[..., Any]) -> Callable[..., Any]:
        """Show expected failures to the agent verbatim; mcp >= 2 hides other exceptions."""

        @functools.wraps(function)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return function(*args, **kwargs)
            except (ValueError, ConfigError, GliderError) as exc:
                raise ToolError(str(exc)) from exc

        return wrapper

    server = FastMCP(
        "glider-memory",
        instructions=(
            "Durable long-term memory. Use remember to store facts worth keeping "
            "across sessions, recall to search them by meaning, forget to delete."
        ),
    )

    @server.tool()
    @reported
    def remember(
        text: str, tags: Optional[Dict[str, str]] = None, scope: str = DEFAULT_SCOPE
    ) -> str:
        """Store a memory. Returns its id (a string). Remembering identical text
        in the same scope again replaces the earlier entry instead of duplicating it.
        Tags are optional string key/value labels usable as filters; the keys
        'text' and 'scope' are reserved."""
        return memory.remember(text, tags, scope)

    @server.tool()
    @reported
    def recall(
        query: str,
        k: int = 5,
        scope: str = DEFAULT_SCOPE,
        tags: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """Search memories in a scope by meaning. Returns up to k entries with
        id, text, score (higher is more similar) and tags, best first. If tags
        are given, only memories carrying all of them are searched."""
        return memory.recall(query, k, scope, tags)

    @server.tool()
    @reported
    def forget(
        id: Union[int, str, None] = None,
        scope: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> int:
        """Delete memories; returns how many were deleted. Pass an id to delete
        one memory, or a scope and/or tags to delete every matching memory.
        At least one argument is required; deleting everything is refused."""
        return memory.forget(id, scope, tags)

    @server.tool()
    @reported
    def memory_count(scope: str = DEFAULT_SCOPE) -> int:
        """Number of memories stored in a scope."""
        return memory.memory_count(scope)

    return server


def main() -> None:
    """Entry point of the ``glider-mcp`` console script."""
    try:
        import mcp  # noqa: F401
        import fastembed  # noqa: F401
    except ImportError as exc:
        sys.exit(
            f"glider-mcp needs the 'mcp' extra ({exc}); install with: "
            'pip install "glider-client[mcp]"'
        )
    client = Client(
        url=os.environ.get("GLIDER_URL", "http://localhost:8080"),
        token=os.environ.get("GLIDER_API_TOKEN") or None,
    )
    collection = os.environ.get("GLIDER_COLLECTION") or None
    if collection is not None:
        try:
            client.collection(collection)
        except ValueError as exc:
            sys.exit(f"GLIDER_COLLECTION: {exc}")
    embedder = FastEmbedder(os.environ.get("GLIDER_EMBED_MODEL", DEFAULT_MODEL))
    # stdout carries the MCP protocol; diagnostics go to stderr only.
    build_server(Memory(client, embedder, collection)).run(transport="stdio")


if __name__ == "__main__":
    main()
