# glider-client

Python client for the [Glider](https://github.com/omerfeyzioglu/glider)
vector database HTTP API, plus an [MCP](https://modelcontextprotocol.io)
server that gives AI agents durable memory backed by Glider.

The client uses only the Python standard library (Python 3.9 or newer). The
MCP server needs Python 3.10 or newer and the `mcp` extra.

## Install

```sh
pip install "git+https://github.com/omerfeyzioglu/glider#subdirectory=clients/python"
```

## Quickstart

Start a server (data is lost when the container stops; see the
[server README](../../README.md) for S3 and persistent storage):

```sh
docker run --rm -p 8080:8080 -e GLIDER_DIMENSIONS=3 \
  -e GLIDER_DATA_DIR=/var/lib/glider/data ghcr.io/omerfeyzioglu/glider:1.1.0
```

```python
from glider_client import Client

client = Client("http://localhost:8080")        # token="..." if the server requires one

client.upsert([
    {"id": 1, "vector": [0, 0, 0], "metadata": {"color": "red"}},
    {"id": 2, "vector": [1, 1, 1], "metadata": {"color": "blue"}},
])
for hit in client.query([1, 1, 0.9], k=2, include_metadata=True):
    print(hit.id, hit.distance, hit.metadata)

client.get(1)                                    # Point, or None if absent
client.delete([1])
```

`query(..., exact=True)`, `get_many`, `scan`, `count` and `delete_by_filter`
need server endpoints that are newer than the 1.0.1 image (`exact` on
`/v1/query`, `/v1/scan`, `/v1/points/get`); use a server built from a release
that includes them.

## API

`Client(url="http://localhost:8080", token=None, timeout=30, max_retries=5)`

| Method | Purpose |
|---|---|
| `status()`, `health()` | `GET /v1/status`; `GET /healthz` as a bool |
| `write(upsert=(), delete=())` | One atomic batch of at most 100 operations; returns the commit sequence |
| `upsert(points)`, `delete(ids)` | Shorthands for `write` |
| `upsert_many(points, batch_size=100)` | Any number of points in batches; returns the count written. **Not atomic across batches**: after an error, earlier batches stay committed. Upserts are idempotent, so repeat the call. |
| `query(vector, k=10, filter=None, exact=False, include_metadata=False, include_vector=False)` | Nearest neighbors as `Hit` objects. `filter` is an equality conjunction on metadata. Unfiltered and most filtered queries are approximate; `exact=True` searches exhaustively and, with a filter, returns `min(k, matches)`. |
| `get(id)` | `Point`, or `None` when absent |
| `get_many(ids, include_vector=True, include_metadata=True)` | List aligned with `ids`; `None` for absent points. Chunks of 1000. |
| `scan(filter=None, include_metadata=False, page_size=1000)` | Generator over matching IDs (or `Point`s with metadata) in ascending ID order, following the server's cursor |
| `count(filter=None)` | Number of matching points |
| `delete_by_filter(filter)` | Scans matching IDs, deletes them in batches of 100, returns how many. **Not atomic**; points written concurrently may match later and are not deleted. An empty filter is refused. |

Points are `Point(id, vector, metadata)` objects or dicts with those keys.
Query results are `Hit` (the same class as `Point`) with `distance` set;
fields the server did not return are `None`. Errors raise `GliderError` with
`.status` (HTTP status, `None` for connection failures) and `.message`.

## Retry semantics

Every write carries a client-generated request ID (`boundary` from the
current `/v1/status` sequence, 16 random bytes as `nonce`), as described in
[Request IDs and retries](../../docs/API.md#request-ids-and-retries). The
same ID is used for all attempts of one call, so a write is applied at most
once however often it is resent.

- `429` (queue full, nothing committed): resend the same request after
  exponential backoff with jitter.
- `502`/`503`/`504` or a timeout or connection error (outcome uncertain): the
  client first asks `GET /v1/requests/{boundary}/{nonce}`. `retained` means
  it committed and the original sequence is returned without resending;
  `unknown` means it did not, and the same request is resent. `expired` or
  `ahead` raise `GliderError` with `.request_id` set; read the affected
  points to decide.
- `400`, `401`, `404`, `409`, `422`, `500` and other statuses raise at once.
- After `max_retries` retries a `GliderError` is raised; for an uncertain
  write its `.request_id` can be resolved later while it is within 128
  commits.

Reads are retried on `429`, `502`-`504` and connection errors.

## MCP memory for agents

`glider-mcp` is an MCP server (stdio) that lets an agent store and search
memories. Text is embedded locally with
[fastembed](https://github.com/qdrant/fastembed) (default
`BAAI/bge-small-en-v1.5`, 384 dimensions); vectors and text live in Glider.

1. Run Glider with the model's dimension and the cosine metric. Dimension
   and metric are fixed when the collection is created. The volume keeps the
   memories across container restarts (S3 works too, see the server README):

   ```sh
   docker run -d --name glider-memory -p 8080:8080 \
     -v glider-memory:/var/lib/glider \
     -e GLIDER_DIMENSIONS=384 -e GLIDER_METRIC=cosine \
     -e GLIDER_DATA_DIR=/var/lib/glider/data \
     ghcr.io/omerfeyzioglu/glider:1.1.0
   ```

2. Install the extra:

   ```sh
   pip install "glider-client[mcp] @ git+https://github.com/omerfeyzioglu/glider#subdirectory=clients/python"
   ```

3. Register it. With Claude Code:

   ```sh
   claude mcp add glider -- glider-mcp
   ```

   With Claude Desktop, add to `claude_desktop_config.json`:

   ```json
   {
     "mcpServers": {
       "glider": {
         "command": "glider-mcp",
         "env": {"GLIDER_URL": "http://localhost:8080"}
       }
     }
   }
   ```

Environment: `GLIDER_URL` (default `http://localhost:8080`),
`GLIDER_API_TOKEN` (bearer token, if the server has one) and
`GLIDER_EMBED_MODEL` (any fastembed text model; the server's
`GLIDER_DIMENSIONS` must equal its dimension). The model is downloaded on
first use, which can take a while, and a dimension mismatch is reported as
an error on the first tool call.

Tools:

| Tool | Behavior |
|---|---|
| `remember(text, tags=None, scope="default")` | Stores `text` and returns its ID (a string). The ID is derived from scope and text, so remembering the same text again replaces the entry instead of duplicating it. `tags` are string labels; the keys `text` and `scope` are reserved. |
| `recall(query, k=5, scope="default", tags=None)` | Returns up to `k` memories of the scope (that carry all given tags) as `{id, text, score, tags}`. `score` is `1 - distance`, i.e. cosine similarity on a cosine collection. Uses exact search, so cost grows with the number of memories in the collection. |
| `forget(id=None, scope=None, tags=None)` | Deletes one memory by ID, or every memory matching `scope` and/or `tags`; returns how many. Refuses to run without any argument. |
| `memory_count(scope="default")` | Number of memories in the scope. |

Durability: a memory is acknowledged only after it is durable in Glider's
storage (S3 or the local data directory), so memories survive restarts of
the agent, the MCP server and Glider. Writes carry request IDs, so a retry
after a lost response does not apply a write twice.

## Tests

```sh
python3 -m unittest discover -s clients/python/tests
```

Unit tests use a fake HTTP server. Integration tests run a real server and
are skipped unless `GLIDER_SERVER_BIN` points to a `glider-server` binary
(`cargo build --release --features server --bin glider-server`); those that
need `exact`, `/v1/scan` or `/v1/points/get` skip themselves on older
binaries. The MCP tests need `pip install mcp`.
