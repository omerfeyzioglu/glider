![Glider](glider.png)

# Glider

**Vector search with S3 as the source of truth.**

[![CI](https://github.com/omerfeyzioglu/glider/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/omerfeyzioglu/glider/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

**[Website and interactive storage simulation](https://glider.oomerfeyzioglu.workers.dev/)**

Glider is a single-node vector database. Every acknowledged write is durable
in S3-compatible object storage before the server answers; local RAM and SSD
only speed up reads and never hold the only copy. It serves nearest-neighbor
search with metadata filters over an HTTP/JSON API, with a Python client and
an MCP memory server for AI agents.

## Quickstart

Start a server with a local directory as storage (data is lost when the
container stops):

```sh
docker run --rm -p 8080:8080 \
  -e GLIDER_DATA_DIR=/var/lib/glider/data ghcr.io/omerfeyzioglu/glider:latest
```

Create a collection, write two points and query them:

```sh
curl -sS localhost:8080/v1/collections -H 'content-type: application/json' \
  -d '{"name":"demo","dimensions":3}'

curl -sS localhost:8080/v1/collections/demo/write -H 'content-type: application/json' \
  -d '{"upsert":[{"id":1,"vector":[0,0,0],"metadata":{"color":"red"}},{"id":2,"vector":[1,1,1]}]}'

curl -sS localhost:8080/v1/collections/demo/query -H 'content-type: application/json' \
  -d '{"vector":[1,1,0.9],"k":2,"include_metadata":true}'
```

The same with the dependency-free Python client:

```python
# pip install "git+https://github.com/omerfeyzioglu/glider#subdirectory=clients/python"
from glider_client import Client

client = Client("http://localhost:8080")
client.create_collection("docs", dimensions=3, metric="cosine")
docs = client.collection("docs")
docs.upsert([{"id": 1, "vector": [0, 0, 1], "metadata": {"lang": "en"}}])
print(docs.query([0, 0, 1], k=1, include_metadata=True))
```

Open <http://localhost:8080/console> to browse collections and run queries
in the browser.

For S3 or MinIO storage, the Docker Compose demo and building from source,
see [installation](docs/INSTALL.md); every setting is listed in
[configuration](docs/CONFIGURATION.md).

### Run on S3

With a bucket and AWS credentials exported in your shell:

```sh
docker run --rm -p 8080:8080 -v glider-cache:/var/lib/glider/cache \
  -e GLIDER_S3_BUCKET=my-bucket -e GLIDER_S3_NAMESPACE=glider \
  -e GLIDER_S3_REGION=eu-central-1 \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN \
  ghcr.io/omerfeyzioglu/glider:latest
```

Nothing else is needed: the bucket is the database; stop the container and start
a new one anywhere with the same settings and access, and the data is there.
See [Run on AWS](docs/INSTALL.md#run-on-aws) for setup and IAM, or
[S3-compatible storage](docs/INSTALL.md#s3-compatible-storage) for MinIO.

## Text search (optional)

Enable server-side embedding to query with `{"text":"a sleepy cat"}` or use
Text mode in the console. Embed documents with `POST /v1/embed`, then write
its vectors with optional `metadata.text`. Stored and acknowledged data stays
vectors. The default local model needs a 384-dimension cosine collection.

```sh
# Local ONNX model; downloaded on first use into the persistent volume.
docker run --rm -p 8080:8080 -v glider-text:/var/lib/glider \
  -e GLIDER_DATA_DIR=/var/lib/glider/data -e GLIDER_EMBED_PROVIDER=local \
  ghcr.io/omerfeyzioglu/glider:latest

# Ollama on the host (pull nomic-embed-text in Ollama first).
docker run --rm -p 8080:8080 -e GLIDER_DATA_DIR=/var/lib/glider/data \
  -e GLIDER_EMBED_PROVIDER=openai -e GLIDER_EMBED_MODEL=nomic-embed-text \
  -e GLIDER_EMBED_URL=http://host.docker.internal:11434/v1 \
  ghcr.io/omerfeyzioglu/glider:latest
```

For OpenAI, set `GLIDER_EMBED_URL=https://api.openai.com/v1`, a model such
as `text-embedding-3-small`, and `GLIDER_EMBED_API_KEY`. Use the response's
dimensions when creating a collection. Embedding is off by default; source
builds opt into `embed-local` and/or `embed-openai`. See
[configuration](docs/CONFIGURATION.md#text-embedding-optional).

## Features

- **Durable on S3.** Each write batch becomes one immutable log object, created
  conditionally before it is acknowledged; nothing is overwritten in place.
- **Crash-safe takeover.** A restarted server waits out the old writer's
  lease, fences it at the object store and replays the log; no operator step
  is needed after a crash.
- **Retry-safe writes.** Every write carries a request ID; resending it
  within the retained 128-commit window returns the original outcome
  instead of applying the write twice. Supply and keep the ID before sending so a lost
  response can be retried ([retry contract](docs/API.md#request-ids-and-retries)).
- **Collections.** One server creates and serves many collections, each with
  its own dimension and metric (squared Euclidean, Manhattan or cosine).
- **Metadata filters.** Equality, set, existence, numeric and logical
  filters; `exact: true` answers any filter exhaustively.
- **Clustered ANN that builds itself.** The server clusters a collection in
  the background at 250,000 rows and rebuilds the clusters as it grows.
- **SSD cache.** A local block cache warms in the background so warm queries
  need no remote reads; losing it loses no data.
- **Operations.** Prometheus metrics, a status endpoint, bearer-token
  authentication, and `glider-admin` for backup and restore.

## Agent memory (MCP)

The Python client includes `glider-mcp`, an MCP server that gives agents
(Claude Code, Claude Desktop, Cursor and other MCP clients) durable
`remember`, `recall` and `forget` tools backed by Glider:

```sh
docker run -d --name glider -p 8080:8080 -v glider-data:/var/lib/glider \
  -e GLIDER_DATA_DIR=/var/lib/glider/data ghcr.io/omerfeyzioglu/glider:latest
pip install "glider-client[mcp] @ git+https://github.com/omerfeyzioglu/glider#subdirectory=clients/python"
claude mcp add glider -e GLIDER_COLLECTION=memory -- glider-mcp
```

The [client guide](clients/python/README.md#mcp-memory-for-agents) covers
other MCP clients and settings.

## Performance

1,000,000 SIFT vectors (128 dimensions, k=10) in AWS S3 Standard, served
from a c7g.2xlarge (8 vCPU, 16 GiB) in eu-central-1 while four writers and
four readers run concurrently:

| Measure | Result |
|---|---:|
| Recall@10, static (mean / p5) | 0.998 / 1.0 |
| Recall@10 after updates, cold cache (mean / p5) | 0.964 / 0.8 |
| Query p95, warm cache | 29.8 ms |
| Query p95, cold cache | 57.0 ms |
| Write p95 | 86.9 ms |
| Open / reopen | 2.93 / 3.84 s |

The run at revision `35f9b46` lost no acknowledged writes and returned equal
results after cache loss and backup restore. Methodology and raw results are in the
[benchmark report](benchmarks/M39.md#current-main-on-s3) and
[BENCHMARKS.md](BENCHMARKS.md).

Many small collections on one server, same instance and S3 Standard:
10,000 collections of 1,000 vectors (10,000,000 vectors) were created and
loaded in 22 minutes with the server killed (`SIGKILL`) halfway; every
tenant was verified with no lost or duplicated write. Warm queries over 64
active collections ran at 2,457 per second with a p95 of 18.5 ms and
recall@10 of 1.0; a cold tenant
opened from S3 and answered its first query in 716 ms (p95). Idle
collections close, so they cost no S3 requests
([details](BENCHMARKS.md#multi-tenant-server-scenario)).

## Architecture

![Glider runtime architecture](docs/architecture/runtime.svg)

- **Write:** a single committer publishes each batch as one conditional
  log object, then acknowledges it.
- **Maintenance:** idle time seals the log into immutable packs and indexes,
  then publishes a new root generation that names them.
- **Read:** queries route to candidate blocks, read bounded byte ranges from
  S3 or the SSD cache, and rerank full vectors.

The [architecture guide](docs/ARCHITECTURE.md) adds an AWS deployment
pattern; [DESIGN.md](DESIGN.md) specifies formats, invariants and recovery.

## Documentation

| Document | Contents |
|---|---|
| [Installation](docs/INSTALL.md) | Docker, Docker Compose with MinIO, building from source |
| [Configuration](docs/CONFIGURATION.md) | Environment variables and serving profile |
| [HTTP API](docs/API.md) | Endpoints, filters, errors, retries and consistency |
| [Python client](clients/python/README.md) | Client API and MCP memory server |
| [Operations](docs/SERVING.md) | Running, backup, restore, cache and clustering |
| [Recovery](docs/RECOVERY.md) | Crash, takeover and restore procedures |
| [Architecture](docs/ARCHITECTURE.md) | Runtime and AWS deployment diagrams |
| [Design](DESIGN.md) | Formats, invariants and guarantees |
| [Rust library](docs/LIBRARY.md) | Embedding the engine in Rust |
| [Benchmarks](BENCHMARKS.md) | Measurements and how to reproduce them |
| [Changelog](CHANGELOG.md) | Release notes |
| [Contributing](CONTRIBUTING.md) | Development workflow and checks |
| [Website](docs/WEBSITE.md) | Previewing the site and regenerating its playground results |
| [Security](SECURITY.md) | Reporting vulnerabilities and deployment security |

## Limitations

- Single node: no replication, sharding or standby; after a crash, the next
  server waits for the writer lease (10 s by default) before taking over.
- Filtered queries are approximate and may return fewer than `k` results
  unless they use `exact: true` or the collection's resident filter.
- Cache loss can lower approximate recall as well as increase latency;
  acknowledged data remains durable. Use `exact: true` for exhaustive results.
- A collection's dimension and metric are fixed when it is created.
- The server speaks plain HTTP; terminate TLS in a reverse proxy.
- The measured 1M-vector collection opens from S3 in a few seconds;
  larger collections have no fixed open-time guarantee.

## Contributing

Contributions are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for the
workflow and the checks CI runs.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you explicitly state
otherwise, any contribution intentionally submitted for inclusion in this
project, as defined in the Apache-2.0 license, shall be dual licensed as
above, without any additional terms or conditions.
