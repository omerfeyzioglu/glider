# Configuration

`glider-server` and `glider-admin` are configured through environment
variables (see `ServerConfig::from_env` in `src/server/config.rs`). An invalid
value stops the server with an error.

## Modes

- **Multi-collection mode** (`GLIDER_DIMENSIONS` unset): one server creates,
  lists, deletes and serves many collections through the
  [collections API](API.md#collections). Leave `GLIDER_METRIC`,
  `GLIDER_RESIDENT_FILTER` and `GLIDER_ROUTED_KEYS` unset; these settings
  belong to each collection. The base prefix contains `catalog/` and `data/`,
  not an engine namespace.
- **Single-collection mode** (`GLIDER_DIMENSIONS` set): the server owns one
  collection at the base prefix. Use the same dimensions, metric, resident
  filter and routed keys every time that prefix is opened.

**Use a base prefix in only one mode.** `glider-admin` currently requires
single-collection mode.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `GLIDER_DIMENSIONS` | unset (multi mode) | Set a positive dimension for single-collection mode; unset selects multi-collection mode. |
| `GLIDER_METRIC` | `squared_euclidean` | `squared_euclidean`, `manhattan` or `cosine`. Fixed at creation. |
| `GLIDER_RESIDENT_FILTER` | unset | One `key=value` equality predicate answered exactly from vectors kept in memory. Fixed at creation. |
| `GLIDER_ROUTED_KEYS` | unset | Up to four comma-separated metadata keys whose values restrict query routing (sorted and deduplicated). Fixed at creation. |
| `GLIDER_DATA_DIR` | unset | Local base directory; when set, the S3 variables are ignored. |
| `GLIDER_S3_BUCKET` | required without `GLIDER_DATA_DIR` | Bucket name. |
| `GLIDER_S3_NAMESPACE` | required without `GLIDER_DATA_DIR` | Base key prefix owned by this server. Do not overlap prefixes or mix modes. |
| `GLIDER_S3_REGION` | `us-east-1` | Bucket region. |
| `GLIDER_S3_ENDPOINT` | unset | Endpoint for S3-compatible stores such as MinIO; an `http://` endpoint enables plain HTTP. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` | unset | Credentials, read by the `object_store` S3 client (`AmazonS3Builder::from_env`). |
| `GLIDER_LISTEN` | `127.0.0.1:8080` | Listen address (`0.0.0.0:8080` in the Docker image). |
| `GLIDER_API_TOKEN` | unset | If set, required as `Authorization: Bearer <token>` on API endpoints. `/healthz`, `/metrics`, `/console` and the redirect from `/` remain public. |
| `GLIDER_CONSOLE` | `1` | Serve the built-in web console at `/console`, with `/` redirecting there. Set `0` to disable both routes. The page is public; its API calls use the token entered in the page. |
| `GLIDER_LEASE_SECONDS` | `10` | Writer lease duration (fractions allowed). After a crash, the next start waits at most this long before taking over. |
| `GLIDER_CACHE_DIR` | `glider-cache` | Local block cache directory, relative to the working directory (`/var/lib/glider/cache` in the Docker image). Any local disk works: instance-store NVMe, EBS or a container volume. |
| `GLIDER_CACHE_BYTES` | `268435456` (256 MiB) | Local cache limit. While idle the server copies the collection into the cache up to this limit; set it above `cache.namespace_bytes` from `/v1/status` to keep everything local. |
| `GLIDER_INDEX_BYTES` | `134217728` (128 MiB) | Positive per-collection write-admission watermark for loaded sketches plus conservative pending-put reservations. Growth above it returns `429` before commit. Recovery, reads, retained retries and deletes remain available; this is not a process RSS limit. |
| `GLIDER_MAX_OPEN_COLLECTIONS` | `64` | Maximum open collections in multi mode. Opening another closes the least recently used idle collection. Per-collection cache budget is `max(16 MiB, GLIDER_CACHE_BYTES / GLIDER_MAX_OPEN_COLLECTIONS)` under `<GLIDER_CACHE_DIR>/<name>-<generation>/`. |
| `GLIDER_COLLECTION_IDLE_SECONDS` | `60` | Close unused collections in multi mode to avoid idle S3 lease renewal requests. Fractions allowed; `0` disables. |
| `GLIDER_LOCAL_BLOCKS` | `24` | Cached blocks a query may rerank in addition to its remote budget. `0` makes results independent of the cache contents. |
| `GLIDER_AUTO_CLUSTER_ROWS` | `250000` | Live sealed rows at which a collection without a clustered view is converted to one in the background. `0` disables. |
| `GLIDER_AUTO_RECLUSTER_FACTOR` | `4` | Rebuild the clustered view with more clusters once the collection holds more than this factor times the rows it was sized for (about 4,000 per cluster). `0` disables. |

The S3 client also supports EC2 instance roles through IMDSv2, ECS task roles
through `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, and web identity through
`AWS_WEB_IDENTITY_TOKEN_FILE` plus `AWS_ROLE_ARN` (the token file must be
readable inside the container). Explicit access keys take precedence.
It does not read AWS CLI profiles or shared credential files. See
[Run on AWS](INSTALL.md#run-on-aws) for IAM and container setup.

## Text embedding (optional)

Disabled by default. Source builds need `embed-local` and/or `embed-openai`
in addition to `server`; the Docker image includes both. Invalid provider
combinations, unsupported local models and missing features stop startup.

| Variable | Default | Meaning |
|---|---|---|
| `GLIDER_EMBED_PROVIDER` | unset / `none` | `none`, `local` or `openai`. |
| `GLIDER_EMBED_MODEL` | `BAAI/bge-small-en-v1.5` for local | Local fastembed model name (or its ONNX repository / enum name); required for openai. |
| `GLIDER_EMBED_URL` | unset | Required for openai: HTTP(S) base URL, e.g. `https://api.openai.com/v1` or `http://localhost:11434/v1`. No URL credentials, query or fragment. |
| `GLIDER_EMBED_API_KEY` | unset | Optional openai Bearer key. Never included in status or errors. |
| `GLIDER_EMBED_CACHE_DIR` | `<GLIDER_DATA_DIR>/models` in local multi mode; `<GLIDER_CACHE_DIR>/models` otherwise | Local model download cache. Persist it to avoid downloading again after container replacement. |

Model settings require an enabled provider. URL and key are openai-only;
model cache is local-only. In single-collection local mode, keep model files
outside `GLIDER_DATA_DIR`: that directory contains only engine objects.
Local model files download on first embedding,
not at startup or image build. The default model has 384 dimensions; use
cosine collections. Other examples: `BAAI/bge-base-en-v1.5` (768),
`intfloat/multilingual-e5-small` (384), `nomic-ai/nomic-embed-text-v1.5` (768).
Local query/document prefixes follow the selected model. OpenAI-compatible
endpoints receive the configured model and unmodified text; `kind` has no
standard remote equivalent, so choose a model that supports this interface.
Match collection dimensions to the provider output and use the same model
for document and query embeddings. Changing models does not re-embed stored points.

Embedding allows two active requests, with no waiting queue (`429` when full).
Remote calls have a 60-second timeout and do not follow redirects; endpoint
failures return a sanitized `503`. No API keys, endpoint URLs or endpoint
error bodies are exposed to clients. Embedding does not write documents:
clients call `/v1/embed` then submit vectors through the write API.

## Fixed serving profile

The server uses the 1,000,000-row serving profile
(`SegmentedServingOptions::m31`): per query 12 candidate blocks, 8
remote range requests and 1 MiB, 32 cluster probes, 8 routing threads; at
most 4 concurrent queries; an admission queue of 8 commands and 1 MiB. These
are not configurable through the environment. Multi mode uses two concurrent
queries and two scoring threads per open collection; other serving settings
are shared. If all open collections have requests in flight, a new open
returns `429` until one becomes idle.
