# Installing and running Glider

Glider runs as one `glider-server` process. It stores its data in an
S3-compatible bucket, or in a local directory for development. The
[configuration guide](CONFIGURATION.md) lists every environment variable.

## Prebuilt image

The image `ghcr.io/omerfeyzioglu/glider:latest` is published for `linux/amd64`
and `linux/arm64`. It contains `glider-server` and `glider-admin`. With a
local directory as storage:

```sh
docker run --rm -p 8080:8080 \
  -e GLIDER_DATA_DIR=/var/lib/glider/data ghcr.io/omerfeyzioglu/glider:latest
```

Data in the container is lost when it stops. Add
`-v glider-data:/var/lib/glider` to keep it in a Docker volume, or point the
server at S3:

```sh
docker run --rm -p 8080:8080 \
  -e GLIDER_S3_BUCKET=my-bucket -e GLIDER_S3_NAMESPACE=glider \
  -e GLIDER_S3_REGION=eu-central-1 \
  -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \
  ghcr.io/omerfeyzioglu/glider:latest
```

Without `GLIDER_DIMENSIONS` the server serves many collections; create them
through the [collections API](API.md#collections).

## Docker Compose with MinIO

This setup needs Docker and Git and builds the server from source. It uses
local MinIO and example credentials for a demo, not an internet-facing
deployment.

```sh
git clone https://github.com/omerfeyzioglu/glider.git
cd glider
docker compose up --build -d
```

This starts MinIO, creates the `glider` bucket and serves one collection,
`demo` (3 dimensions, resident filter `color=red`), at `localhost:8080` in
single-collection mode, so the data routes have no collection prefix. Once
`docker compose logs glider` shows `listening on 0.0.0.0:8080`, try a write
and a nearest-neighbor query:

```sh
curl -sS localhost:8080/v1/write -H 'content-type: application/json' \
  -d '{"upsert":[{"id":1,"vector":[0,0,0],"metadata":{"color":"red"}},{"id":2,"vector":[1,1,1]}]}'

curl -sS localhost:8080/v1/query -H 'content-type: application/json' \
  -d '{"vector":[1,1,0.9],"k":2,"include_metadata":true}'
```

The write returns a `sequence` and `request_id`; the query returns two hits
ordered by distance. Try an [exact query on `color=red`](API.md#post-v1query),
[read a point](API.md#get-v1pointsid), or inspect
[`/v1/status`](API.md#get-v1status). `docker compose down` stops the demo
and keeps its data; `docker compose down -v` **deletes the demo data**.

## From source

Requires Rust 1.98.1 (the version CI uses).

```sh
cargo build --release --features server --bin glider-server --bin glider-admin
GLIDER_DATA_DIR=./data target/release/glider-server
```

`GLIDER_DATA_DIR` stores collections in a local directory, which is
convenient for development. For S3, set `GLIDER_S3_BUCKET`,
`GLIDER_S3_NAMESPACE` and AWS credentials instead:

```sh
GLIDER_S3_BUCKET=my-bucket GLIDER_S3_NAMESPACE=glider \
GLIDER_S3_REGION=eu-central-1 \
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  target/release/glider-server
```

The crate can also be embedded as a Rust library; see the
[library guide](LIBRARY.md).

## AWS

For AWS, you provision the bucket, compute host, networking, credentials and
TLS edge. See the [deployment pattern](ARCHITECTURE.md#aws-deployment-pattern)
and the [serving guide](SERVING.md) for backups and S3 Versioning.
