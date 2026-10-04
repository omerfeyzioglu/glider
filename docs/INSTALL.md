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

## Run on AWS

1. Create a general-purpose S3 bucket in the same region as your EC2 instance.
   Block all public access and use the default S3-managed encryption (SSE-S3).
   Versioning is optional; see [backups and restore](SERVING.md#backup-and-restore)
   for versioning, lifecycle and backup guidance.
2. Grant the server this IAM policy, replacing `BUCKET` and `NAMESPACE` with
   your bucket name and `GLIDER_S3_NAMESPACE` (for example, `glider`):

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": "s3:ListBucket",
         "Resource": "arn:aws:s3:::BUCKET",
         "Condition": {"StringLike": {"s3:prefix": "NAMESPACE/*"}}
       },
       {
         "Effect": "Allow",
         "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
         "Resource": "arn:aws:s3:::BUCKET/NAMESPACE/*"
       }
     ]
   }
   ```

   These [permissions](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html)
   cover full/range GET, conditional PUT (`If-None-Match: *`), LIST and
   DELETE. The engine makes no HEAD or multipart-upload requests. SSE-KMS
   encryption requires additional KMS permissions.
3. Prefer an EC2 instance role with that policy: no access keys are needed.
   The client obtains credentials through IMDSv2; enable metadata access and
   [set the response hop limit to 2 for Docker](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-IMDS-new-instances.html).
   Alternatively, export `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, plus
   `AWS_SESSION_TOKEN` for temporary credentials, and add
   `-e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN` below.
   AWS CLI profiles are not read by the server.
4. Install Docker on EC2, export `GLIDER_API_TOKEN` with your chosen token,
   and run one server for this prefix:

   ```sh
   docker run -d --name glider -p 127.0.0.1:8080:8080 \
     -v glider-cache:/var/lib/glider/cache \
     -e GLIDER_S3_BUCKET=BUCKET -e GLIDER_S3_NAMESPACE=NAMESPACE \
     -e GLIDER_S3_REGION=eu-central-1 -e GLIDER_API_TOKEN \
     ghcr.io/omerfeyzioglu/glider:latest
   ```

   Set the actual bucket region. Place Docker's volume storage on local
   instance-store NVMe or EBS; the cache is disposable and losing it loses no
   acknowledged writes. Stop the old container before starting its replacement
   with the same bucket and prefix, even on another host.
5. Terminate TLS in a reverse proxy before exposing the service. See the
   [security deployment notes](../SECURITY.md#deployment-notes) and
   [AWS deployment pattern](ARCHITECTURE.md#aws-deployment-pattern) for networking
   and the bigger picture.
6. Create a collection and write points using the
   [Quickstart](../README.md#quickstart) (add `Authorization: Bearer <token>`).
   Then verify the objects are in S3 with the AWS CLI:

   ```sh
   aws s3 ls s3://BUCKET/NAMESPACE/ --recursive | head
   ```

## S3-compatible storage

The store must provide strongly consistent GET/LIST and atomic conditional
create: PUT with `If-None-Match: *` must fail when the key already exists.
This is how a write is acknowledged and how the single writer is enforced.
AWS S3 and MinIO are tested; MinIO is used by the
[Docker Compose demo](#docker-compose-with-minio) and CI integration tests.
Other stores implementing these semantics should work but are untested.

For MinIO, add `-e GLIDER_S3_ENDPOINT=http://minio:9000` to the container
command, using a hostname reachable from the container and MinIO credentials
in the AWS key variables. An `http://` endpoint enables plain HTTP; use HTTPS
for remote storage. `GLIDER_S3_REGION` defaults to `us-east-1`.
