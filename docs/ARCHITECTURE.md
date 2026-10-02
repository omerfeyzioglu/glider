# Architecture

Glider runs one collection per `glider-server` process. The server keeps
routing state in memory and may cache vector blocks on local storage. The
object store is the durable authority: replacing the process or clearing its
cache does not discard acknowledged writes.

## Runtime

![Glider runtime architecture](architecture/runtime.svg)

The HTTP API sends requests through bounded admission. One committer handles
writes and maintenance. Up to four reader threads serve queries and point
reads against immutable published snapshots. A successful write is
acknowledged after its complete mutation batch has been created conditionally
as an immutable log object. Idle maintenance creates packs, indexes and
manifests before publishing the root that names them.

Search uses in-memory routing state to choose candidate blocks, reads cached
blocks or bounded ranges from object storage, then scores full vectors for
the final results. The unsealed log tail also participates in reads. Exact
kNN remains the correctness reference for approximate search; a cold cache
can change latency and approximate recall while leaving durability intact.

On restart, the server selects a complete root and replays the contiguous
newer log tail. The segmented engine uses a lease and permanent fences so a
previous writer cannot publish after takeover. See [DESIGN.md](../DESIGN.md)
for format versions, publication order, failure cases and recovery rules.

## AWS deployment pattern

![Glider AWS deployment pattern](architecture/aws.svg)

This diagram is one deployment pattern for the existing single-node server,
not infrastructure shipped by this repository. An Application Load Balancer
terminates HTTPS in public subnets. A private-subnet EC2 instance runs one
`glider-server` process for one collection; an S3 gateway endpoint gives it
access to the bucket. Give each process a distinct S3 namespace prefix. S3
stores mutation logs, vector data, root generations and writer ownership
objects. An EBS volume holds only a disposable block cache. The operator
must provision the VPC, load balancer, compute host, endpoint, bucket,
credentials and monitoring. This is still one active server, not a
multi-node availability design.

The server speaks HTTP; configure authentication and terminate HTTPS at the
edge before exposing it to clients. The repository does not currently ship
an AWS deployment template or a published prebuilt image. The
[README quickstart](../README.md#quickstart) is a local Compose demo, while
the [configuration guide](../README.md#configuration) describes the S3
environment variables.

## Diagram sources

The editable [runtime](architecture/runtime.drawio) and
[AWS](architecture/aws.drawio) sources open in
[diagrams.net](https://app.diagrams.net/). Export each page as SVG with
embedded images to update the corresponding image in this directory.
The AWS diagram uses the official
[AWS Architecture Icons](https://aws.amazon.com/architecture/icons/) package
(July 2026 release). Generic runtime icons are from
[Lucide](https://lucide.dev/) under its
[ISC license](architecture/LUCIDE-LICENSE.txt).
