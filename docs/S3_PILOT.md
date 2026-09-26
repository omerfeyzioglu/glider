# Bounded S3 correctness pilot (M14a)

Daily tests and CI use disposable MinIO. After M14, this small pilot checks a real
AWS S3 Standard general-purpose bucket before further provider assumptions. It is
not a latency benchmark, capacity test, long soak or proof of provider hardware
durability. AWS acceptance remains pending until an actual AWS report passes.

## Local rehearsal

```sh
python3 tools/s3_pilot.py target/s3-pilot-local --minio
```

Choose a new output directory per run. The runner generates credentials, starts
the pinned MinIO image, runs the same workload as AWS and removes the container.
It does not read existing AWS credentials. CI retains the JSON reports.

## AWS prerequisites and invocation

Use an active **Free account plan**, not merely a paid account with credits.
Do not upgrade the plan or join an AWS Organization for this test. AWS documents
[plan behavior](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier-plans.html)
and the [plan-state API](https://docs.aws.amazon.com/cli/latest/reference/freetier/get-account-plan-state.html).
The script never upgrades an account or provisions compute, buckets or paid services.

1. Create a dedicated private general-purpose bucket named
   `glider-pilot-<unique-suffix>`, with S3 Standard storage and versioning never
   enabled. Frankfurt (`eu-central-1`) is the initial suggested region. No EC2
   machine is needed; the client runs on the development computer.
2. Install AWS CLI v2 supporting `freetier get-account-plan-state`. Configure
   credentials locally through `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and,
   for temporary credentials, `AWS_SESSION_TOKEN`. Never put keys in reports,
   source files or chat. They must remain valid through the short test/cleanup.
3. Grant `freetier:GetAccountPlanState`, bucket `s3:GetBucketLocation`,
   `s3:GetBucketVersioning`, prefix-scoped `s3:ListBucket`, and object
   `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` under `glider-pilot/*`.
   Configure ListBucket's prefix condition to allow `glider-pilot/*`.
4. Set the non-secret settings and run explicitly:

```sh
export GLIDER_S3_BUCKET=glider-pilot-your-unique-suffix
export GLIDER_S3_REGION=eu-central-1
python3 tools/s3_pilot.py target/s3-pilot-aws-first --aws
```

Use an actual lowercase bucket name. The runner derives the HTTPS endpoint,
uses the same explicit credentials for the CLI and engine, ignores profiles and
configured endpoint overrides, and disables automatic retries. Before writes it
requires plan type FREE, status ACTIVE, at least USD 1 credit and 30 minutes of
remaining plan time; it checks bucket ownership, region and absence of versioning.
A denied API call or unknown/missing information stops the run. It cannot prevent
an operator changing the account plan concurrently: keep the account on Free.

## Workload and stopping rules

The fixed workload uses 2,000 rows, 64 dimensions, seed 42 and generator
`mod65536-v1`. It checks conditional PUT immutability; writes batches of 100;
reopens from a separate process; checks all vectors/metadata and exact top-k with
and without a filter; overwrites/deletes; deliberately discards one successful
mutation response; and checks poisoning, isolated takeover and backup/restore.
The discarded response comes after a real server PUT, not a simulated commit.
Real server termination and large fault matrices remain MinIO tests.

| Process | HTTP client-attempt limit | Request + response payload budget | Supervisor time limit |
|---|---:|---:|---:|
| Initial write | 2,000 | 25 MiB | 160 s |
| Recovery and backup | 6,000 | 65 MiB | 310 s |
| Cleanup reserve | 2,000 | 10 MiB | 70 s |

All store handles/namespaces within each process share its transport budget;
listing pages, failed client calls and cleanup are included. Counters observe
the SDK's HTTP service boundary, not packets or redirects inside the HTTP client.
Transport deadlines
stop admission before the supervisor kills the process group. Up to three
read-only AWS preflight calls precede this workload, each bounded to 15 seconds.
Local compilation is separate from the remote workload's sub-10-minute bound.

Payload accounting excludes HTTP headers/TLS and provider-internal traffic.
Responses are checked per received chunk: the crossing chunk is counted and
stops that phase, so received bytes can exceed a phase budget by one chunk.
These are workload stopping rules, not an exact billing cap; the account staying
on Free is the billing boundary. Expected missing-key/conditional-write responses
appear in HTTP error counters and are asserted by the workload.

## Reports and cleanup

`run.json` records backend, endpoint, bucket, region, fresh random prefix, Git
revision and working-tree status. Phase reports record pass/fail, counts,
elapsed time, workload identity and limits. Timing is observational and includes
the client's network; do not apply loopback MinIO latency gates to AWS.

All five test namespaces must be empty before writing. A local ownership marker
then authorizes cleanup of only these generated namespaces. Cleanup uses its own
reserved budget, verifies an empty listing, and cannot replace an earlier error.
Versioned buckets are refused because deleting visible keys would leave billed
historical versions. Never remove other bucket contents to make a probe pass.

On a hard timeout, missing report, expired credential or cleanup error, retain
the local reports/marker and inspect only the recorded prefixes in the bucket.
Late requests may leave additional objects after a failed run; those prefixes
must never be reused. Remove leftovers once the test client and its outstanding
requests have stopped. A missing/failed report is not provider acceptance.
