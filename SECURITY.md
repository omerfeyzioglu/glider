# Security policy

## Supported versions

Security fixes are made on `main` and released in the next version. Only
the latest release is supported.

## Reporting a vulnerability

Report vulnerabilities privately through
[GitHub security advisories](https://github.com/omerfeyzioglu/glider/security/advisories/new),
not in public issues. Include the version or commit, the object store, and
steps to reproduce. You should receive a response within 7 days.

## Deployment notes

Glider serves plain HTTP with an optional static bearer token
(`GLIDER_API_TOKEN`). Terminate TLS in a reverse proxy, keep the server on a
private network, and give it credentials scoped to its own bucket prefix.
See the [AWS deployment pattern](docs/ARCHITECTURE.md#aws-deployment-pattern).
