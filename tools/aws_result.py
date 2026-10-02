#!/usr/bin/env python3
"""Assemble an in-region run.json from the acceptance example's reports.

Applies the same declared gates and cost model as tools/m24_acceptance.py.
Usage: aws_result.py LOAD.json SERVE.json VERIFY.json VISIBLE_BYTES ROWS ORACLE
"""
import json
from pathlib import Path
import platform
import subprocess
import sys
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parent))
from m24_acceptance import ENVELOPES, cost, digest, gates  # noqa: E402


def instance_type():
    try:
        token = urllib.request.urlopen(urllib.request.Request(
            "http://169.254.169.254/latest/api/token", method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"}), timeout=2).read().decode()
        return urllib.request.urlopen(urllib.request.Request(
            "http://169.254.169.254/latest/meta-data/instance-type",
            headers={"X-aws-ec2-metadata-token": token}), timeout=2).read().decode()
    except OSError:
        return "unknown"


def main():
    load, serve, verify = (json.loads(Path(p).read_text()) for p in sys.argv[1:4])
    visible, rows, oracle = int(sys.argv[4]), int(sys.argv[5]), sys.argv[6]
    envelope = ENVELOPES[rows]
    batches = serve.pop("acknowledged_batches")
    serve["acknowledged_batch_count"] = len(batches)
    checks = gates(load, serve, verify, visible, envelope)
    run = lambda *args: subprocess.run(args, capture_output=True, text=True).stdout.strip()
    result = {
        "version": 1, "dataset": envelope["dataset"], "rows": rows, "dimensions": 128,
        "metric": "squared_euclidean", "k": 10, "filter": "cohort=one-percent (id % 100 == 0)",
        "backend": "aws-s3-standard-eu-central-1", "instance_type": instance_type(),
        "oracle": oracle, "oracle_sha256": digest(Path(oracle)),
        "git_revision": run("git", "rev-parse", "HEAD"),
        "working_tree_dirty": bool(run("git", "status", "--porcelain")),
        "load": load, "serve": serve, "verify": verify,
        "visible_payload_bytes_after_serve": visible,
        "gates": checks, "accepted": all(checks.values()), "cost_model": cost(serve, visible),
        "environment": platform.platform(), "rustc": run("rustc", "--version"),
    }
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
