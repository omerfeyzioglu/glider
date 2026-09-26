#!/usr/bin/env python3
"""Bounded provider correctness probe. Default CI use is explicit --minio."""
import argparse
from contextlib import nullcontext
from datetime import datetime, timezone, timedelta
import json
import math
import os
from pathlib import Path
import re
import secrets
import sys

from minio_harness import container_scope, ready, run
from test_s3 import IMAGE


def free_plan(document, now=None):
    now = now or datetime.now(timezone.utc)
    credit = document.get("accountPlanRemainingCredits", {})
    amount = float(credit.get("amount", 0))
    expiry = datetime.fromisoformat(document.get("accountPlanExpirationDate", "").replace("Z", "+00:00"))
    if (document.get("accountPlanType") != "FREE"
            or document.get("accountPlanStatus") != "ACTIVE"
            or credit.get("unit") != "USD" or not math.isfinite(amount) or amount < 1
            or expiry < now + timedelta(minutes=30)):
        raise ValueError("requires active FREE plan, at least $1 remaining credit and 30 minutes remaining")
    account = document.get("accountId", "")
    if not re.fullmatch(r"[0-9]{12}", account):
        raise ValueError("missing AWS account identity")
    return account


def aws_environment(source):
    env = dict(source)
    for key in list(env):
        if key.startswith("AWS_ENDPOINT_URL") or key in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE"):
            del env[key]
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "GLIDER_S3_BUCKET", "GLIDER_S3_REGION"):
        if not env.get(key):
            raise ValueError(f"set {key} locally before the AWS probe")
    if not re.fullmatch(r"glider-pilot-[a-z0-9-]{3,49}", env["GLIDER_S3_BUCKET"]):
        raise ValueError("use a dedicated bucket named glider-pilot-<unique-suffix>")
    if not re.fullmatch(r"(?:eu|us|ap|ca|sa|af|me|il|mx)-[a-z]+-[0-9]+", env["GLIDER_S3_REGION"]):
        raise ValueError("unsupported AWS commercial region")
    env.update(AWS_CONFIG_FILE=os.devnull, AWS_SHARED_CREDENTIALS_FILE=os.devnull,
               AWS_MAX_ATTEMPTS="1", AWS_CLI_AUTO_PROMPT="off", AWS_PAGER="",
               GLIDER_S3_ENDPOINT=f"https://s3.{env['GLIDER_S3_REGION']}.amazonaws.com")
    return env


def check_aws(env):
    def aws(*args, region):
        raw = run("aws", *args, "--region", region, "--output", "json",
                  "--no-cli-pager", "--cli-connect-timeout", "5",
                  "--cli-read-timeout", "10", env=env, capture=True, timeout=15)
        return json.loads(raw or "{}")
    account = free_plan(aws("freetier", "get-account-plan-state", region="us-east-1"))
    bucket = ["--bucket", env["GLIDER_S3_BUCKET"], "--expected-bucket-owner", account]
    region = env["GLIDER_S3_REGION"]
    location = aws("s3api", "get-bucket-location", *bucket, region=region).get("LocationConstraint")
    location = "us-east-1" if location is None else "eu-west-1" if location == "EU" else location
    if location != region:
        raise ValueError("bucket region differs from GLIDER_S3_REGION")
    if aws("s3api", "get-bucket-versioning", *bucket, region=region).get("Status"):
        raise ValueError("pilot requires a fresh bucket without versioning or retained historical versions")


def exercise(output, env):
    marker = output / "owned-prefix.txt"
    failed = False
    def phase(name, seconds):
        run("target/release/examples/s3_pilot", name, str(output / f"{name}.json"),
            str(marker), env=env, timeout=seconds, stage=f"S3 pilot {name}")
    try:
        phase("write", 160)
        phase("recover", 310)
    except BaseException:
        failed = True
        raise
    finally:
        # Never remove a preexisting namespace or an unknown caller-supplied prefix.
        if marker.exists() and marker.read_text() == env["GLIDER_S3_NAMESPACE"]:
            try:
                phase("cleanup", 70)
            except Exception:
                if not failed:
                    raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new report directory")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--minio", action="store_true")
    mode.add_argument("--aws", action="store_true", help="requires active Free plan and dedicated bucket")
    args = parser.parse_args()
    env = aws_environment(os.environ) if args.aws else {
        k: v for k, v in os.environ.items() if not k.startswith(("AWS_", "GLIDER_S3_", "MINIO_"))}
    args.output.mkdir(parents=True, exist_ok=False)
    env["GLIDER_S3_NAMESPACE"] = "glider-pilot/" + secrets.token_hex(16)
    # Build locally before spending AWS credits or starting the timed workload.
    run("cargo", "build", "--release", "--locked", "--features", "s3", "--example", "s3_pilot", env=env)
    if args.aws:
        check_aws(env)  # Fail closed: no writes if plan, credits, identity or bucket checks fail.
    name = "glider-pilot-" + secrets.token_hex(6)
    with container_scope(name) if args.minio else nullcontext():
        if args.minio:
            env.update(MINIO_ROOT_USER="glider-" + secrets.token_hex(8), MINIO_ROOT_PASSWORD=secrets.token_hex(24))
            run("docker", "run", "-d", "--name", name, "-p", "127.0.0.1::9000",
                "-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD", IMAGE,
                "server", "/data", env=env, capture=True)
            port = run("docker", "port", name, "9000/tcp", capture=True).strip().split(":")[-1]
            endpoint = "http://127.0.0.1:" + port
            ready(endpoint, name)
            run("docker", "exec", name, "mc", "mb", "test/glider-pilot-test", capture=True)
            env.update(AWS_ACCESS_KEY_ID=env["MINIO_ROOT_USER"], AWS_SECRET_ACCESS_KEY=env["MINIO_ROOT_PASSWORD"],
                       GLIDER_S3_ENDPOINT=endpoint, GLIDER_S3_REGION="us-east-1", GLIDER_S3_BUCKET="glider-pilot-test")
        (args.output / "run.json").write_text(json.dumps({
            "version": 1, "backend": "aws" if args.aws else "minio", "prefix": env["GLIDER_S3_NAMESPACE"],
            "endpoint": env["GLIDER_S3_ENDPOINT"], "bucket": env["GLIDER_S3_BUCKET"],
            "region": env["GLIDER_S3_REGION"], "git_revision": run("git", "rev-parse", "HEAD", capture=True).strip(),
            "git_status": run("git", "status", "--short", capture=True).strip(),
        }, indent=2))
        exercise(args.output, env)
    print(f"Provider pilot passed; reports: {args.output}")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        from minio_harness import redact
        print(redact(str(error)), file=sys.stderr)
        sys.exit(1)
