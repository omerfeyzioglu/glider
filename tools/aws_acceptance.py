#!/usr/bin/env python3
"""Run the M31 acceptance on a tagged EC2 instance next to S3 Standard.

The instance gets 3-hour session credentials from the local AWS profile,
builds REVISION from GitHub, reads the verified SIFT files from the test
prefix, runs load/serve/verify against S3, uploads run.json and its log,
deletes its namespace and terminates itself. A hard shutdown cap bounds its
lifetime even if the run hangs. This driver waits for the result, then
terminates the instance explicitly and verifies it, and removes the run's
objects (datasets are removed with --cleanup-datasets).
"""
import argparse
import base64
import json
from pathlib import Path
import secrets
import subprocess
import sys
import time

TAG = "glider"


def aws(*args, capture=True):
    result = subprocess.run(["aws", *args], capture_output=capture, text=True)
    if result.returncode:
        raise RuntimeError(f"aws {' '.join(args[:3])}: {result.stderr.strip()}")
    return result.stdout


def exists(uri):
    return subprocess.run(["aws", "s3", "ls", uri], capture_output=True).returncode == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new local directory for run.json and run.log")
    parser.add_argument("--revision", required=True)
    parser.add_argument("--rows", type=int, default=1000000)
    parser.add_argument("--rounds", type=int, default=300)
    parser.add_argument("--scenario", choices=("acceptance", "tenants"), default="acceptance")
    parser.add_argument("--tenants", type=int, default=1000)
    parser.add_argument("--per-tenant", type=int, default=1000)
    parser.add_argument("--bucket", default="glider-pilot-test-t1g1p")
    parser.add_argument("--prefix", default="glider-pilot")
    parser.add_argument("--region", default="eu-central-1")
    parser.add_argument("--instance-type", default="m7i-flex.large")
    parser.add_argument("--max-minutes", type=int, default=150)
    parser.add_argument("--cleanup-datasets", action="store_true")
    parser.add_argument("--clustered", action="store_true",
                        help="convert to an M37 clustered view after load (GLIDER_M24_CLUSTERED=1)")
    args = parser.parse_args()
    if args.tenants < 1 or args.per_tenant < 1:
        parser.error("--tenants and --per-tenant must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    run_id = ("m39-" if args.scenario == "acceptance" else "tenants-") + secrets.token_hex(4)
    results = f"s3://{args.bucket}/{args.prefix}/results/{run_id}"
    creds = json.loads(aws("sts", "get-session-token", "--duration-seconds", "10800"))["Credentials"]
    script = Path(__file__).with_name("aws_user_data.sh").read_text()
    for key, value in {
        "MAX_MINUTES": args.max_minutes, "KEY": creds["AccessKeyId"],
        "SECRET": creds["SecretAccessKey"], "TOKEN": creds["SessionToken"],
        "REGION": args.region, "BUCKET": args.bucket, "PREFIX": args.prefix,
        "ROWS": args.rows, "ROUNDS": args.rounds, "RUN": run_id, "REVISION": args.revision,
        "CLUSTERED": int(args.clustered),
        "SCENARIO": args.scenario, "TENANTS": args.tenants,
        "PER_TENANT": args.per_tenant, "INSTANCE_TYPE": args.instance_type,
    }.items():
        script = script.replace(f"@@{key}@@", str(value))
    assert "@@" not in script
    # Graviton types (c7g, m7g, t4g) need the arm64 image; others are x86_64.
    arch = "arm64" if args.instance_type.split(".")[0].endswith("g") else "x86_64"
    ami = aws("ssm", "get-parameter", "--region", args.region, "--name",
              f"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-{arch}",
              "--query", "Parameter.Value", "--output", "text").strip()
    tags = f"{{Key=project,Value={TAG}}},{{Key=run,Value={run_id}}}"
    instance = json.loads(aws(
        "ec2", "run-instances", "--region", args.region, "--image-id", ami,
        "--instance-type", args.instance_type, "--count", "1",
        "--instance-initiated-shutdown-behavior", "terminate",
        "--block-device-mappings",
        '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":30,"VolumeType":"gp3","DeleteOnTermination":true}}]',
        "--tag-specifications", f"ResourceType=instance,Tags=[{tags}]",
        f"ResourceType=volume,Tags=[{tags}]",
        "--user-data", base64.b64encode(script.encode()).decode(),
    ))["Instances"][0]["InstanceId"]
    print(f"{run_id}: launched {instance} ({args.instance_type})", flush=True)
    started = time.monotonic()
    outcome = "timeout"
    try:
        while time.monotonic() - started < (args.max_minutes + 10) * 60:
            time.sleep(60)
            if args.scenario == "tenants" and exists(f"{results}/FAILED"):
                outcome = "failed"
                break
            if exists(f"{results}/run.json"):
                outcome = "done"
                break
            if exists(f"{results}/FAILED"):
                outcome = "failed"
                break
            state = aws("ec2", "describe-instances", "--region", args.region, "--instance-ids",
                        instance, "--query", "Reservations[0].Instances[0].State.Name",
                        "--output", "text").strip()
            print(f"{int(time.monotonic() - started) // 60} min: instance {state}", flush=True)
            if state in ("shutting-down", "terminated"):
                time.sleep(30)
                outcome = ("failed" if args.scenario == "tenants" and exists(f"{results}/FAILED")
                           else "done" if exists(f"{results}/run.json") else "failed")
                break
    finally:
        subprocess.run(["aws", "ec2", "terminate-instances", "--region", args.region,
                        "--instance-ids", instance], capture_output=True)
        aws("ec2", "wait", "instance-terminated", "--region", args.region, "--instance-ids", instance)
        print(f"{instance} terminated", flush=True)
        for name in ("run.json", "run.log"):
            subprocess.run(["aws", "s3", "cp", "--only-show-errors", f"{results}/{name}",
                            str(args.output / name)], capture_output=True)
        for uri in (results, f"s3://{args.bucket}/{args.prefix}/{run_id}",
                    f"s3://{args.bucket}/{args.prefix}/{run_id}-backup"):
            subprocess.run(["aws", "s3", "rm", "--only-show-errors", "--recursive", uri + "/"],
                           capture_output=True)
        if args.cleanup_datasets:
            subprocess.run(["aws", "s3", "rm", "--only-show-errors", "--recursive",
                            f"s3://{args.bucket}/{args.prefix}/datasets/"], capture_output=True)
        # `aws s3 ls` exits non-zero when nothing matches, which is the goal.
        left = subprocess.run(["aws", "s3", "ls", "--recursive",
                               f"s3://{args.bucket}/{args.prefix}/{run_id}"],
                              capture_output=True, text=True).stdout
        print(f"remaining run objects: {len(left.splitlines())}", flush=True)
    print(f"{run_id}: {outcome}", flush=True)
    sys.exit(0 if outcome == "done" else 1)


if __name__ == "__main__":
    main()
