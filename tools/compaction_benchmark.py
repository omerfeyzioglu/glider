#!/usr/bin/env python3
"""Dedicated M4 workload: 300 mutations -> checkpoint -> compaction -> warm recovery.

Uses the existing Cargo harness unchanged; JSON remains a recovery report.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess

from benchmarks import reports

LABEL = "M4 layout experiment; desktop load and power uncontrolled"


def command(backend, compact=True, root="target", label=LABEL):
    args = ["cargo", "bench", "--locked"]
    if backend == "s3":
        args += ["--features", "s3"]
    return args + ["--bench", "baseline", "--", "--scenario", "recovery", "--backend", backend,
                   "--rows", "100", "--dimensions", "32", "--mutations", "300", "--samples", "3",
                   "--checkpoint-at", "300", "--compact-at", "300" if compact else "0", "--seed", "42",
                   "--feature", "compaction", "--phase", "after" if compact else "before",
                   "--comparison-group", "m4-layout-300", "--root", str(root), "--label", label]


def validate(document, backend, compact, secrets=()):
    reports(document)
    raw = json.dumps(document)
    for secret in secrets:
        assert not secret or secret not in raw
    config = document["config"]
    for key, expected in dict(rows=100, dimensions=32, mutations=300, samples=3,
                              checkpoint_at=300, seed=42, scenario="recovery").items():
        assert config[key] == expected, key
    assert config.get("compact_at", 0) == (300 if compact else 0)
    assert len(document["results"]) == 1
    result = document["results"][0]
    assert result["backend"] == backend and result["scenario"] == "recovery"
    assert result["dataset_sha256"] and result["dataset_seed"] == 42
    assert result["checkpoint"]["sequence"] == 300
    assert result["checkpoint"]["creates"] == 1
    assert result["inventory"]["logical_objects"] == (2 if compact else 302)
    samples = result["measured_store_calls_per_sample"]
    assert len(samples) == 3
    for counts in samples:
        assert counts["get_calls"] == 2 and counts["list_calls"] == 1
        assert counts["create_calls"] == counts.get("remove_calls", 0) == 0
    if compact:
        maintenance = result["compaction"]
        assert maintenance["sequence"] == 300
        assert maintenance["creates"] == maintenance["lists"] == 1
        assert maintenance["removes"] == 301
        assert maintenance["logical_bytes_read"] == 0
        assert maintenance["logical_bytes_written"] > 0
        assert maintenance["additional_read_amplification"] == 0
        assert maintenance["additional_write_amplification"] == maintenance["logical_bytes_written"] / maintenance["input_mutation_payload_bytes"]
        if backend == "s3":
            http = maintenance["http_requests"]
            assert http["put"] == http["list"] == 1 and http["delete"] == 301
            assert http["get"] == http["other"] == http["http_errors"] == http["transport_errors"] == 0
    if backend == "s3":
        assert len(result["http_requests_per_sample"]) == 3
        for counts in result["http_requests_per_sample"]:
            assert counts["get"] == 2 and counts["list"] == 1
            assert counts["put"] == counts["delete"] == counts["http_errors"] == counts["transport_errors"] == 0


def measure(backend, compact=True, root="target", label=LABEL, env=None, secrets=()):
    raw = subprocess.run(command(backend, compact, root, label), env=env, check=True,
                         text=True, stdout=subprocess.PIPE).stdout
    config_env = os.environ if env is None else env
    credentials = tuple(config_env.get(k, "") for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"))
    validate(json.loads(raw), backend, compact, tuple(secrets) + credentials)
    return raw


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", choices=("local", "s3"), default="local")
    parser.add_argument("--checkpoint-only", action="store_true", help="matching layout without compaction")
    parser.add_argument("--root", type=Path, default=Path("target"))
    parser.add_argument("--label", default=LABEL)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists; raw reports are never overwritten")
    args.root.mkdir(parents=True, exist_ok=True)
    raw = measure(args.backend, not args.checkpoint_only, args.root, args.label)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x") as file:
        file.write(raw)


if __name__ == "__main__":
    main()
