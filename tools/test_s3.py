#!/usr/bin/env python3
"""Run S3 integration tests in an isolated, disposable MinIO container."""
import os
import secrets
import subprocess
import time
import urllib.error
import urllib.request

IMAGE = "quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"


def run(*args, env=None, capture=False):
    return subprocess.run(args, env=env, check=True, text=True,
                          stdout=subprocess.PIPE if capture else None).stdout


def ready(endpoint):
    # Bounded service-start readiness polling, never retries database operations.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(endpoint + "/minio/health/ready", timeout=1) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(0.1)
    raise RuntimeError("MinIO did not become ready within 30 seconds")


def main():
    name = "glider-m2-" + secrets.token_hex(6)
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("AWS_", "GLIDER_S3_", "MINIO_"))}
    env.update(MINIO_ROOT_USER="glider-test", MINIO_ROOT_PASSWORD=secrets.token_hex(24))
    run("cargo", "test", "--locked", "--features", "s3", "--lib", "--no-run", env=env)
    try:
        run("docker", "run", "-d", "--name", name, "-p", "127.0.0.1::9000",
            "-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD", IMAGE,
            "server", "/data", env=env, capture=True)
        port = run("docker", "port", name, "9000/tcp", capture=True).strip().split(":")[-1]
        endpoint = "http://127.0.0.1:" + port
        ready(endpoint)
        run("docker", "exec", name, "/bin/sh", "-c",
            'mc alias set test http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc mb test/glider-test',
            capture=True)
        env.update(AWS_ACCESS_KEY_ID=env["MINIO_ROOT_USER"],
                   AWS_SECRET_ACCESS_KEY=env["MINIO_ROOT_PASSWORD"],
                   GLIDER_S3_ENDPOINT=endpoint, GLIDER_S3_BUCKET="glider-test")
        args = ("cargo", "test", "--locked", "--features", "s3", "--lib")
        run(*args, "store::s3::tests::minio_", "--", "--ignored", "--nocapture", env=env)
        run(*args, "store::s3::tests::server_restart_prepare", "--", "--ignored", env=env)
        run("docker", "kill", "--signal", "KILL", name, capture=True)
        run("docker", "start", name, capture=True)
        # Docker may assign another ephemeral host port when starting again.
        port = run("docker", "port", name, "9000/tcp", capture=True).strip().split(":")[-1]
        env["GLIDER_S3_ENDPOINT"] = "http://127.0.0.1:" + port
        ready(env["GLIDER_S3_ENDPOINT"])
        run(*args, "store::s3::tests::server_restart_verify", "--", "--ignored", env=env)
        print("S3 integration and abrupt MinIO restart checks passed.", flush=True)
    finally:
        subprocess.run(["docker", "rm", "-fv", name], check=False, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
