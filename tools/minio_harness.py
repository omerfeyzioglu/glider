"""Bounded subprocesses and diagnostics for disposable local MinIO tests."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ARTIFACTS = Path("target/minio-diagnostics")
COMMAND_SECONDS = 600
DOCKER_SECONDS = 120
CLEANUP_SECONDS = 10
DIAGNOSTIC_SECONDS = 5
_secrets = set()


class HarnessFailure(RuntimeError):
    def __init__(self, stage, reason, returncode=None):
        super().__init__(f"{stage}: {reason}")
        self.returncode = returncode


def redact(value):
    for secret in sorted(_secrets, key=len, reverse=True):
        value = value.replace(secret, "[REDACTED]")
    return value


def record(event):
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    with (ARTIFACTS / "events.jsonl").open("a") as output:
        output.write(redact(json.dumps(event)) + "\n")


def run(*args, env=None, capture=False, timeout=None, stage=None, retain_output=False):
    """Kill the command's process group on timeout; retain sanitized failure output."""
    for key, value in (os.environ if env is None else env).items():
        if value and any(part in key.upper() for part in
                         ("SECRET", "TOKEN", "PASSWORD", "ACCESS_KEY", "MINIO_ROOT_USER")):
            _secrets.add(value)
    stage = redact(stage or " ".join(str(arg) for arg in args))
    timeout = timeout if timeout is not None else (
        DOCKER_SECONDS if args[0] == "docker" else COMMAND_SECONDS)
    start = time.monotonic()
    status = "failed"
    stdout = stderr = ""
    print(f"[start] {stage} (limit {timeout:g}s)", file=sys.stderr, flush=True)
    try:
        with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
            process = subprocess.Popen(args, env=env, stdout=out, stderr=err,
                                       start_new_session=True)
            try:
                process.wait(timeout=timeout)
            except BaseException:
                # Kill descendants too: cargo and test runners spawn child processes.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
                raise
            finally:
                out.seek(0)
                err.seek(0)
                stdout = out.read().decode(errors="replace")
                stderr = err.read().decode(errors="replace")
            if process.returncode:
                raise HarnessFailure(stage, f"exit {process.returncode}", process.returncode)
            status = "passed"
    except subprocess.TimeoutExpired:
        status = "timeout"
        raise HarnessFailure(stage, f"exceeded {timeout:g}s") from None
    finally:
        elapsed = time.monotonic() - start
        event = dict(stage=stage, status=status, elapsed_seconds=round(elapsed, 3),
                     timeout_seconds=timeout)
        if status != "passed" or retain_output:
            event["output"] = redact(stdout + stderr)[-65536:]
        record(event)
        print(f"[{status}] {stage} ({elapsed:.2f}s)", file=sys.stderr, flush=True)
        if not capture and stdout:
            print(redact(stdout), end="", flush=True)
        if stderr:
            print(redact(stderr), end="", file=sys.stderr, flush=True)
    return stdout if capture else None


def ready(endpoint, container, timeout=30):
    # Only startup readiness is polled; database requests are never retried.
    deadline = time.monotonic() + timeout
    while (remaining := deadline - time.monotonic()) > 0:
        try:
            with urllib.request.urlopen(endpoint + "/minio/health/ready",
                                        timeout=min(1, remaining)) as response:
                healthy = response.status == 200
        except (urllib.error.URLError, TimeoutError, ConnectionError):
            healthy = False
        remaining = deadline - time.monotonic()
        if healthy and remaining > 0:
            try:
                run("docker", "exec", container, "/bin/sh", "-c",
                    'mc alias set test http://127.0.0.1:9000 "$MINIO_ROOT_USER" '
                    '"$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 && mc ls test >/dev/null 2>&1',
                    timeout=remaining, capture=True, stage="MinIO authenticated readiness")
                return
            except HarnessFailure as error:
                if error.returncode is None:
                    raise
        time.sleep(min(0.1, max(0, deadline - time.monotonic())))
    raise HarnessFailure("MinIO readiness", f"health/authentication exceeded {timeout:g}s")


def diagnostics(name):
    for command in (("docker", "inspect", "--format", "{{json .State}}", name),
                    ("docker", "logs", "--tail", "80", name)):
        try:
            run(*command, capture=True, timeout=DIAGNOSTIC_SECONDS, retain_output=True)
        except Exception as error:
            record(dict(stage="MinIO diagnostic unavailable", error=redact(str(error))))


@contextmanager
def container_scope(name):
    """Retain the primary failure even when diagnostics or cleanup also fail."""
    failed = False
    try:
        yield
    except BaseException as error:
        failed = True
        record(dict(stage="MinIO workload", error=redact(str(error))))
        diagnostics(name)
        raise
    finally:
        try:
            run("docker", "rm", "-fv", name, capture=True,
                timeout=CLEANUP_SECONDS, stage="MinIO cleanup " + name)
        except Exception:
            if not failed:
                diagnostics(name)
                raise
