"""Failure tests for CI process deadlines and diagnostic/cleanup behavior."""
from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch
import urllib.error

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import minio_harness as h


class MinioHarnessTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        p = patch.object(h, "ARTIFACTS", self.root / "diagnostics")
        p.start()
        self.addCleanup(p.stop)
        self.output = io.StringIO()
        for context in (redirect_stdout(self.output), redirect_stderr(self.output)):
            context.__enter__()
            self.addCleanup(context.__exit__, None, None, None)

    def events(self):
        return [json.loads(line) for line in (h.ARTIFACTS / "events.jsonl").read_text().splitlines()]

    def test_timeout_kills_descendants_and_keeps_failure_stage(self):
        marker = self.root / "survived"
        script = ("import subprocess,sys,time; "
                  "subprocess.Popen([sys.executable,'-c',"
                  "'import time,pathlib; time.sleep(0.6); pathlib.Path(sys.argv[1]).touch()',"
                  "sys.argv[1]]); time.sleep(10)")
        # Child imports sys explicitly; it must not outlive the timed-out parent.
        script = script.replace("import time,pathlib", "import time,pathlib,sys")
        start = time.monotonic()
        with self.assertRaisesRegex(h.HarnessFailure, "stuck test: exceeded"):
            h.run(sys.executable, "-c", script, str(marker), timeout=0.2, stage="stuck test")
        self.assertLess(time.monotonic() - start, 3)
        time.sleep(0.7)
        self.assertFalse(marker.exists())
        self.assertEqual(self.events()[0]["status"], "timeout")

    def test_authenticated_probe_obeys_remaining_readiness_deadline(self):
        response = MagicMock()
        response.__enter__.return_value.status = 200
        original = h.run
        def stuck_probe(*args, **kwargs):
            self.assertLessEqual(kwargs["timeout"], 0.1)
            return original(sys.executable, "-c", "import time; time.sleep(10)", **kwargs)
        start = time.monotonic()
        with patch.object(h.urllib.request, "urlopen", return_value=response), \
                patch.object(h, "run", side_effect=stuck_probe) as command:
            with self.assertRaisesRegex(h.HarnessFailure, "authenticated readiness: exceeded"):
                h.ready("http://unused", "container", timeout=0.1)
        self.assertEqual(command.call_count, 1)
        self.assertLess(time.monotonic() - start, 3)

    def test_failed_startup_has_a_deadline(self):
        with patch.object(h.urllib.request, "urlopen", side_effect=urllib.error.URLError("down")):
            with self.assertRaisesRegex(h.HarnessFailure, "health/authentication exceeded"):
                h.ready("http://unused", "container", timeout=0.02)

    def test_probe_nonzero_then_ready_is_startup_polling(self):
        response = MagicMock()
        response.__enter__.return_value.status = 200
        with patch.object(h.urllib.request, "urlopen", return_value=response), \
                patch.object(h, "run", side_effect=[h.HarnessFailure("auth", "exit 1", 1), ""]):
            h.ready("http://unused", "container", timeout=1)

    def test_test_failure_survives_stuck_cleanup(self):
        original = h.run
        def command(*args, **kwargs):
            if args[0] == "docker":
                self.assertEqual(kwargs["timeout"], 0.1)
                return original(sys.executable, "-c", "import time; time.sleep(10)", **kwargs)
            return original(*args, **kwargs)
        with patch.object(h, "CLEANUP_SECONDS", 0.1), \
                patch.object(h, "diagnostics") as diagnostic, patch.object(h, "run", side_effect=command):
            with self.assertRaisesRegex(h.HarnessFailure, "test failure: exit 7"):
                with h.container_scope("test"):
                    h.run(sys.executable, "-c", "raise SystemExit(7)", stage="test failure")
        diagnostic.assert_called_once_with("test")
        self.assertEqual(self.events()[-1]["status"], "timeout")

    def test_cleanup_failure_alone_fails_run(self):
        with patch.object(h, "diagnostics") as diagnostic, \
                patch.object(h, "run", side_effect=h.HarnessFailure("cleanup", "failed")):
            with self.assertRaisesRegex(h.HarnessFailure, "cleanup"):
                with h.container_scope("test"):
                    pass
        diagnostic.assert_called_once_with("test")

    def test_success_always_cleans_container(self):
        with patch.object(h, "run") as command:
            with h.container_scope("test"):
                pass
        command.assert_called_once_with("docker", "rm", "-fv", "test", capture=True,
                                        timeout=h.CLEANUP_SECONDS, stage="MinIO cleanup test")

    def test_secrets_are_removed_from_output_and_artifacts(self):
        secret = 'private-test-credential'
        env = dict(os.environ, AWS_SESSION_TOKEN=secret)
        with self.assertRaises(h.HarnessFailure):
            h.run(sys.executable, "-c",
                  "import os,sys; print(os.environ['AWS_SESSION_TOKEN']); "
                  "print(os.environ['AWS_SESSION_TOKEN'],file=sys.stderr); sys.exit(1)", env=env)
        self.assertNotIn(secret, self.output.getvalue())
        self.assertNotIn(secret, (h.ARTIFACTS / "events.jsonl").read_text())
        self.assertIn("[REDACTED]", self.events()[0]["output"])

    def test_diagnostics_are_bounded_and_do_not_mask_workload_error(self):
        with patch.object(h, "run", side_effect=h.HarnessFailure("docker unavailable", "timeout")) as command:
            with self.assertRaisesRegex(ValueError, "original"):
                with h.container_scope("test"):
                    raise ValueError("original")
        self.assertEqual(command.call_count, 3)
        self.assertEqual([c.kwargs["timeout"] for c in command.call_args_list],
                         [h.DIAGNOSTIC_SECONDS, h.DIAGNOSTIC_SECONDS, h.CLEANUP_SECONDS])


if __name__ == "__main__":
    unittest.main()
