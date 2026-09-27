"""Probe-only diagnostics must not silently expand into another soak."""
from contextlib import nullcontext
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import m19_benchmark as runner


class ProbeOnlyTests(unittest.TestCase):
    def test_passing_probe_does_not_launch_final_or_verify(self):
        phases = []

        def run(*args, **kwargs):
            if args[0] == "target/release/examples/m19_capacity":
                phases.append(args[1:5])
                Path(args[6]).write_text(json.dumps({"performance_accepted": True}))
            if args[:2] == ("docker", "port"):
                return "127.0.0.1:9000\n"
            return "test"

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            with patch.object(sys, "argv", ["m19_benchmark", str(output), "--smoke", "--probe-only", "10000"]), \
                    patch.object(runner, "run", side_effect=run), \
                    patch.object(runner, "ready"), \
                    patch.object(runner, "container_scope", return_value=nullcontext()):
                runner.main()
            self.assertEqual(phases, [("prepare", "10000", "2", "smoke"),
                                      ("serve", "10000", "2", "smoke")])
            self.assertIsNone(json.loads((output / "decision.json").read_text())["final"])


if __name__ == "__main__":
    unittest.main()
