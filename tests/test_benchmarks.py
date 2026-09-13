"""Synthetic numbers test calculations; these fixtures are never archived as runs."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("benchmark_archive", REPO / "tools/benchmarks.py")
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def report(phase="baseline", revision="abc", latency=100):
    return {
        "schema_version": 2, "feature": "segments", "phase": phase,
        "comparison_group": "segments-fixed-workload", "git_revision": revision,
        "measurement_protocol": "local-v2-individual-query-timers-rusage",
        "generator": "splitmix64-high24-uniform-f32-v1", "metric": "squared_euclidean",
        "config": {"scenario": "search", "rows": 10, "dimensions": 4, "queries": 2,
                   "samples": 3, "k": 2, "seed": 42, "root": "/disk", "label": "idle, AC"},
        "environment": {"git_revision": revision, "architecture": "aarch64", "cpu": "test CPU",
                        "memory": "16000000000", "os": "test OS", "rustc": "rustc test", "cargo": "cargo test",
                        "profile": "bench", "profile_environment": {}, "rustflags": None,
                        "cargo_encoded_rustflags": None, "debug_assertions": False, "logical_parallelism": 4,
                        "filesystem_mount": "Filesystem blocks used available capacity mount\n/dev/test 100 10 90 10% /disk"},
        "results": [{"scenario": "search", "backend": "local", "cache": "warm", "warmup_queries": 2,
                     "dataset_seed": 42, "query_seed": 99, "dataset_sha256": "data", "query_sha256": "queries",
                     "query_latency": {"p50_sample_ns": latency, "p95_sample_ns": latency*2,
                                       "p99_sample_ns": latency*3, "max_sample_ns": latency*4},
                     "timing": {"operations_per_timed_second": 1000},
                     "resources": {"user_cpu_ns": 100, "system_cpu_ns": 20, "process_max_rss_bytes": 10000},
                     "measured_store_calls": {"get_calls": 0, "create_calls": 0, "get_payload_bytes": 0, "create_payload_bytes": 0},
                     "inventory": {"logical_objects": 11, "physical_files": 22, "file_length_bytes": 1234}}],
    }


def rows(value):
    data = json.dumps(value).encode()
    return bench.normalize(value, "runs/test.json", bench.digest(data), 0)


class MetricsAndMetadata(unittest.TestCase):
    def test_actual_legacy_archive_is_supported_without_filling_missing_fields(self):
        data = (REPO / "benchmarks/baselines/2026-09-13-local.json").read_bytes()
        legacy = bench.reports(bench.decode(data))
        self.assertEqual(len(legacy), 9)
        for item in legacy:
            for row in rows(item):
                self.assertIsNone(row["feature"])
                self.assertIsNone(row["phase"])
                self.assertIsNone(row["comparison_group"])
                self.assertIsNone(row["comparison_key"])
                for key in ("p99_latency_ns", "user_cpu_ns", "system_cpu_ns", "process_max_rss_bytes"):
                    self.assertIsNone(row["metrics"][key])
        search = rows(legacy[0])[0]
        self.assertEqual(search["latency_scope"], "query batch")
        self.assertEqual(search["metrics"]["p50_latency_ns"], legacy[0]["results"][0]["timing"]["p50_sample_ns"])

    def test_metadata_inputs_environment_and_all_metrics_are_preserved(self):
        value = report()
        row = rows(value)[0]
        self.assertEqual(row["feature"], "segments")
        self.assertEqual(row["phase"], "baseline")
        self.assertEqual(row["git_revision"], "abc")
        self.assertEqual(row["workload"]["inputs"]["query_seed"], 99)
        self.assertEqual(row["workload"]["inputs"]["dataset_sha256"], "data")
        self.assertEqual(row["environment"], value["environment"])
        self.assertEqual(set(row["metrics"]), set(bench.METRICS))
        self.assertEqual(row["metrics"]["get_count"], 0)
        self.assertEqual(row["metrics"]["p99_latency_ns"], 300)
        self.assertEqual(row["metrics"]["file_footprint_bytes"], 1234)
        del value["results"][0]["query_latency"]["p99_sample_ns"]
        del value["results"][0]["measured_store_calls"]["get_calls"]
        self.assertIsNone(rows(value)[0]["metrics"]["p99_latency_ns"])
        self.assertIsNone(rows(value)[0]["metrics"]["get_count"])

    def test_partial_counter_samples_do_not_look_like_complete_totals(self):
        counts = {"get_calls": 2, "create_calls": 0, "get_payload_bytes": 5, "create_payload_bytes": 0}
        self.assertEqual(bench.totals([counts, counts])["get_calls"], 4)
        self.assertIsNone(bench.totals([counts, {}])["get_calls"])
        self.assertIsNone(bench.totals([])["get_calls"])
        self.assertIsNone(bench.delta(0, 10))
        self.assertIsNone(bench.delta(None, 10))
        self.assertEqual(bench.delta(100, 75), -25)
        self.assertIsNone(bench.delta(1e-300, 1e300))

    def test_recovery_counts_are_measured_totals_and_component_cpu_is_unknown(self):
        item = report()
        item["config"]["scenario"] = "recovery"
        result = item["results"][0]
        result["scenario"] = "recovery"
        result["total_open"] = result["query_latency"]
        result["local_store_open"] = result["query_latency"]
        result["database_replay"] = result["query_latency"]
        result["measured_store_calls_per_sample"] = [
            {"get_calls": 4, "create_calls": 0, "get_payload_bytes": 20, "create_payload_bytes": 0},
            {"get_calls": 4, "create_calls": 0, "get_payload_bytes": 20, "create_payload_bytes": 0},
        ]
        total, local, replay = rows(item)
        self.assertEqual(total["metrics"]["get_count"], 8)
        self.assertEqual(total["metrics"]["logical_bytes_read"], 40)
        self.assertEqual(total["metrics"]["user_cpu_ns"], 100)
        self.assertEqual(replay["metrics"]["get_count"], 8)
        self.assertIsNone(local["metrics"]["get_count"])
        self.assertIsNone(replay["metrics"]["user_cpu_ns"])

    def test_invalid_metadata_and_versions_are_rejected(self):
        for key, value in (("phase", "during"), ("feature", ""), ("comparison_group", " "), ("schema_version", 99)):
            item = report()
            item[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                bench.reports(item)
        with self.assertRaises(ValueError):
            bench.decode('{"metric": NaN}')


class Comparisons(unittest.TestCase):
    def test_matching_feature_before_after_and_signed_deltas(self):
        before, after = report("before", "old", 100), report("after", "new", 80)
        # Vary only transient disk utilization and revision; these do not change
        # the workload or stable environment.
        after["environment"]["filesystem_mount"] = "header\n/dev/test 100 20 80 20% /disk"
        pair = bench.comparisons(rows(before) + rows(after))[0]
        self.assertEqual(pair["delta_percent"]["p50_latency_ns"], -20)
        self.assertEqual(pair["delta_percent"]["throughput_ops_per_second"], 0)
        self.assertIsNone(pair["delta_percent"]["get_count"])
        summary = bench.markdown(rows(before) + rows(after), [pair])
        self.assertIn("segments / segments-fixed-workload / search", summary)
        self.assertIn("| p50_latency_ns | 100 | 80 | -20.000 |", summary)
        self.assertIn("old", summary)
        self.assertIn("new", summary)

    def test_incompatible_or_incomplete_pairs_are_not_compared(self):
        changes = [
            ("feature", "compaction"), ("comparison_group", "another-group"),
            ("measurement_protocol", "different timer"),
            ("config.rows", 20), ("config.dimensions", 8), ("config.seed", 43),
            ("config.samples", 9), ("config.scenario", "all"),
            ("environment.cpu", "other CPU"), ("environment.rustc", "other compiler"),
            ("environment.filesystem_mount", "header\n/dev/other 100 20 80 20% /disk"),
            ("results.0.dataset_sha256", "other data"), ("results.0.query_sha256", "other queries"),
            ("results.0.query_seed", 123), ("results.0.cache", "cold"),
        ]
        for path, replacement in changes:
            before, after = report("before"), report("after", "new")
            node = after
            keys = path.split(".")
            for key in keys[:-1]:
                node = node[int(key)] if isinstance(node, list) else node[key]
            node[keys[-1]] = replacement
            with self.subTest(path=path):
                self.assertEqual(bench.comparisons(rows(before) + rows(after)), [])
        after = report("after")
        del after["environment"]["cpu"]
        self.assertEqual(bench.comparisons(rows(report("before")) + rows(after)), [])

    def test_each_workload_is_paired_independently(self):
        all_rows = []
        for n in [10, 20]:
            for phase in ["before", "after"]:
                item = report(phase)
                item["config"]["rows"] = n
                all_rows.extend(rows(item))
        self.assertEqual(len(bench.comparisons(all_rows)), 2)

    def test_ambiguous_repeats_and_baselines_are_not_cherry_picked(self):
        before, after = rows(report("before")), rows(report("after", "new"))
        self.assertEqual(bench.comparisons(before + before + after), [])
        self.assertEqual(bench.comparisons(rows(report()) + after), [])
        self.assertIn("Unpaired runs", bench.markdown(before + before + after, []))


class Archival(unittest.TestCase):
    def test_archive_is_byte_preserving_idempotent_and_summary_is_rebuildable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "input.json"
            data = json.dumps(report(), indent=4).encode() + b"\n"
            source.write_bytes(data)
            archive = root / "archive"
            target = bench.archive(source, archive)
            self.assertEqual(target.read_bytes(), data)
            expected_summary = (archive / "SUMMARY.md").read_bytes()
            expected_index = (archive / "index.json").read_bytes()
            self.assertEqual(bench.archive(source, archive), target)
            self.assertEqual(len(list((archive / "runs").glob("*.json"))), 1)
            bench.summarize(archive, check=True)
            self.assertEqual((archive / "SUMMARY.md").read_bytes(), expected_summary)
            (archive / "index.json").unlink()
            (archive / "SUMMARY.md").unlink()
            bench.summarize(archive)
            self.assertEqual((archive / "index.json").read_bytes(), expected_index)
            self.assertEqual((archive / "SUMMARY.md").read_bytes(), expected_summary)
            (archive / "SUMMARY.md").write_text("stale")
            with self.assertRaisesRegex(ValueError, "stale"):
                bench.summarize(archive, check=True)
            self.assertEqual((archive / "SUMMARY.md").read_text(), "stale")
            target.write_text("corrupted")
            with self.assertRaisesRegex(ValueError, "digest mismatch"):
                bench.summarize(archive)
            with self.assertRaisesRegex(ValueError, "refusing to overwrite"):
                bench.archive(source, archive)

    def test_output_order_does_not_depend_on_import_order(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inputs = []
            for i, phase in enumerate(("before", "after")):
                path = root / f"{i}.json"
                path.write_text(json.dumps(report(phase, str(i), 100 - i*20)))
                inputs.append(path)
            for path in inputs:
                bench.archive(path, root / "a")
            for path in reversed(inputs):
                bench.archive(path, root / "b")
            for name in ("SUMMARY.md", "index.json"):
                self.assertEqual((root / "a" / name).read_bytes(), (root / "b" / name).read_bytes())

    def test_invalid_input_never_creates_an_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "bad.json"
            source.write_text("{}")
            with self.assertRaises(ValueError):
                bench.archive(source, root / "archive")
            self.assertFalse((root / "archive").exists())

    def test_legacy_archive_import_does_not_duplicate_original_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "baselines").mkdir()
            original = root / "baselines/legacy.json"
            data = (REPO / "benchmarks/baselines/2026-09-13-local.json").read_bytes()
            original.write_bytes(data)
            self.assertEqual(bench.archive(original, root), original)
            self.assertFalse((root / "runs").exists())
            self.assertEqual(original.read_bytes(), data)
            bench.summarize(root, check=True)


if __name__ == "__main__":
    unittest.main()
