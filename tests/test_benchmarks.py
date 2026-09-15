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
        self.assertIn("| p50 (ns) | 100 | 80 | -20 |", summary)
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
            expected_index = (archive / "latest.json").read_bytes()
            self.assertEqual(bench.archive(source, archive), target)
            self.assertEqual(len(list((archive / "runs").glob("*.json"))), 1)
            bench.summarize(archive, check=True)
            self.assertEqual((archive / "SUMMARY.md").read_bytes(), expected_summary)
            (archive / "latest.json").unlink()
            (archive / "SUMMARY.md").unlink()
            bench.summarize(archive)
            self.assertEqual((archive / "latest.json").read_bytes(), expected_index)
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
            for name in ("SUMMARY.md", "latest.json"):
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


class BackendReports(unittest.TestCase):
    @staticmethod
    def s3_report(phase="baseline"):
        item = report(phase)
        item["schema_version"] = 3
        item["config"]["backend"] = "s3"
        item["measurement_protocol"] = "s3-v1-individual-query-timers-rusage"
        item["environment"]["s3"] = {
            "endpoint": "https://storage.invalid", "region": "region", "bucket": "bucket",
            "namespace_prefix": "bench", "service_label": "test fixture"}
        result = item["results"][0]
        result["backend"] = "s3"
        result["measured_http_requests"] = dict(get=2, list=3, put=4, other=0,
            request_body_bytes=100, http_errors=1, transport_errors=2)
        result["inventory"] = dict(logical_objects=11, object_length_bytes=1200,
                                    physical_files=None, file_length_bytes=None)
        return item

    def test_s3_metrics_environment_and_json_roundtrip(self):
        item = self.s3_report()
        bench.reports(bench.decode(json.dumps(item)))
        row = rows(item)[0]
        self.assertEqual(row["backend"], "s3")
        self.assertEqual(row["metrics"]["http_list_count"], 3)
        self.assertEqual(row["metrics"]["http_put_count"], 4)
        self.assertEqual(row["metrics"]["request_body_bytes"], 100)
        self.assertEqual(row["metrics"]["http_error_count"], 1)
        self.assertEqual(row["metrics"]["transport_error_count"], 2)
        self.assertEqual(row["metrics"]["object_footprint_bytes"], 1200)
        self.assertIsNone(row["metrics"]["file_footprint_bytes"])
        self.assertIsNone(row["metrics"]["physical_file_count"])
        self.assertEqual(row["comparison_environment"]["s3"], item["environment"]["s3"])
        summary = bench.markdown([row], [])
        for value in ["s3", "https://storage.invalid", "test fixture", "segments", "abc"]:
            self.assertIn(value, summary)

    def test_backends_and_remote_environments_cannot_be_feature_pairs(self):
        before = report("before")
        after = self.s3_report("after")
        # Even if someone deliberately gives both the same feature/group/protocol.
        after["measurement_protocol"] = before["measurement_protocol"]
        self.assertEqual(bench.comparisons(rows(before) + rows(after)), [])
        before = self.s3_report("before")
        after = self.s3_report("after")
        self.assertEqual(len(bench.comparisons(rows(before) + rows(after))), 1)
        for field in ["endpoint", "bucket", "region", "namespace_prefix", "service_label"]:
            changed = self.s3_report("after")
            changed["environment"]["s3"][field] = "different"
            self.assertEqual(bench.comparisons(rows(before) + rows(changed)), [])

    def test_recovery_s3_totals_and_missing_metadata(self):
        item = self.s3_report()
        result = item["results"][0]
        result["scenario"] = "recovery"
        result["store_open"] = result["query_latency"]
        result["http_requests_per_sample"] = [result["measured_http_requests"]] * 2
        total, store, replay = rows(item)
        self.assertEqual(store["scope"], "recovery/s3-store")
        self.assertEqual(total["metrics"]["http_list_count"], 6)
        self.assertEqual(replay["metrics"]["http_get_count"], 4)
        self.assertIsNone(store["metrics"]["http_list_count"])
        result["http_requests_per_sample"].append({})
        self.assertIsNone(rows(item)[0]["metrics"]["http_get_count"])
        del item["environment"]["s3"]["endpoint"]
        with self.assertRaises(ValueError):
            bench.reports(item)

    def test_legacy_local_transport_is_unknown_not_zero(self):
        row = rows(report())[0]
        for field in ["http_get_count", "http_list_count", "http_put_count", "request_body_bytes", "http_error_count", "transport_error_count", "object_footprint_bytes"]:
            self.assertIsNone(row["metrics"][field])
        self.assertEqual(row["metrics"]["file_footprint_bytes"], 1234)


class SummaryPresentation(unittest.TestCase):
    def test_grouped_metrics_samples_and_missing_values(self):
        local = rows(report())
        remote = rows(BackendReports.s3_report())
        legacy = rows(bench.reports(bench.decode(
            (REPO / "benchmarks/baselines/2026-09-13-local.json").read_bytes()))[0])
        summary = bench.markdown(local + remote + legacy, [])
        self.assertIn("### local / search", summary)
        self.assertIn("### s3 / search", summary)
        self.assertIn("| search | 6 | individual query |", summary)
        self.assertIn("| search | 5 | query batch |", summary)
        self.assertIn("N/A", summary)
        self.assertNotIn("null", summary)
        self.assertIn("HTTP errors", summary)
        self.assertIn("Transport errors", summary)
        self.assertIn("Object bytes", summary)
        self.assertIn("File bytes", summary)
        self.assertIn("](runs/test.json)", summary)
        self.assertNotIn("comparison_key", summary)

    def test_every_measured_metric_is_rendered_without_mutating_inputs(self):
        row = rows(report())[0]
        row["metrics"] = {m: 10001 + i for i, m in enumerate(bench.METRICS)}
        original = json.dumps(row, sort_keys=True)
        summary = bench.markdown([row], [])
        for metric, value in row["metrics"].items():
            self.assertIn(bench.METRIC_LABELS[metric], summary)
            self.assertIn(f"| {value} |", summary)
        self.assertEqual(json.dumps(row, sort_keys=True), original)

    def test_run_and_environment_metadata_appear_once(self):
        item = report()
        item["results"] *= 2
        summary = bench.markdown(rows(item), [])
        self.assertEqual(summary.count("test CPU"), 1)
        self.assertEqual(summary.count("idle, AC"), 1)
        self.assertEqual(summary.count("segments-fixed-workload"), 1)
        self.assertEqual(summary.count("](runs/test.json)"), 1)
        self.assertIn("| 1 | 1 | 2 | 0 | 0 | 0 |", summary)

    def test_repeatability_and_smoke_are_not_feature_improvements(self):
        before, after = report("before"), report("after", latency=50)
        for item in (before, after):
            item["environment"]["source_sha256"] = "same-source"
        items = rows(before) + rows(after) + rows(BackendReports.s3_report())
        pairs = bench.comparisons(items)
        summary = bench.markdown(items, pairs)
        self.assertEqual(len(pairs), 1)
        self.assertIn("Repeatability; no feature effect", summary)
        self.assertIn("local-vs-S3 smoke results are not feature improvements", summary)
        self.assertIn("| 50 |", summary)
        self.assertIn("| -50 |", summary)
        after["environment"]["source_sha256"] = "different-source"
        items = rows(before) + rows(after)
        self.assertIn("Same revision; source changes unverified",
                      bench.markdown(items, bench.comparisons(items)))

    def test_sample_units_inventory_and_missing_configuration(self):
        row = rows(report())[0]
        self.assertEqual(bench.sample_count(row), 6)
        row["latency_scope"] = "query batch"
        self.assertEqual(bench.sample_count(row), 3)
        row["scope"] = "recovery/total"
        row["latency_scope"] = "full open"
        self.assertEqual(bench.sample_count(row), 3)
        row["scope"] = "commit/insert"
        row["workload"]["config"]["operations"] = 7
        self.assertEqual(bench.sample_count(row), 7)
        row["latency_scope"] = "inventory only"
        self.assertIsNone(bench.sample_count(row))
        row["scope"] = "search"
        row["latency_scope"] = "individual query"
        del row["workload"]["config"]["queries"]
        self.assertIsNone(bench.sample_count(row))

    def test_rendering_order_escaping_and_empty_archive(self):
        a, b = report("before", "old"), report("after", "new")
        a["feature"] = b["feature"] = "feature | <unsafe>"
        items = rows(a) + rows(b)
        pairs = bench.comparisons(items)
        summary = bench.markdown(items, pairs)
        self.assertEqual(summary, bench.markdown(list(reversed(items)), list(reversed(pairs))))
        self.assertIn("feature &#124; &lt;unsafe&gt;", summary)
        empty = bench.markdown([], [])
        self.assertIn("| 0 | 0 | 0 | 0 | 0 | 0 |", empty)
        self.assertIn("No unambiguous compatible", empty)


class SegmentBenchmarkMetadata(unittest.TestCase):
    def test_checkpoint_setup_is_visible_and_not_silently_paired(self):
        before, after = report("before"), report("after")
        after["config"]["checkpoint_at"] = 9
        values = rows(before) + rows(after)
        self.assertEqual(bench.comparisons(values), [])
        summary = bench.markdown(values, [])
        self.assertIn("checkpoint_at=9", summary)
        self.assertIn("Unpaired runs", summary)


class CompactInspectionAndComparison(unittest.TestCase):
    def test_latest_is_explicit_deterministic_and_separate_from_pairing(self):
        old, new = report("baseline"), report("after", "new")
        old["environment"]["unix_seconds"] = 10
        new["environment"]["unix_seconds"] = 20
        values = rows(old) + rows(new)
        selected = bench.latest_entries(values)
        self.assertEqual([x["git_revision"] for x in selected], ["abc", "new"])
        self.assertEqual(selected, bench.latest_entries(list(reversed(values))))
        self.assertEqual(bench.comparisons(values), [])
        self.assertEqual(bench.latest_entries(rows(report())), [])
        new["environment"]["unix_seconds"] = 10
        tied = rows(old) + rows(new)
        self.assertEqual(bench.latest_entries(tied), bench.latest_entries(list(reversed(tied))))

    def test_full_outputs_are_optional_rebuildable_and_raw_is_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "input.json"
            source.write_text(json.dumps(report()))
            target = bench.archive(source, root / "archive")
            raw = target.read_bytes()
            self.assertFalse((target.parent.parent / "index.json").exists())
            bench.summarize(target.parent.parent, full=True)
            bench.summarize(target.parent.parent, check=True, full=True)
            index = json.loads((target.parent.parent / "index.json").read_text())
            self.assertEqual(index["rows"][0]["environment"], report()["environment"])
            self.assertIn("Benchmark archive", (target.parent.parent / "HISTORY.md").read_text())
            self.assertEqual(target.read_bytes(), raw)

    def test_comparison_is_explicit_no_latency_gate_and_checks_counters(self):
        with tempfile.TemporaryDirectory() as directory:
            a, b = Path(directory) / "a.json", Path(directory) / "b.json"
            before, after = report(), report(latency=100000)
            a.write_text(json.dumps(before)); b.write_text(json.dumps(after))
            result = bench.compare_files(a, b, True)
            self.assertFalse(result["counter_check_failed"])
            after["results"][0]["measured_store_calls"]["get_calls"] = 1
            b.write_text(json.dumps(after))
            self.assertTrue(bench.compare_files(a, b, True)["counter_check_failed"])
            self.assertFalse(bench.compare_files(a, b)["counter_check_failed"])
            after["config"]["seed"] = 43
            b.write_text(json.dumps(after))
            with self.assertRaisesRegex(ValueError, "incompatible"):
                bench.compare_files(a, b)
            before["environment"].pop("cpu")
            a.write_text(json.dumps(before))
            with self.assertRaisesRegex(ValueError, "incomplete"):
                bench.compare_files(a, b)

    def test_compare_cli_reports_counter_regression_and_missing_data(self):
        import subprocess
        import sys
        with tempfile.TemporaryDirectory() as directory:
            a, b = Path(directory) / "a.json", Path(directory) / "b.json"
            before, after = report(), report()
            after["results"][0]["measured_store_calls"].pop("get_calls")
            a.write_text(json.dumps(before)); b.write_text(json.dumps(after))
            command = [sys.executable, str(REPO / "tools/benchmarks.py"), "compare", str(a), str(b)]
            checked = subprocess.run(command + ["--check-counters"], capture_output=True, text=True)
            self.assertEqual(checked.returncode, 1)
            data = json.loads(checked.stdout)
            self.assertTrue(data["counter_check_failed"])
            self.assertEqual(len(data["comparisons"][0]["before_raw_sha256"]), 64)
            advisory = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(advisory.returncode, 0)


    def test_compaction_runner_preserves_existing_reports_and_validation(self):
        import sys
        sys.path.insert(0, str(REPO / "tools"))
        import compaction_benchmark as compaction
        for backend, filename in [
            ("local", "8e922662f8e08c4a87fc61937389763456c862b684c416ba6bc743e56f908a58"),
            ("s3", "78f2dbf2f5dc871683a6c51e0a6f3337031afe7b75e709a4f721d73169e6be2e")]:
            document = json.loads((REPO / f"benchmarks/runs/{filename}.json").read_text())
            compaction.validate(document, backend, True)
            command = compaction.command(backend)
            self.assertIn("--locked", command)
            self.assertEqual(command[command.index("--checkpoint-at") + 1], "300")
            self.assertEqual(command[command.index("--compact-at") + 1], "300")
            normalized = rows(document)[0]
            self.assertEqual(normalized["maintenance"]["compaction"]["removes"], 301)
            document["results"][0]["compaction"]["removes"] = 300
            with self.assertRaises(AssertionError):
                compaction.validate(document, backend, True)
        remote = BackendReports.s3_report()
        remote["results"][0]["measured_http_requests"]["delete"] = 7
        self.assertEqual(rows(remote)[0]["metrics"]["http_delete_count"], 7)
        self.assertIsNone(rows(report())[0]["metrics"]["http_delete_count"])


if __name__ == "__main__":
    unittest.main()
