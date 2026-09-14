"""Validate real small all-scenario benchmark output, not latency thresholds."""
from benchmarks import reports


def validate(report, backend, secrets=()):
    import json
    reports(report)
    encoded = json.dumps(report)
    for secret in secrets:
        assert not secret or secret not in encoded, "credential leaked into benchmark JSON"
    assert report["schema_version"] == (2 if backend == "local" else 3)
    assert [r["scenario"] for r in report["results"]] == ["search", "commit", "recovery"]
    if backend == "local":
        from pathlib import Path
        legacy = json.loads((Path(__file__).resolve().parents[1] / "benchmarks/runs/73aef1437e76edc44d9f2e0282c28d66da452bf9ad1fba9054f33a69c8d5d4c9.json").read_text())
        def same_shape(actual, original, path=""):
            if path == "/environment/profile_environment":
                return  # This is a user-supplied map, not fixed schema fields.
            if isinstance(original, dict):
                assert isinstance(actual, dict) and actual.keys() == original.keys(), path
                for key in original:
                    same_shape(actual[key], original[key], path + "/" + key)
            elif isinstance(original, list) and original:
                assert isinstance(actual, list) and actual, path
                if path in ("/results", "/results/1/phases"):
                    assert len(actual) == len(original), path
                    for i, (a, b) in enumerate(zip(actual, original)):
                        same_shape(a, b, path + "/" + str(i))
                else:
                    same_shape(actual[0], original[0], path + "/0")
        same_shape(report, legacy)
    config = report["config"]
    for result in report["results"]:
        assert result["backend"] == backend
        inventory = result["inventory"]
        assert inventory["logical_objects"] > 0
        if backend == "local":
            assert inventory["physical_files"] == inventory["logical_objects"] * 2
            assert inventory["file_length_bytes"] > 0
            assert "object_length_bytes" not in inventory
            assert "namespace" not in result
        else:
            assert inventory["physical_files"] is None
            assert inventory["file_length_bytes"] is None
            assert inventory["object_length_bytes"] > 0
            assert result["namespace"].startswith(report["environment"]["s3"]["namespace_prefix"] + "/")
        timing = result.get("query_latency", result.get("total_open"))
        records = result.get("phases", [result])
        for record in records:
            samples = record["timing"] if result["scenario"] == "commit" else timing
            for key in ("p50_sample_ns", "p95_sample_ns", "p99_sample_ns", "max_sample_ns", "operations_per_timed_second"):
                assert isinstance(samples[key], (int, float)) and samples[key] >= 0
            for key in ("user_cpu_ns", "system_cpu_ns", "process_max_rss_bytes"):
                assert key in record["resources"]  # unsupported platforms may report null
        if result["scenario"] == "recovery":
            assert len(result["measured_store_calls_per_sample"]) == config["samples"]
            if backend == "local":
                assert "local_store_open" in result and "store_open" not in result
                assert "http_requests_per_sample" not in result
            else:
                assert "store_open" in result and "local_store_open" not in result
                for c in result["http_requests_per_sample"]:
                    assert c["get"] == config["mutations"] + 1
                    assert c["list"] == 1  # smoke workload fits one native page
                    assert c["put"] == c["request_body_bytes"] == c["http_errors"] == c["transport_errors"] == 0
        elif result["scenario"] == "commit":
            for p in result["phases"]:
                assert p["measured_store_calls"]["create_calls"] == config["operations"]
                if backend == "local":
                    assert "measured_http_requests" not in p
                else:
                    c = p["measured_http_requests"]
                    assert c["put"] == config["operations"]
                    assert c["get"] == c["list"] == c["http_errors"] == c["transport_errors"] == 0
                    assert c["request_body_bytes"] == p["measured_store_calls"]["create_payload_bytes"] + 48 * config["operations"]
        else:
            assert all(c == 0 for c in result["measured_store_calls"].values())
            if backend == "local":
                assert "measured_http_requests" not in result
            else:
                assert all(c == 0 for c in result["measured_http_requests"].values())
                assert result["build_http_requests"]["put"] == config["rows"] + 1
    assert report["feature"] and report["phase"] == "baseline" and report["comparison_group"]
    assert "git_revision" in report and report["environment"]["cpu"]
