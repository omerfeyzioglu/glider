#!/usr/bin/env python3
"""Immutable raw benchmark reports and deterministic, conservative comparisons.

Standard library only. Run `python3 tools/benchmarks.py --help`.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
from urllib.parse import quote

METRICS = (
    "p50_latency_ns", "p95_latency_ns", "p99_latency_ns", "max_latency_ns",
    "throughput_ops_per_second", "user_cpu_ns", "system_cpu_ns",
    "process_max_rss_bytes", "logical_bytes_read", "logical_bytes_written",
    "get_count", "create_count", "logical_object_count", "physical_file_count",
    "file_footprint_bytes", "list_count", "object_footprint_bytes",
    "http_get_count", "http_list_count", "http_put_count", "http_other_count",
    "request_body_bytes", "http_error_count", "transport_error_count", "http_delete_count",
)
HTTP_FIELDS = ("get", "list", "put", "delete", "other", "request_body_bytes", "http_errors", "transport_errors")
COUNT_FIELDS = ("list_calls", "get_calls", "create_calls", "get_payload_bytes", "create_payload_bytes")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def reject_constant(value):
    raise ValueError(f"non-finite JSON number: {value}")


def decode(data):
    return json.loads(data, parse_constant=reject_constant)


def reports(document):
    if not isinstance(document, dict):
        raise ValueError("expected a report object")
    if "archive_version" in document:
        if document["archive_version"] != 1 or not isinstance(document.get("runs"), list):
            raise ValueError("unsupported legacy archive")
        runs = document["runs"]
    else:
        runs = [document]
    if not runs:
        raise ValueError("empty archive")
    for run in runs:
        if not isinstance(run, dict) or run.get("schema_version") not in (1, 2, 3):
            raise ValueError("unsupported report schema")
        if not isinstance(run.get("config"), dict) or not isinstance(run.get("environment"), dict):
            raise ValueError("report must contain config and environment")
        if not isinstance(run.get("results"), list) or not run["results"]:
            raise ValueError("report must contain results")
        if run["schema_version"] >= 2:
            for key in ("feature", "comparison_group", "measurement_protocol"):
                if not isinstance(run.get(key), str) or not run[key].strip():
                    raise ValueError(f"missing {key}")
            if run.get("phase") not in ("baseline", "before", "after"):
                raise ValueError("invalid report phase")
            if "git_revision" not in run:
                raise ValueError("missing git_revision")
        if run["schema_version"] == 3:
            s3 = obj(run["environment"].get("s3"))
            if run["config"].get("backend") != "s3" or not all(isinstance(s3.get(k), str) and s3[k].strip() for k in ("endpoint", "bucket", "region", "namespace_prefix")):
                raise ValueError("S3 report requires backend and S3 environment metadata")
        for result in run["results"]:
            if not isinstance(result, dict):
                raise ValueError("expected a result object")
            if result.get("backend") not in (None, "local", "s3"):
                raise ValueError("invalid result backend")
            if run["schema_version"] == 3 and result.get("backend") != "s3":
                raise ValueError("S3 report contains mismatched result backend")
            if not isinstance(result, dict) or result.get("scenario") not in ("search", "commit", "recovery"):
                raise ValueError("unknown result scenario")
            if result["scenario"] == "commit":
                phases = result.get("phases")
                if not isinstance(phases, list) or any(not isinstance(p, dict) for p in phases) or [p.get("phase") for p in phases] != ["insert", "overwrite", "delete"]:
                    raise ValueError("commit report must have insert/overwrite/delete phases")
    return runs


def number(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0 and math.isfinite(value):
        return value
    return None


def obj(value):
    return value if isinstance(value, dict) else {}


def totals(samples, fields=COUNT_FIELDS):
    """Sum complete measured counters, never substitute zero for missing samples."""
    result = {}
    for key in fields:
        values = [number(obj(s).get(key)) for s in samples] if isinstance(samples, list) else []
        result[key] = sum(values) if values and all(v is not None for v in values) else None
    return result


def metrics(timing=None, counts=None, resources=None, inventory=None, throughput=None, http=None):
    timing, counts, resources, inventory = map(obj, (timing, counts, resources, inventory))
    # Old reports retain the metrics they actually recorded. In particular, do
    # not reconstruct missing p99/CPU/RSS or treat installed RAM as process RSS.
    result = {key: None for key in METRICS}
    for percentile in (50, 95, 99):
        result[f"p{percentile}_latency_ns"] = number(timing.get(f"p{percentile}_sample_ns"))
    result["max_latency_ns"] = number(timing.get("max_sample_ns"))
    result["throughput_ops_per_second"] = number(obj(throughput if throughput is not None else timing).get("operations_per_timed_second"))
    for key in ("user_cpu_ns", "system_cpu_ns", "process_max_rss_bytes"):
        result[key] = number(resources.get(key))
    for dest, source in (("logical_bytes_read", "get_payload_bytes"), ("logical_bytes_written", "create_payload_bytes"), ("get_count", "get_calls"), ("list_count", "list_calls"), ("create_count", "create_calls")):
        result[dest] = number(counts.get(source))
    for dest, source in (("object_footprint_bytes", "object_length_bytes"), ("logical_object_count", "logical_objects"), ("physical_file_count", "physical_files"), ("file_footprint_bytes", "file_length_bytes")):
        result[dest] = number(inventory.get(source))
    for dest, source in (("http_get_count", "get"), ("http_list_count", "list"), ("http_put_count", "put"),
                         ("http_other_count", "other"), ("http_delete_count", "delete"), ("request_body_bytes", "request_body_bytes"),
                         ("http_error_count", "http_errors"), ("transport_error_count", "transport_errors")):
        result[dest] = number(obj(http).get(source))
    return result


def environment_identity(run):
    env = run["environment"]
    # df capacity/free-space changes every run. Compare the actual device and
    # mount, not transient utilization or the command's header spacing.
    mount = env.get("filesystem_mount")
    columns = mount.splitlines()[-1].split() if isinstance(mount, str) else []
    filesystem = [columns[0], columns[-1]] if len(columns) >= 6 else None
    fields = ("architecture", "cpu", "memory", "os", "rustc", "cargo", "profile",
              "profile_environment", "rustflags", "cargo_encoded_rustflags", "debug_assertions", "logical_parallelism")
    identity = {key: env.get(key) for key in fields}
    identity.update(filesystem=filesystem, root=run["config"].get("root"), label=run["config"].get("label"))
    complete = all(env.get(key) is not None for key in ("architecture", "cpu", "memory", "os", "rustc", "cargo", "profile", "logical_parallelism"))
    complete = complete and all(key in env for key in fields) and filesystem is not None and identity["root"] is not None and identity["label"] is not None
    if run["config"].get("backend") == "s3":
        identity["s3"] = env.get("s3")
        complete = complete and all(obj(identity["s3"]).get(k) for k in ("endpoint", "bucket", "region", "namespace_prefix"))
    return identity, complete


def normalize(run, raw_file, raw_hash, run_index):
    rows = []
    env_key, env_complete = environment_identity(run)
    for result_index, result in enumerate(run["results"]):
        scenario = result["scenario"]
        if scenario == "search":
            timing_key = "query_latency" if run["schema_version"] >= 2 else "timing"
            scopes = [("search", result, metrics(result.get(timing_key), result.get("measured_store_calls"), result.get("resources"), result.get("inventory"), result.get("timing"), http=result.get("measured_http_requests")), "individual query" if timing_key == "query_latency" else "query batch")]
        elif scenario == "commit":
            scopes = [(f"commit/{p['phase']}", p, metrics(p.get("timing"), p.get("measured_store_calls"), p.get("resources"), http=p.get("measured_http_requests")), "individual commit") for p in result["phases"]]
            # The original harness inventories only after the final phase. Do
            # not invent per-phase footprints from object-format assumptions.
            scopes.append(("commit/final-footprint", result, metrics(inventory=result.get("inventory")), "inventory only"))
        else:
            counts = totals(result.get("measured_store_calls_per_sample"))
            http = totals(result.get("http_requests_per_sample"), HTTP_FIELDS)
            scopes = [
                ("recovery/total", result, metrics(result.get("total_open"), counts, result.get("resources"), result.get("inventory"), http=http), "full open"),
                ("recovery/s3-store" if result.get("backend") == "s3" else "recovery/local-store", result, metrics(result.get("store_open" if result.get("backend") == "s3" else "local_store_open")), "S3 client construction" if result.get("backend") == "s3" else "local store open"),
                ("recovery/replay", result, metrics(result.get("database_replay"), counts, http=http), "database replay"),
            ]
        for scope, source, values, latency_scope in scopes:
            if scenario == "search" and "ann" in result:
                ann = result["ann"]
                values["index_build_latency_ns"] = number(ann.get("build_ns"))
                for name, samples in (
                    ("recall_at_k", ann.get("recall_at_k", [])),
                    ("vector_distances_per_query", [v["vector"] for v in ann.get("distance_evaluations", [])]),
                    ("centroid_distances_per_query", [v["centroid"] for v in ann.get("distance_evaluations", [])]),
                ):
                    values[name] = sum(samples) / len(samples) if samples else None
            config = {k: v for k, v in run["config"].items() if k not in ("feature", "phase", "comparison_group", "root", "label")}
            inputs = {key: source.get(key) for key in ("dataset_seed", "query_seed", "dataset_sha256", "query_sha256")}
            if scope == "commit/final-footprint":
                inputs["phase_inputs"] = [{key: p.get(key) for key in ("phase", "dataset_seed", "dataset_sha256")} for p in result["phases"]]
            workload = {"config": config, "inputs": inputs, "generator": run.get("generator"), "metric": run.get("metric"),
                        "backend": result.get("backend"), "cache": result.get("cache"), "history": result.get("history"),
                        "warmup_queries": result.get("warmup_queries"), "warmup_reopens": result.get("warmup_reopens"), "warmup_operations": result.get("warmup_operations")}
            identity = {"scope": scope, "latency_scope": latency_scope, "workload": workload,
                        "measurement_protocol": run.get("measurement_protocol"), "environment": env_key}
            required_inputs = bool(inputs.get("dataset_sha256"))
            if scope == "commit/final-footprint":
                required_inputs = all(p.get("dataset_sha256") for p in inputs["phase_inputs"])
            elif scope != "commit/delete":
                required_inputs = required_inputs and inputs["dataset_seed"] is not None
            if scenario == "search":
                required_inputs = required_inputs and isinstance(inputs.get("query_sha256"), str) and inputs["dataset_seed"] is not None and inputs["query_seed"] is not None
            required_config = {"scenario", "dimensions", "seed"}
            required_config.update({"rows", "queries", "samples", "k"} if scenario == "search" else {"rows", "mutations", "samples"} if scenario == "recovery" else {"operations"})
            complete = env_complete and required_inputs and required_config.issubset(config) and all(run.get(k) for k in ("feature", "comparison_group", "measurement_protocol", "generator", "metric")) and bool(result.get("backend"))
            revision = run.get("git_revision", run["environment"].get("git_revision"))
            complete = bool(complete and revision)
            state = result["phases"][-1] if scope == "commit/final-footprint" else source
            observed = {"live_documents": state.get("live_documents_after", state.get("live_documents")),
                        "mutation_history": state.get("mutation_history_after", state.get("mutation_history"))}
            rows.append({"id": f"{raw_hash[:12]}:{run_index}:{result_index}:{scope}", "raw_file": raw_file, "raw_sha256": raw_hash,
                         "run_index": run_index, "feature": run.get("feature"), "phase": run.get("phase"),
                         "comparison_group": run.get("comparison_group"), "git_revision": revision,
                         "backend": result.get("backend"), "scope": scope, "latency_scope": latency_scope, "workload": workload, "observed_state": observed,
                         "environment": run["environment"], "comparison_environment": env_key,
                         "measurement_protocol": run.get("measurement_protocol"),
                         "comparison_key": digest(canonical(identity).encode()) if complete else None,
                         "maintenance": {k: result[k] for k in ("checkpoint", "compaction") if k in result} if scope == "recovery/total" else {},
                         "metrics": values})
    return rows


def delta(before, after):
    if before is None or after is None or before == 0:
        return None
    value = (after - before) / before * 100
    return value if math.isfinite(value) else None


def comparisons(rows):
    groups = {}
    for row in rows:
        if row["comparison_key"] is not None and row["phase"] in ("before", "after"):
            key = (row["feature"], row["comparison_group"], row["comparison_key"])
            groups.setdefault(key, {"before": [], "after": []})[row["phase"]].append(row)
    pairs = []
    for key, sides in sorted(groups.items()):
        # No silently chosen 'latest' run and no averaging of unrelated repeats.
        if len(sides["before"]) == len(sides["after"]) == 1:
            before, after = sides["before"][0], sides["after"][0]
            pairs.append({"feature": key[0], "comparison_group": key[1], "comparison_key": key[2],
                          "before": before["id"], "after": after["id"],
                          "delta_percent": {m: delta(before["metrics"][m], after["metrics"][m]) for m in METRICS}})
    return pairs


def text(value):
    if value is None:
        return "N/A"
    return str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("|", "&#124;").replace("\r", " ").replace("\n", " ").replace("`", "&#96;")


def display(value):
    if value is None:
        return "N/A"
    return str(value) if isinstance(value, int) else f"{value:.3f}".rstrip("0").rstrip(".")


# Presentation only: normalization, archive bytes and comparison eligibility stay unchanged.
METRIC_LABELS = dict(zip(METRICS, (
    "p50 (ns)", "p95 (ns)", "p99 (ns)", "Max (ns)", "Ops/s",
    "User CPU (ns)", "System CPU (ns)", "Peak RSS (B)",
    "Read (B)", "Written (B)", "GET", "CREATE", "Objects", "Files",
    "File bytes", "LIST", "Object bytes", "HTTP GET", "HTTP LIST", "HTTP PUT",
    "HTTP other", "Request body (B)", "HTTP errors", "Transport errors", "HTTP DELETE",
)))


def sample_count(row):
    config = row["workload"]["config"]
    if row["latency_scope"] == "inventory only":
        return None
    if row["scope"].startswith("commit/"):
        return config.get("operations")
    samples = config.get("samples")
    if row["latency_scope"] == "individual query":
        queries = config.get("queries")
        return samples * queries if samples is not None and queries is not None else None
    return samples


def markdown(rows, pairs):
    rows = sorted(rows, key=lambda r: r["id"])
    run_key = lambda r: (r["raw_file"], r["raw_sha256"], r["run_index"])
    runs = {}
    for row in rows:
        runs.setdefault(run_key(row), row)
    run_ids = {key: f"R{i}" for i, key in enumerate(sorted(runs), 1)}
    environments = sorted({canonical(r["comparison_environment"]) for r in rows})
    env_ids = {key: f"E{i}" for i, key in enumerate(environments, 1)}
    by_id = {r["id"]: r for r in rows}
    used = {p[side] for p in pairs for side in ("before", "after")}
    unmatched = [r for r in rows if r["phase"] in ("before", "after") and r["id"] not in used]

    def ref(row):
        return run_ids[run_key(row)]

    def table(headers, values):
        return ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)] + [
            "| " + " | ".join(text(v) for v in cells) + " |" for cells in values]

    def pair_kind(before, after):
        fingerprint = before["environment"].get("source_sha256")
        if fingerprint and fingerprint == after["environment"].get("source_sha256"):
            return "Repeatability; no feature effect"
        if before["git_revision"] == after["git_revision"]:
            return "Same revision; source changes unverified"
        return "Before/after; attribution requires source review"

    repeatability = sum(pair_kind(by_id[p["before"]], by_id[p["after"]]).startswith("Repeatability") for p in pairs)
    lines = ["# Benchmark archive", "", "Generated from raw JSON; do not edit. Full metadata and exact metric keys: [index.json](index.json).", ""]
    lines += table(["Raw reports", "Runs", "Result rows", "Valid pairs", "Repeatability pairs", "Unpaired before/after rows"],
                   [[len({r["raw_sha256"] for r in rows}), len(runs), len(rows), len(pairs), repeatability, len(unmatched)]])
    lines += ["", "## Before / after", "",
              "Pairs require one before and one after with identical backend, workload, inputs, protocol and stable environment. Delta is (after − before) / before; positive means an increase, not necessarily an improvement.",
              "Repeatability and local-vs-S3 smoke results are not feature improvements."]
    if not pairs:
        lines += ["", "No unambiguous compatible before/after pair is available."]
    for pair in sorted(pairs, key=lambda p: (p["feature"], p["comparison_group"], p["before"], p["after"])):
        before, after = by_id[pair["before"]], by_id[pair["after"]]
        lines += ["", f"### {text(pair['feature'])} / {text(pair['comparison_group'])} / {text(before['scope'])}", ""]
        lines += table(["Backend", "Before → after", "Samples each", "Latency scope", "Interpretation"],
                       [[before["backend"], f"{ref(before)} → {ref(after)}", sample_count(before), before["latency_scope"], pair_kind(before, after)]])
        names = [m for m in METRICS if before["metrics"][m] is not None or after["metrics"][m] is not None]
        lines += [""] + table(["Metric", "Before", "After", "Change %"],
                              [[METRIC_LABELS[m], display(before["metrics"][m]), display(after["metrics"][m]), display(pair["delta_percent"][m])] for m in names])
    if unmatched:
        lines += ["", "Unpaired runs: " + ", ".join(sorted({ref(r) for r in unmatched})) + ". Missing counterpart/metadata, incompatible inputs/environment, or duplicate phase; no percentage is inferred."]

    lines += ["", "## Run catalog", "", "Run references apply to every table below. All archived runs are retained; no latest-run selection or averaging.", ""]
    catalog = []
    for key in sorted(runs):
        row = runs[key]
        config = row["workload"]["config"]
        scenario = config.get("scenario")
        fields = {"search": ("rows", "queries", "samples", "k"),
                  "commit": ("operations",), "recovery": ("rows", "mutations", "samples")}
        keys = ("scenario", "dimensions", "seed", "checkpoint_at", "compact_at") + fields.get(scenario, ("rows", "mutations", "operations", "queries", "samples", "k"))
        workload = "; ".join(f"{k}={config[k]}" for k in keys if k in config)
        catalog.append([f"[{ref(row)}]({quote(row['raw_file'], safe='/')})", row["backend"], row["feature"], row["phase"], row["comparison_group"], row["git_revision"][:12] if row["git_revision"] else None, env_ids[canonical(row["comparison_environment"]) ], workload])
    # Links are generated from escaped paths; table cells still escape untrusted metadata.
    lines += table(["Run / raw JSON", "Backend", "Feature", "Phase", "Group", "Git (short)", "Env", "Workload"], catalog)
    lines += ["", "### Environments", ""]
    env_rows = []
    for key in environments:
        env = json.loads(key)
        remote = obj(env.get("s3"))
        service = "; ".join(str(remote[k]) for k in ("endpoint", "region", "bucket", "service_label") if remote.get(k)) or None
        env_rows.append([env_ids[key], env.get("cpu"), env.get("architecture"), env.get("label"), service])
    lines += table(["Env", "CPU", "Arch", "Conditions", "S3 service"], env_rows)

    lines += ["", "## Results by backend and scenario", "",
              "Latency and CPU: ns; throughput: operations/s; memory, I/O and footprints: bytes. N/A means unavailable or not applicable, never zero. Entirely unmeasured metric columns/rows are omitted.",
              "Samples count timed queries (queries × batches), legacy query batches, individual commits, or reopens according to the latency scope."]
    groups = sorted({(r["backend"] or "unknown", r["scope"].split("/")[0]) for r in rows})
    sections = (
        ("Latency", METRICS[:5]),
        ("Client process resources", METRICS[5:8]),
        ("Logical I/O", ("get_count", "list_count", "create_count", "logical_bytes_read", "logical_bytes_written")),
        ("Footprint", ("logical_object_count", "physical_file_count", "file_footprint_bytes", "object_footprint_bytes")),
        ("HTTP requests", METRICS[17:]),
    )
    for backend, scenario in groups:
        group = sorted((r for r in rows if (r["backend"] or "unknown") == backend and r["scope"].split("/")[0] == scenario),
                       key=lambda r: (run_key(r), {"insert": 0, "overwrite": 1, "delete": 2, "final-footprint": 3, "total": 0, "local-store": 1, "s3-store": 1, "replay": 2}.get(r["scope"].split("/")[-1], 0), r["id"]))
        lines += ["", f"### {text(backend)} / {text(scenario)}"]
        for title, metrics in sections:
            names = [m for m in metrics if any(r["metrics"][m] is not None for r in group)]
            if not names:
                continue
            values = []
            for row in group:
                if not any(row["metrics"][m] is not None for m in names):
                    continue
                cells = [ref(row), row["scope"].split("/", 1)[-1]]
                if title == "Latency":
                    cells += [sample_count(row), row["latency_scope"]]
                if title == "Footprint":
                    cells += [row["observed_state"].get("live_documents"), row["observed_state"].get("mutation_history")]
                values.append(cells + [display(row["metrics"][m]) for m in names])
            headers = ["Run", "Scope"]
            if title == "Latency":
                headers += ["Samples", "Latency scope"]
            if title == "Footprint":
                headers += ["Live docs", "Mutation history"]
            lines += ["", f"**{title}**", ""] + table(headers + [METRIC_LABELS[m] for m in names], values)

    lines += ["", "## Measurement notes", "",
              "- Recovery loads a checkpoint plus its tail, or the full log when disabled. LocalStore still validates and synchronizes all retained files. Recovery total/store/replay rows overlap; do not sum them.",
              "- CPU covers measured loops; RSS is the client process lifetime peak including setup. Counters are totals across measured operations/reopens. HTTP bytes count request bodies, not wire traffic; logical bytes exclude envelopes. Untimed inventory and search setup requests are excluded from measured I/O.",
              "- Legacy batch latency is not per-query latency. Small samples do not establish population tails; uncontrolled load/cache and MinIO smoke runs do not establish production or cross-backend speedups. Missing measurements and zero-denominator deltas remain N/A.", ""]
    return "\n".join(lines)


def load_archive(root):
    rows, seen = [], set()
    for path in sorted([*root.glob("baselines/*.json"), *root.glob("runs/*.json")]):
        data = path.read_bytes()
        raw_hash = digest(data)
        if path.parent.name == "runs" and path.stem != raw_hash:
            raise ValueError(f"raw archive digest mismatch: {path}")
        if raw_hash in seen:
            continue
        seen.add(raw_hash)
        for index, report in enumerate(reports(decode(data))):
            rows.extend(normalize(report, path.relative_to(root).as_posix(), raw_hash, index))
    return sorted(rows, key=lambda r: (r["feature"] or "", r["comparison_group"] or "", r["scope"], canonical(r["workload"]), r["phase"] or "", r["id"]))


def publish(path, data):
    """Replace a derived file atomically; raw paths are SHA-256 content addressed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as file:
            temporary = Path(file.name)
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
        if os.name == "posix":
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def latest_entries(rows):
    """Inspection pointers only. Never used to select comparison counterparts."""
    entries = []
    for role in ("baseline", "observation"):
        selected = {}
        for row in rows:
            if row["scope"] not in ("search", "commit/insert", "commit/overwrite", "commit/delete", "recovery/total"):
                continue
            timestamp = number(row["environment"].get("unix_seconds"))
            if timestamp is None or (role == "baseline" and row["phase"] != "baseline"):
                continue
            key = (row["backend"] or "unknown", row["scope"])
            rank = (timestamp, row["raw_sha256"], row["run_index"], row["id"])
            if key not in selected or rank > selected[key][0]:
                selected[key] = (rank, row)
        for key, (rank, row) in sorted(selected.items()):
            entries.append({"role": role, "backend": key[0], "scope": key[1],
                            "unix_seconds": rank[0], "raw_file": row["raw_file"],
                            "raw_sha256": row["raw_sha256"], "run_index": row["run_index"],
                            "git_revision": row["git_revision"], "row_id": row["id"]})
    return entries


def compact_summary(rows, pairs, entries):
    by_id = {row["id"]: row for row in rows}
    lines = ["# Benchmark summary", "",
             f"{len({r['raw_sha256'] for r in rows})} raw reports; {len(pairs)} compatible before/after result pairs. History is preserved in `runs/` and `baselines/`.", "",
             "Start here or with [latest.json](latest.json). Full tables/index: `python3 tools/benchmarks.py summary --full` (generates ignored `HISTORY.md` and `index.json`).", "",
             "Latest means greatest recorded timestamp per backend/scope; ties use content hash and run index. Baselines require phase=baseline. Observations may be experiments with different workloads. Missing timestamps are excluded. These pointers never select comparison pairs.", "",
             "| Kind | Backend / scope | Raw run | Commit | Workload | p50 ns | Objects |",
             "|---|---|---|---|---|---|---|"]
    for entry in entries:
        row = by_id[entry["row_id"]]
        config = row["workload"]["config"]
        workload = "; ".join(f"{k}={config[k]}" for k in ("rows", "dimensions", "mutations", "operations", "queries", "samples", "seed", "checkpoint_at", "compact_at", "distribution", "ivf_partitions", "ivf_probes", "ivf_iterations") if k in config)
        ref = f"[{row['raw_sha256'][:12]}:{row['run_index']}]({quote(row['raw_file'], safe='/')})"
        values = [entry["role"], f"{entry['backend']} / {entry['scope']}", ref,
                  (row["git_revision"] or "unknown")[:12], workload,
                  display(row["metrics"]["p50_latency_ns"]), display(row["metrics"]["logical_object_count"])]
        lines.append("| " + " | ".join(text(v) for v in values) + " |")
    maintenance = [(entry, by_id[entry["row_id"]]["maintenance"].get("compaction"))
                   for entry in entries if entry["role"] == "observation" and entry["scope"] == "recovery/total"]
    maintenance = [(entry, value) for entry, value in maintenance if value]
    if maintenance:
        lines += ["", "Latest observed compaction (single maintenance operation; logical payload amplification):", "",
                  "| Backend | Latency ns | Written B | Removed objects | Additional write amplification | HTTP DELETE |",
                  "|---|---|---|---|---|---|"]
        for entry, value in maintenance:
            values = [entry["backend"], value.get("latency_ns"), value.get("logical_bytes_written"),
                      value.get("removes"), value.get("additional_write_amplification"),
                      obj(value.get("http_requests")).get("delete")]
            lines.append("| " + " | ".join(text(display(v) if isinstance(v, (int, float)) else v) for v in values) + " |")
    lines += ["", "Timing is descriptive, not a regression gate. Compare explicit raw reports with `python3 tools/benchmarks.py compare BEFORE AFTER`; incompatible or incomplete identities are rejected. Seeds, source fingerprints, environment, raw samples and backend metrics remain in the linked reports.", ""]
    return "\n".join(lines)


def compare_files(before_path, after_path, check_counters=False):
    def read(path):
        data = path.read_bytes()
        return [row for i, run in enumerate(reports(decode(data)))
                for row in normalize(run, path.name, digest(data), i)]
    before, after = read(before_path), read(after_path)
    def keyed(items):
        result = {}
        for row in items:
            key = (row["feature"], row["comparison_group"], row["comparison_key"])
            if key[-1] is None or key in result:
                raise ValueError("incomplete or ambiguous comparison identity")
            result[key] = row
        return result
    old, new = keyed(before), keyed(after)
    if old.keys() != new.keys():
        raise ValueError("incompatible workload, inputs, backend, protocol, or environment; no comparison made")
    results = []
    failed = False
    for key in sorted(old):
        a, b = old[key], new[key]
        am, bm = dict(a["metrics"]), dict(b["metrics"])
        # Single maintenance observations are not percentiles or recovery samples.
        for row, values in ((a, am), (b, bm)):
            for operation, data in row["maintenance"].items():
                for name, value in data.items():
                    if isinstance(value, dict):
                        for field, count in value.items():
                            values[f"{operation}.{name}.{field}"] = number(count)
                    else:
                        values[f"{operation}.{name}"] = number(value)
        changes = {}
        for name in sorted(am.keys() | bm.keys()):
            av, bv = am.get(name), bm.get(name)
            if av is None and bv is None:
                continue
            deterministic = name not in METRICS[:8] and not name.endswith("latency_ns")
            changed = av != bv
            failed |= check_counters and deterministic and changed
            changes[name] = {"before": av, "after": bv, "delta_percent": delta(av, bv),
                             "counter_changed": deterministic and changed}
        same_source = bool(a["environment"].get("source_sha256")) and a["environment"].get("source_sha256") == b["environment"].get("source_sha256")
        results.append({"scope": a["scope"], "backend": a["backend"], "before": a["id"], "after": b["id"],
                        "before_revision": a["git_revision"], "after_revision": b["git_revision"],
                        "before_raw_sha256": a["raw_sha256"], "after_raw_sha256": b["raw_sha256"],
                        "comparison_key": a["comparison_key"],
                        "interpretation": "repeatability; same source" if same_source else "source attribution requires review",
                        "metrics": changes})
    return {"schema_version": 1, "counter_check_failed": failed, "comparisons": results}


def summarize(root, check=False, full=False):
    rows = load_archive(root)
    pairs = comparisons(rows)
    entries = latest_entries(rows)
    outputs = {"latest.json": (json.dumps({"schema_version": 1, "entries": entries}, indent=2, sort_keys=True) + "\n").encode(),
               "SUMMARY.md": compact_summary(rows, pairs, entries).encode()}
    if full:
        outputs.update({"index.json": (json.dumps({"schema_version": 2, "rows": rows, "comparisons": pairs}, sort_keys=True, indent=2, allow_nan=False) + "\n").encode(),
                        "HISTORY.md": markdown(rows, pairs).encode()})
    for name, data in outputs.items():
        path = root / name
        if check:
            if not path.exists() or path.read_bytes() != data:
                raise ValueError(f"stale generated file: {path}")
        else:
            publish(path, data)
    return rows


def archive(source, root):
    data = source.read_bytes()
    reports(decode(data))  # Validate before creating anything.
    raw_hash = digest(data)
    # Importing an existing legacy archive is idempotent, without duplicating it.
    existing = [p for p in root.glob("baselines/*.json") if p.read_bytes() == data]
    target = existing[0] if existing else root / "runs" / f"{raw_hash}.json"
    if target.exists():
        if target.read_bytes() != data:
            raise ValueError(f"refusing to overwrite archive content: {target}")
    else:
        publish(target, data)
    summarize(root)
    return target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    add = sub.add_parser("archive", help="archive raw JSON unchanged and rebuild compact views")
    add.add_argument("report", type=Path)
    add.add_argument("--archive", type=Path, default=Path("benchmarks"))
    summary = sub.add_parser("summary", help="rebuild deterministic derived files")
    summary.add_argument("--archive", type=Path, default=Path("benchmarks"))
    summary.add_argument("--check", action="store_true")
    summary.add_argument("--full", action="store_true", help="also generate full ignored index and history tables")
    compare = sub.add_parser("compare", help="compare explicitly chosen compatible reports; no latency threshold")
    compare.add_argument("before", type=Path)
    compare.add_argument("after", type=Path)
    compare.add_argument("--check-counters", action="store_true", help="fail on any deterministic counter/footprint change; for fixed-workload regression checks")
    args = parser.parse_args()
    try:
        if args.command == "archive":
            print(archive(args.report, args.archive))
        elif args.command == "compare":
            result = compare_files(args.before, args.after, args.check_counters)
            print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
            if result["counter_check_failed"]:
                parser.exit(1, "deterministic benchmark counters changed; review required\n")
        else:
            summarize(args.archive, args.check, args.full)
    except (ValueError, OSError, KeyError, TypeError) as error:
        parser.exit(1, f"benchmark archive: {error}\n")


if __name__ == "__main__":
    main()
