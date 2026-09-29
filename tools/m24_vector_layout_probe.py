#!/usr/bin/env python3
"""Offline quality probe for balanced vector-local blocks; not serving evidence."""
import argparse
from collections import Counter
import hashlib
import json
import platform
from pathlib import Path
import sys
import time

import numpy as np

BASE_SHA256 = "fab6b3f6c68d8bca09c72b0ee84a8126b80aebc635765fb52c0ab3efbda51960"
QUERY_SHA256 = "f7fc9be140accdfd64116c2fa2365ecdb69b8f084970c6b0532db5ff79ac8fdc"
MAX_ROWS = 170
MAX_BLOCKS = 8
PROTOTYPES = 16


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def read_fvecs(path, count):
    words = np.fromfile(path, dtype="<f4", count=count * 129).reshape(count, 129)
    if not np.all(words[:, 0].view("<u4") == 128):
        raise ValueError("unexpected fvecs dimension")
    return np.ascontiguousarray(words[:, 1:])


def balanced_blocks(vectors, max_rows):
    """Bisect by a variance axis, then refine balanced centroid cuts three times."""
    blocks = []
    stack = [np.arange(len(vectors), dtype=np.int32)]
    while stack:
        ids = stack.pop()
        if len(ids) <= max_rows:
            blocks.append(ids)
            continue
        leaves = (len(ids) + max_rows - 1) // max_rows
        left_rows = round(len(ids) * (leaves // 2) / leaves)
        rows = vectors[ids]
        axis = np.argmax(rows.var(axis=0, dtype=np.float64))
        projection = rows[:, axis]
        order = np.lexsort((ids, projection))
        left = ids[order[:left_rows]]
        right = ids[order[left_rows:]]
        for _ in range(3):
            direction = vectors[right].mean(axis=0) - vectors[left].mean(axis=0)
            projection = rows @ direction
            order = np.lexsort((ids, projection))
            left = ids[order[:left_rows]]
            right = ids[order[left_rows:]]
        stack.append(right)
        stack.append(left)
    return blocks


def layout(vectors):
    blocks = balanced_blocks(vectors, MAX_ROWS)
    assignment = np.empty(len(vectors), dtype=np.int32)
    centers = np.empty((len(blocks), vectors.shape[1]), dtype=np.float32)
    for ordinal, ids in enumerate(blocks):
        assignment[ids] = ordinal
        centers[ordinal] = vectors[ids].mean(axis=0)
    if not np.array_equal(np.sort(np.concatenate(blocks)), np.arange(len(vectors))):
        raise AssertionError("layout omitted or duplicated rows")
    return blocks, assignment, centers


def representatives(vectors, blocks):
    selected = np.empty((len(blocks), PROTOTYPES, vectors.shape[1]), dtype=np.float32)
    for ordinal, ids in enumerate(blocks):
        rows = vectors[ids]
        center = rows.mean(axis=0)
        first = np.argmin(np.sum((rows - center) ** 2, axis=1))
        selected[ordinal, 0] = rows[first]
        nearest = np.sum((rows - rows[first]) ** 2, axis=1)
        for slot in range(1, PROTOTYPES):
            next_row = np.argmax(nearest)
            selected[ordinal, slot] = rows[next_row]
            nearest = np.minimum(nearest, np.sum((rows - rows[next_row]) ** 2, axis=1))
    return selected


def quantized_routing(vectors, queries, assignment, block_count, exact_ids, filtered, bits):
    levels = (1 << bits) - 1
    minima = vectors.min(axis=0)
    scales = (vectors.max(axis=0) - minima) / levels
    scales[scales == 0] = 1
    codes = np.rint((vectors - minima) / scales).clip(0, levels).astype(np.uint8)
    reconstructed = codes.astype(np.float32) * scales + minima
    norms = np.sum(reconstructed * reconstructed, axis=1)
    hits = []
    for query, neighbors in zip(queries, exact_ids, strict=True):
        ids = np.array(neighbors, dtype=np.int32)
        if filtered:
            ids //= 100
        distances = norms - 2 * (reconstructed @ query)
        block_scores = np.full(block_count, np.inf, dtype=np.float32)
        np.minimum.at(block_scores, assignment, distances)
        selected = np.lexsort((np.arange(block_count), block_scores))[:MAX_BLOCKS]
        hits.append(np.isin(assignment[ids], selected).sum().item())
    return hits, {
        "bits_per_component": bits,
        "packed_code_bytes": (len(vectors) * vectors.shape[1] * bits + 7) // 8,
        "max_abs_reconstruction_error": float(np.max(np.abs(reconstructed - vectors))),
    }


def summary(values):
    ordered = sorted(values)
    return {
        "mean_recall_at_10": sum(values) / (len(values) * 10),
        "fifth_percentile_recall_at_10": ordered[len(values) * 5 // 100 - 1] / 10,
        "queries_below_0_9": sum(value < 9 for value in values),
        "queries_below_0_8": sum(value < 8 for value in values),
    }


def measure(vectors, queries, exact_ids, blocks, assignment, centers, samples, filtered):
    oracle_hits = []
    centroid_hits = []
    prototype_hits = {8: [], 16: []}
    distinct = []
    chosen_rows = []
    sample_norms = np.sum(samples * samples, axis=2)
    for query, neighbors in zip(queries, exact_ids, strict=True):
        if filtered:
            neighbors = np.array(neighbors, dtype=np.int32) // 100
        else:
            neighbors = np.array(neighbors, dtype=np.int32)
        group_ids = assignment[neighbors]
        counts = Counter(group_ids.tolist())
        oracle_hits.append(sum(sorted(counts.values(), reverse=True)[:MAX_BLOCKS]))
        distinct.append(len(counts))
        distances = np.sum((centers.astype(np.float64) - query) ** 2, axis=1)
        selected = np.lexsort((np.arange(len(blocks)), distances))[:MAX_BLOCKS]
        centroid_hits.append(np.isin(group_ids, selected).sum().item())
        chosen_rows.append(sum(len(blocks[i]) for i in selected))
        sample_scores = sample_norms - 2 * (samples.reshape(-1, 128) @ query).reshape(
            len(blocks), PROTOTYPES)
        for count in prototype_hits:
            block_scores = sample_scores[:, :count].min(axis=1)
            selected = np.lexsort((np.arange(len(blocks)), block_scores))[:MAX_BLOCKS]
            prototype_hits[count].append(np.isin(group_ids, selected).sum().item())
    result = {
        "blocks": len(blocks),
        "min_rows_per_block": min(map(len, blocks)),
        "max_rows_per_block": max(map(len, blocks)),
        "oracle_eight_block": summary(oracle_hits),
        "nearest_eight_centroids": summary(centroid_hits),
        "nearest_eight_by_8_representatives": summary(prototype_hits[8]),
        "nearest_eight_by_16_representatives": summary(prototype_hits[16]),
        "distinct_exact_blocks": dict(sorted(Counter(distinct).items())),
        "selected_rows_mean": sum(chosen_rows) / len(chosen_rows),
        "selected_rows_max": max(chosen_rows),
    }
    for bits in (4, 8):
        hits, details = quantized_routing(vectors, queries, assignment, len(blocks),
                                          exact_ids, filtered, bits)
        result[f"quantized_{bits}_bit_min_score"] = {**summary(hits), **details}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--oracle", type=Path, default=Path("benchmarks/m24/layout-id-sorted/run.json"))
    args = parser.parse_args()
    base_path = args.data / "sift1m_base_250000.fvecs"
    query_path = args.data / "sift1m_query.fvecs"
    if sha256(base_path) != BASE_SHA256 or sha256(query_path) != QUERY_SHA256:
        raise ValueError("M21 dataset hash mismatch")
    oracle = json.loads(args.oracle.read_text())
    if oracle["queries"] != 200 or oracle["rows"] != 250000:
        raise ValueError("unexpected oracle identity")
    start = time.monotonic()
    base = read_fvecs(base_path, 250000)
    queries = read_fvecs(query_path, 200)
    all_blocks, all_assignment, all_centers = layout(base)
    all_samples = representatives(base, all_blocks)
    cohort = base[::100].copy()
    cohort_blocks, cohort_assignment, cohort_centers = layout(cohort)
    cohort_samples = representatives(cohort, cohort_blocks)
    result = {
        "version": 1,
        "dataset_sha256": {base_path.name: BASE_SHA256, query_path.name: QUERY_SHA256},
        "oracle_sha256": sha256(args.oracle),
        "source_sha256": sha256(Path(__file__)),
        "numpy": np.__version__,
        "python": sys.version.split()[0],
        "environment": platform.platform(),
        "rows": 250000,
        "dimensions": 128,
        "queries": 200,
        "k": 10,
        "max_rows_per_block": MAX_ROWS,
        "max_blocks_selected": MAX_BLOCKS,
        "algorithm": "balanced recursive two-center cuts, three refinement rounds, tie by ID",
        "representative_selection": "nearest centroid, then farthest from nearest selected representative",
        "representative_vectors_per_block": PROTOTYPES,
        "unfiltered": measure(base, queries, oracle["unfiltered_exact_ids"], all_blocks,
                              all_assignment, all_centers, all_samples, False),
        "one_percent_filter": measure(cohort, queries, oracle["filtered_exact_ids"], cohort_blocks,
                                      cohort_assignment, cohort_centers, cohort_samples, True),
        "elapsed_ms": (time.monotonic() - start) * 1000,
    }
    args.output.mkdir(parents=True, exist_ok=False)
    with (args.output / "run.json").open("x") as output:
        json.dump(result, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
