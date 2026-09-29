#!/usr/bin/env python3
"""Measure the eight-block recall ceiling of the current SIFT1M layout."""
import argparse
import hashlib
import json
import platform
from pathlib import Path
import subprocess
import tempfile

EXPECTED = {
    "sift1m_base_250000.fvecs": "fab6b3f6c68d8bca09c72b0ee84a8126b80aebc635765fb52c0ab3efbda51960",
    "sift1m_query.fvecs": "f7fc9be140accdfd64116c2fa2365ecdb69b8f084970c6b0532db5ff79ac8fdc",
}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def run(*args: str, timeout: int = 1200) -> str:
    result = subprocess.run(args, check=True, text=True, capture_output=True, timeout=timeout)
    return result.stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="new result directory")
    parser.add_argument("--data", required=True, type=Path, help="verified SIFT1M prefix and query directory")
    args = parser.parse_args()
    for name, expected in EXPECTED.items():
        if sha256(args.data / name) != expected:
            raise ValueError(f"unexpected SIFT1M digest: {name}")
    run("cargo", "build", "--locked", "--release", "--features", "experimental-segmented",
        "--example", "m24_layout_probe")
    with tempfile.TemporaryDirectory(prefix="glider-m24-layout-") as workdir:
        result = json.loads(run("target/release/examples/m24_layout_probe",
                                str(args.data / "sift1m_base_250000.fvecs"),
                                str(args.data / "sift1m_query.fvecs"), workdir))
    if result["queries"] != 200 or result["blocks"] < 1000:
        raise ValueError("layout probe returned an unexpected workload")
    if result["unfiltered_exact_ids"][0] != [36538, 236647, 36267, 2176, 3752,
                                               68299, 49874, 882, 87578, 224263]:
        raise ValueError("exact oracle disagrees with the independent M23 query")
    result["dataset_sha256"] = EXPECTED
    result["source_sha256"] = {str(path): sha256(path) for path in (
        Path("src/segmented.rs"), Path("examples/m24_layout_probe.rs"),
        Path("tools/m24_layout_probe.py"))}
    result["environment"] = platform.platform()
    result["hardware"] = (run("sysctl", "-n", "machdep.cpu.brand_string")
                          if platform.system() == "Darwin" else platform.processor())
    result["rustc"] = run("rustc", "--version")
    args.output.mkdir(parents=True, exist_ok=False)
    with (args.output / "run.json").open("x") as output:
        json.dump(result, output, indent=2, sort_keys=True)
        output.write("\n")
    print(f"M24 layout probe saved to {args.output / 'run.json'}")


if __name__ == "__main__":
    main()
