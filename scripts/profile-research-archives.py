#!/usr/bin/env python3
"""DAT-03 bounded capture/Java-profile control plane; never event ETL or a coverage proof."""
import argparse
import csv
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
MAX_BYTES = 32 * 1024 * 1024
RESERVE_BYTES = 1024 * 1024 * 1024
BASE_URL = "https://www.data.jma.go.jp/eqev/data/bulletin/data/hypo/"
PROFILE_CLASS = "ie212.earthquake.spark.jma.JmaResearchProfileJob"


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_new(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def safe_root(path):
    path = Path(path).absolute()
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError("PROFILE_SYMLINK_PATH")
    path = path.resolve()
    if not path.is_relative_to(ROOT / "staging") or path == ROOT / "staging":
        raise ValueError("PROFILE_ROOT_MUST_BE_DEDICATED_STAGING_CHILD")
    return path


def inventory(years=None):
    rows = []
    for name in ("hypocenter_archives_v1.csv", "research_boundary_archives_v1.csv"):
        with (ROOT / "config/jma" / name).open(newline="") as stream:
            rows.extend(row for row in csv.DictReader(stream) if 2000 <= int(row["year"]) <= 2024)
    if sorted(int(row["year"]) for row in rows) != list(range(2000, 2025)):
        raise ValueError("PROFILE_INVENTORY_YEAR_SET")
    for row in rows:
        year = int(row["year"])
        if (row["archive_name"] != f"h{year}.zip" or row["member_name"] != f"h{year}"
                or row["source_url"] != BASE_URL + row["archive_name"]
                or row["segment"] != "full-year" or row["media_type"] != "application/zip"
                or row["record_format"] != "jma-hypocenter-96-byte-v1" or row["catalog_era"] != "UNIFIED"
                or row["native_start_jst"] != f"{year}-01-01T00:00:00+09:00"
                or row["native_end_jst"] != f"{year + 1}-01-01T00:00:00+09:00"
                or not 0 < int(row["observed_content_length_bytes"]) <= MAX_BYTES):
            raise ValueError("PROFILE_INVENTORY_ROW_INVALID")
    if years is not None:
        if not years or len(set(years)) != len(years) or any(y < 2000 or y > 2024 for y in years):
            raise ValueError("PROFILE_EXPLICIT_YEARS_INVALID")
        rows = [row for row in rows if int(row["year"]) in years]
    return rows


def verified_capture(folder, row):
    for name in (row["archive_name"], "capture.json"):
        path = folder / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("PROFILE_INCOMPLETE_CAPTURE_USE_NEW_ROOT")
    capture = json.loads((folder / "capture.json").read_text())
    archive = folder / row["archive_name"]
    if (capture["inventory"] != row or capture["bytes"] != archive.stat().st_size
            or not 0 < archive.stat().st_size <= MAX_BYTES or capture["sha256"] != sha(archive)
            or capture["http_status"] != 200 or capture["final_url"] != row["source_url"]):
        raise ValueError("PROFILE_CAPTURE_PIN_MISMATCH")
    return capture


def http_headers(text):
    # Last response block, not CONNECT or an earlier redirect.
    blocks = re.split(r"\r?\n\r?\n", text.strip())
    block = next((block for block in reversed(blocks) if block.startswith("HTTP/")), None)
    if block is None:
        raise ValueError("PROFILE_HTTP_HEADERS_MISSING")
    headers = {}
    for line in block.splitlines()[1:]:
        if ":" in line:
            key, value = line.split(":", 1)
            headers[key.lower()] = value.strip()
    return headers


def capture_one(root, row):
    folder = root / row["member_name"]
    if folder.exists():
        return verified_capture(folder, row)
    # Reserve against the hard per-file bound, not a potentially stale Content-Length hint.
    if shutil.disk_usage(root).free < RESERVE_BYTES + MAX_BYTES:
        raise ValueError("PROFILE_CAPTURE_DISK_RESERVE")
    folder.mkdir()
    archive = folder / row["archive_name"]
    headers_path = folder / "http-headers.txt"
    # curl never writes over an existing archive; failures remain visible, not auto-retried/deleted.
    result = subprocess.run([
        "curl", "--fail", "--location", "--max-redirs", "3", "--proto", "=https",
        "--proto-redir", "=https", "--tlsv1.2", "--max-time", "90", "--max-filesize", str(MAX_BYTES),
        "--no-clobber", "--silent", "--show-error", "--dump-header", str(headers_path),
        "--output", str(archive), "--write-out", "%{http_code}\n%{url_effective}\n", row["source_url"],
    ], check=True, text=True, capture_output=True, timeout=100)
    status, final_url = result.stdout.strip().splitlines()
    headers = http_headers(headers_path.read_text())
    if (status != "200" or final_url != row["source_url"]
            or headers.get("content-type", "").split(";")[0] != "application/zip"
            or archive.is_symlink() or not 0 < archive.stat().st_size <= MAX_BYTES
            or int(headers["content-length"]) != archive.stat().st_size):
        raise ValueError("PROFILE_HTTP_CAPTURE_INVALID")
    modified = parsedate_to_datetime(headers["last-modified"]).astimezone(timezone.utc)
    modified_text = modified.isoformat(timespec="seconds").replace("+00:00", "Z")
    digest = sha(archive)
    receipt = {
        "version": "dat03-capture-v1", "inventory": row, "http_status": 200,
        "final_url": final_url, "content_type": headers["content-type"],
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "last_modified_utc": modified_text, "etag": headers.get("etag"),
        "bytes": archive.stat().st_size, "sha256": digest,
        "catalog_release": f"jma-lm-{modified.strftime('%Y%m%dT%H%M%SZ')}-sha256-{digest[:12]}",
        "inventory_metadata_drift": modified_text != row["observed_last_modified_utc"]
        or archive.stat().st_size != int(row["observed_content_length_bytes"]),
        "source_authenticity_verified": False, "bronze_ready": False,
    }
    write_new(folder / "capture.json", receipt)
    return receipt


def profile_one(root, row, java, jar):
    folder = root / row["member_name"]
    capture = verified_capture(folder, row)
    runner_sha = sha(jar)
    # A changed runner gets a new profile, never overwrites historical evidence.
    output = folder / f"profile-{runner_sha}.json"
    metrics = folder / f"metrics-{runner_sha}.json"
    if output.exists() or metrics.exists():
        if (output.is_symlink() or metrics.is_symlink() or not output.is_file() or not metrics.is_file()):
            raise ValueError("PROFILE_INCOMPLETE_RESULT_USE_NEW_ROOT")
        report, measured = json.loads(output.read_text()), json.loads(metrics.read_text())
        if (measured["runner_sha256"] != runner_sha or measured["profile_sha256"] != sha(output)
                or report["archive_sha256"] != capture["sha256"]):
            raise ValueError("PROFILE_RESULT_PIN_MISMATCH")
        return report, measured
    started = time.monotonic()
    # Explicit local subprocess. No Docker/build/download/warehouse access here.
    command = [java, "-Xmx512m", "-cp", str(jar), PROFILE_CLASS,
               str(folder / row["archive_name"]), row["member_name"], capture["sha256"], str(output)]
    timing = folder / f"time-{runner_sha}.txt"
    if timing.exists() or timing.is_symlink():
        raise ValueError("PROFILE_TIMING_CONFLICT")
    timed = sys.platform == "darwin" and Path("/usr/bin/time").is_file()
    if timed:
        command = ["/usr/bin/time", "-l", "-o", str(timing), *command]
    subprocess.run(command, check=True, timeout=300)
    if sha(jar) != runner_sha:
        raise ValueError("PROFILE_RUNNER_CHANGED_DURING_RUN")
    peak_rss = None
    if timed:
        match = re.search(r"^\s*(\d+)\s+maximum resident set size\s*$", timing.read_text(), re.MULTILINE)
        if match is None:
            raise ValueError("PROFILE_RSS_MEASUREMENT_MISSING")
        peak_rss = int(match[1])  # macOS time -l reports bytes, not Linux KiB.
    measured = {"runner_sha256": runner_sha, "profile_sha256": sha(output),
                "elapsed_seconds": round(time.monotonic() - started, 3), "max_heap_mib": 512,
                "peak_rss_measured": timed, "peak_rss_bytes": peak_rss, "full_etl_benchmark": False}
    write_new(metrics, measured)
    return json.loads(output.read_text()), measured


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("plan", "capture", "profile"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--years", help="explicit comma-separated subset; default 2000–2024")
    parser.add_argument("--java", default="java")
    parser.add_argument("--jar", type=Path, default=ROOT / "spark/target/japan-earthquake-etl-runner.jar")
    args = parser.parse_args()
    root = safe_root(args.root)
    rows = inventory(None if args.years is None else [int(y) for y in args.years.split(",")])
    if args.mode == "plan":
        print(json.dumps({"archives": len(rows), "observed_compressed_bytes": sum(int(r["observed_content_length_bytes"]) for r in rows),
                          "years": [int(r["year"]) for r in rows], "network": False, "lake_writes": False}, indent=2))
        return
    root.mkdir(parents=True, exist_ok=True)
    if args.mode == "capture":
        remaining = sum(MAX_BYTES for r in rows if not (root / r["member_name"]).exists())
        if shutil.disk_usage(root).free < RESERVE_BYTES + remaining:
            raise ValueError("PROFILE_CAPTURE_DISK_RESERVE")
    results = []
    for row in rows:
        folder = root / row["member_name"]
        if folder.is_symlink():
            raise ValueError("PROFILE_SYMLINK_CAPTURE")
        if args.mode == "capture":
            result = capture_one(root, row)
            print(f"event=dat03_capture_pinned year={row['year']} bytes={result['bytes']} metadata_drift={str(result['inventory_metadata_drift']).lower()}", flush=True)
        else:
            report, metrics = profile_one(root, row, args.java, args.jar)
            capture = verified_capture(folder, row)
            results.append({"year": int(row["year"]), "role": "UTC_BOUNDARY_ONLY" if row["year"] == "2024" else "RESEARCH_SOURCE",
                            "capture": capture, "profile": report, "measurement": metrics})
    if args.mode == "profile":
        runner_sha = sha(args.jar)
        output = root / f"summary-{runner_sha}-{'-'.join(r['year'] for r in rows)}.json"
        summary = {"version": "dat03-research-profiles-v1", "research_ready": False, "coverage_verified": False,
                   "full_etl_benchmark": False, "archives": results}
        if output.exists():
            if output.is_symlink() or json.loads(output.read_text()) != summary:
                raise ValueError("PROFILE_SUMMARY_CONFLICT")
        else:
            write_new(output, summary)
        print(f"event=dat03_profiles_complete archives={len(results)} summary={output.relative_to(ROOT)} research_ready=false")


if __name__ == "__main__":
    main()
