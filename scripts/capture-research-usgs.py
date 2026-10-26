#!/usr/bin/env python3
"""DAT-03 exact 2023 USGS original captures. Java validates payloads; never ETL or publication."""
import argparse
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
from urllib.parse import urlencode
import zipfile

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("dat03_rollout", ROOT / "scripts/research-rollout.py")
r = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(r)
VERSION = "dat03-usgs-monthly-captures-v1"
CHECK = "ie212.earthquake.spark.usgs.ResearchUsgsCaptureCheck"
MAX_BYTES = 16 * 1024**2
MONTHS = r.months("2023-01", "2024-01")


def scope(month):
    r.require(month in MONTHS, "RESEARCH_USGS_MONTH_REQUIRED")
    number = int(month[-2:]); seed = datetime(2023, 1, 1, tzinfo=timezone.utc)
    start = max(datetime(2023, number, 1, tzinfo=timezone.utc) - timedelta(seconds=16), seed)
    end = (datetime(2024, 1, 1, tzinfo=timezone.utc) if number == 12 else datetime(2023, number + 1, 1, tzinfo=timezone.utc)) + timedelta(seconds=16)
    def iso(value):
        return value.isoformat(timespec="milliseconds" if value.microsecond else "seconds").replace("+00:00", "Z")
    query = {"format": "geojson", "eventtype": "earthquake", "starttime": iso(start), "endtime": iso(end - timedelta(milliseconds=1)),
             "minlatitude": "20.0", "maxlatitude": "50.0", "minlongitude": "120.0", "maxlongitude": "155.0",
             "orderby": "time-asc", "limit": "20000", "offset": "1"}
    return {"month": month, "url": "https://earthquake.usgs.gov/fdsnws/event/1/query?" + urlencode(query),
            "window_start_utc": iso(start), "window_end_utc_exclusive": iso(end)}


def root_path(path):
    path = r.safe(path)
    r.require(path.is_relative_to(ROOT / "staging") and path != ROOT / "staging", "RESEARCH_USGS_DEDICATED_STAGING_REQUIRED")
    return path


def code(jar):
    jar = r.safe(jar)
    r.require(jar.is_file() and jar.stat().st_size <= 64 * 1024**2, "RESEARCH_USGS_PREBUILT_JAR_REQUIRED")
    with zipfile.ZipFile(jar) as archive:
        r.require(CHECK.replace(".", "/") + ".class" in archive.namelist(), "RESEARCH_USGS_BUILD_LOCAL_JAR_FIRST")
    return r.profile.sha(jar)


def verify_metadata(folder, month):
    folder = r.safe(folder); meta = r.load(folder / "capture.json", 65536); raw = r.safe(folder / "response.geojson")
    r.require(meta.get("version") == VERSION and meta.get("month") == month and meta.get("url") == scope(month)["url"]
              and type(meta.get("http_status")) is int and meta["http_status"] == 200
              and type(meta.get("bytes")) is int and raw.is_file() and 0 < raw.stat().st_size == meta["bytes"] <= MAX_BYTES
              and r.profile.sha(raw) == meta.get("sha256"), "RESEARCH_USGS_CAPTURE_PIN_CHANGED")
    return meta


def validate(folder, month, java, jar, pin):
    meta = verify_metadata(folder, month)
    checked = subprocess.run([java, "-Xmx256m", "-cp", str(jar), CHECK, str(folder / "capture.json"), str(folder / "response.geojson")],
                             check=True, capture_output=True, text=True, timeout=45)
    result = json.loads(checked.stdout)
    expected = scope(month)
    r.require(result.get("version") == "dat03-usgs-capture-check-v1" and result.get("status") == "CAPTURE_STRUCTURE_VERIFIED"
              and result.get("month") == month and result.get("raw_sha256") == meta["sha256"]
              and type(result.get("feature_count")) is int and 0 <= result["feature_count"] < 20000
              and all(result.get(k) == expected[k] for k in ("window_start_utc", "window_end_utc_exclusive"))
              and all(result.get(k) is False for k in ("bronze_ready", "coverage_verified", "research_ready"))
              and r.profile.sha(jar) == pin, "RESEARCH_USGS_JAVA_CHECK_INVALID")
    return result


def capture_one(root, month, reuse_december=None):
    folder = r.safe(root / month)
    if folder.exists():
        return verify_metadata(folder, month)  # Reuse only; no HEAD, download, or overwrite.
    r.require(shutil.disk_usage(root).free >= 1024**3 + MAX_BYTES, "RESEARCH_USGS_DISK_RESERVE")
    source = None
    if month == "2023-12" and reuse_december is not None:
        old_root = r.safe(reuse_december); old = r.load(old_root / "capture.json", 65536)
        source = r.safe(old_root / "usgs-december2023.geojson")
        r.require(old.get("url") == scope(month)["url"] and type(old.get("http_status")) is int and old["http_status"] == 200
                  and type(old.get("bytes")) is int and source.is_file() and 0 < source.stat().st_size == old["bytes"] <= MAX_BYTES
                  and r.profile.sha(source) == old.get("sha256"), "RESEARCH_USGS_DECEMBER_REUSE_PIN_CHANGED")
    folder.mkdir(mode=0o700); raw = folder / "response.geojson"
    if source is not None:
        # Preserve the original retrieval time/bytes, not a claim of a new API observation.
        with source.open("rb") as src, raw.open("xb") as dst:
            os.chmod(raw, 0o600); shutil.copyfileobj(src, dst, 1024**2)
        meta = dict(old, version=VERSION, month=month, capture_method="REUSED_PUBLIC_CAPTURE",
                    origin_capture_receipt_sha256=r.profile.sha(old_root / "capture.json"))
    else:
        url = scope(month)["url"]
        result = subprocess.run(["curl", "--fail", "--proto", "=https", "--tlsv1.2", "--max-time", "60", "--max-filesize", str(MAX_BYTES),
                                 "--no-clobber", "--silent", "--show-error", "--dump-header", str(folder / "headers.private"),
                                 "--output", str(raw), "--write-out", "%{http_code}\n%{url_effective}\n%{content_type}\n", url],
                                check=True, capture_output=True, text=True, timeout=70)
        status, final, media = result.stdout.strip().splitlines()
        r.require(status == "200" and final == url and media.split(";")[0].strip() in ("application/json", "application/geo+json")
                  and raw.is_file() and not raw.is_symlink() and 0 < raw.stat().st_size <= MAX_BYTES, "RESEARCH_USGS_HTTP_INVALID")
        os.chmod(raw, 0o600)
        meta = {"version": VERSION, "month": month, "url": url, "http_status": 200, "content_type": media,
                "sha256": r.profile.sha(raw), "bytes": raw.stat().st_size, "capture_method": "PUBLIC_HTTPS_GET",
                "retrieved_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")}
    r.save(folder / "capture.json", meta)
    return verify_metadata(folder, month)


def execute(root, java, jar, mode, reuse_december=None):
    root = root_path(root); pin = code(jar); jar = r.safe(jar)
    if mode == "capture":
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        remaining = sum(MAX_BYTES for m in MONTHS if not (root / m).exists())
        r.require(shutil.disk_usage(root).free >= 1024**3 + remaining, "RESEARCH_USGS_DISK_RESERVE")
    r.require(root.is_dir(), "RESEARCH_USGS_CAPTURE_ROOT_MISSING")
    entries = []
    for month in MONTHS:
        meta = capture_one(root, month, reuse_december) if mode == "capture" else verify_metadata(root / month, month)
        result = validate(root / month, month, java, jar, pin)
        item = {"month": month, "capture_sha256": r.profile.sha(root / month / "capture.json"), "raw_sha256": meta["sha256"],
                "bytes": meta["bytes"], "retrieved_at_utc": meta["retrieved_at_utc"], "capture_method": meta["capture_method"], "check": result}
        entries.append(item)
        print("event=dat03_usgs_capture_checked month=" + month + " records=" + str(result["feature_count"]), flush=True)
    summary = {"version": VERSION, "status": "CAPTURES_STRUCTURE_VERIFIED", "runner_sha256": pin, "captures": entries,
               "capture_count": 12, "sum_response_features_including_halo_duplicates": sum(e["check"]["feature_count"] for e in entries),
               "response_bytes": sum(e["bytes"] for e in entries), "bronze_ready": False, "coverage_verified": False,
               "research_ready": False, "official_exported": False}
    target = root / ("summary-" + pin + ".json")
    if mode == "capture" and not target.exists():
        r.save(target, summary)
    else:
        r.require(r.load(target) == summary, "RESEARCH_USGS_SUMMARY_PIN_CHANGED")
    print("event=dat03_usgs_captures_verified months=12 summary_sha256=" + r.profile.sha(target), flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("mode", choices=("plan", "capture", "verify"))
    parser.add_argument("--root", type=Path, required=True); parser.add_argument("--java", default="java")
    parser.add_argument("--jar", type=Path, default=ROOT / "spark/target/japan-earthquake-etl-runner.jar")
    parser.add_argument("--reuse-december", type=Path); args = parser.parse_args()
    root_path(args.root)
    if args.mode == "plan":
        print(json.dumps({"version": VERSION, "requests": [scope(m) for m in MONTHS], "max_capture_bytes": 12 * MAX_BYTES,
                          "network": False, "lake_writes": False, "coverage_verified": False}, indent=2)); return
    execute(args.root, args.java, args.jar, args.mode, args.reuse_december)


if __name__ == "__main__":
    main()
