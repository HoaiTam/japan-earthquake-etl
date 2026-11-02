#!/usr/bin/env python3
"""Pinned SEC-01 cloud scope: bounded bytes, exact Drive GETs, no writes to cloud.

Exit 1 = finding; 2 = incomplete cloud acceptance; no inferred overall Done.
Existing rclone authorization is used in memory; run a scoped rclone listing
first if its access token has expired. Never print config/token/API exceptions.
"""
import argparse
import configparser
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import re
import ssl
import subprocess
import sys
import urllib.parse
import urllib.request

import security_cloud as cloud
import security_handoff as h

ROOT_ID = "17Wntw5LIRvFdzaU0RpnBRL60gzsRdFTB"
ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("sec_cloud_scan", ROOT / "scripts/audit-security.py")
scanner = importlib.util.module_from_spec(spec); spec.loader.exec_module(scanner)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("SEC_CLOUD_REDIRECT_REJECTED")


class DriveReader:
    def __init__(self):
        config = configparser.ConfigParser(interpolation=None)
        config.read(Path.home() / ".config/rclone/rclone.conf")
        remote = config["dat03_team"]
        h.require(remote.get("type") == "drive" and remote.get("root_folder_id") == ROOT_ID)
        self.token = json.loads(remote["token"])["access_token"]
        from pip._vendor import certifi
        self.opener = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPSHandler(
            context=ssl.create_default_context(cafile=certifi.where())))

    def read(self, endpoint, params):
        h.require(re.fullmatch(r"files(?:/[A-Za-z0-9_-]{10,128}(?:/permissions)?)?", endpoint))
        request = urllib.request.Request("https://www.googleapis.com/drive/v3/" + endpoint
            + "?" + urllib.parse.urlencode(params), headers={"Authorization": "Bearer " + self.token}, method="GET")
        with self.opener.open(request, timeout=30) as response:
            raw = response.read(h.FILE_LIMIT + 1)
        h.require(len(raw) <= h.FILE_LIMIT)
        return raw

    def json(self, endpoint, params):
        return h.strict_json(self.read(endpoint, params))


def collect(manifest, online=False, deployed_root=None):
    h.require(manifest["version"] == "sec-01-cloud-scope-v1")
    report = {"version": manifest["version"], "observed_at_utc": datetime.now(timezone.utc).isoformat(),
              "artifacts": [], "drive_objects": [], "receipts": [], "findings": [], "pending": [],
              "cloud_mutations": False, "can_mark_done": False,
              "local_acceptance": "historical_20261023_not_rerun",
              "scope": "explicit_pilot_and_notebook_objects_not_full_account_or_scientific_approval"}
    h.require(1 <= len(manifest["artifacts"]) <= 32)
    h.require(isinstance(manifest.get("drive_objects", []), list) and len(manifest.get("drive_objects", [])) <= 32)
    secrets = scanner.configured_secrets(deployed_root or ROOT)
    def scan(text, ml=False):
        categories = scanner.scan_text(text, ml=ml)
        if any(s in text for s in secrets):
            categories.append("configured_secret_in_artifact")
        return categories
    for index, item in enumerate(manifest["artifacts"]):
        if "git_revision" in item:
            h.require(re.fullmatch(r"[a-f0-9]{40}", item["git_revision"]))
            name = h.safe_name(item["git_path"])
            object_name = item["git_revision"] + ":" + name
            size = int(subprocess.check_output(["git", "cat-file", "-s", object_name], cwd=ROOT,
                                               stderr=subprocess.DEVNULL, timeout=30))
            h.require(size <= h.FILE_LIMIT)
            data = subprocess.check_output(["git", "show", object_name], cwd=ROOT,
                                           stderr=subprocess.DEVNULL, timeout=30)
            result = cloud.inspect_bytes(name, data, item["sha256"], scan)
        else:
            result = cloud.inspect_file(item["path"], item["sha256"], scan)
        result["artifact_index"] = index
        report["artifacts"].append(result)
        report["findings"].extend({"artifact_index": index, **f} for f in result["findings"])
        report["pending"].extend({"artifact_index": index, "reason": p} for p in result["pending"])
        if item.get("embedded_receipt") is not None:
            try:
                h.require(result["passed"] and "git_revision" in item)
                value = h.strict_json(data)
                pointer = item["embedded_receipt"]
                h.require(isinstance(pointer, list) and 1 <= len(pointer) <= 8)
                for key in pointer:
                    value = value[key]
                canonical = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
                receipt = cloud.receipt_summary(canonical, cloud.HANDOFF_SHA, cloud.PILOT_ID)
                receipt["representation"] = "canonical_embedded_json_not_original_receipt_file"
                receipt["containing_artifact_sha256"] = result["sha256"]
                report["receipts"].append(receipt)
            except Exception:
                report["pending"].append({"artifact_index": index, "reason": "embedded_receipt_invalid"})
    if online:
        reader = DriveReader()
        for item in manifest.get("drive_objects", []):
            file_id = item["id"]
            try:
                h.require(re.fullmatch(r"[A-Za-z0-9_-]{10,128}", file_id))
                meta = reader.json("files/" + file_id, {"fields": "id,mimeType,parents,capabilities(canDownload),trashed",
                                                       "supportsAllDrives": "true"})
                h.require(meta.get("trashed") is False)
                result = cloud.permission_summary(meta, cloud.permissions_for(reader.json, file_id))
                if item.get("notebook") is True:
                    raw = reader.read("files/" + file_id, {"alt": "media"})
                    result["actual_notebook"] = cloud.inspect_bytes("actual.ipynb", raw, cloud.digest(raw), scan)
                    nb = h.strict_json(raw)
                    h.require(nb.get("nbformat") == 4 and isinstance(nb.get("cells"), list))
                    result["actual_notebook"]["executed_code_cells"] = sum(
                        c.get("execution_count") is not None for c in nb["cells"] if c.get("cell_type") == "code")
                    result["actual_notebook"]["code_sources_sha256"] = cloud.digest(json.dumps(
                        [c["source"] for c in nb["cells"] if c.get("cell_type") == "code"],
                        sort_keys=True, separators=(",", ":")).encode())
                    report["findings"].extend({"drive_id": file_id, **f} for f in result["actual_notebook"]["findings"])
                    result["pending"].extend(result["actual_notebook"]["pending"])
                report["drive_objects"].append(result)
                report["findings"].extend({"drive_id": file_id, "category": f} for f in result["findings"])
                report["pending"].extend({"drive_id": file_id, "reason": p} for p in result["pending"])
            except Exception:
                report["pending"].append({"drive_id_sha256": cloud.digest(str(file_id).encode()),
                                          "reason": "drive_metadata_permissions_or_media_unavailable"})
    else:
        report["pending"].append({"reason": "actual_drive_not_requested"})
    # Do not manufacture human confirmations from a local flag or filenames.
    report["pending"].append({"reason": "three_recipient_identity_download_and_session_review_required"})
    report["scoped_bytes_passed"] = all(r["passed"] for r in report["artifacts"])
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--drive", action="store_true")
    parser.add_argument("--deployment-root", type=Path)
    args = parser.parse_args()
    # The manifest is private operator input. Do not include its paths in report.
    h.require(not any(p.is_symlink() for p in (args.manifest.absolute(), *args.manifest.absolute().parents))
              and args.manifest.stat().st_size <= 1048576)
    deployment = scanner.deployment_root(ROOT, args.deployment_root) if args.deployment_root else ROOT
    report = collect(h.strict_json(args.manifest.read_bytes()), args.drive, deployment)
    h.require(not any(p.is_symlink() for p in (args.output.absolute(), *args.output.absolute().parents))
              and not args.output.exists())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        os.chmod(args.output, 0o600)
        json.dump(report, stream, indent=2, sort_keys=True); stream.write("\n")
    print(json.dumps({"findings": len(report["findings"]), "pending": len(report["pending"]),
                      "scoped_bytes_passed": report["scoped_bytes_passed"], "can_mark_done": False}))
    return 1 if report["findings"] else 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        print("SEC_CLOUD_INPUT_OR_CONNECTION_ERROR", file=sys.stderr)
        sys.exit(2)
