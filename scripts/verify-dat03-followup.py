#!/usr/bin/env python3
"""Bounded original-metadata receipt verification; never ETL, SQL, or publication."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import zipfile

_spec = importlib.util.spec_from_file_location("dat03_lineage", Path(__file__).with_name("audit-dat03-lineage.py"))
lineage = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lineage)
require = lineage.require
decode = lineage.decode


def file_sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def pinned_file(path, expected, limit):
    path = Path(path)
    require(not path.is_symlink() and path.is_file(), "REGULAR_INPUT_REQUIRED")
    require(path.stat().st_size <= limit, "INPUT_SIZE_LIMIT")
    require(isinstance(expected, str) and lineage.SHA.fullmatch(expected), "INVALID_EXPECTED_SHA")
    require(file_sha(path) == expected, "INPUT_PIN_MISMATCH")
    return path


def read_phase(archive, name, expected=None):
    lineage.key(lineage.PREFIX + name)
    info = archive.getinfo(name)
    require(not info.is_dir() and info.file_size <= 2 * 1024**2, "PHASE_SIZE_LIMIT")
    raw = archive.read(info)
    if expected is not None:
        require(lineage.digest(raw) == expected, "PHASE_PIN_MISMATCH")
    return decode(raw), lineage.digest(raw)


def verify(plan):
    """Plan and expected SHA values must come from a trusted external checkpoint."""
    require(plan.get("version") == "dat03-followup-verification-plan-v1", "UNKNOWN_PLAN_VERSION")
    request_path = pinned_file(plan["request"], plan["request_sha256"], 1024**2)
    request = decode(request_path.read_bytes())
    require(request.get("version") == "dat03-followup-original-metadata-request-v1", "UNKNOWN_REQUEST_VERSION")
    require(request.get("owner") == plan["owner"], "OWNER_MISMATCH")
    require(0 < len(plan["metadata_archives"]) <= 8, "ARCHIVE_COUNT_LIMIT")
    paths = [pinned_file(p["path"], p["sha256"], 8 * 1024**2) for p in plan["metadata_archives"]]
    objects, inventories = lineage.load_archives(paths)
    entries = request["objects"]
    require(0 < len(entries) <= 256 and request["object_count"] == len(entries), "REQUEST_COUNT_MISMATCH")
    keys = [lineage.key(e["uri"]) for e in entries]
    require(len(set(keys)) == len(keys), "DUPLICATE_REQUEST_OBJECT")
    matched = []
    for item, key in zip(entries, keys):
        require(item["kind"] in ("gold_commit", "gold_identity"), "UNSUPPORTED_FOLLOWUP_KIND")
        require(key in objects, "MISSING_PINNED_METADATA")
        raw = objects[key]
        record = {"uri": item["uri"], "kind": item["kind"], "raw_file_sha256": lineage.digest(raw)}
        if item["kind"] == "gold_identity":
            payload_sha, _ = lineage.verify_gold_identity(raw, item["sha256"])
            record["identity_payload_sha256"] = payload_sha
        else:
            require(lineage.digest(raw) == item["sha256"], "COMMIT_PIN_MISMATCH")
        matched.append(record)

    data_path = pinned_file(plan["data_archive"], plan["data_archive_sha256"], 2 * 1024**3)
    require(plan["data_archive_sha256"] == request["basis_data_archive_sha256"], "DATA_BASIS_MISMATCH")
    publication_uris = request["unpinned_publication_uris"]
    require(0 < len(publication_uris) <= 256 and len(set(publication_uris)) == len(publication_uris), "PUBLICATION_SET_INVALID")
    receipts, snapshot_count = [], 0
    with zipfile.ZipFile(data_path) as archive:
        infos = archive.infolist()
        require(len(infos) <= 10000 and len({i.filename for i in infos}) == len(infos), "DATA_ARCHIVE_ENTRY_SET_INVALID")
        phase_by_run = {}
        for info in infos:
            if info.filename.endswith("/publish.json"):
                phase, phase_sha = read_phase(archive, info.filename)
                run = phase["gold_run_id"]
                require(run not in phase_by_run, "DUPLICATE_PHASE_RUN")
                phase_by_run[run] = (info.filename, phase, phase_sha)
        # A follow-up may request only the final head (Trí); earlier phases
        # remain in the independently pinned archive and were audited separately.
        require(len(phase_by_run) >= len(publication_uris), "PUBLICATION_PHASE_COUNT_MISMATCH")
        for uri in publication_uris:
            key = lineage.key(uri)
            require(key in objects, "MISSING_PUBLICATION")
            raw = objects[key]
            pub = decode(raw)
            run = pub["gold_run_id"]
            require(uri == lineage.PREFIX + "silver/gold-verification/" + run + "/publication.json", "PUBLICATION_URI_MISMATCH")
            require(run in phase_by_run, "MISSING_PINNED_PHASE")
            phase_member, phase, phase_sha = phase_by_run[run]
            require(pub == phase, "PUBLICATION_PHASE_CONTENT_MISMATCH")
            require(pub.get("verify_status") == "PASSED" and pub.get("published") is True, "PUBLICATION_NOT_PASSED")
            prefix = "silver/gold-commits/" + run + "/"
            require(prefix + "commit.json" in objects and prefix + "identity.json" in objects, "MISSING_CHAIN_OBJECT")
            commit_raw, identity_raw = objects[prefix + "commit.json"], objects[prefix + "identity.json"]
            commit, identity = decode(commit_raw), decode(identity_raw)
            lineage.verify_gold_identity(identity_raw, commit["identity_sha256"])
            require(pub["bundle_sha256"] == lineage.digest(commit_raw), "PUBLICATION_COMMIT_PIN_MISMATCH")
            require(pub["identity_sha256"] == commit["identity_sha256"], "PUBLICATION_IDENTITY_MISMATCH")
            require(commit["gold_run_id"] == run, "COMMIT_RUN_MISMATCH")
            require(pub["namespace"] == commit["namespace"] == identity["namespace"], "NAMESPACE_MISMATCH")
            require(commit["scope"] == identity["scope"], "IDENTITY_SCOPE_MISMATCH")
            gold_member = phase_member.removesuffix("/publish.json") + "/gold.json"
            gold, _ = read_phase(archive, gold_member)
            require(gold == commit, "COMMIT_PHASE_CONTENT_MISMATCH")
            require(commit["tables"] == pub["snapshot_bundle"] and len(commit["tables"]) == 6, "SIX_SNAPSHOT_BUNDLE_MISMATCH")
            require(set(commit["tables"]) == {"event_current", "event_source_bridge", "dim_date", "dim_region", "dim_magnitude_band", "dim_depth_band"}, "SIX_TABLE_SET_MISMATCH")
            for ref in commit["tables"].values():
                require(type(ref["snapshot_id"]) is int and 0 < ref["snapshot_id"] < 2**63, "INVALID_SNAPSHOT_ID")
                snapshot_count += 1
            checks = pub.get("checks")
            require(isinstance(checks, dict) and checks, "RECORDED_CHECKS_REQUIRED")
            require(all(isinstance(c, dict) and "actual" in c and "expected" in c and c["actual"] == c["expected"] for c in checks.values()), "RECORDED_CHECK_FAILED")
            if run == request["head_run_id"]:
                require(phase_member == request["head_phase_publish_member"], "HEAD_PHASE_MISMATCH")
                require(phase_sha == request["head_phase_publish_sha256"], "HEAD_PUBLISH_PIN_MISMATCH")
                read_phase(archive, request["head_phase_gold_member"], request["head_phase_gold_sha256"])
            receipts.append({"uri": uri, "raw_file_sha256": lineage.digest(raw), "phase_member": phase_member,
                             "phase_raw_sha256": phase_sha, "recorded_checks_matched": len(checks)})
    require(any(r["phase_member"] == request["head_phase_publish_member"] for r in receipts), "HEAD_PUBLICATION_REQUIRED")
    return {"version": "dat03-followup-verification-receipt-v1", "owner": plan["owner"],
            "request_sha256": plan["request_sha256"], "metadata_archives": inventories,
            "data_archive_sha256": plan["data_archive_sha256"], "head_run_id": request["head_run_id"],
            "pinned_objects_matched": len(matched), "publication_receipts_matched": len(receipts),
            "phase_groups_in_data_archive": len(phase_by_run),
            "other_phase_groups_not_verified_by_this_receipt": len(phase_by_run) - len(receipts),
            "snapshot_references_matched": snapshot_count, "objects": matched, "publications": receipts,
            "full_transitive_closure_verified": False, "portable_import_ready": False,
            "official_export_ready": False, "research_ready": False, "ml_upstream_eligible": False,
            "note": "Offline original-metadata verification. Recorded checks are historical evidence, not live SQL. Snapshot IDs are not portable snapshot files."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan_path = pinned_file(args.plan, args.plan_sha256, 1024**2)
    result = verify(decode(plan_path.read_bytes()))
    result["plan_sha256"] = args.plan_sha256
    require(not args.output.exists() and not args.output.is_symlink(), "OUTPUT_ALREADY_EXISTS")
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    print(json.dumps({k: result[k] for k in ("owner", "pinned_objects_matched", "publication_receipts_matched", "snapshot_references_matched", "portable_import_ready")}))


if __name__ == "__main__":
    main()
