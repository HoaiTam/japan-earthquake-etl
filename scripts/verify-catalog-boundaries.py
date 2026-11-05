#!/usr/bin/env python3
"""DAT-03 cross-boundary catalog integration verification harness.

Verifies month contiguity, timestamp monotonicity, and ID disjointness
across the three worker/catalog boundaries:
  - Boundary 1: 2008-05 (Catalog nền) <-> 2008-06 (HoaiTam)
  - Boundary 2: 2013-07 (HoaiTam) <-> 2013-08 (ThanhTris)
  - Boundary 3: 2018-09 (ThanhTris) <-> 2018-10 (rosy179)

Fail-Closed Policy:
- Missing inputs, unverified empty months, or absent boundary data return NOT_CHECKED / INCOMPLETE.
- Overall status PASS is strictly prohibited unless real package manifests and boundary events
  are provided and pass all contiguity, timestamp, and ID disjointness audits.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Optional, Sequence, Set

ROOT = Path(__file__).resolve().parents[1]

EXPECTED_SEGMENTS = [
    {
        "segment_id": "catalog_base_101",
        "owner": "Catalog nền",
        "start_month": "2000-01",
        "end_month": "2008-05",
        "expected_months": 101,
    },
    {
        "segment_id": "hoaitam_62",
        "owner": "HoaiTam",
        "start_month": "2008-06",
        "end_month": "2013-07",
        "expected_months": 62,
    },
    {
        "segment_id": "thanhtris_62",
        "owner": "ThanhTris",
        "start_month": "2013-08",
        "end_month": "2018-09",
        "expected_months": 62,
    },
    {
        "segment_id": "rosy179_63",
        "owner": "rosy179",
        "start_month": "2018-10",
        "end_month": "2023-12",
        "expected_months": 63,
    },
]

EXPECTED_BOUNDARIES = [
    {
        "name": "Boundary 1 (Base -> HoaiTam)",
        "prev_segment": "catalog_base_101",
        "next_segment": "hoaitam_62",
        "prev_month": "2008-05",
        "next_month": "2008-06",
    },
    {
        "name": "Boundary 2 (HoaiTam -> ThanhTris)",
        "prev_segment": "hoaitam_62",
        "next_segment": "thanhtris_62",
        "prev_month": "2013-07",
        "next_month": "2013-08",
    },
    {
        "name": "Boundary 3 (ThanhTris -> rosy179)",
        "prev_segment": "thanhtris_62",
        "next_segment": "rosy179_63",
        "prev_month": "2018-09",
        "next_month": "2018-10",
    },
]


def month_to_int(m_str: str) -> int:
    """Convert YYYY-MM to linear integer month index."""
    if not isinstance(m_str, str) or not re.fullmatch(r"20\d{2}-(0[1-9]|1[0-2])", m_str):
        raise ValueError(f"INVALID_MONTH_FORMAT: {m_str}")
    year, month = map(int, m_str.split("-"))
    return year * 12 + (month - 1)


def int_to_month(m_int: int) -> str:
    """Convert linear integer month index to YYYY-MM."""
    year = m_int // 12
    month = (m_int % 12) + 1
    return f"{year:04d}-{month:02d}"


def expand_month_range(start: str, end_inclusive: str) -> List[str]:
    """Generate all months inclusively between start and end."""
    a, b = month_to_int(start), month_to_int(end_inclusive)
    if b < a:
        raise ValueError(f"INVALID_RANGE: {start} to {end_inclusive}")
    return [int_to_month(i) for i in range(a, b + 1)]


def verify_manifest_contiguity(manifests: Optional[Sequence[Dict[str, Any]]]) -> Dict[str, Any]:
    """Verify that actual package manifests cover 2000-01 to 2023-12 without gaps or overlaps.

    Fail-closed policy:
    - If manifests is None or empty: returns NOT_CHECKED.
    - If any manifest has SHA mismatch or missing fields: returns FAIL.
    """
    if not manifests:
        return {
            "status": "NOT_CHECKED",
            "reason": "MISSING_PACKAGE_MANIFESTS",
            "total_months": 0,
            "expected_months": 288,
            "missing_months": expand_month_range("2000-01", "2023-12"),
            "overlapping_months": [],
            "unexpected_months": [],
            "packages": [],
        }

    all_months: List[str] = []
    seen_months: Set[str] = set()
    overlaps: List[str] = []
    package_details = []
    violations = []

    for pkg in manifests:
        pkg_id = pkg.get("package_id", "unknown_pkg")
        start_m = pkg.get("start_month")
        end_m = pkg.get("end_month")
        cov_months = pkg.get("covered_months")

        if not start_m or not end_m:
            violations.append(f"PACKAGE_MISSING_MONTH_RANGE:{pkg_id}")
            continue

        try:
            expected_range = expand_month_range(start_m, end_m)
        except Exception as ex:
            violations.append(f"INVALID_MONTH_RANGE:{pkg_id}:{ex}")
            continue

        if cov_months is not None:
            if list(cov_months) != expected_range:
                violations.append(
                    f"COVERED_MONTHS_MISMATCH:{pkg_id}: expected {len(expected_range)} months, got {len(cov_months)}"
                )
            m_list = list(cov_months)
        else:
            m_list = expected_range

        # SHA-256 validation if raw_bytes or expected_sha is provided
        expected_sha = pkg.get("package_sha256")
        manifest_bytes = pkg.get("manifest_bytes")
        if expected_sha and manifest_bytes:
            computed_sha = hashlib.sha256(manifest_bytes).hexdigest()
            if computed_sha != expected_sha:
                violations.append(f"MANIFEST_SHA256_MISMATCH:{pkg_id}: expected {expected_sha}, computed {computed_sha}")

        for m in m_list:
            if m in seen_months:
                overlaps.append(m)
            seen_months.add(m)
            all_months.append(m)

        package_details.append({
            "package_id": pkg_id,
            "owner": pkg.get("owner", "unknown"),
            "start_month": start_m,
            "end_month": end_m,
            "month_count": len(m_list),
            "row_count": pkg.get("row_count"),
            "package_sha256": expected_sha,
        })

    expected_full = expand_month_range("2000-01", "2023-12")
    missing = [m for m in expected_full if m not in seen_months]
    unexpected = [m for m in seen_months if m not in set(expected_full)]

    passed = (
        len(all_months) == 288
        and len(missing) == 0
        and len(overlaps) == 0
        and len(unexpected) == 0
        and len(violations) == 0
    )

    return {
        "status": "PASS" if passed else "FAIL",
        "violations": violations,
        "total_months": len(all_months),
        "expected_months": 288,
        "missing_months": missing,
        "overlapping_months": overlaps,
        "unexpected_months": unexpected,
        "packages": package_details,
    }


def verify_segment_contiguity(segments: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Verify that a sequence of segment descriptors covers 2000-01 to 2023-12 without gaps or overlaps.

    Note: This validates theoretical calendar contiguity. For manifest-backed package contiguity,
    use verify_manifest_contiguity.
    """
    all_months: List[str] = []
    seen_months: Set[str] = set()
    overlaps: List[str] = []
    segment_details = []

    for seg in segments:
        s_start = seg["start_month"]
        s_end = seg["end_month"]
        m_list = expand_month_range(s_start, s_end)
        expected_cnt = seg.get("expected_months", len(m_list))
        actual_cnt = len(m_list)

        if actual_cnt != expected_cnt:
            raise ValueError(
                f"SEGMENT_COUNT_MISMATCH: {seg['segment_id']} expected {expected_cnt}, got {actual_cnt}"
            )

        for m in m_list:
            if m in seen_months:
                overlaps.append(m)
            seen_months.add(m)
            all_months.append(m)

        segment_details.append(
            {
                "segment_id": seg["segment_id"],
                "owner": seg.get("owner", "unknown"),
                "start_month": s_start,
                "end_month": s_end,
                "month_count": actual_cnt,
            }
        )

    expected_full = expand_month_range("2000-01", "2023-12")
    missing = [m for m in expected_full if m not in seen_months]
    unexpected = [m for m in seen_months if m not in set(expected_full)]

    passed = len(all_months) == 288 and len(missing) == 0 and len(overlaps) == 0 and len(unexpected) == 0

    return {
        "status": "PASS" if passed else "FAIL",
        "total_months": len(all_months),
        "expected_months": 288,
        "missing_months": missing,
        "overlapping_months": overlaps,
        "unexpected_months": unexpected,
        "segments": segment_details,
    }


def verify_id_disjointness(
    prev_ids: Optional[Sequence[str]],
    next_ids: Optional[Sequence[str]],
    id_type: str = "canonical_event_id",
    allow_empty: bool = False,
) -> Dict[str, Any]:
    """Verify that ID sets across two adjacent packages have no collision.

    Fail-closed policy:
    - If either ID set is None, returns NOT_CHECKED.
    - If an ID set is empty and allow_empty is False, returns FAIL (UNVERIFIED_EMPTY_INPUT).
    - If duplicates exist, returns FAIL.
    """
    if prev_ids is None or next_ids is None:
        return {
            "id_type": id_type,
            "status": "NOT_CHECKED",
            "reason": "MISSING_ID_SETS",
            "duplicate_count": 0,
            "sample_duplicates": [],
        }

    s_prev = set(prev_ids)
    s_next = set(next_ids)

    if not allow_empty and (len(s_prev) == 0 or len(s_next) == 0):
        return {
            "id_type": id_type,
            "status": "FAIL",
            "reason": "UNVERIFIED_EMPTY_INPUT: Boundary check requires non-empty event ID sets unless explicitly verified empty by manifest",
            "duplicate_count": 0,
            "sample_duplicates": [],
        }

    intersection = s_prev.intersection(s_next)
    return {
        "id_type": id_type,
        "status": "PASS" if not intersection else "FAIL",
        "reason": None if not intersection else "DUPLICATE_EVENT_IDS",
        "duplicate_count": len(intersection),
        "sample_duplicates": sorted(list(intersection))[:5],
    }


def verify_boundary_time_window(
    prev_month: str,
    max_prev_dt_iso: Optional[str],
    next_month: str,
    min_next_dt_iso: Optional[str],
    allow_empty: bool = False,
) -> Dict[str, Any]:
    """Verify that event timestamps in adjacent months conform to UTC month boundaries.

    Fail-closed policy:
    - If timestamps are missing and allow_empty is False, returns NOT_CHECKED.
    - If max_prev_dt >= next_month_start, returns FAIL (PREV_MONTH_OVERFLOW).
    - If min_next_dt < next_month_start, returns FAIL (NEXT_MONTH_UNDERFLOW).
    - If max_prev_dt > min_next_dt, returns FAIL (TIMESTAMP_INVERSION).
    """
    results: Dict[str, Any] = {
        "prev_month": prev_month,
        "next_month": next_month,
        "status": "PASS",
        "violations": [],
    }

    if not max_prev_dt_iso or not min_next_dt_iso:
        if allow_empty:
            results["status"] = "PASS"
            results["note"] = "VERIFIED_EMPTY_MONTH_TIMESTAMP_SKIPPED"
            return results
        results["status"] = "NOT_CHECKED"
        results["violations"].append("MISSING_BOUNDARY_TIMESTAMPS")
        return results

    try:
        dt_prev = datetime.fromisoformat(max_prev_dt_iso.replace("Z", "+00:00"))
        dt_next = datetime.fromisoformat(min_next_dt_iso.replace("Z", "+00:00"))
        next_month_start = datetime.fromisoformat(f"{next_month}-01T00:00:00+00:00")
    except Exception as ex:
        results["status"] = "FAIL"
        results["violations"].append(f"INVALID_TIMESTAMP_FORMAT: {ex}")
        return results

    # 1. Overflow check
    if dt_prev >= next_month_start:
        results["status"] = "FAIL"
        results["violations"].append(
            f"PREV_MONTH_OVERFLOW: max timestamp {max_prev_dt_iso} exceeds {prev_month} cutoff"
        )

    # 2. Underflow check
    if dt_next < next_month_start:
        results["status"] = "FAIL"
        results["violations"].append(
            f"NEXT_MONTH_UNDERFLOW: min timestamp {min_next_dt_iso} is before {next_month} start"
        )

    # 3. Monotonicity check
    if dt_prev > dt_next:
        results["status"] = "FAIL"
        results["violations"].append(
            f"TIMESTAMP_INVERSION: max prev timestamp {max_prev_dt_iso} is later than min next timestamp {min_next_dt_iso}"
        )

    return results


def verify_all_boundaries(
    boundary_records: Optional[Dict[str, Dict[str, Any]]] = None,
    package_manifests: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Execute end-to-end audit for the 3 boundaries across the 4 catalog segments.

    Fail-closed policy:
    - If boundary_records is None or empty: returns NOT_CHECKED.
    - If package_manifests is provided, audits manifest contiguity; otherwise checks theoretical contiguity.
    - Requires all 3 boundaries to be verified with PASS to yield overall PASS.
    """
    if package_manifests is not None:
        contiguity = verify_manifest_contiguity(package_manifests)
    else:
        # Theoretical calendar verification
        theo = verify_segment_contiguity(EXPECTED_SEGMENTS)
        contiguity = {
            "status": "PASS_THEORETICAL",
            "note": "CALENDAR_MATH_ONLY_NO_MANIFESTS_VERIFIED",
            "total_months": theo["total_months"],
            "expected_months": theo["expected_months"],
            "missing_months": theo["missing_months"],
            "overlapping_months": theo["overlapping_months"],
            "unexpected_months": theo["unexpected_months"],
            "segments": theo["segments"],
        }

    if not boundary_records:
        boundary_results = []
        for b in EXPECTED_BOUNDARIES:
            boundary_results.append({
                "boundary": b["name"],
                "status": "NOT_CHECKED",
                "reason": "MISSING_BOUNDARY_INPUT",
                "prev_month": b["prev_month"],
                "next_month": b["next_month"],
                "disjointness": {"status": "NOT_CHECKED", "reason": "NO_DATA"},
                "time_window": {"status": "NOT_CHECKED", "violations": ["NO_DATA"]},
            })

        return {
            "status": "NOT_CHECKED",
            "gate_status": "PENDING_CATALOG_MERGE",
            "reason": "NO_BOUNDARY_RECORDS_PROVIDED: Cross-boundary deduplication and timestamp checks require boundary input data.",
            "contiguity": contiguity,
            "boundaries": boundary_results,
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    boundary_results = []
    for b in EXPECTED_BOUNDARIES:
        b_name = b["name"]
        data = boundary_records.get(b_name)
        if not data:
            boundary_results.append({
                "boundary": b_name,
                "status": "NOT_CHECKED",
                "reason": "MISSING_BOUNDARY_INPUT",
                "prev_month": b["prev_month"],
                "next_month": b["next_month"],
                "disjointness": {"status": "NOT_CHECKED", "reason": "NO_DATA"},
                "time_window": {"status": "NOT_CHECKED", "violations": ["NO_DATA"]},
            })
            continue

        prev_ids = data.get("prev_ids")
        next_ids = data.get("next_ids")
        allow_empty = bool(data.get("allow_empty", False))

        disjoint_res = verify_id_disjointness(
            prev_ids=prev_ids,
            next_ids=next_ids,
            allow_empty=allow_empty,
        )

        time_res = verify_boundary_time_window(
            prev_month=b["prev_month"],
            max_prev_dt_iso=data.get("max_prev_time_utc"),
            next_month=b["next_month"],
            min_next_dt_iso=data.get("min_next_time_utc"),
            allow_empty=allow_empty,
        )

        b_pass = disjoint_res["status"] == "PASS" and time_res["status"] == "PASS"
        b_not_checked = disjoint_res["status"] == "NOT_CHECKED" or time_res["status"] == "NOT_CHECKED"

        boundary_status = "PASS" if b_pass else ("NOT_CHECKED" if b_not_checked else "FAIL")

        boundary_results.append({
            "boundary": b_name,
            "status": boundary_status,
            "prev_month": b["prev_month"],
            "next_month": b["next_month"],
            "disjointness": disjoint_res,
            "time_window": time_res,
        })

    has_fail = any(b["status"] == "FAIL" for b in boundary_results) or contiguity["status"] == "FAIL"
    has_not_checked = any(b["status"] == "NOT_CHECKED" for b in boundary_results) or contiguity["status"] != "PASS"

    if has_fail:
        overall_status = "FAIL"
    elif has_not_checked:
        overall_status = "NOT_CHECKED"
    else:
        overall_status = "PASS"

    return {
        "status": overall_status,
        "gate_status": "VERIFIED_PASS" if overall_status == "PASS" else "PENDING_CATALOG_MERGE",
        "contiguity": contiguity,
        "boundaries": boundary_results,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--boundary-data",
        type=Path,
        default=None,
        help="Path to JSON file containing boundary event IDs and timestamps for the 3 boundaries.",
    )
    parser.add_argument(
        "--manifests-file",
        type=Path,
        default=None,
        help="Path to JSON file containing list of 4 package manifests.",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="Optional path to export audit JSON report.",
    )
    args = parser.parse_args()

    boundary_records = None
    if args.boundary_data:
        if not args.boundary_data.is_file():
            print(f"ERROR: Boundary data file not found: {args.boundary_data}", file=sys.stderr)
            return 1
        boundary_records = json.loads(args.boundary_data.read_text(encoding="utf-8"))

    package_manifests = None
    if args.manifests_file:
        if not args.manifests_file.is_file():
            print(f"ERROR: Manifests file not found: {args.manifests_file}", file=sys.stderr)
            return 1
        package_manifests = json.loads(args.manifests_file.read_text(encoding="utf-8"))

    report = verify_all_boundaries(
        boundary_records=boundary_records,
        package_manifests=package_manifests,
    )
    formatted = json.dumps(report, indent=2)
    print(formatted)

    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(formatted, encoding="utf-8")
        print(f"\nSaved report to: {args.json_output}")

    if report["status"] == "PASS":
        return 0
    elif report["status"] == "NOT_CHECKED":
        print("\n[FAIL-CLOSED] Boundary verification audit is NOT_CHECKED. Input data required.", file=sys.stderr)
        return 2
    else:
        print("\n[FAIL-CLOSED] Boundary verification audit FAILED.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
