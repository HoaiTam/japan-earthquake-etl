#!/usr/bin/env python3
"""DAT-03 cross-boundary catalog integration verification harness.

Verifies month contiguity, timestamp monotonicity, and ID disjointness
across the three worker/catalog boundaries:
  - Boundary 1: 2008-05 (Catalog nền) <-> 2008-06 (HoaiTam)
  - Boundary 2: 2013-07 (HoaiTam) <-> 2013-08 (ThanhTris)
  - Boundary 3: 2018-09 (ThanhTris) <-> 2018-10 (rosy179)
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
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


def verify_segment_contiguity(segments: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Verify that a sequence of segments covers 2000-01 to 2023-12 without gaps or overlaps."""
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
    prev_ids: Set[str], next_ids: Set[str], id_type: str = "canonical_event_id"
) -> Dict[str, Any]:
    """Verify that ID sets across two adjacent packages have no collision."""
    intersection = prev_ids.intersection(next_ids)
    return {
        "id_type": id_type,
        "status": "PASS" if not intersection else "FAIL",
        "duplicate_count": len(intersection),
        "sample_duplicates": sorted(list(intersection))[:5],
    }


def verify_boundary_time_window(
    prev_month: str,
    max_prev_dt_iso: Optional[str],
    next_month: str,
    min_next_dt_iso: Optional[str],
) -> Dict[str, Any]:
    """Verify that event timestamps in adjacent months conform to UTC month boundaries."""
    results: Dict[str, Any] = {
        "prev_month": prev_month,
        "next_month": next_month,
        "status": "PASS",
        "violations": [],
    }

    if max_prev_dt_iso:
        dt_prev = datetime.fromisoformat(max_prev_dt_iso.replace("Z", "+00:00"))
        # max_prev must be in or before prev_month
        next_month_start = datetime.fromisoformat(f"{next_month}-01T00:00:00+00:00")
        if dt_prev >= next_month_start:
            results["status"] = "FAIL"
            results["violations"].append(
                f"PREV_MONTH_OVERFLOW: max timestamp {max_prev_dt_iso} exceeds {prev_month} cutoff"
            )

    if min_next_dt_iso:
        dt_next = datetime.fromisoformat(min_next_dt_iso.replace("Z", "+00:00"))
        # min_next must be on or after next_month
        next_month_start = datetime.fromisoformat(f"{next_month}-01T00:00:00+00:00")
        if dt_next < next_month_start:
            results["status"] = "FAIL"
            results["violations"].append(
                f"NEXT_MONTH_UNDERFLOW: min timestamp {min_next_dt_iso} is before {next_month} start"
            )

    return results


def verify_all_boundaries(
    boundary_records: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Execute end-to-end audit for the 3 boundaries between the 4 segments."""
    contiguity = verify_segment_contiguity(EXPECTED_SEGMENTS)
    boundary_results = []

    for b in EXPECTED_BOUNDARIES:
        b_name = b["name"]
        data = (boundary_records or {}).get(b_name, {})
        prev_ids = set(data.get("prev_ids", []))
        next_ids = set(data.get("next_ids", []))
        disjoint_res = verify_id_disjointness(prev_ids, next_ids)

        time_res = verify_boundary_time_window(
            b["prev_month"],
            data.get("max_prev_time_utc"),
            b["next_month"],
            data.get("min_next_time_utc"),
        )

        b_pass = disjoint_res["status"] == "PASS" and time_res["status"] == "PASS"
        boundary_results.append(
            {
                "boundary": b_name,
                "status": "PASS" if b_pass else "FAIL",
                "prev_month": b["prev_month"],
                "next_month": b["next_month"],
                "disjointness": disjoint_res,
                "time_window": time_res,
            }
        )

    all_pass = contiguity["status"] == "PASS" and all(
        b["status"] == "PASS" for b in boundary_results
    )

    return {
        "status": "PASS" if all_pass else "FAIL",
        "contiguity": contiguity,
        "boundaries": boundary_results,
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="Optional path to export audit JSON report.",
    )
    args = parser.parse_args()

    report = verify_all_boundaries()
    formatted = json.dumps(report, indent=2)
    print(formatted)

    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(formatted, encoding="utf-8")
        print(f"\nSaved report to: {args.json_output}")

    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
