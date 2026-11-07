#!/usr/bin/env python3
"""DAT-03 cross-boundary catalog integration verification harness.

Verifies month contiguity, timestamp monotonicity, and ID disjointness
across the three worker/catalog boundaries:
  - Boundary 1: 2008-05 (Catalog nền) <-> 2008-06 (HoaiTam)
  - Boundary 2: 2013-07 (HoaiTam) <-> 2013-08 (ThanhTris)
  - Boundary 3: 2018-09 (ThanhTris) <-> 2018-10 (rosy179)

Fail-Closed Policy:
- Missing inputs, unverified empty months, or absent boundary data return NOT_CHECKED / INCOMPLETE.
- Overall status PASS and gate_status VERIFIED_PASS are strictly prohibited unless real
  trusted package manifests and boundary events are provided and pass all contiguity,
  two-sided timestamp extrema, and ID disjointness audits.
- Caller-declared manifest bytes and self-computed hashes are NOT trusted without independent
  registry pins. Empty manifest JSON `{}` is strictly rejected.
- Non-empty boundary months require all 4 timestamp extrema (min_prev, max_prev, min_next, max_next)
  with explicit timezone offsets.
- Distinguishes manifest_sha256 (hash of manifest JSON) from package_sha256 (hash of archive ZIP).
- Descriptors or fixtures without verified independent registry pins yield FIXTURE_PASS or
  NOT_CHECKED, never VERIFIED_PASS.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

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

OFFICIAL_PACKAGES = {
    "catalog_base_101": {
        "aliases": {"catalog_base_101", "pkg_base_101", "pkg1"},
        "start_month": "2000-01",
        "end_month": "2008-05",
        "expected_months": 101,
        "owner": "Catalog nền",
    },
    "hoaitam_62": {
        "aliases": {"hoaitam_62", "pkg_hoaitam_62", "pkg_hoaitam", "pkg2"},
        "start_month": "2008-06",
        "end_month": "2013-07",
        "expected_months": 62,
        "owner": "HoaiTam",
    },
    "thanhtris_62": {
        "aliases": {"thanhtris_62", "pkg_thanhtris_62", "pkg_thanhtris", "pkg3"},
        "start_month": "2013-08",
        "end_month": "2018-09",
        "expected_months": 62,
        "owner": "ThanhTris",
    },
    "rosy179_63": {
        "aliases": {"rosy179_63", "pkg_rosy179_63", "pkg_rosy179", "pkg4"},
        "start_month": "2018-10",
        "end_month": "2023-12",
        "expected_months": 63,
        "owner": "rosy179",
    },
}

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


def get_month_boundaries_utc(m_str: str) -> Tuple[datetime, datetime]:
    """Return (start_utc, end_utc) for a given YYYY-MM month."""
    m_int = month_to_int(m_str)
    start_dt = datetime.fromisoformat(f"{m_str}-01T00:00:00+00:00")
    next_m_str = int_to_month(m_int + 1)
    end_dt = datetime.fromisoformat(f"{next_m_str}-01T00:00:00+00:00")
    return start_dt, end_dt


def parse_iso_utc(ts_str: Any, field_name: str) -> datetime:
    """Parse an ISO timestamp and verify it has an explicit timezone offset, converting to UTC."""
    if not isinstance(ts_str, str):
        raise ValueError(f"NON_STRING_TIMESTAMP:{field_name}:{type(ts_str).__name__}")
    s = ts_str.strip()
    if not re.search(r"(Z|[+-]\d{2}:?\d{2})$", s):
        raise ValueError(
            f"MISSING_TIMEZONE_OFFSET:{field_name}:'{s}' requires explicit timezone offset (e.g. 'Z' or '+00:00')"
        )
    iso_clean = s[:-1] + "+00:00" if s.endswith("Z") else s
    dt = datetime.fromisoformat(iso_clean)
    return dt.astimezone(timezone.utc)


def resolve_package_segment(package_id: str) -> Optional[str]:
    """Resolve given package_id to standard segment key if matching official aliases."""
    for seg_key, info in OFFICIAL_PACKAGES.items():
        if package_id in info["aliases"]:
            return seg_key
    return None


def verify_manifest_contiguity(
    manifests: Optional[Sequence[Dict[str, Any]]],
    require_trusted_registry: bool = True,
    require_row_count: bool = False,
    require_manifest_bytes: bool = False,
    trusted_registry_pins: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Verify that actual package manifests cover 2000-01 to 2023-12 without gaps or overlaps.

    Fail-closed policy:
    - If manifests is None or empty: returns NOT_CHECKED.
    - If require_trusted_registry is True:
        - Must contain exactly 4 packages matching the 4 official segment registry pins.
        - Fabricated single packages claiming the whole range are rejected.
    - Each manifest must supply covered_months explicitly.
    - Raw manifest bytes must be parsed as a valid non-empty JSON object.
    - An empty JSON object `{}` is strictly rejected.
    - package_id, coverage, and certified row count inside parsed manifest are cross-verified.
    - Distinguishes manifest_sha256 (hash of manifest JSON) from package_sha256 (hash of archive ZIP).
    - If trusted_registry_pins is supplied, hashes are checked against the independent trusted pins.
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
            "violations": ["MISSING_PACKAGE_MANIFESTS"],
        }

    violations: List[str] = []

    if require_trusted_registry:
        if len(manifests) != 4:
            violations.append(
                f"UNEXPECTED_PACKAGE_COUNT: expected 4 official packages, got {len(manifests)}"
            )
        recognized_segs: Set[str] = set()
        for p in manifests:
            p_id = p.get("package_id", "")
            seg_key = resolve_package_segment(p_id)
            if not seg_key:
                violations.append(f"UNTRUSTED_OR_UNKNOWN_PACKAGE_ID: {p_id}")
            else:
                if seg_key in recognized_segs:
                    violations.append(f"DUPLICATE_PACKAGE_SEGMENT: {seg_key} ({p_id})")
                recognized_segs.add(seg_key)

    all_months: List[str] = []
    seen_months: Set[str] = set()
    overlaps: List[str] = []
    package_details = []

    for pkg in manifests:
        pkg_id = pkg.get("package_id", "unknown_pkg")
        seg_key = resolve_package_segment(pkg_id)
        start_m = pkg.get("start_month")
        end_m = pkg.get("end_month")
        cov_months = pkg.get("covered_months")
        row_count = pkg.get("row_count")

        if not start_m or not end_m:
            violations.append(f"PACKAGE_MISSING_MONTH_RANGE:{pkg_id}")
            continue

        try:
            expected_range = expand_month_range(start_m, end_m)
        except Exception as ex:
            violations.append(f"INVALID_MONTH_RANGE:{pkg_id}:{ex}")
            continue

        if cov_months is None:
            violations.append(f"MISSING_COVERED_MONTHS_LIST:{pkg_id}: covered_months must be an explicit list")
            m_list = []
        else:
            m_list = list(cov_months)
            if m_list != expected_range:
                violations.append(
                    f"COVERED_MONTHS_MISMATCH:{pkg_id}: expected {len(expected_range)} months, got {len(m_list)}"
                )

        if require_row_count:
            if row_count is None:
                violations.append(f"MISSING_ROW_COUNT:{pkg_id}: manifest must state certified row_count")
            elif not isinstance(row_count, int) or row_count < 0:
                violations.append(f"INVALID_ROW_COUNT:{pkg_id}: {row_count}")

        # Resolve raw bytes safely from str, bytes, or file
        manifest_bytes = pkg.get("manifest_bytes")
        manifest_content = pkg.get("manifest_content")
        manifest_file = pkg.get("manifest_file")

        raw_b: Optional[bytes] = None
        if manifest_file:
            mf_path = Path(manifest_file)
            if mf_path.is_file():
                raw_b = mf_path.read_bytes()
            else:
                violations.append(f"MANIFEST_FILE_NOT_FOUND:{pkg_id}:{manifest_file}")
        elif manifest_bytes is not None:
            if isinstance(manifest_bytes, str):
                raw_b = manifest_bytes.encode("utf-8")
            elif isinstance(manifest_bytes, (bytes, bytearray)):
                raw_b = bytes(manifest_bytes)
            else:
                violations.append(f"INVALID_MANIFEST_BYTES_TYPE:{pkg_id}:{type(manifest_bytes).__name__}")
        elif manifest_content is not None:
            raw_b = str(manifest_content).encode("utf-8")

        # Parse manifest payload JSON (Blocker 1 requirement: reject empty {} or unparsed content)
        parsed_payload: Optional[Dict[str, Any]] = None
        certified_empty_for_pkg: Set[str] = set()
        parsed_rows: Optional[int] = None
        payload_has_id: bool = False
        payload_has_cov: bool = False
        pkg_violations: List[str] = []

        if raw_b is not None:
            try:
                parsed_raw = json.loads(raw_b.decode("utf-8"))
            except Exception as ex:
                err = f"MANIFEST_JSON_PARSE_ERROR:{pkg_id}:{ex}"
                violations.append(err)
                pkg_violations.append(err)
                parsed_raw = None

            if parsed_raw is not None:
                if not isinstance(parsed_raw, dict):
                    err = f"MANIFEST_PAYLOAD_NOT_OBJECT:{pkg_id}: manifest JSON root must be an object/dict"
                    violations.append(err)
                    pkg_violations.append(err)
                elif len(parsed_raw) == 0:
                    err = f"EMPTY_MANIFEST_PAYLOAD:{pkg_id}: manifest JSON payload is empty {{}}"
                    violations.append(err)
                    pkg_violations.append(err)
                else:
                    parsed_payload = parsed_raw
                    # Check package identity inside manifest payload
                    if "package_id" in parsed_payload or "segment_id" in parsed_payload:
                        payload_has_id = True
                        parsed_p_id = str(parsed_payload.get("package_id") or parsed_payload.get("segment_id"))
                        parsed_seg = resolve_package_segment(parsed_p_id)
                        if parsed_p_id != pkg_id and parsed_seg != seg_key:
                            err = f"MANIFEST_PACKAGE_ID_MISMATCH:{pkg_id}: manifest payload declares {parsed_p_id}, descriptor is {pkg_id}"
                            violations.append(err)
                            pkg_violations.append(err)
                    elif require_trusted_registry:
                        err = f"MANIFEST_PAYLOAD_MISSING_PACKAGE_ID:{pkg_id}: manifest JSON payload must declare 'package_id' or 'segment_id'"
                        violations.append(err)
                        pkg_violations.append(err)

                    # Check covered months or range inside payload
                    if "covered_months" in parsed_payload:
                        payload_has_cov = True
                        parsed_m_list = list(parsed_payload["covered_months"])
                        if parsed_m_list != expected_range:
                            err = f"MANIFEST_COVERED_MONTHS_MISMATCH:{pkg_id}: payload covered_months does not match expected range"
                            violations.append(err)
                            pkg_violations.append(err)
                    elif "start_month" in parsed_payload and "end_month" in parsed_payload:
                        payload_has_cov = True
                        if parsed_payload["start_month"] != start_m or parsed_payload["end_month"] != end_m:
                            err = f"MANIFEST_RANGE_MISMATCH:{pkg_id}: payload range {parsed_payload['start_month']}..{parsed_payload['end_month']} != {start_m}..{end_m}"
                            violations.append(err)
                            pkg_violations.append(err)
                    elif require_trusted_registry:
                        err = f"MANIFEST_PAYLOAD_MISSING_COVERAGE:{pkg_id}: manifest JSON payload must declare 'covered_months' or ('start_month' and 'end_month')"
                        violations.append(err)
                        pkg_violations.append(err)

                    # Check row count
                    if "row_count" in parsed_payload:
                        parsed_rows = parsed_payload.get("row_count")
                    elif "total_rows" in parsed_payload:
                        parsed_rows = parsed_payload.get("total_rows")

                    if require_row_count and parsed_rows is None:
                        err = f"MANIFEST_PAYLOAD_MISSING_ROW_COUNT:{pkg_id}: manifest JSON payload must declare certified 'row_count' or 'total_rows'"
                        violations.append(err)
                        pkg_violations.append(err)
                    elif parsed_rows is not None:
                        if not isinstance(parsed_rows, int) or parsed_rows < 0:
                            err = f"INVALID_MANIFEST_PAYLOAD_ROW_COUNT:{pkg_id}: payload row_count must be non-negative integer, got {parsed_rows}"
                            violations.append(err)
                            pkg_violations.append(err)
                        if row_count is not None and parsed_rows != row_count:
                            err = f"MANIFEST_ROW_COUNT_MISMATCH:{pkg_id}: descriptor={row_count}, manifest payload={parsed_rows}"
                            violations.append(err)
                            pkg_violations.append(err)

                    # Certified empty evidence per month from manifest payload strictly
                    # Strictly NO fallback to descriptor row_count to certify empty months!
                    if parsed_rows == 0:
                        certified_empty_for_pkg.update(expected_range)

                    monthly_counts = parsed_payload.get("monthly_counts") or parsed_payload.get("months_row_counts") or {}
                    if isinstance(monthly_counts, dict):
                        for m_key, c_val in monthly_counts.items():
                            if isinstance(c_val, int) and c_val == 0:
                                certified_empty_for_pkg.add(m_key)

                    if "empty_months" in parsed_payload and isinstance(parsed_payload["empty_months"], list):
                        for em in parsed_payload["empty_months"]:
                            if isinstance(em, str):
                                certified_empty_for_pkg.add(em)

        # Distinguish manifest_sha256 from package_sha256 (Blocker 1 requirement)
        expected_manifest_sha = pkg.get("manifest_sha256")
        expected_pkg_sha = pkg.get("package_sha256")

        if expected_pkg_sha and not expected_manifest_sha and raw_b is not None:
            err = f"SEPARATE_SHA_REQUIRED:{pkg_id}: manifest_bytes cannot be validated against package_sha256; manifest_sha256 is required"
            violations.append(err)
            pkg_violations.append(err)

        if require_manifest_bytes:
            if raw_b is None:
                err = f"MISSING_MANIFEST_BYTES:{pkg_id}: official catalog gate requires raw manifest bytes or file"
                violations.append(err)
                pkg_violations.append(err)
            if not expected_manifest_sha and not (trusted_registry_pins and seg_key in trusted_registry_pins):
                err = f"MISSING_MANIFEST_SHA256:{pkg_id}: official catalog gate requires trusted manifest_sha256 pin"
                violations.append(err)
                pkg_violations.append(err)

        computed_manifest_sha: Optional[str] = None
        if raw_b is not None:
            computed_manifest_sha = hashlib.sha256(raw_b).hexdigest()

        # Cross-reference against trusted registry pins if supplied
        expected_trusted_sha: Optional[str] = None
        if trusted_registry_pins is not None:
            if seg_key and seg_key in trusted_registry_pins:
                expected_trusted_sha = trusted_registry_pins[seg_key]
            elif pkg_id in trusted_registry_pins:
                expected_trusted_sha = trusted_registry_pins[pkg_id]
            elif seg_key and seg_key in OFFICIAL_PACKAGES:
                for al in OFFICIAL_PACKAGES[seg_key]["aliases"]:
                    if al in trusted_registry_pins:
                        expected_trusted_sha = trusted_registry_pins[al]
                        break

            if not expected_trusted_sha:
                err = f"UNPINNED_PACKAGE:{pkg_id}: missing trusted registry pin in trusted_registry_pins"
                violations.append(err)
                pkg_violations.append(err)
            elif computed_manifest_sha and computed_manifest_sha != expected_trusted_sha:
                err = f"MANIFEST_SHA256_MISMATCH_WITH_TRUSTED_REGISTRY:{pkg_id}: expected {expected_trusted_sha}, computed {computed_manifest_sha}"
                violations.append(err)
                pkg_violations.append(err)

        # Cross-reference against descriptor's declared manifest_sha256
        if expected_manifest_sha and computed_manifest_sha:
            if computed_manifest_sha != expected_manifest_sha:
                err = f"MANIFEST_SHA256_MISMATCH:{pkg_id}: descriptor expected {expected_manifest_sha}, computed {computed_manifest_sha}"
                violations.append(err)
                pkg_violations.append(err)
            if expected_trusted_sha and expected_manifest_sha != expected_trusted_sha:
                err = f"CALLER_PIN_DISAGREEMENT_WITH_TRUSTED_REGISTRY:{pkg_id}: caller declared {expected_manifest_sha}, trusted pin is {expected_trusted_sha}"
                violations.append(err)
                pkg_violations.append(err)

        for m in m_list:
            if m in seen_months:
                overlaps.append(m)
            seen_months.add(m)
            all_months.append(m)

        trusted_pin_verified = (
            expected_trusted_sha is not None
            and computed_manifest_sha == expected_trusted_sha
            and parsed_payload is not None
            and (not require_trusted_registry or (payload_has_id and payload_has_cov))
            and (not require_row_count or (parsed_rows is not None and isinstance(parsed_rows, int) and parsed_rows >= 0 and (row_count is None or parsed_rows == row_count)))
            and len(pkg_violations) == 0
        )

        package_details.append({
            "package_id": pkg_id,
            "owner": pkg.get("owner", "unknown"),
            "start_month": start_m,
            "end_month": end_m,
            "month_count": len(m_list),
            "row_count": row_count,
            "manifest_sha256": expected_manifest_sha or expected_trusted_sha,
            "computed_manifest_sha256": computed_manifest_sha,
            "package_sha256": expected_pkg_sha,
            "certified_empty_months": sorted(list(certified_empty_for_pkg)),
            "trusted_pin_verified": trusted_pin_verified,
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
    """Verify that a sequence of segment descriptors covers 2000-01 to 2023-12 without gaps or overlaps."""
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
    allow_empty_prev: Optional[bool] = None,
    allow_empty_next: Optional[bool] = None,
) -> Dict[str, Any]:
    """Verify that ID sets across two adjacent packages have no collision.

    Fail-closed policy:
    - If either ID set is None, returns NOT_CHECKED.
    - If prev_ids is empty and not allowed, returns FAIL (UNVERIFIED_EMPTY_INPUT).
    - If next_ids is empty and not allowed, returns FAIL (UNVERIFIED_EMPTY_INPUT).
    - If duplicates exist, returns FAIL.
    """
    if prev_ids is None or next_ids is None:
        return {
            "id_type": id_type,
            "status": "NOT_CHECKED",
            "reason": "MISSING_ID_SETS",
            "duplicate_check_scope": "ADJACENT_BOUNDARY_MONTHS",
            "duplicate_count": 0,
            "sample_duplicates": [],
        }

    s_prev = set(prev_ids)
    s_next = set(next_ids)

    can_prev_be_empty = allow_empty if allow_empty_prev is None else allow_empty_prev
    can_next_be_empty = allow_empty if allow_empty_next is None else allow_empty_next

    if len(s_prev) == 0 and not can_prev_be_empty:
        return {
            "id_type": id_type,
            "status": "FAIL",
            "reason": "UNVERIFIED_EMPTY_INPUT: Boundary check requires non-empty event ID sets unless explicitly verified empty by manifest (prev month is empty without certified empty proof)",
            "duplicate_check_scope": "ADJACENT_BOUNDARY_MONTHS",
            "duplicate_count": 0,
            "sample_duplicates": [],
        }

    if len(s_next) == 0 and not can_next_be_empty:
        return {
            "id_type": id_type,
            "status": "FAIL",
            "reason": "UNVERIFIED_EMPTY_INPUT: Boundary check requires non-empty event ID sets unless explicitly verified empty by manifest (next month is empty without certified empty proof)",
            "duplicate_check_scope": "ADJACENT_BOUNDARY_MONTHS",
            "duplicate_count": 0,
            "sample_duplicates": [],
        }

    intersection = s_prev.intersection(s_next)
    return {
        "id_type": id_type,
        "status": "PASS" if not intersection else "FAIL",
        "reason": None if not intersection else "DUPLICATE_EVENT_IDS",
        "duplicate_check_scope": "ADJACENT_BOUNDARY_MONTHS",
        "duplicate_count": len(intersection),
        "sample_duplicates": sorted(list(intersection))[:5],
    }


def verify_boundary_time_window(
    prev_month: str,
    max_prev_dt_iso: Optional[str],
    next_month: str,
    min_next_dt_iso: Optional[str],
    min_prev_dt_iso: Optional[str] = None,
    max_next_dt_iso: Optional[str] = None,
    allow_empty: bool = False,
    prev_month_certified_empty: bool = False,
    next_month_certified_empty: bool = False,
    certified_empty: bool = False,
) -> Dict[str, Any]:
    """Verify that event timestamps in adjacent months conform to two-sided UTC boundaries.

    Mandatory 4-extrema checks for non-empty months:
    - If prev_month is not certified empty: requires BOTH min_prev_time_utc and max_prev_time_utc.
    - If next_month is not certified empty: requires BOTH min_next_time_utc and max_next_time_utc.
    - Missing any required extremum returns status NOT_CHECKED.
    - All timestamps must have explicit timezone offsets (e.g. 'Z' or '+00:00').

    Two-sided extrema checks:
    - prev_month [prev_start, prev_end):
        min_prev >= prev_start (PREV_MONTH_UNDERFLOW)
        max_prev < prev_end (PREV_MONTH_OVERFLOW)
        min_prev <= max_prev (PREV_EXTREMA_INVERSION)
    - next_month [next_start, next_end):
        min_next >= next_start (NEXT_MONTH_UNDERFLOW)
        max_next < next_end (NEXT_MONTH_OVERFLOW)
        min_next <= max_next (NEXT_EXTREMA_INVERSION)
    - cross-boundary monotonicity:
        max_prev <= min_next (TIMESTAMP_INVERSION)
    """
    results: Dict[str, Any] = {
        "prev_month": prev_month,
        "next_month": next_month,
        "status": "PASS",
        "timezone": "UTC",
        "violations": [],
    }

    can_empty_prev = allow_empty and (certified_empty or prev_month_certified_empty)
    can_empty_next = allow_empty and (certified_empty or next_month_certified_empty)

    # Check for missing extrema in previous month
    missing_prev = []
    if not can_empty_prev:
        if not min_prev_dt_iso:
            missing_prev.append("min_prev_time_utc")
        if not max_prev_dt_iso:
            missing_prev.append("max_prev_time_utc")

    # Check for missing extrema in next month
    missing_next = []
    if not can_empty_next:
        if not min_next_dt_iso:
            missing_next.append("min_next_time_utc")
        if not max_next_dt_iso:
            missing_next.append("max_next_time_utc")

    if missing_prev or missing_next:
        results["status"] = "NOT_CHECKED"
        if missing_prev:
            results["violations"].append(
                f"MISSING_PREV_MONTH_EXTREMA:{prev_month}: non-empty prev_month requires both min and max ({', '.join(missing_prev)} missing)"
            )
        if missing_next:
            results["violations"].append(
                f"MISSING_NEXT_MONTH_EXTREMA:{next_month}: non-empty next_month requires both min and max ({', '.join(missing_next)} missing)"
            )
        return results

    if can_empty_prev and can_empty_next and not (min_prev_dt_iso or max_prev_dt_iso or min_next_dt_iso or max_next_dt_iso):
        results["status"] = "PASS"
        results["note"] = "VERIFIED_EMPTY_MONTH_TIMESTAMP_SKIPPED"
        return results

    try:
        prev_start, prev_end = get_month_boundaries_utc(prev_month)
        next_start, next_end = get_month_boundaries_utc(next_month)

        dt_min_prev = parse_iso_utc(min_prev_dt_iso, "min_prev_time_utc") if min_prev_dt_iso else None
        dt_max_prev = parse_iso_utc(max_prev_dt_iso, "max_prev_time_utc") if max_prev_dt_iso else None
        dt_min_next = parse_iso_utc(min_next_dt_iso, "min_next_time_utc") if min_next_dt_iso else None
        dt_max_next = parse_iso_utc(max_next_dt_iso, "max_next_time_utc") if max_next_dt_iso else None
    except Exception as ex:
        results["status"] = "FAIL"
        results["violations"].append(f"INVALID_TIMESTAMP_FORMAT: {ex}")
        return results

    # 1. Previous month boundaries
    if dt_min_prev is not None:
        if dt_min_prev < prev_start:
            results["status"] = "FAIL"
            results["violations"].append(
                f"PREV_MONTH_UNDERFLOW: min timestamp {min_prev_dt_iso} is before {prev_month} start {prev_start.isoformat()}"
            )
    if dt_max_prev is not None:
        if dt_max_prev >= prev_end:
            results["status"] = "FAIL"
            results["violations"].append(
                f"PREV_MONTH_OVERFLOW: max timestamp {max_prev_dt_iso} exceeds {prev_month} cutoff {prev_end.isoformat()}"
            )
    if dt_min_prev is not None and dt_max_prev is not None:
        if dt_min_prev > dt_max_prev:
            results["status"] = "FAIL"
            results["violations"].append(
                f"PREV_EXTREMA_INVERSION: min timestamp {min_prev_dt_iso} > max timestamp {max_prev_dt_iso}"
            )

    # 2. Next month boundaries
    if dt_min_next is not None:
        if dt_min_next < next_start:
            results["status"] = "FAIL"
            results["violations"].append(
                f"NEXT_MONTH_UNDERFLOW: min timestamp {min_next_dt_iso} is before {next_month} start {next_start.isoformat()}"
            )
    if dt_max_next is not None:
        if dt_max_next >= next_end:
            results["status"] = "FAIL"
            results["violations"].append(
                f"NEXT_MONTH_OVERFLOW: max timestamp {max_next_dt_iso} exceeds {next_month} cutoff {next_end.isoformat()}"
            )
    if dt_min_next is not None and dt_max_next is not None:
        if dt_min_next > dt_max_next:
            results["status"] = "FAIL"
            results["violations"].append(
                f"NEXT_EXTREMA_INVERSION: min timestamp {min_next_dt_iso} > max timestamp {max_next_dt_iso}"
            )

    # 3. Cross-boundary monotonicity
    if dt_max_prev is not None and dt_min_next is not None:
        if dt_max_prev > dt_min_next:
            results["status"] = "FAIL"
            results["violations"].append(
                f"TIMESTAMP_INVERSION: max prev timestamp {max_prev_dt_iso} > min next timestamp {min_next_dt_iso}"
            )

    if results["violations"]:
        results["status"] = "FAIL"

    return results


def verify_all_boundaries(
    boundary_records: Optional[Dict[str, Dict[str, Any]]] = None,
    package_manifests: Optional[Sequence[Dict[str, Any]]] = None,
    audit_scope: str = "OFFICIAL_CATALOG_GATE",
    trusted_registry_pins: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Execute end-to-end audit for the 3 boundaries across the 4 catalog segments.

    Fail-closed policy:
    - If boundary_records is None or empty: returns NOT_CHECKED.
    - If package_manifests is provided, audits manifest contiguity with trusted package registry.
    - Requires all 3 boundaries and contiguity to PASS.
    - Only returns gate_status: VERIFIED_PASS if:
        - overall status is PASS,
        - audit_scope is OFFICIAL_CATALOG_GATE,
        - real verified package manifests are provided with valid non-empty JSON payloads,
        - trusted_registry_pins is supplied and covers all 4 official segments,
        - boundary timestamps supply all 4 mandatory extrema with explicit timezones.
    - Otherwise gate_status remains PENDING_CATALOG_MERGE or FIXTURE_PASS.
    """
    is_official_scope = (audit_scope == "OFFICIAL_CATALOG_GATE")

    if package_manifests is not None:
        contiguity = verify_manifest_contiguity(
            package_manifests,
            require_trusted_registry=is_official_scope,
            require_row_count=is_official_scope,
            require_manifest_bytes=is_official_scope,
            trusted_registry_pins=trusted_registry_pins,
        )
    else:
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
            "violations": [],
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
            "audit_scope": audit_scope,
            "reason": "NO_BOUNDARY_RECORDS_PROVIDED: Cross-boundary deduplication and timestamp checks require boundary input data.",
            "contiguity": contiguity,
            "boundaries": boundary_results,
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    # Map verified empty months from packages with verified manifest payloads
    certified_empty_months: Set[str] = set()
    if package_manifests is not None and contiguity.get("status") == "PASS":
        for p_info in contiguity.get("packages", []):
            if is_official_scope:
                # Under official scope: only certify empty months if trusted registry pin was verified
                if trusted_registry_pins and p_info.get("trusted_pin_verified"):
                    certified_empty_months.update(p_info.get("certified_empty_months", []))
            else:
                certified_empty_months.update(p_info.get("certified_empty_months", []))

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
        prev_m = b["prev_month"]
        next_m = b["next_month"]

        if is_official_scope:
            # Under official gate: caller self-declared certified_empty is strictly rejected without verified manifest proof
            prev_certified_empty = (prev_m in certified_empty_months)
            next_certified_empty = (next_m in certified_empty_months)
            certified_empty = False
            can_empty_prev = allow_empty and prev_certified_empty
            can_empty_next = allow_empty and next_certified_empty
            effective_allow_empty = allow_empty and (prev_certified_empty or next_certified_empty)
        else:
            prev_certified_empty = bool(data.get("prev_certified_empty", False)) or (prev_m in certified_empty_months)
            next_certified_empty = bool(data.get("next_certified_empty", False)) or (next_m in certified_empty_months)
            certified_empty = bool(data.get("certified_empty", False))
            can_empty_prev = allow_empty and (prev_certified_empty or certified_empty)
            can_empty_next = allow_empty and (next_certified_empty or certified_empty)
            effective_allow_empty = allow_empty and (prev_certified_empty or next_certified_empty or certified_empty)

        disjoint_res = verify_id_disjointness(
            prev_ids=prev_ids,
            next_ids=next_ids,
            allow_empty=effective_allow_empty,
            allow_empty_prev=can_empty_prev,
            allow_empty_next=can_empty_next,
        )

        time_res = verify_boundary_time_window(
            prev_month=prev_m,
            max_prev_dt_iso=data.get("max_prev_time_utc"),
            next_month=next_m,
            min_next_dt_iso=data.get("min_next_time_utc"),
            min_prev_dt_iso=data.get("min_prev_time_utc"),
            max_next_dt_iso=data.get("max_next_time_utc"),
            allow_empty=effective_allow_empty,
            prev_month_certified_empty=prev_certified_empty,
            next_month_certified_empty=next_certified_empty,
            certified_empty=certified_empty,
        )

        b_has_fail = disjoint_res["status"] == "FAIL" or time_res["status"] == "FAIL"
        b_has_not_checked = disjoint_res["status"] == "NOT_CHECKED" or time_res["status"] == "NOT_CHECKED"

        if b_has_fail:
            boundary_status = "FAIL"
        elif b_has_not_checked:
            boundary_status = "NOT_CHECKED"
        else:
            boundary_status = "PASS"

        boundary_results.append({
            "boundary": b_name,
            "status": boundary_status,
            "prev_month": prev_m,
            "next_month": next_m,
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

    # Only assign VERIFIED_PASS if real manifests with verified trusted registry pins are supplied
    if overall_status == "PASS":
        all_trusted_pins_verified = False
        if package_manifests is not None and len(package_manifests) == 4 and trusted_registry_pins:
            pkgs = contiguity.get("packages", [])
            all_trusted_pins_verified = len(pkgs) == 4 and all(
                p.get("trusted_pin_verified") is True for p in pkgs
            )

        if package_manifests is not None and is_official_scope and all_trusted_pins_verified:
            gate_status = "VERIFIED_PASS"
        else:
            gate_status = "FIXTURE_PASS"
    else:
        gate_status = "PENDING_CATALOG_MERGE"

    return {
        "status": overall_status,
        "gate_status": gate_status,
        "audit_scope": audit_scope,
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
        "--trusted-pins-file",
        type=Path,
        default=None,
        help="Path to JSON file containing trusted manifest SHA-256 pins for the official packages.",
    )
    parser.add_argument(
        "--audit-scope",
        type=str,
        default="OFFICIAL_CATALOG_GATE",
        help="Audit scope (OFFICIAL_CATALOG_GATE or DESCRIPTOR_FIXTURE).",
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
        try:
            boundary_records = json.loads(args.boundary_data.read_text(encoding="utf-8"))
        except Exception as ex:
            print(f"ERROR: Failed to parse boundary data JSON: {ex}", file=sys.stderr)
            return 1

    package_manifests = None
    if args.manifests_file:
        if not args.manifests_file.is_file():
            print(f"ERROR: Manifests file not found: {args.manifests_file}", file=sys.stderr)
            return 1
        try:
            package_manifests = json.loads(args.manifests_file.read_text(encoding="utf-8"))
        except Exception as ex:
            print(f"ERROR: Failed to parse manifests file JSON: {ex}", file=sys.stderr)
            return 1

    trusted_pins = None
    if args.trusted_pins_file:
        if not args.trusted_pins_file.is_file():
            print(f"ERROR: Trusted pins file not found: {args.trusted_pins_file}", file=sys.stderr)
            return 1
        try:
            trusted_pins = json.loads(args.trusted_pins_file.read_text(encoding="utf-8"))
        except Exception as ex:
            print(f"ERROR: Failed to parse trusted pins JSON: {ex}", file=sys.stderr)
            return 1

    report = verify_all_boundaries(
        boundary_records=boundary_records,
        package_manifests=package_manifests,
        audit_scope=args.audit_scope,
        trusted_registry_pins=trusted_pins,
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
