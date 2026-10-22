"""QA-01 design-only metadata planner. No DAG, ETL, network, catalog or publication writes.

This does not enable ORC-06/07 v2. Closed-month production guards remain intact.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re


VERSION = "current-month-asof-v1"
FIELDS = {"policy_version", "release_status", "coverage_mode", "publication_quality",
          "source_policy", "required_event_sources", "jma_role", "jma_inventory_end_year",
          "usgs_seed_start_utc", "revision_window_days", "max_affected_months", "max_rows",
          "bootstrap_strategy", "absence_is_tombstone", "research_ready",
          "publication_allowed", "ml_upstream_eligible"}


def require(condition, reason):
    if not condition:
        raise ValueError("ASOF_" + reason)


def utc(value):
    require(isinstance(value, str) and bool(re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z", value)), "UTC_Z_REQUIRED")
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def stamp(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def validate_policy(policy):
    require(isinstance(policy, dict) and set(policy) == FIELDS, "EXACT_POLICY_FIELDS_REQUIRED")
    expected = {"policy_version": VERSION, "release_status": "DESIGN_ONLY",
                "coverage_mode": "OBSERVED_AS_OF", "publication_quality": "PROVISIONAL",
                "source_policy": "USGS_OPERATIONAL_WITH_JMA_HISTORICAL_BASELINE",
                "required_event_sources": ["USGS"], "jma_role": "HISTORICAL_READINESS_ONLY",
                "bootstrap_strategy": "REFETCH_AFFECTED_MONTHS_TO_EVENT_CUTOFF",
                "usgs_seed_start_utc": "2023-01-01T00:00:00Z"}
    for key, value in expected.items():
        require(policy[key] == value, "POLICY_VALUE_CONFLICT:" + key)
    for key, value in (("jma_inventory_end_year", 2023), ("revision_window_days", 3),
                       ("max_affected_months", 2), ("max_rows", 100000)):
        require(type(policy[key]) is int and policy[key] == value, "TYPED_POLICY_LIMIT_REQUIRED:" + key)
    for key in ("absence_is_tombstone", "research_ready", "publication_allowed", "ml_upstream_eligible"):
        require(policy[key] is False, "DESIGN_MUST_NOT_AUTHORIZE_PUBLICATION:" + key)


def _unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def load_policy(raw):
    require(isinstance(raw, bytes) and 0 < len(raw) <= 8192, "BOUNDED_POLICY_REQUIRED")
    value = json.loads(raw, object_pairs_hook=_unique,
                       parse_constant=lambda _: require(False, "FINITE_JSON_REQUIRED"))
    validate_policy(value)
    return value


def plan_scheduled_run(run, policy, *, now=None):
    """Compute required event-time intervals, NOT observed source coverage or live acceptance."""
    validate_policy(policy)
    require(isinstance(run, dict) and set(run) == {
        "dag_id", "run_id", "run_type", "data_interval_start", "data_interval_end", "conf"},
        "EXACT_RUN_FIELDS_REQUIRED")
    require(run["dag_id"] == "orc_02_daily_sources" and run["run_type"] == "scheduled"
            and isinstance(run["run_id"], str) and re.fullmatch(
                r"scheduled__\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)", run["run_id"])
            and isinstance(run["conf"], dict) and run["conf"] == {}, "SCHEDULED_EMPTY_CONF_REQUIRED")
    # Validate the timestamp's calendar, but read the interval from Airflow metadata,
    # not the run-id timestamp (timetable/version conventions can differ).
    utc(run["run_id"][len("scheduled__"):].replace("+00:00", "Z"))
    start, end = utc(run["data_interval_start"]), utc(run["data_interval_end"])
    clock = datetime.now(timezone.utc) if now is None else now
    require(isinstance(clock, datetime) and clock.utcoffset() is not None, "AWARE_CLOCK_REQUIRED")
    require(end - start == timedelta(days=1) and end <= clock, "CLOSED_DAILY_INTERVAL_REQUIRED")
    # The existing daily profile runs no earlier than 00:15 UTC.
    require(end.hour > 0 or end.minute >= 15, "DAILY_BOUNDARY_TOO_EARLY")
    require(end.second == 0 and end.microsecond == 0, "CRON_MINUTE_BOUNDARY_REQUIRED")
    cutoff = end.replace(hour=0, minute=0, second=0, microsecond=0)
    seed = utc(policy["usgs_seed_start_utc"])
    require(cutoff > seed, "TARGET_PRECEDES_SOURCE_BASELINE")
    revision_start = max(seed, cutoff - timedelta(days=policy["revision_window_days"]))
    cursor = revision_start.replace(day=1)
    scopes = []
    while cursor < cutoff:
        following = (cursor.replace(day=28) + timedelta(days=4)).replace(day=1)
        scopes.append({"month": cursor.strftime("%Y-%m"), "source_system": "USGS",
                       "event_start_utc": stamp(max(seed, cursor)),
                       "event_end_utc_exclusive": stamp(min(following, cutoff)),
                       "coverage_mode": "OBSERVED_AS_OF", "observation_cutoff_utc": None,
                       "capture_status": "NOT_CAPTURED"})
        cursor = following
    require(0 < len(scopes) <= policy["max_affected_months"], "MONTH_SCOPE_LIMIT")
    result = {"plan_version": VERSION, "status": "DESIGN_PLAN_ONLY", "deployable": False,
              "run_id": run["run_id"], "dag_id": run["dag_id"], "run_context_sha256": digest(run),
              "policy_sha256": digest(policy), "event_cutoff_utc": stamp(cutoff),
              "target_start_utc": stamp(cutoff - timedelta(days=1)),
              "target_end_utc_exclusive": stamp(cutoff), "revision_start_utc": stamp(revision_start),
              "required_capture_scopes": scopes, "jma_role": policy["jma_role"],
              "required_event_sources": list(policy["required_event_sources"]),
              "observed_coverage_proven": False, "publication_quality": "PROVISIONAL",
              "published": False, "ml_upstream_eligible": False, "research_ready": False}
    result["plan_sha256"] = digest(result)
    return result
