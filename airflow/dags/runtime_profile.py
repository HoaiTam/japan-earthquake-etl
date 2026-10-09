"""ORC-05 conservative local policy; pools/task slots do not replace run leases."""
import os

VERSION = "orc-05-local-v1"
MUTATING_RETRIES = 0
VERIFY_RETRIES = 1
MAX_ACTIVE_TASKS = 1


def source_heap(environment=None):
    env = os.environ if environment is None else environment
    value = env.get("SOURCE_RUNNER_HEAP", "384m")
    if value not in {"128m", "192m", "256m", "384m", "512m"}:
        raise ValueError("INVALID_SOURCE_RUNNER_HEAP")
    return value


def archive_concurrency(environment=None):
    env = os.environ if environment is None else environment
    requested = int(env.get("JMA_BACKFILL_MAX_CONCURRENCY", "1"))
    if not 1 <= requested <= 4:
        raise ValueError("JMA_BACKFILL_MAX_CONCURRENCY must be within 1..4")
    # Existing .env=2 remains readable; local profile has a hard ceiling of one.
    return min(requested, MAX_ACTIVE_TASKS)
