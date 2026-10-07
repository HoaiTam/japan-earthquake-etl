"""Offline preview wrapper; does not read .env, download archives or write Bronze."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import uuid

repository = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repository / "airflow" / "dags"))
from jma_backfill_runtime import JmaRunnerError, resolve_plan

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", required=True, help="Comma-separated years, e.g. 1997,2023")
    args = parser.parse_args()
    try:
        years = [int(value.strip()) for value in args.years.split(",")]
        plan = resolve_plan({"run_id": "preview-" + uuid.uuid4().hex,
                             "logical_date": datetime.now(timezone.utc),
                             "dag_run": {"conf": {"years": years, "preview": True}}},
                            {"JMA_INVENTORY_PATH": str(repository / "config/jma/hypocenter_archives_v1.csv"),
                             "JMA_STAGING_ROOT": str(repository / "staging/jma")})
        print(json.dumps(plan, indent=2))
    except (ValueError, OSError, JmaRunnerError) as exception:
        parser.error(str(exception))
