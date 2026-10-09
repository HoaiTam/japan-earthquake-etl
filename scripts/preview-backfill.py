"""Offline ORC-03 preview only; cannot download, execute an adapter or write lake data."""
import argparse
import json
from pathlib import Path
import sys

repository = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repository / "airflow/dags"))
from backfill_runtime import BackfillError, USGS_SETTINGS, preview_summary, resolve
from etl_pipeline_runtime import EtlContractError, _unique_json_object
from jma_backfill_runtime import JmaRunnerError

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--conf", required=True, help="Exact JSON configuration file")
    args = parser.parse_args()
    try:
        conf = json.loads(Path(args.conf).read_text(), object_pairs_hook=_unique_json_object)
        conf["preview"] = True  # This CLI has no real execution flag.
        # Public example settings only. Do not load the user's .env/credentials.
        settings = {}
        for line in (repository / ".env.example").read_text().splitlines():
            key, separator, value = line.partition("=")
            if separator and key in {*USGS_SETTINGS, "DATA_BUCKET", "BRONZE_PREFIX", "USGS_SEED_START_UTC", "PIPELINE_OVERLAP_DAYS"}:
                settings[key] = value
        plan = resolve({"dag_run": {"conf": conf}}, {**settings,
            "JMA_INVENTORY_PATH": str(repository / "config/jma/hypocenter_archives_v1.csv"),
            "USGS_MAX_WINDOW_DAYS": "3", "CONFIG_VERSION": "1"})
        print(json.dumps(preview_summary(plan), indent=2, ensure_ascii=False))
    except (OSError, ValueError, TypeError, BackfillError, EtlContractError, JmaRunnerError):
        parser.error("INVALID_BACKFILL_PREVIEW; inspect explicit configuration fields")
