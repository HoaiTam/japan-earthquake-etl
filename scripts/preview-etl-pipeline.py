"""Preview or execute the ORC-01 offline mock chain, never a real adapter."""

import argparse
import json
from pathlib import Path
import sys

repository = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repository / "airflow" / "dags"))
from etl_pipeline_runtime import (EtlContractError, PHASES, execute_phase,
                                  publication_summary, resolve_run_context)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute-mock", action="store_true", help="Validate all six fixture phases offline")
    args = parser.parse_args()
    try:
        context = resolve_run_context({"run_id": "orc-01-offline-fixture", "dag_run": {"conf": {"mode": "mock"}}},
                                      {"CONFIG_VERSION": "orc-01-fixture-v1"})
        output = {"run_context": context, "phases": list(PHASES), "real_adapter_invoked": False}
        if args.execute_mock:
            upstream = None
            for phase in PHASES:
                upstream = execute_phase(phase, context, upstream, {})
            output["summary"] = publication_summary(context, upstream)
        print(json.dumps(output, indent=2))
    except EtlContractError as exception:
        parser.error(str(exception))
