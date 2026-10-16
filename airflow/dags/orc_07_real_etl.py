"""Manual exact full-month reprocessing; same adapters as the sole ORC-02 daily owner."""
import pendulum
from airflow.sdk import dag, task, get_current_context
from source_run_guard import lease
import real_etl_runtime as runtime


@dag(dag_id="orc_07_real_etl", schedule=None, start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
     catchup=False, is_paused_upon_creation=True, max_active_runs=1, max_active_tasks=1,
     default_args={"retries": 0, "trigger_rule": "all_success"}, tags=["orc-07", "real", "scoped"])
def real_etl():
    @task(task_id="resolve_request")
    def resolve_request():
        ctx = get_current_context(); conf = ctx["dag_run"].conf
        runtime.require(isinstance(conf, dict) and set(conf) == {"request_key", "request_sha256", "window_start_utc", "window_end_utc"}, "EXACT_REAL_CONF_REQUIRED")
        return runtime.resolve("orc_07_real_etl", ctx["run_id"], conf["request_key"], conf["request_sha256"],
                               conf["window_start_utc"], conf["window_end_utc"])

    @task(task_id="acquire")
    def acquire(context):
        return lease("acquire", {"dag_id": context["dag_id"], "run_id": context["run_id"]})

    @task(task_id="execute")
    def execute(context, acquired):
        return runtime.execute(context)

    @task(task_id="release")
    def release(context, summary):
        return runtime.release(context, summary)

    @task(task_id="completion_gate")
    def completion_gate(summary, released):
        runtime.require(summary["published"] is True and released["status"] == "RELEASED", "REAL_COMPLETION_REQUIRED")
        return summary

    context = resolve_request(); acquired = acquire(context)
    summary = execute(context, acquired); released = release(context, summary)
    completion_gate(summary, released)


real_etl()
