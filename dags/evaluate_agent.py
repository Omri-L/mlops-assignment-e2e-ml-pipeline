import sys
import json
from datetime import datetime
from pathlib import Path
from airflow.decorators import dag, task
from airflow.models.param import Param

# make pipeline/ importable from the project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.helpers import (
    build_run_config,
    prepare_run_dir,
    run_agent_batch,
    run_swebench_eval,
    collect_metrics,
    log_mlflow_run,
    write_manifest,
    get_run_dir,
)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "model": Param(default="nebius/moonshotai/Kimi-K2.6", type="string", description="The model to evaluate."),
        "subset": Param(default="verified", type="string", description="The subset of the dataset to evaluate on."),
        "split": Param(default="test", type="string", description="The split of the dataset to evaluate on."),
        "workers": Param(default=5, type="integer", description="Number of parallel workers."),
        "task_slice": Param(default="0:3", type="string", description="Slice of tasks to run, e.g. '0:3'."),
        "run_id": Param(default=None, type=["null", "string"], description="Run ID. Auto-generated if empty."),
        "cost_limit": Param(default="1.5", type="string", description="Cost limit for the agent. '0' = no limit.")
    }
)
def evaluate_agent():

    @task
    def prepare_run(**context) -> dict:
        """Build config, create run directory, write config.json. Returns config dict."""
        run_config = build_run_config(context["params"])
        prepare_run_dir(run_config)
        return run_config

    @task
    def run_agent(run_config: dict) -> str:
        """Run mini-swe-agent batch and write outputs to run-agent/. Returns preds.json path."""
        preds_path = run_agent_batch(run_config, get_run_dir(run_config))
        return str(preds_path)

    @task
    def run_eval(run_config: dict, preds_path: str) -> str:
        """Run SWE-bench evaluation and write results to run-eval/. Returns eval dir path."""
        eval_dir = run_swebench_eval(run_config, Path(preds_path), get_run_dir(run_config))
        return str(eval_dir)

    @task
    def summarize_and_log(run_config: dict, eval_dir: str) -> str:
        """Parse metrics, write metrics.json, and log everything to MLflow."""
        run_dir = get_run_dir(run_config)
        eval_path = Path(eval_dir)

        metrics = collect_metrics(run_config, eval_path)

        with open(run_dir / "metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)

        manifest_path = write_manifest(run_config, eval_path)

        log_mlflow_run(run_config, metrics, [
            str(run_dir / "config.json"),
            str(run_dir / "metrics.json"),
            str(manifest_path),
        ])

        return str(run_dir / "metrics.json")

    run_config = prepare_run()
    preds_path = run_agent(run_config)
    eval_dir   = run_eval(run_config, preds_path)
    summarize_and_log(run_config, eval_dir)


evaluate_agent()
