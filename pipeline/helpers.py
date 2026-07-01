import os
import json
import subprocess
import uuid
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def get_run_dir(run_config: dict) -> Path:
    """Returns the run directory path for the given run config."""
    return PROJECT_ROOT / "runs" / run_config["run_id"]


def build_run_config(params: dict) -> dict:
    """
    Build the run config dict from Airflow params.
    Auto-generates run_id if not provided.
    """
    run_id = params["run_id"] if params["run_id"] else str(uuid.uuid4())[:8]
    return {**params, "run_id": run_id}


def prepare_run_dir(run_config: dict) -> Path:
    """
    Create the run directory structure and write config.json.
    Returns the run directory path.
    """
    run_dir = PROJECT_ROOT / "runs" / run_config["run_id"]
    run_dir.mkdir(parents=True, exist_ok=True)

    with open(run_dir / "config.json", "w") as f:
        json.dump(run_config, f, indent=2)

    return run_dir


def run_agent_batch(run_config: dict, run_dir: Path) -> Path:
    """
    Run mini-swe-agent on the configured task slice.
    Writes trajectories and preds.json to run_dir/run-agent/.
    Returns the path to preds.json.
    """
    outputs_dir = run_dir / "run-agent"
    outputs_dir.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            "uv", "run", "mini-extra", "swebench",
            "--subset", run_config["subset"],
            "--split", run_config["split"],
            "--model", run_config["model"],
            "--slice", run_config["task_slice"],
            "--workers", str(run_config["workers"]),
            "-o", str(outputs_dir),
        ],
        cwd=PROJECT_ROOT,
        env={**os.environ, "MSWEA_COST_TRACKING": "ignore_errors"},
        check=True,
    )

    preds_path = outputs_dir / "preds.json"
    if not preds_path.exists():
        raise FileNotFoundError(
            f"preds.json not found in {outputs_dir}, agent may have produced no output"
        )

    return preds_path


def run_swebench_eval(run_config: dict, preds_path: Path, run_dir: Path) -> Path:
    """
    Run SWE-bench evaluation on preds.json.
    Writes logs and reports to run_dir/run-eval/.
    Returns the path to the eval output directory.
    """
    eval_dir = run_dir / "run-eval"
    eval_dir.mkdir(parents=True, exist_ok=True)

    subprocess.run(
        [
            "python", "-m", "swebench.harness.run_evaluation",
            "--dataset_name", "princeton-nlp/SWE-bench_Verified",
            "--predictions_path", str(preds_path),
            "--max_workers", str(run_config["workers"]),
            "--run_id", run_config["run_id"],
            "--report_dir", str(eval_dir),
        ],
        cwd=str(eval_dir),
        check=True,
    )

    return eval_dir


def collect_metrics(run_config: dict, eval_dir: Path) -> dict:
    """
    Parse the SWE-bench evaluation results JSON and compute metrics.
    Returns a dict of metrics ready for MLflow logging.
    """
    model_slug = run_config["model"].replace("/", "__")
    results_file = eval_dir / f"{model_slug}.{run_config['split']}.json"

    with open(results_file, "r") as f:
        results = json.load(f)

    submitted = results["submitted_instances"]
    completed = results["completed_instances"]

    return {
        # raw counts
        "total_instances":       results["total_instances"],
        "submitted_instances":   submitted,
        "completed_instances":   completed,
        "resolved_instances":    results["resolved_instances"],
        "unresolved_instances":  results["unresolved_instances"],
        "empty_patch_instances": results["empty_patch_instances"],
        "error_instances":       results["error_instances"],

        # rates
        "resolved_rate":         results["resolved_instances"]    / submitted if submitted > 0 else 0.0,
        "completion_rate":       completed                        / submitted if submitted > 0 else 0.0,
        "error_rate":            results["error_instances"]       / submitted if submitted > 0 else 0.0,
        "empty_patch_rate":      results["empty_patch_instances"] / submitted if submitted > 0 else 0.0,
        "resolved_of_completed": results["resolved_instances"]    / completed if completed > 0 else 0.0,
    }


def log_mlflow_run(run_config: dict, metrics: dict, artifact_uris: list[str]) -> None:
    """
    Log params, metrics, and artifact references to MLflow.
    All artifacts are logged inside a single MLflow run.
    """
    import mlflow

    with mlflow.start_run(run_name=run_config["run_id"]):
        mlflow.log_params(run_config)
        mlflow.log_metrics(metrics)
        for uri in artifact_uris:
            mlflow.log_artifact(uri)
