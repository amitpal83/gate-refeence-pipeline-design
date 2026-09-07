"""Great Expectations validation entry point used by the Airflow DAG."""
import sys
import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.ingestion_framework import load_contract  # noqa: E402

HARD_RULES = {"null_check", "duplicate_check", "schema_validation",
              "budget_utilized_le_allocated", "end_date_after_start_date"}


def _try_real_great_expectations(df: pd.DataFrame, schema_path: Path):
    try:
        import great_expectations as gx  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "Great Expectations is required for the reference implementation. "
            "Install the production dependencies before running validation."
        ) from None

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import gx_common_suite
    # NOTE: hardcoded to project_monitoring's custom suite — same known
    # single-dataset limitation as validate()'s hardcoded schema filename
    # below (see README "Known follow-ups"). A multi-dataset version would
    # look this up by convention (e.g. gx_custom_suite_{dataset}) instead.
    from gx_custom_suite_project_monitoring import build_custom_suite

    result = gx_common_suite.run_common_suite(
        df, schema_path=str(schema_path), custom_suite_fn=build_custom_suite,
    )
    return result


def validate(staged_csv_paths: list[str], config_dir: Path) -> dict:
    dfs = [pd.read_csv(p, dtype=str, keep_default_na=False, na_values=[""]) for p in staged_csv_paths]
    df = pd.concat(dfs, ignore_index=True)

    contract = load_contract(config_dir / "schema_project_monitoring.yaml")

    real_gx_result = _try_real_great_expectations(df, config_dir / "schema_project_monitoring.yaml")
    success = bool(getattr(real_gx_result, "success", False))
    results = [{
        "rule": "great_expectations_suite",
        "column": "*",
        "severity": "hard",
        "success": success,
        "detail": "Great Expectations suite completed",
    }]
    dqi = 100.0 if success else 0.0

    report = {
        "dataset": contract["dataset"],
        "engine": "great_expectations",
        "validated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": len(df),
        "hard_rule_pass": success,
        "data_quality_index": dqi,
        "rule_results": results,
    }
    return report, df


if __name__ == "__main__":
    import glob
    config_dir = Path(__file__).resolve().parents[1] / "config"
    staging_root = Path(__file__).resolve().parents[1] / "staging"
    paths = glob.glob(str(staging_root / "**" / "raw_*.csv"), recursive=True)
    report, df = validate(paths, config_dir)
    print(json.dumps(report, indent=2, default=str))
