"""Great Expectations validation entry point used by the Airflow DAG."""
import sys
import json
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common.config_registry import ConfigRegistry  # noqa: E402


def _try_real_great_expectations(df: pd.DataFrame, contract: dict, dataset: str):
    try:
        import great_expectations as gx  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "Great Expectations is required for the reference implementation. "
            "Install the production dependencies before running validation."
        ) from None

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import gx_common_suite
    custom_builder = None
    custom_module = Path(__file__).resolve().parent / f"gx_custom_suite_{dataset}.py"
    if custom_module.exists():
        module = __import__(f"gx_custom_suite_{dataset}", fromlist=["build_custom_suite"])
        custom_builder = getattr(module, "build_custom_suite", None)

    result = gx_common_suite.run_common_suite(
        df, schema=contract, custom_suite_fn=custom_builder,
    )
    return result


def validate(staged_csv_paths: list[str], config_dir: Path, dataset: str = "project_monitoring") -> dict:
    """`dataset` must name a dataset ConfigRegistry can resolve — either a
    config/schema_{dataset}.yaml file or a row in the config database's
    dataset_registry table. Callers (the Airflow DAG factory) must always
    pass the dataset explicitly; the default here only covers ad-hoc/CLI
    use against the reference dataset."""
    if not staged_csv_paths:
        raise ValueError("No staged files were provided for validation")
    dfs = [pd.read_csv(p, dtype=str, keep_default_na=False, na_values=[""]) for p in staged_csv_paths]
    df = pd.concat(dfs, ignore_index=True)

    contract = ConfigRegistry(yaml_dir=config_dir).get_dataset(dataset)
    canonical_fields = [field["name"] for field in contract["canonical_schema"]["fields"]]
    technical_fields = {
        "_cdc_operation", "_source_table", "_operation_timestamp",
        "_source_object", "_ingested_at", "_batch_id",
    }
    unexpected = set(df.columns) - set(canonical_fields) - technical_fields
    if unexpected:
        raise ValueError(f"Unexpected staged columns for {dataset}: {sorted(unexpected)}")
    validation_df = df[[column for column in df.columns if column in canonical_fields]]
    # Every staged CSV is read with dtype=str above (deliberately, to avoid
    # pandas' own type-inference surprises across sources) — but that means
    # every column is still a raw string here, including budget/numeric
    # fields. Left uncast, expect_column_values_to_be_of_type("float64")
    # always fails (observed type is str), and worse,
    # expect_column_pair_values_A_to_be_greater_than_B compares them as
    # STRINGS ('7500000.0' < '900000.0' lexicographically, even though it's
    # numerically larger) — silently wrong, not just a failed check. Cast
    # each canonical field to its declared dtype before validating.
    for field in contract["canonical_schema"]["fields"]:
        name = field["name"]
        if name not in validation_df.columns:
            continue
        if field["dtype"] == "float":
            validation_df[name] = pd.to_numeric(validation_df[name], errors="coerce")
        elif field["dtype"] == "integer":
            validation_df[name] = pd.to_numeric(validation_df[name], errors="coerce").astype("Int64")

    real_gx_result = _try_real_great_expectations(validation_df, contract, dataset)
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
