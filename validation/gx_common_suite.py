"""
gx_common_suite.py
Common Quality & Error Framework (Great Expectations) — applies the shared
null / duplicate / schema / permissible-value checks to ANY dataset, driven
entirely by that dataset's schema_*.yaml. Dataset-specific checks are added
separately in a custom suite (see gx_custom_suite_project_monitoring.py).
"""
import yaml
import great_expectations as gx


def load_schema(schema_path: str) -> dict:
    with open(schema_path, "r") as f:
        return yaml.safe_load(f)


def build_common_suite(validator, schema: dict):
    """Apply the four common-framework checks using only the schema file —
    no dataset-specific logic lives here."""

    field_names = [f["name"] for f in schema["fields"]]

    # 1) NULL CHECK — every field marked required: true
    for f in schema["fields"]:
        if f.get("required"):
            validator.expect_column_values_to_not_be_null(f["name"])

    # 2) DUPLICATE CHECK — every field(s) listed under unique_fields
    for col in schema.get("unique_fields", []):
        validator.expect_column_values_to_be_unique(col)

    # 3) SCHEMA VALIDATION — column set must match the schema exactly
    validator.expect_table_columns_to_match_set(field_names, exact_match=True)

    for f in schema["fields"]:
        ge_type = {"string": "str", "float": "float64",
                   "integer": "int64", "date": "str"}.get(f["dtype"])
        if ge_type:
            validator.expect_column_values_to_be_of_type(f["name"], ge_type)

    # 4) PERMISSIBLE VALUES — any field with a permissible_values list
    for f in schema["fields"]:
        if "permissible_values" in f:
            validator.expect_column_values_to_be_in_set(
                f["name"], f["permissible_values"]
            )

    return validator


def run_common_suite(df, schema_path: str, suite_name: str = "common_quality_suite",
                      custom_suite_fn=None):
    """custom_suite_fn: optional callable(validator) -> validator, e.g.
    gx_custom_suite_project_monitoring.build_custom_suite. Passing it here
    is what actually combines the common + custom suites into ONE
    checkpoint — omitting it (the previous behavior) silently checkpoints
    the common suite alone."""
    schema = load_schema(schema_path)

    context = gx.get_context()
    datasource = context.sources.add_or_update_pandas(name="gates_common_ds")
    asset = datasource.add_dataframe_asset(name=schema["dataset"])
    batch_request = asset.build_batch_request(dataframe=df)

    context.add_or_update_expectation_suite(suite_name)
    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name=suite_name
    )

    build_common_suite(validator, schema)
    if custom_suite_fn is not None:
        custom_suite_fn(validator)
    validator.save_expectation_suite(discard_failed_expectations=False)

    checkpoint = context.add_or_update_checkpoint(
        name=f"{schema['dataset']}_common_checkpoint",
        validator=validator,
    )
    result = checkpoint.run()
    return result


if __name__ == "__main__":
    import pandas as pd
    from gx_custom_suite_project_monitoring import build_custom_suite

    df = pd.read_parquet("/staging/dost-pchrd/project_monitoring/2026-08-25/raw.parquet")
    result = run_common_suite(df, schema_path="schema_project_monitoring.yaml",
                               custom_suite_fn=build_custom_suite)
    print("Common + custom suite success:", result.success)
