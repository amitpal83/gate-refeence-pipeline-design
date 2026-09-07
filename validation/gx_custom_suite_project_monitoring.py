"""
gx_custom_suite_project_monitoring.py
Dataset-specific customization layered on top of the common suite —
business rules unique to R&D Project Monitoring.
"""
def build_custom_suite(validator):
    # Budget utilized cannot exceed budget allocated
    validator.expect_column_pair_values_A_to_be_greater_than_B(
        column_A="budget_allocated_php",
        column_B="budget_utilized_php",
        or_equal=True,
    )

    # end_date must be after start_date when populated
    validator.expect_column_pair_values_A_to_be_greater_than_B(
        column_A="end_date", column_B="start_date", or_equal=False,
        parse_strings_as_datetimes=True, ignore_row_if="either_value_is_missing",
    )

    # start_date must fall within the program's validity window
    validator.expect_column_values_to_be_between(
        "start_date", min_value="2017-01-01", max_value="2026-12-31",
        parse_strings_as_datetimes=True,
    )

    validator.save_expectation_suite(discard_failed_expectations=False)
    return validator
