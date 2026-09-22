from forecasting.data_quality import REQUIRED_MONTHS, evaluate_month_coverage
from forecasting.schemas import SILVER_NON_NULL_COLUMNS, SILVER_REQUIRED_COLUMNS


def test_silver_schema_expectations_include_foundation_time_fields():
    assert "pickup_datetime" in SILVER_REQUIRED_COLUMNS
    assert "five_minute_slot" in SILVER_REQUIRED_COLUMNS
    assert "year" in SILVER_NON_NULL_COLUMNS
    assert "month" in SILVER_NON_NULL_COLUMNS


def test_required_months_cover_warmup_train_validation_and_test():
    assert REQUIRED_MONTHS[0] == "2022-08"
    assert REQUIRED_MONTHS[-1] == "2023-02"


def test_month_coverage_reports_missing_periods():
    result = evaluate_month_coverage(["2022-10", "2022-12", "2023-01", "2023-02"])

    assert result.passed is False
    assert result.missing_months == ["2022-08", "2022-09", "2022-11"]


def test_month_coverage_passes_when_all_required_months_exist():
    result = evaluate_month_coverage(REQUIRED_MONTHS)

    assert result.passed is True
    assert result.missing_months == []
