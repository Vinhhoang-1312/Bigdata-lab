"""Data-quality gates for the experimental ML workflow."""

from __future__ import annotations

from dataclasses import dataclass


REQUIRED_MONTHS = [
    "2022-08",
    "2022-09",
    "2022-10",
    "2022-11",
    "2022-12",
    "2023-01",
    "2023-02",
]


@dataclass(frozen=True)
class CoverageResult:
    observed_months: list[str]
    required_months: list[str]
    missing_months: list[str]

    @property
    def passed(self) -> bool:
        return not self.missing_months


def evaluate_month_coverage(observed_months: list[str]) -> CoverageResult:
    observed = sorted(set(observed_months))
    missing = [month for month in REQUIRED_MONTHS if month not in observed]
    return CoverageResult(
        observed_months=observed,
        required_months=REQUIRED_MONTHS,
        missing_months=missing,
    )
