"""Reusable data quality checks for Databricks jobs."""

from __future__ import annotations

from collections.abc import Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as f


class DataQualityError(Exception):
    """Raised when a data quality rule is violated."""


def assert_non_empty(df: DataFrame, dataset_name: str) -> None:
    """Fail explicitly when a dataset has no rows."""

    if df.limit(1).count() == 0:
        raise DataQualityError(f"{dataset_name} is empty")


def assert_no_nulls(
    df: DataFrame,
    columns: Sequence[str],
    dataset_name: str,
) -> None:
    """Fail when any required column contains null values."""

    null_condition = None
    for column in columns:
        condition = f.col(column).isNull()
        null_condition = condition if null_condition is None else null_condition | condition

    if null_condition is not None and df.where(null_condition).limit(1).count() > 0:
        raise DataQualityError(
            f"{dataset_name} has null values in required columns: {list(columns)}"
        )


def assert_unique_key(
    df: DataFrame,
    key_columns: Sequence[str],
    dataset_name: str,
) -> None:
    """Fail when a dataset has duplicate business keys."""

    duplicates = (
        df.groupBy(*key_columns)
        .count()
        .where(f.col("count") > 1)
        .limit(1)
        .count()
    )

    if duplicates > 0:
        raise DataQualityError(
            f"{dataset_name} has duplicate keys for columns: {list(key_columns)}"
        )


def assert_expected_month_count(
    df: DataFrame,
    expected_count: int,
    dataset_name: str,
) -> None:
    """Fail when a monthly dataset does not have the expected number of rows."""

    actual_count = df.count()
    if actual_count != expected_count:
        raise DataQualityError(
            f"{dataset_name} expected {expected_count} rows, got {actual_count}"
        )

