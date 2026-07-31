"""Idempotency evidence queries for the BCB pipeline."""

from __future__ import annotations

from desafio_bcb.config import PipelineConfig, table_name


TABLE_KEYS = {
    "bronze_selic_raw": ["series_name", "data", "source_file"],
    "bronze_ipca_raw": ["series_name", "data", "source_file"],
    "silver_selic": ["series_name", "reference_date"],
    "silver_ipca": ["series_name", "reference_date"],
    "gold_monthly_real_interest": ["reference_month"],
}


def run_idempotency_report(spark, config: PipelineConfig) -> None:
    """Print row counts and duplicate counts after repeated workflow runs."""

    for table, key_columns in TABLE_KEYS.items():
        full_name = table_name(config, table)
        key_expr = ", ".join(key_columns)
        duplicates = spark.sql(
            f"""
            SELECT COUNT(*) AS duplicate_key_count
            FROM (
                SELECT {key_expr}, COUNT(*) AS row_count
                FROM {full_name}
                GROUP BY {key_expr}
                HAVING COUNT(*) > 1
            )
            """
        ).collect()[0]["duplicate_key_count"]
        row_count = spark.table(full_name).count()

        print(
            f"{full_name}: rows={row_count}, "
            f"duplicate_key_count={duplicates}"
        )

        if duplicates > 0:
            raise ValueError(f"{full_name} has duplicate business keys")

