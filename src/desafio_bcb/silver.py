"""Silver layer: typed, standardized, idempotent Delta tables."""

from __future__ import annotations

from pyspark.sql import functions as f

from desafio_bcb.config import PipelineConfig, table_name
from desafio_bcb.quality import assert_no_nulls, assert_non_empty, assert_unique_key


SERIES = ("selic", "ipca")


def run_silver(spark, config: PipelineConfig) -> None:
    """Transform Bronze tables into typed Silver tables with MERGE semantics."""

    for series_name in SERIES:
        source_table = table_name(config, f"bronze_{series_name}_raw")
        target_table = table_name(config, f"silver_{series_name}")

        silver_df = _transform_series(spark, source_table)
        assert_non_empty(silver_df, f"silver source {series_name}")
        assert_no_nulls(
            silver_df,
            ["series_name", "reference_date", "value_pct"],
            f"silver source {series_name}",
        )
        assert_unique_key(
            silver_df,
            ["series_name", "reference_date"],
            f"silver source {series_name}",
        )

        _create_silver_table(spark, target_table)
        _merge_silver(spark, silver_df, target_table, series_name)

        persisted_df = spark.table(target_table)
        assert_unique_key(
            persisted_df,
            ["series_name", "reference_date"],
            f"persisted silver {series_name}",
        )


def _transform_series(spark, source_table: str):
    return (
        spark.table(source_table)
        .select(
            f.col("series_name"),
            f.to_date("data", "dd/MM/yyyy").alias("reference_date"),
            f.regexp_replace("valor", ",", ".")
            .cast("decimal(18,8)")
            .alias("value_pct"),
            f.col("source_file"),
            f.current_timestamp().alias("updated_at"),
        )
    )


def _create_silver_table(spark, target_table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            series_name STRING,
            reference_date DATE,
            value_pct DECIMAL(18,8),
            source_file STRING,
            updated_at TIMESTAMP
        )
        USING DELTA
        """
    )


def _merge_silver(spark, silver_df, target_table: str, series_name: str) -> None:
    source_view = f"source_silver_{series_name}"
    silver_df.createOrReplaceTempView(source_view)

    spark.sql(
        f"""
        MERGE INTO {target_table} AS target
        USING {source_view} AS source
            ON target.series_name = source.series_name
           AND target.reference_date = source.reference_date
        WHEN MATCHED THEN UPDATE SET
            target.value_pct = source.value_pct,
            target.source_file = source.source_file,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            series_name,
            reference_date,
            value_pct,
            source_file,
            updated_at
        )
        VALUES (
            source.series_name,
            source.reference_date,
            source.value_pct,
            source.source_file,
            source.updated_at
        )
        """
    )

