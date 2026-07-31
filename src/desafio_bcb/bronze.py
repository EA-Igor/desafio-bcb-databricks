"""Bronze layer: idempotent ingestion of raw JSON files from a UC Volume."""

from __future__ import annotations

from pyspark.sql import functions as f

from desafio_bcb.config import PipelineConfig, ensure_catalog_objects, table_name
from desafio_bcb.quality import assert_no_nulls, assert_non_empty, assert_unique_key


SERIES_FILES = {
    "selic": "selic.json",
    "ipca": "ipca.json",
}


def run_bronze(spark, config: PipelineConfig) -> None:
    """Load raw BCB JSON files into Bronze Delta tables."""

    ensure_catalog_objects(spark, config)

    for series_name, file_name in SERIES_FILES.items():
        target_table = table_name(config, f"bronze_{series_name}_raw")
        source_path = f"{config.raw_volume_path.rstrip('/')}/{file_name}"

        bronze_df = _read_raw_file(spark, source_path, series_name)
        assert_non_empty(bronze_df, f"bronze source {series_name}")
        assert_no_nulls(
            bronze_df,
            ["series_name", "data", "valor", "source_file"],
            f"bronze source {series_name}",
        )
        assert_unique_key(
            bronze_df,
            ["series_name", "data", "source_file"],
            f"bronze source {series_name}",
        )

        _create_bronze_table(spark, target_table)
        _merge_bronze(spark, bronze_df, target_table, series_name)

        persisted_df = spark.table(target_table)
        assert_unique_key(
            persisted_df,
            ["series_name", "data", "source_file"],
            f"persisted bronze {series_name}",
        )


def _read_raw_file(spark, source_path: str, series_name: str):
    return (
        spark.read.option("multiLine", "true")
        .json(source_path)
        .select(
            f.lit(series_name).alias("series_name"),
            f.col("data").cast("string").alias("data"),
            f.col("valor").cast("string").alias("valor"),
            f.lit(source_path).alias("source_file"),
            f.current_timestamp().alias("ingested_at"),
        )
    )


def _create_bronze_table(spark, target_table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            series_name STRING,
            data STRING,
            valor STRING,
            source_file STRING,
            ingested_at TIMESTAMP
        )
        USING DELTA
        """
    )


def _merge_bronze(spark, bronze_df, target_table: str, series_name: str) -> None:
    source_view = f"source_bronze_{series_name}"
    bronze_df.createOrReplaceTempView(source_view)

    spark.sql(
        f"""
        MERGE INTO {target_table} AS target
        USING {source_view} AS source
            ON target.series_name = source.series_name
           AND target.data = source.data
           AND target.source_file = source.source_file
        WHEN NOT MATCHED THEN INSERT (
            series_name,
            data,
            valor,
            source_file,
            ingested_at
        )
        VALUES (
            source.series_name,
            source.data,
            source.valor,
            source.source_file,
            source.ingested_at
        )
        """
    )

