"""Gold layer: monthly consolidated SELIC/IPCA analytical table."""

from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as f

from desafio_bcb.config import PipelineConfig, table_name
from desafio_bcb.quality import (
    assert_expected_month_count,
    assert_no_nulls,
    assert_non_empty,
    assert_unique_key,
)


def run_gold(spark, config: PipelineConfig) -> None:
    """Build the monthly Gold table with real interest metrics."""

    gold_df = _build_gold_dataframe(spark, config)
    assert_non_empty(gold_df, "gold consolidated")
    assert_expected_month_count(gold_df, 60, "gold consolidated")
    assert_no_nulls(
        gold_df,
        [
            "reference_month",
            "selic_avg_month_pct",
            "ipca_month_pct",
            "real_interest_month_pct",
        ],
        "gold consolidated",
    )
    assert_unique_key(gold_df, ["reference_month"], "gold consolidated")

    target_table = table_name(config, "gold_monthly_real_interest")
    _create_gold_table(spark, target_table)
    _merge_gold(spark, gold_df, target_table)

    persisted_df = spark.table(target_table)
    assert_unique_key(
        persisted_df,
        ["reference_month"],
        "persisted gold consolidated",
    )


def _build_gold_dataframe(spark, config: PipelineConfig):
    selic_table = table_name(config, "silver_selic")
    ipca_table = table_name(config, "silver_ipca")

    selic_monthly = (
        spark.table(selic_table)
        .withColumn("reference_month", f.trunc("reference_date", "month"))
        .groupBy("reference_month")
        .agg(f.avg("value_pct").cast("decimal(18,8)").alias("selic_avg_month_pct"))
    )

    ipca_monthly = (
        spark.table(ipca_table)
        .select(
            f.trunc("reference_date", "month").alias("reference_month"),
            f.col("value_pct").cast("decimal(18,8)").alias("ipca_month_pct"),
        )
    )

    monthly = (
        selic_monthly.join(ipca_monthly, "reference_month", "inner")
        .withColumn(
            "real_interest_month_pct",
            (
                (
                    (f.lit(1) + f.col("selic_avg_month_pct") / f.lit(100))
                    / (f.lit(1) + f.col("ipca_month_pct") / f.lit(100))
                )
                - f.lit(1)
            )
            * f.lit(100),
        )
    )

    window_12m = Window.orderBy("reference_month").rowsBetween(-11, 0)

    return (
        monthly.withColumn(
            "real_interest_accumulated_12m_pct",
            (
                f.exp(
                    f.sum(
                        f.log(f.lit(1) + f.col("real_interest_month_pct") / f.lit(100))
                    ).over(window_12m)
                )
                - f.lit(1)
            )
            * f.lit(100),
        )
        .withColumn("updated_at", f.current_timestamp())
        .select(
            f.col("reference_month").cast("date"),
            f.col("selic_avg_month_pct").cast("decimal(18,8)"),
            f.col("ipca_month_pct").cast("decimal(18,8)"),
            f.col("real_interest_month_pct").cast("decimal(18,8)"),
            f.col("real_interest_accumulated_12m_pct").cast("decimal(18,8)"),
            "updated_at",
        )
    )


def _create_gold_table(spark, target_table: str) -> None:
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            reference_month DATE,
            selic_avg_month_pct DECIMAL(18,8),
            ipca_month_pct DECIMAL(18,8),
            real_interest_month_pct DECIMAL(18,8),
            real_interest_accumulated_12m_pct DECIMAL(18,8),
            updated_at TIMESTAMP
        )
        USING DELTA
        """
    )


def _merge_gold(spark, gold_df, target_table: str) -> None:
    source_view = "source_gold_monthly_real_interest"
    gold_df.createOrReplaceTempView(source_view)

    spark.sql(
        f"""
        MERGE INTO {target_table} AS target
        USING {source_view} AS source
            ON target.reference_month = source.reference_month
        WHEN MATCHED THEN UPDATE SET
            target.selic_avg_month_pct = source.selic_avg_month_pct,
            target.ipca_month_pct = source.ipca_month_pct,
            target.real_interest_month_pct = source.real_interest_month_pct,
            target.real_interest_accumulated_12m_pct =
                source.real_interest_accumulated_12m_pct,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (
            reference_month,
            selic_avg_month_pct,
            ipca_month_pct,
            real_interest_month_pct,
            real_interest_accumulated_12m_pct,
            updated_at
        )
        VALUES (
            source.reference_month,
            source.selic_avg_month_pct,
            source.ipca_month_pct,
            source.real_interest_month_pct,
            source.real_interest_accumulated_12m_pct,
            source.updated_at
        )
        """
    )

