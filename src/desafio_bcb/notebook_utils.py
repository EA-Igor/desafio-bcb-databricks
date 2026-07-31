"""Small helpers for Databricks notebook entrypoints."""

from __future__ import annotations

from desafio_bcb.config import (
    DEFAULT_CATALOG,
    DEFAULT_RAW_VOLUME_PATH,
    DEFAULT_SCHEMA,
    PipelineConfig,
)


def config_from_widgets(dbutils) -> PipelineConfig:
    """Read Databricks widgets into a PipelineConfig."""

    dbutils.widgets.text("catalog", DEFAULT_CATALOG)
    dbutils.widgets.text("schema", DEFAULT_SCHEMA)
    dbutils.widgets.text("raw_volume_path", DEFAULT_RAW_VOLUME_PATH)

    return PipelineConfig(
        catalog=dbutils.widgets.get("catalog"),
        schema=dbutils.widgets.get("schema"),
        raw_volume_path=dbutils.widgets.get("raw_volume_path"),
    )

