"""Shared configuration helpers for the BCB Databricks pipeline."""

from __future__ import annotations

import re
from dataclasses import dataclass


DEFAULT_CATALOG = "desafio_bcb"
DEFAULT_SCHEMA = "default"
DEFAULT_RAW_VOLUME_PATH = "/Volumes/desafio_bcb/default/raw_files"

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class PipelineConfig:
    """Runtime configuration used by all pipeline layers."""

    catalog: str = DEFAULT_CATALOG
    schema: str = DEFAULT_SCHEMA
    raw_volume_path: str = DEFAULT_RAW_VOLUME_PATH

    def __post_init__(self) -> None:
        validate_identifier(self.catalog, "catalog")
        validate_identifier(self.schema, "schema")


def validate_identifier(value: str, field_name: str) -> None:
    """Validate Unity Catalog identifiers accepted by this project."""

    if not IDENTIFIER_PATTERN.match(value):
        raise ValueError(
            f"Invalid {field_name} '{value}'. Use only letters, numbers, "
            "and underscores, starting with a letter or underscore."
        )


def table_name(config: PipelineConfig, table: str) -> str:
    """Return a fully qualified Unity Catalog table name."""

    validate_identifier(table, "table")
    return f"`{config.catalog}`.`{config.schema}`.`{table}`"


def ensure_catalog_objects(spark, config: PipelineConfig) -> None:
    """Create catalog/schema objects when the current user has permission."""

    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{config.catalog}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{config.catalog}`.`{config.schema}`")

