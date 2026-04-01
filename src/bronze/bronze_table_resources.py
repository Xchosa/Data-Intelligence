from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp

import sys
import os

from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp

from config import config


def build_schema_location(reference: str) -> str:
    """Build schema location for a given reference type"""
    return (
        f"/Volumes/{config.catalog_name}/"
        f"{config.schema_name}/"
        f"{config.meta_valume}/"
        f"{reference}/"
        f"schema"
    )


def create_bronze_table(spec: dict):
    """Factory function to create bronze tables dynamically"""
    @dp.table(
        name=spec["config_name"],
        comment=spec["comment"],
        table_properties={"quality": "bronze"},
    )
    def _bronze_table():
        schema_location = build_schema_location(spec["reference"])
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("multiLine", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.schemaLocation", schema_location)
            .load(spec["path"])
            .withColumn("_source_file", col("_metadata.file_path"))
            .withColumn("_ingested_at", current_timestamp())
        )
    
    return _bronze_table


# Dynamically create all tables
for spec in config.table_specs:
    globals()[spec["name"]] = create_bronze_table(spec)