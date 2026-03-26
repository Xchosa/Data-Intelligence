
from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    explode_outer,
    upper,
    trim,
    current_timestamp,
    regexp_like,
    lit,
    when
)
import sys
import os


if "__file__" in globals():
    current_dir = os.path.dirname(os.path.abspath(__file__))
else:
    current_dir = os.getcwd()

src_root = os.path.abspath(os.path.join(current_dir, '..'))
if src_root not in sys.path:
    sys.path.append(src_root)

extract_data_root = os.path.join(src_root, 'extract_data')
if extract_data_root not in sys.path:
    sys.path.append(extract_data_root)

from extract_data.config import config


@dp.table(
    name=config.silver_table_aircrafts,
    comment=config.silver_aircrafts_comment,
    table_properties={"quality": "silver"},
)
def aircraft_silver():
    df = spark.readStream.table("data_catalog.bronze.bronze_table_aircrafts")
    
    df = df.select(
        explode_outer(col("AircraftResource.AircraftSummaries.AircraftSummary")).alias("aircraft"),
        col("_source_file"),
        col("_ingested_at"),
    )
    
    df = df.withColumn(
        "AirlineEquipCode",
        trim(col("aircraft.AirlineEquipCode"))
    ).withColumn(
        "aircraft_code",
        upper(trim(col("aircraft.AircraftCode")))
    ).withColumn(
        "aircraft_name",
        trim(col("aircraft.Names.Name.`$`"))
    ).withColumn(
        "language_code",
        trim(col("aircraft.Names.Name.`@LanguageCode`"))
    ).withColumn(
        "silver_processed_at",
        current_timestamp()
    )

    final_columns = [
        "aircraft_code",
        "AirlineEquipCode",
        "aircraft_name",
        "language_code",
        "_source_file",
        "_ingested_at",
        "silver_processed_at",
    ]
    
    return df.select(*final_columns).dropDuplicates(["aircraft_code"])


@dp.table(
    name="quarantine_aircrafts",
    comment="Quarantine table for aircraft records missing AircraftResource",
    table_properties={"quality": "quarantine"},
)
def aircraft_quarantine():
    """Captures records that don't have valid AircraftResource data"""
    df = spark.readStream.table("data_catalog.bronze.bronze_table_aircrafts")

    df = df.select(
        explode_outer(col("AircraftResource.AircraftSummaries.AircraftSummary")).alias("aircraft"),
        col("_source_file"),
        col("_ingested_at"),
    )
    
    df = df.withColumn(
        "aircraft_code",
        upper(trim(col("aircraft.AircraftCode")))
    ).withColumn(
        "quarantine_reason",
        when(col("aircraft").isNull(), "Missing aircraft object")
        .when(col("aircraft.AircraftCode").isNull(), "Missing AircraftCode")
        .otherwise("Invalid aircraft data")
    ).withColumn(
        "quarantine_timestamp",
        current_timestamp()
    )
    
    df = df.filter(
        (col("aircraft").isNull()) | 
        (col("aircraft.AircraftCode").isNull()) |
        (trim(col("aircraft.AircraftCode")) == "")
    )
    
    final_columns = [
        "aircraft_code",
        "quarantine_reason",
        "_source_file",
        "_ingested_at",
        "quarantine_timestamp",
    ]
    
    return df.select(*final_columns).dropDuplicates(["_ingested_at"])
