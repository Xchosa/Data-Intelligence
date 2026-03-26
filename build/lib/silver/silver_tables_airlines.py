
from pyspark import pipelines as dp
from pyspark.sql.functions import col

from pyspark.sql.functions import (
    col,
    explode_outer,
    upper,
    trim,
    current_timestamp,
    when,
    regexp_like,
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


from extract_data.config import config



@dp.table(
    name=config.silver_table_airlines,
    comment="Cleaned airline master data",
    table_properties={"quality": "silver"},
)
def airlines_silver():
    df = spark.readStream.table("data_catalog.bronze.bronze_table_airlines")
    
    df = df.select(
        explode_outer(col("AirlineResource.Airlines.Airline")).alias("airline"),
        col("_source_file"),
        col("_ingested_at"),
    )
    
    df = df.withColumn(
        "airline_id",
        upper(trim(col("airline.AirlineID")))
    ).withColumn(
        "airline_id_icao",
        upper(trim(col("airline.AirlineID_ICAO")))
    ).withColumn(
        "raw_name",
        col("airline.Names.Name")
    ).withColumn(
        "silver_processed_at",
        current_timestamp()
    )
    
    final_columns = [
        "airline_id",
        "airline_id_icao",
        "raw_name",
        "_source_file",
        "_ingested_at",
        "silver_processed_at",
    ]
    
    return df.select(*final_columns).dropDuplicates(["airline_id"])


@dp.table(
    name="quarantine_airlines",
    comment="Quarantine table for airlines records failing data quality checks",
    table_properties={"quality": "quarantine"},
)
def airlines_quarantine():
    """Captures records that don't pass data quality validations"""
    df = spark.readStream.table("data_catalog.bronze.bronze_table_airlines")
    
    df = df.select(
        explode_outer(col("AirlineResource.Airlines.Airline")).alias("airline"),
        col("_source_file"),
        col("_ingested_at"),
    )
    
    df = df.withColumn(
        "airline_id",
        upper(trim(col("airline.AirlineID")))
    ).withColumn(
        "airline_id_icao",
        upper(trim(col("airline.AirlineID_ICAO")))
    )
    
    # Filter for records that fail validation
    df = df.filter(
        (col("airline").isNull()) |
        (col("airline_id").isNull() | (trim(col("airline_id")) == "")) |
        (col("airline_id_icao").isNull())
    )
    
    df = df.withColumn(
        "quarantine_reason",
        when(col("airline").isNull(), "Missing airline object")
        .when(col("airline_id").isNull() | (trim(col("airline_id")) == ""), "Invalid airline_id")
        .when(col("airline_id_icao").isNull(), "Invalid airline_id_icao")
        .otherwise("Unknown validation error")
    ).withColumn(
        "quarantine_timestamp",
        current_timestamp()
    )
    
    final_columns = [
        "airline_id",
        "airline_id_icao",
        "quarantine_reason",
        "_source_file",
        "_ingested_at",
        "quarantine_timestamp",
    ]
    
    return df.select(*final_columns).dropDuplicates(["airline_id"])