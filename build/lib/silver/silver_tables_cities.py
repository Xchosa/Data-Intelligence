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
    name=config.silver_table_cities,
    comment="Cleaned city master data",
    table_properties={"quality": "silver"},
)
def cities_silver():
    df = spark.readStream.table("data_catalog.bronze.bronze_table_cities")
    
    df = df.select(
        explode_outer(col("CityResource.Cities.City")).alias("city"),
        col("_source_file"),
        col("_ingested_at"),
    )
    
    df = df.withColumn(
        "city_code",
        upper(trim(col("city.CityCode")))
    ).withColumn(
        "country_code",
        upper(trim(col("city.CountryCode")))
    ).withColumn(
        "city_name",
        trim(col("city.Names.Name"))
    ).withColumn(
        "time_zone_id",
        trim(col("city.TimeZoneId"))
    ).withColumn(
        "utc_offset",
        trim(col("city.UtcOffset"))
    ).withColumn(
        "airports",
        col("city.Airports")
    ).withColumn(
        "silver_processed_at",
        current_timestamp()
    )
    
    final_columns = [
        "city_code",
        "country_code",
        "city_name",
        "time_zone_id",
        "utc_offset",
        "airports",
        "_source_file",
        "_ingested_at",
        "silver_processed_at",
    ]
    
    return df.select(*final_columns).dropDuplicates(["city_code"])


@dp.table(
    name="quarantine_cities",
    comment="Quarantine table for cities records failing data quality checks",
    table_properties={"quality": "quarantine"},
)
def cities_quarantine():
    """Captures records that don't pass data quality validations"""
    df = spark.readStream.table("data_catalog.bronze.bronze_table_cities")
    
    df = df.select(
        explode_outer(col("CityResource.Cities.City")).alias("city"),
        col("_source_file"),
        col("_ingested_at"),
    )
    
    df = df.withColumn(
        "city_code",
        upper(trim(col("city.CityCode")))
    ).withColumn(
        "country_code",
        upper(trim(col("city.CountryCode")))
    ).withColumn(
        "time_zone_id",
        trim(col("city.TimeZoneId"))
    ).withColumn(
        "utc_offset",
        trim(col("city.UtcOffset"))
    )
    
    # Filter for records that fail validation
    df = df.filter(
        (col("city").isNull()) |
        (col("city_code").isNull() | (trim(col("city_code")) == "")) |
        (col("country_code").isNull()) |
        (col("time_zone_id").isNull()) |
        (~col("Utc_Offset").rlike( "^[+-][0-9]{2}:[0-9]{2}$"))
    )
    
    df = df.withColumn(
        "quarantine_reason",
        when(col("city").isNull(), "Missing city object")
        .when(col("city_code").isNull() | (trim(col("city_code")) == ""), "Invalid city_code")
        .when(col("country_code").isNull(), "Invalid country_code")
        .when(col("time_zone_id").isNull(), "Invalid time_zone_id")
        .when(~col("utc_offset").rlike( "^[+-][0-9]{2}:[0-9]{2}$"), "Invalid utc_offset format")
        .otherwise("Unknown validation error")
    ).withColumn(
        "quarantine_timestamp",
        current_timestamp()
    )
    
    final_columns = [
        "city_code",
        "country_code",
        "time_zone_id",
        "utc_offset",
        "quarantine_reason",
        "_source_file",
        "_ingested_at",
        "quarantine_timestamp",
    ]
    
    return df.select(*final_columns).dropDuplicates(["city_code"])