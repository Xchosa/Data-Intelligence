from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    explode_outer,
    upper,
    trim,
    current_timestamp,
    regexp_like,
    lit,
    when,
    expr,
    get_json_object
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
    name=config.silver_table_airports,
    comment="Cleaned airport master data",
    table_properties={"quality": "silver"},
)
def airports_silver():
    """Captures records that don't pass data quality validations"""
    df = spark.readStream.table(f"{config.catalog_name}.{config.schema_name}.{config.bronze_table_airports}")
    
    df = df.select(
        explode_outer(col("AirportResource.Airports.Airport")).alias("airport"),
        col("_source_file"),
        col("_ingested_at"),
    )
    
    df = df.withColumn(
        "airport_code",
        upper(trim(col("airport.AirportCode")))
    ).withColumn(
        "city_code",
        upper(trim(col("airport.CityCode")))
    ).withColumn(
        "country_code",
        upper(trim(col("airport.CountryCode")))
    ).withColumn(
        "location_type",
        trim(col("airport.LocationType"))
    # ).withColumn(
    #     "airport_name",
    #     trim(col("airport.Names.Name"))
    ).withColumn(
    "airport_name_EN",
    trim(
        expr(
            "filter(from_json(airport.Names.Name, 'array<struct<`@LanguageCode`:string,`$`:string>>'), x -> x.`@LanguageCode` = 'EN')[0].`$`"
        )
    )).withColumn(
        "latitude",
        col("airport.Position.Coordinate.Latitude")
    ).withColumn(
        "longitude",
        col("airport.Position.Coordinate.Longitude")
    ).withColumn(
        "time_zone_id",
        trim(col("airport.TimeZoneId"))
    ).withColumn(
        "utc_offset",
        trim(col("airport.UtcOffset"))
    ).withColumn(
        "silver_processed_at",
        current_timestamp()
    )
    
    final_columns = [
        "airport_code",
        "city_code",
        "country_code",
        "location_type",
        "airport_name_EN",
        "latitude",
        "longitude",
        "time_zone_id",
        "utc_offset",
        "_source_file",
        "_ingested_at",
        "silver_processed_at",
    ]
    
    return df.select(*final_columns).dropDuplicates(["airport_code"])


@dp.table(
    name="quarantine_airports",
    comment="Quarantine table for airports records failing data quality checks",
    table_properties={"quality": "quarantine"},
)
def airports_quarantine():
    """Captures records that don't pass data quality validations"""
    """Captures records that don't pass data quality validations"""
    df = spark.readStream.table(f"{config.catalog_name}.{config.schema_name}.{config.bronze_table_airports}")
    
    df = df.select(
        explode_outer(col("AirportResource.Airports.Airport")).alias("airport"),
        col("_source_file"),
        col("_ingested_at"),
    )
    
    df = df.withColumn(
        "airport_code",
        upper(trim(col("airport.AirportCode")))
    ).withColumn(
        "city_code",
        upper(trim(col("airport.CityCode")))
    ).withColumn(
        "country_code",
        upper(trim(col("airport.CountryCode")))
    # ).withColumn(
    #     "airport_name_EN",
    #     trim(col("airport.Names.Name`@LanguageCode`EN, `$`"))
    ).withColumn(
    "airport_name_EN",
    trim(
        expr(
            "filter(from_json(airport.Names.Name, 'array<struct<`@LanguageCode`:string,`$`:string>>'), x -> x.`@LanguageCode` = 'EN')[0].`$`"
        )
    )).withColumn(
        "time_zone_id",
        trim(col("airport.TimeZoneId"))
    ).withColumn(
        "utc_offset",
        trim(col("airport.UtcOffset"))
    )
    
    # Filter for records that fail validation
    df = df.filter(
        (col("airport").isNull()) |
        (col("airport_code").isNull() | (trim(col("airport_code")) == "")) |
        (col("city_code").isNull()) |
        (col("country_code").isNull()) |
        (col("airport_name_EN").isNull()) |
        (col("time_zone_id").isNull()) |
        (~(col("utc_offset").rlike( "^[+-][0-9]{2}:[0-9]{2}$")))
    )
    
    df = df.withColumn(
        "quarantine_reason",
        when(col("airport").isNull(), "Missing airport object")
        .when(col("airport_code").isNull() | (trim(col("airport_code")) == ""), "Invalid airport_code")
        .when(col("city_code").isNull(), "Invalid city_code")
        .when(col("country_code").isNull(), "Invalid country_code")
        .when(col("airport_name_EN").isNull(), "Invalid airport_name")
        .when(col("time_zone_id").isNull(), "Invalid time_zone_id")
        .when(~col("utc_offset").rlike( "^[+-][0-9]{2}:[0-9]{2}$"), "Invalid utc_offset format")
        .otherwise("Unknown validation error")
    ).withColumn(
        "quarantine_timestamp",
        current_timestamp()
    )
    
    final_columns = [
        "airport_code",
        "city_code",
        "country_code",
        "airport_name_EN",
        "time_zone_id",
        "utc_offset",
        "quarantine_reason",
        "_source_file",
        "_ingested_at",
        "quarantine_timestamp",
    ]
    
    return df.select(*final_columns).dropDuplicates(["airport_code"])