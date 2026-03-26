from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp

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


from extract_data.config import config



#handle dubliplate dropDuplicates 
#preserves row even with null (explode_outer)
# @dp.table(
#     name=config.silver_table_countries,
#     comment=config.silver_countries_comment,
#     table_properties={"quality": "silver"},
# )
# def countries_silver():
#     return (
#         spark.readStream.table("data_catalog.bronze.bronze_table_countries")
#         .select(
#             explode_outer(col("CountryResource.Countries.Country")).alias("country"),
#             col("_source_file"),
#             col("_ingested_at"),
#         )
#         .select(
#             upper(trim(col("country.CountryCode"))).alias("country_code"),
#             col("country.Names.Name").alias("country_name"),
#             col("_source_file"),
#             col("_ingested_at"),
#             current_timestamp().alias("silver_processed_at"),
#         )
#         .dropDuplicates(["country_code"])
#     )


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
    
    df = df.filter(
        (col("aircraft").isNull()) | 
        (col("aircraft.AircraftCode").isNull()) |
        (trim(col("aircraft.AircraftCode")) == "")
    )
    
    df = df.withColumn(
        "quarantine_reason",
        when(col("aircraft").isNull(), "Missing aircraft object")
        .when(col("aircraft.AircraftCode").isNull(), "Missing AircraftCode")
        .otherwise("Invalid aircraft data")
    ).withColumn(
        "quarantine_timestamp",
        current_timestamp()
    )
    
    return df.dropDuplicates(["_ingested_at"])

    # add country-specific transformations here
    # df = df.withColumn(...)




@dp.table(
    name=config.silver_table_cities,
    comment="Cleaned city master data",
    table_properties={"quality": "silver"},
)
def cities_silver():
    df = spark.readStream.table("data_catalog.bronze.bronze_table_cities")
    
    df = df.filter(~col("_source_file").contains("logs"))
    
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


# @dp.table(
#     name="quarantine_cities",
#     comment="Quarantine table for cities records failing data quality checks",
#     table_properties={"quality": "quarantine"},
# )
# def cities_quarantine():
#     """Captures records that don't pass data quality validations"""
#     df = spark.readStream.table("data_catalog.bronze.bronze_table_cities")
    
#     df = df.select(
#         explode_outer(col("CityResource.Cities.City")).alias("city"),
#         col("_source_file"),
#         col("_ingested_at"),
#     )
    
#     df = df.withColumn(
#         "city_code",
#         upper(trim(col("city.CityCode")))
#     ).withColumn(
#         "country_code",
#         upper(trim(col("city.CountryCode")))
#     ).withColumn(
#         "time_zone_id",
#         trim(col("city.TimeZoneId"))
#     ).withColumn(
#         "utc_offset",
#         trim(col("city.UtcOffset"))
#     )
    
#     # Filter for records that fail validation
#     df = df.filter(
#         (col("city_code").isNull() | (trim(col("city_code")) == "")) |  # valid_city_code
#         (col("country_code").isNull()) |  # valid_country_code
#         (col("time_zone_id").isNull()) |  # valid_time_zone_id
#         (~regexp_like(col("utc_offset"), "^[+-][0-9]{2}:[0-9]{2}$"))  # valid_utc_offset_format
#     )
    
#     df = df.withColumn(
#         "quarantine_reason",
#         when(col("city_code").isNull() | (trim(col("city_code")) == ""), "Invalid city_code")
#         .when(col("country_code").isNull(), "Invalid country_code")
#         .when(col("time_zone_id").isNull(), "Invalid time_zone_id")
#         .when(~regexp_like(col("utc_offset"), "^[+-][0-9]{2}:[0-9]{2}$"), "Invalid utc_offset format")
#         .otherwise("Unknown validation error")
#     ).withColumn(
#         "quarantine_timestamp",
#         current_timestamp()
#     )
    
#     final_columns = [
#         "city_code",
#         "country_code",
#         "time_zone_id",
#         "utc_offset",
#         "quarantine_reason",
#         "_source_file",
#         "_ingested_at",
#         "quarantine_timestamp",
#     ]
    
#     return df.select(*final_columns).dropDuplicates(["city_code"])









# @dp.table(
#     name=config.silver_table_airlines,
#     comment="Cleaned airline master data",
#     table_properties={"quality": "silver"},
# )
# @dp.expect_or_drop("valid_airline_id", "airline_id IS NOT NULL AND trim(airline_id) <> ''")
# @dp.expect("valid_airline_id_icao", "airline_id_icao IS NOT NULL")
# def airlines_silver():
#     return (
#         spark.readStream.table("data_catalog.bronze.bronze_table_airlines")
#         .select(
#             explode_outer(col("AirlineResource.Airlines.Airline")).alias("airline"),
#             col("_source_file"),
#             col("_ingested_at"),
#         )
#         .select(
#             upper(trim(col("airline.AirlineID"))).alias("airline_id"),
#             upper(trim(col("airline.AirlineID_ICAO"))).alias("airline_id_icao"),
#             col("airline.Names.Name").alias("raw_name"),
#             col("_source_file"),
#             col("_ingested_at"),
#             current_timestamp().alias("silver_processed_at"),
#         )
#         .dropDuplicates(["airline_id"])
#     )

#not tested
