from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp

import sys
import os


# if "__file__" in globals():
#     current_dir = os.path.dirname(os.path.abspath(__file__))
# else:
#     current_dir = os.getcwd()

# # Go up one level to reach 'src'
# # This ensures that 'import utils.helpers' will work correctly
# src_root = os.path.abspath(os.path.join(current_dir, '..'))

# if src_root not in sys.path:
#     sys.path.append(src_root)

from config import config

# spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_cities};"),
# spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_countries};"),
# spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_airports};"),
# spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_airlines};"),
# spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_aircrafts};"),

@dp.table(
    # spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_cities};"),
    name=config.bronze_table_cities,
    comment="Raw countries JSON from Lufthansa landing volume",
    table_properties={"quality": "bronze"},
)
def countries_bronze():
    
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(config.path_cities)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

    # add country-specific transformations here
    # df = df.withColumn(...)

    return df




@dp.table(
    # spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_countries};"),
    name=config.bronze_table_countries,
    comment="Raw cities JSON from Lufthansa landing volume",
    table_properties={"quality": "bronze"},
)
def cities_bronze():
    
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(config.path_countries)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

    # add city-specific transformations here
    # df = df.withColumn(...)

    return df


@dp.table(
    # spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_airports};"),
    name=config.bronze_table_airports,
    comment="Raw airports JSON from Lufthansa landing volume",
    table_properties={"quality": "bronze"},
)
def airports_bronze():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(config.path_airports)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

    # add airport-specific transformations here
    # df = df.withColumn(...)

    return df


@dp.table(
    # spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_airlines};"),
    name=config.bronze_table_airlines,
    comment="Raw airlines JSON from Lufthansa landing volume",
    table_properties={"quality": "bronze"},
)
def airlines_bronze():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(config.path_airlines)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

    # add airline-specific transformations here
    # df = df.withColumn(...)

    return df


@dp.table(
    # spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_aircrafts};"),
    name=config.bronze_table_aircrafts,
    comment="Raw aircraft JSON from Lufthansa landing volume",
    table_properties={"quality": "bronze"},
)
def aircraft_bronze():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(config.path_aircraft)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

    # add aircraft-specific transformations here
    # df = df.withColumn(...)

    return df

# spark.sql(f"""
# CREATE TABLE current_employees_ctas
# AS
# SELECT ID, FirstName, Country, Role 
# FROM read_files(
#   '/Volumes/{catalog_name}/{schema_name}/{volume_name}/',
#   format => 'json',
#   header => true,
#   inferSchema => true
#  );"")

# #Display available tables in your schema
# spark.sql(f"SHOW TABLES;").display()






# #together a part of a pipile 
# @dp.table(
#         name="countries_bronze",
#         comment="Raw countries JSON from Lufthansa landing volume",
#         table_properties={"quality": "bronze"}
# )

# def cities_bronze():
#     return (
#         spark.readStream
#             .format("cloudFiles")
#             .option("cloudFiles.format", "json")
#             .load({path_cities})
#             .withColumn("_source_file", col("_metadata.file_path"))
#             .withColumn("_ingested_at", current_timestamp())
#             # .toTable("cities_bronze")
#     )


