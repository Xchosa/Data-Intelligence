from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, input_file_name

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


def build_schema_location(reference: str) -> str:
    """Build schema location for a given reference type"""
    return (
        f"/Volumes/{config.catalog_name}/"
        f"{config.schema_name}/"
        f"{config.meta_valume}/"
        f"{reference}/"
        f"schema"
    )


@dp.table(
    name="bronze_table_departure_fra",
    comment="Raw departures JSON from Lufthansa landing volume",
    table_properties={"quality": "bronze"}, 
)
def departure_bronze_a():
    schema_location = build_schema_location(config.airport_code_a)
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", schema_location)
       
        .load(config.path_depature_airport_a)
       
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

    # add country-specific transformations here
    # df = df.withColumn(...)

    return df

@dp.table(
    name="bronze_table_departure_muc",
    comment="Raw departures JSON from Lufthansa landing volume",
    table_properties={"quality": "bronze"}, 
)
def departure_bronze_b():
    schema_location = build_schema_location(config.airport_code_b)
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
       # .option("cloudFiles.schemaHints", "time int") collomn time as int
        .option("cloudFiles.schemaLocation", schema_location)
        .load(config.path_depature_airport_b)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

    # add country-specific transformations here
    # df = df.withColumn(...)

    return df








# TABLE_SPECS = [
#     {
#         "airport_code": airport_code,
#         "name": f"departure_{airport_code}_bronze",
#         "path": path,
#     }
#     for airport_code, path in zip(config.airport_codes, config.departure_paths)
# ]


# def register_departure_table(table_name: str, input_path: str, airport_code: str):
#     @dp.table(
#         name=table_name,
#         comment=f"Raw departures JSON for {airport_code} from Lufthansa landing volume",
#         table_properties={"quality": "bronze"},
#     )
#     def _table(input_path=input_path, airport_code=airport_code):
#         return (
#             spark.readStream
#             .format("cloudFiles")
#             .option("cloudFiles.format", "json")
#             .load(input_path)
#             .withColumn("_source_file", col("_metadata.file_path"))
#             .withColumn("_ingested_at", current_timestamp())
#             .withColumn("_airport_code", lit(airport_code))
#         )

#     return _table


# for spec in TABLE_SPECS:
#     globals()[f"departure_{spec['airport_code']}_bronze_fn"] = register_departure_table(
#         table_name=spec["name"],
#         input_path=spec["path"],
#         airport_code=spec["airport_code"],
#     )




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

    
