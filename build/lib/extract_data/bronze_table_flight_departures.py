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
    name=config.bronze_table_departures,
    comment="Raw departures JSON from all airports in Lufthansa landing volume",
    table_properties={"quality": "bronze"}, 
)
def departures_bronze():
    # A single schema location for the unified table
    schema_location = build_schema_location("departures")
    
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        #.option("cloudfiles.allowEmptyDirectory", "true")
        .option("cloudFiles.schemaLocation", schema_location)
        # Load from the path with a wildcard for the airport code
        .load(config.path_departures)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )

#@dp.table(
#    name="bronze_table_departure_fra",
#    comment="Raw departures JSON from Lufthansa landing volume",
#    table_properties={"quality": "bronze"}, 
#)
#def departure_bronze_a():
#    schema_location = build_schema_location(config.airport_code_a)
#    df = (
#        spark.readStream
#        .format("cloudFiles")
#        .option("cloudFiles.format", "json")
#        .option("multiLine", "true")
#        .option("cloudFiles.inferColumnTypes", "true")
#        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#        .option("cloudFiles.schemaLocation", schema_location)
       
#        .load(config.path_depature_airport_a)
       
#        .withColumn("_source_file", col("_metadata.file_path"))
#        .withColumn("_ingested_at", current_timestamp())
#    )

#    # add country-specific transformations here
#    # df = df.withColumn(...)

#    return df

#@dp.table(
#    name="bronze_table_departure_muc",
#    comment="Raw departures JSON from Lufthansa landing volume",
#    table_properties={"quality": "bronze"}, 
#)
#def departure_bronze_b():
#    schema_location = build_schema_location(config.airport_code_b)
#    df = (
#        spark.readStream
#        .format("cloudFiles")
#        .option("cloudFiles.format", "json")
#        .option("multiLine", "true")
#        .option("cloudFiles.inferColumnTypes", "true")
#        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#        .option("cloudFiles.schemaLocation", schema_location)
#        .load(config.path_depature_airport_b)
#        .withColumn("_source_file", col("_metadata.file_path"))
#        .withColumn("_ingested_at", current_timestamp())
#    )

#    # add country-specific transformations here
#    # df = df.withColumn(...)

#    return df






