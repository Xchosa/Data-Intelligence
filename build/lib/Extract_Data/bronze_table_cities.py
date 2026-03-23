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


def build_schema_location(directory:str ) -> str:
    return (
        f"/Volumes/{config.catalog_name}/"
        f"{config.schema_name}/"
        f"{config.meta_valume}/"
        f"{directory}/"
        f"schema"
    )


@dp.table(
    # spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_countries};"),
    name=config.bronze_table_cities,
    comment="Raw cities JSON from Lufthansa landing volume",
    table_properties={"quality": "bronze"},
)
def build_stream():
    schema_location = build_schema_location(config.mds_reference[0])
    
    # df = (
    #     spark.readStream
    #     .format("cloudFiles")
    #     .option("cloudFiles.format", "binaryFile")
    #     .option("cloudFiles.schemaLocation", schema_location)
    #     .load(source_path)
    # )
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")

        .option("multiLine", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")


        .option("cloudFiles.schemaLocation", schema_location)
    #    .option("cloudFiles.schemaHints", "time int") collomn time as int
        .load(config.path_cities)
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_ingested_at", current_timestamp())
    )
    return df 


# @dp.table(
#     # spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_countries};"),
#     name=config.bronze_table_countries,
#     comment="Raw cities JSON from Lufthansa landing volume",
#     table_properties={"quality": "bronze"},
# )
# def cities_bronze():
    
#     df = (
#         spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "json")

#         .option("multiLine", "true")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#         .load(config.path_countries)
#         .withColumn("_source_file", col("_metadata.file_path"))
#         .withColumn("_ingested_at", current_timestamp())
#     )

#     # add city-specific transformations here
#     # df = df.withColumn(...)

#     return df