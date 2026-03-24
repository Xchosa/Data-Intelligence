
from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp



import sys
import os
# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

# Get the absolute path of the directory containing THIS script (src/ingestion)
if "__file__" in globals():
    current_dir = os.path.dirname(os.path.abspath(__file__))
else:
    current_dir = os.getcwd()

# Go up one level to reach 'src'
# This ensures that 'import utils.helpers' will work correctly
src_root = os.path.abspath(os.path.join(current_dir, '..'))

if src_root not in sys.path:
    sys.path.append(src_root)

# Now you can import your utils


def build_schema_location(reference: str) -> str:
    """Build schema location for a given reference type"""
    return (
        f"/Volumes/{config.catalog_name}/"
        f"{config.schema_name}/"
        f"{config.meta_valume}/"
        f"{reference}/"
        f"schema"
    )



from extract_data.config import config




@dp.table(
    name=config.bronze_table_dep_a,
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

    