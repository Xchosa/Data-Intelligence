from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, input_file_name

import sys
import os
from src.Extract_Data.config import config
# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.
          


path_cities = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/cities"
path_countries = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/countries"
path_airports = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/airports"
path_airlines = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/airlines"
path_aircraft = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/aircraft"

TABLE_SPECS = [
    {"name": "countries_bronze", "path": path_countries, "source": "countries"},
    {"name": "cities_bronze", "path": path_cities, "source": "cities"},
    {"name": "airports_bronze", "path": path_airports, "source": "airports"},
    {"name": "airlines_bronze", "path": path_airlines, "source": "airlines"},
    {"name": "aircraft_bronze", "path": path_aircraft, "source": "aircraft"},
]

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



#
def register_bronze_table(table_name: str, input_path: str, source_name: str):
    """ creating new decorated functions for each table:
         countries cities, airports, airlines, aircrafts 
         """
    @dp.table(
        name=table_name,
        comment=f"Raw {source_name} JSON from Lufthansa landing volume",
        table_properties={"quality": "bronze"},
    )
    def _table(input_path=input_path):
        return (
            spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(input_path)
            .withColumn("_source_file", col("_metadata.file_path"))
            .withColumn("_ingested_at", current_timestamp())
        )

    return _table

for spec in TABLE_SPECS:
    globals()[f"load_{spec['name']}"] = register_bronze_table(
        table_name=spec["name"],
        input_path=spec["path"],
        source_name=spec["source"],
    )






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

    
