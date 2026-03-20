
from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, input_file_name

from src.Extract_Data.config import config
# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

# @dp.table
# def createTable():
path_cities = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/cities"
#together a part of a pipile 
@dp.table(
        name="cities_bronze",
        comment="Raw countries JSON from Lufthansa landing volume",
        table_properties={"quality": "bronze"}
)
def cities_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load({path_cities})
            .withColumn("_source_file", col("_metadata.file_path"))
            .withColumn("_ingested_at", current_timestamp())
            # .toTable("cities_bronze")
    )




# def cities_bronze():
#     spark.readStream( 
#         .format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .load({path_cities})
#         .withColumn("_source_file", col("_metadata.file_path"))
#         .withColumn("_ingested_at", current_timestamp())
#             # .toTable("cities_bronze")
#     )
    

