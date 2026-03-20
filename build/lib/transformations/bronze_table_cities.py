
# @dp.table
# def createTable():
#     # Read from the "sample_trips" table, then sum all the fares
#     return (
#         spark.read.table(f"sample_trips_data_lufthansa")
#         .groupBy(col("pickup_zip"))
#         .agg(sum("fare_amount").alias("total_fare"))
#     )


from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, input_file_name

from src.Extract_Data.config import config
# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

# @dp.table
# def createTable():

#together a part of a pipile 
@dp.table(
        name="countries_bronze",
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

    