from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, input_file_name

from src.Extract_Data.config import config
# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.
spark.sql(f"DROP TABLE IF EXISTS bronze_table_cities)
bronze_table_cities = data_catalog.bronze.cities

path_cities = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/cities"
path_countries = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/countries"
path_airports = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/airports"
path_airlines = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/airlines"
path_aircraft = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/aircraft"


spark.sql(f"""
CREATE TABLE current_employees_ctas
AS
SELECT ID, FirstName, Country, Role 
FROM read_files(
  '/Volumes/{catalog_name}/{schema_name}/{volume_name}/',
  format => 'json',
  header => true,
  inferSchema => true
 );"")

#Display available tables in your schema
spark.sql(f"SHOW TABLES;").display()






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

    
def cities_bronze_write():
    cites_bronze().writeStream.format("delta").option("checkpointLocation",
                                                       f"{path_cities}/checkpoint").start(f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/cities")
    toTable("cities_bronze")

@dp.table(
        name="airports_bronze",
        comment="Raw airports JSON from Lufthansa landing volume",
        table_properties={"quality": "bronze"}

def countries_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load({path_countries})
            .withColumn("_source_file", col("_metadata.file_path"))
            .withColumn("_ingested_at", current_timestamp())
    )
)
    
# def airports_bronze():
#     return (
#         spark.readStream
#             .format("cloudFiles")
#             .option("cloudFiles.format", "json")
#             .load(f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/airports")
#             .withColumn("_source_file",col("_metadata.file_path")))
#             .withColumn("_ingested_at", current_timestamp())
#     )

#     @dp.table(
#         name="airlines_bronze",
#         comment="Raw airlines JSON from Lufthansa landing volume",
#         table_properties={"quality": "bronze"}
# )
# def airlines_bronze():
#     return (
#         spark.readStream
#             .format("cloudFiles")
#             .option("cloudFiles.format", "json")
#             .load(f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/airlines")
#             .withColumn("_source_file", input_file_name())
#             .withColumn("_ingested_at", current_timestamp())
#     )

def display():
    spark.catalog.setCurrentCatalog(config.catalog_name)
    spark.catalog.setCurrentDatabase(config.schema_name)

    # Display available tables in your schema
    spark.catalog.listTables(schema_name)
    spark.sql(f"LIST '/Volumes/{catalog_name}/{schema_name}/{volume_name}/' ").display()

# if __name__ == "__main__":
#     cities_bronze()
#     cities_bronze_write()
#     # display()
