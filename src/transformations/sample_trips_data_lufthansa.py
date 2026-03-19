from pyspark import pipelines as dp
from pyspark.sql.functions import col


# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.


@dp.table
def sample_cities_lufthansa():
    
    catalog_name = "data_catalog"
    schema_name = "transforming_data"
    volume_name = "myfiles"


    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")
    
    return spark.read.table("samples.nyctaxi.trips")
