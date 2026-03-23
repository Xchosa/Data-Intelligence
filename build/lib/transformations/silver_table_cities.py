
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


from extract_data.config import config

path_cities = f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/cities"
#together a part of a pipile 
@dp.table(
        name="cities_bronze",
        comment="Raw countries JSON from Lufthansa landing volume",
        table_properties={"quality": "bronze"}
)
@dp.expect_or_drop("non_negative_amount", "amount >=0")
def cities_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(path_cities)
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
    

