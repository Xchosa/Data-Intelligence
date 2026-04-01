from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, input_file_name

import os
import sys

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


@dp.table(
    name="bronze_logs",
    comment="Consolidated log files from all ETL runs",
    table_properties={"quality": "gold"},
)
def logs_cities_bronze():
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .load(f"/Volumes/{config.catalog_name}/{config.schema_name}/{config.volume_name}/cities_logs/*/run_*/aircraft_final.log/")
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingested_at", current_timestamp())
        .select(
            col("value").alias("log_content"),
            col("_source_file"),
            col("_ingested_at"),
        )
    )
    
    return df
