from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp, input_file_name

from pyspark.sql.functions import (
    col,
    explode_outer,
    upper,
    trim,
    current_timestamp,
    regexp_like,
)
import sys
import os


if "__file__" in globals():
    current_dir = os.path.dirname(os.path.abspath(__file__))
else:
    current_dir = os.getcwd()

src_root = os.path.abspath(os.path.join(current_dir, '..'))

if src_root not in sys.path:
    sys.path.append(src_root)


from extract_data.config import config


# @dp.table(
#     name="delays_cargo",
#     comment="Cleaned FRA departure flight status data for downstream delay analysis",
#     table_properties={"quality": "silver"},
# )
# def logs_gold():
#     df = (
#         spark.readStream
#         .format("cloudFiles")
#         .option("cloudFiles.format", "text")
#         .load("/Volumes/data_catalog/bronze/bronze_volume/aircraft/logs/*/run_*/tmp_logs/")
#         .withColumn("_source_file", input_file_name())
#         .withColumn("_ingested_at", current_timestamp())
#         .select(
#             col("value").alias("log_content"),
#             col("_source_file"),
#             col("_ingested_at"),
#         )
#     )
    
#     return df
