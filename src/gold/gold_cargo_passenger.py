from pyspark import pipelines as dp
from pyspark.sql.functions import col, count
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

@dp.table(
    name="gold_service_type_counts",
    comment="Total flight counts for Passenger and Cargo service types.",
    table_properties={"quality": "gold"}
)
def gold_service_type_counts():
    # Read the denormalized gold flight details table
    flight_details_df = spark.read.table(f"{config.catalog_name}.silver.{config.silver_table_departures}")

    # Group by service_type, count, and filter for only Passenger and Cargo
    # The service_type is already uppercased in the silver layer
    service_counts_df = flight_details_df.groupBy("service_type").agg(
        count("*").alias("flight_count")
    ).filter(
        col("service_type").isin(["PASSENGER", "CARGO"])
    )

    return service_counts_df