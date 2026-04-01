from pyspark import pipelines as dp
from pyspark.sql.functions import col, avg, max, count, when
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
    name="gold_delay_comparison_by_service",
    comment="Compares average and max departure delays between Passenger and Cargo flights.",
    table_properties={"quality": "gold"}
)
def gold_delay_comparison_by_service():
    # Read the silver departures table which contains the delay information
    departures_df = spark.read.table(f"{config.catalog_name}.silver.{config.silver_table_departures}")

    # Filter for only Passenger and Cargo flights that have a delay value
    filtered_df = departures_df.filter(
        col("service_type").isin(["PASSENGER", "CARGO"]) &
        col("dep_delay_minutes_actual").isNotNull()
    )

    # Group by service_type and calculate aggregate delay metrics
    delay_comparison_df = filtered_df.groupBy("service_type").agg(
        avg("dep_delay_minutes_actual").alias("average_delay_minutes"),
        max("dep_delay_minutes_actual").alias("max_delay_minutes"),
        count("*").alias("total_flights"),
        count(when(col("dep_delay_minutes_actual") > 0, 1)).alias("delayed_flights_count")
    )

    return delay_comparison_df