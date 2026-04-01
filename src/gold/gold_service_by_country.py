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
    name="gold_service_by_arrival_country",
    comment="Aggregated flight counts by service type for each arrival country.",
    table_properties={"quality": "gold"}
)
def gold_service_by_arrival_country():
    # Read the necessary silver tables
    departures_df = spark.read.table(f"{config.catalog_name}.silver.{config.silver_table_departures}")
    airports_df = spark.read.table(f"{config.catalog_name}.silver.{config.silver_table_airports}")
    countries_df = spark.read.table(f"{config.catalog_name}.silver.{config.silver_table_countries}")

    # Join departures with airports to get the country code of the arrival airport
    departures_with_country_code = departures_df.alias("dep").join(
        airports_df.alias("arr_ap"),
        col("dep.arrival_airport_code") == col("arr_ap.airport_code"),
        "left"
    )

    # Join with countries to get the full country name
    departures_with_country_name = departures_with_country_code.alias("dep_cc").join(
        countries_df.alias("country"),
        col("dep_cc.country_code") == col("country.country_code"),
        "left"
    )
    

    # Aggregate to get the counts
    agg_df = departures_with_country_name.groupBy(
        col("country.country_name").alias("arrival_country_name"),
        col("dep_cc.service_type")
    ).agg(
        count("*").alias("flight_count")
    ).orderBy(col("flight_count").desc())

    return agg_df