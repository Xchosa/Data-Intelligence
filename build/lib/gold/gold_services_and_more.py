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
    name="gold_flight_details",
    comment="Enriched and denormalized flight departure details for analysis.",
    table_properties={"quality": "gold"}
)
def gold_flight_details():
    # Read the necessary silver tables
    departures_df = spark.read.table(f"{config.catalog_name}.silver.{config.silver_table_departures}")
    airports_df = spark.read.table(f"{config.catalog_name}.silver.{config.silver_table_airports}")
    countries_df = spark.read.table(f"{config.catalog_name}.silver.{config.silver_table_countries}")

    # Join departure airport details
    df = departures_df.join(
        airports_df.select(
            col("airport_code").alias("dep_airport_code"),
            col("airport_name").alias("departure_airport_name"),
            col("country_code").alias("dep_country_code")
        ),
        on=departures_df.departure_airport_code == col("dep_airport_code"),
        how="left"
    ).drop("dep_airport_code")

    # Join arrival airport details
    df = df.join(
        airports_df.select(
            col("airport_code").alias("arr_airport_code"),
            col("airport_name").alias("arrival_airport_name"),
            col("country_code").alias("arr_country_code")
        ),
        on=df.arrival_airport_code == col("arr_airport_code"),
        how="left"
    ).drop("arr_airport_code")

    # Join departure country name
    df = df.join(
        countries_df.select(
            col("country_code").alias("dep_country_code_lookup"),
            col("country_name").alias("departure_country_name")
        ),
        on=df.dep_country_code == col("dep_country_code_lookup"),
        how="left"
    ).drop("dep_country_code_lookup")

    # Join arrival country name
    df = df.join(
        countries_df.select(
            col("country_code").alias("arr_country_code_lookup"),
            col("country_name").alias("arrival_country_name")
        ),
        on=df.arr_country_code == col("arr_country_code_lookup"),
        how="left"
    ).drop("arr_country_code_lookup")

    # Select and rename final columns for the gold table
    # Note: 'dep_delay_minutes_actual' is not in the silver table, so it's excluded.
    # You can add it to the silver layer first if needed.
    final_df = df.select(
        "departure_airport_code",
        "departure_airport_name",
        "departure_country_name",
        "arrival_airport_code",
        "arrival_airport_name",
        "arrival_country_name",
        "marketing_airline_id",
        "marketing_flight_number",
        "_ingested_at"
    )

    return final_df