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



@dp.table(
    name="gold_departures_cargo_delays",
    comment="Departure and arrival airports with delay information and airline names",
    table_properties={"quality": "gold"},
)
def gold_departures_delays():
    
    # Read silver departure data (streaming)
    df_departures = spark.readStream.table(
        f"{config.catalog_name}.silver.{config.silver_table_departures}"
    )
    
    # Read silver dimension tables (batch)
    df_airlines = spark.read.table(
        f"{config.catalog_name}.silver.{config.silver_table_airlines}"
    )
    df_airports = spark.read.table(
        f"{config.catalog_name}.silver.{config.silver_table_airports}"
    )
    df_countries = spark.read.table(
        f"{config.catalog_name}.silver.{config.silver_table_countries}"
    )
    
    # Join with airlines to get airline name
    df = df_departures.join(
        df_airlines.select(
            col("airline_id").alias("airline_id_lookup"),
            col("airline_name")
        ),
        on=col("operating_airline_id") == col("airline_id_lookup"),
        how="left"
    ).drop("airline_id_lookup")

    # Join with airports to get arrival airport name and country code
    df = df.join(
        df_airports.select(
            col("airport_code").alias("arr_airport_code"),
            col("airport_name").alias("arrival_airport_name"),
            col("country_code").alias("arr_country_code")
        ),
        on=col("arrival_airport_code") == col("arr_airport_code"),
        how="left"
    ).drop("arr_airport_code")

    # Join with countries to get arrival country name
    df = df.join(
        df_countries.select(
            col("country_code").alias("country_code_lookup"),
            col("country_name").alias("arrival_country_name")
        ),
        on=col("arr_country_code") == col("country_code_lookup"),
        how="left"
    ).drop("country_code_lookup", "arr_country_code")
    
    # Select final columns
    final_columns = [
        "departure_airport_code",
        "arrival_airport_code",
        "arrival_airport_name",
        "arrival_country_name",
        "airline_name",
        "dep_delay_minutes_actual",
        "service_type",
    ]
    
    return df.select(*final_columns)