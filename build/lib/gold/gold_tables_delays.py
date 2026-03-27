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




# airport name destination auschreiben
# airport departure ausschreiben  verknuepfen mit airport Names silver tables 

# Flight delays in minute 

# unterschied zwischen Cargo und passenger 
# Flight duration 

@dp.table(
    name="gold_departures_delays",
    comment="Departure delays enriched with airport and country names",
    table_properties={"quality": "gold"},
)
def gold_departures_delays():
    from pyspark.sql.functions import col
    
    # Read silver departure data (streaming)
    df_departures = spark.readStream.table(
        f"{config.catalog_name}.silver.{config.silver_table_dep_a}"
    )
    
    # Read silver airports (batch - dimension table)
    df_airports = spark.read.table(
        f"{config.catalog_name}.silver.{config.silver_table_airports}"
    )
    
    # Read silver countries (batch - dimension table)
    df_countries = spark.read.table(
        f"{config.catalog_name}.silver.{config.silver_table_countries}"
    )
    
    # Join departure airport details
    df = df_departures.join(
        df_airports.select(
            col("airport_code").alias("dep_airport_code"),
            col("airport_name").alias("departure_airport_name"),
            col("country_code").alias("dep_country_code")
        ),
        on=col("departure_airport_code") == col("dep_airport_code"),
        how="left"
    )
    
    # Join arrival airport details
    df = df.join(
        df_airports.select(
            col("airport_code").alias("arr_airport_code"),
            col("airport_name").alias("arrival_airport_name"),
            col("country_code").alias("arr_country_code")
        ),
        on=col("arrival_airport_code") == col("arr_airport_code"),
        how="left"
    )
    
    # Join departure country name
    df = df.join(
        df_countries.select(
            col("country_code").alias("dep_country_code_lookup"),
            col("country_name").alias("departure_country_name")
        ),
        on=col("dep_country_code") == col("dep_country_code_lookup"),
        how="left"
    )
    
    # Join arrival country name
    df = df.join(
        df_countries.select(
            col("country_code").alias("arr_country_code_lookup"),
            col("country_name").alias("arrival_country_name")
        ),
        on=col("arr_country_code") == col("arr_country_code_lookup"),
        how="left"
    )
    
    # Select final columns
    final_columns = [
        "departure_airport_code",
        "departure_airport_name",
        "departure_country_name",
        "arrival_airport_code",
        "arrival_airport_name",
        "arrival_country_name",
        "dep_delay_minutes_actual",
        "marketing_airline_id",
        "marketing_flight_number",
        "_ingested_at",
    ]
    
    return df.select(*final_columns)