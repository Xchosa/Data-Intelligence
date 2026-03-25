
from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp

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



#handle dubliplate dropDuplicates 
#preserves row even with null (explode_outer)
@dp.table(
    name=config.silver_table_countries,
    comment=config.silver_countries_comment,
    table_properties={"quality": "silver"},
)
def countries_silver():
    return (
        spark.readStream.table("data_catalog.bronze.bronze_table_countries")
        .select(
            explode_outer(col("CountryResource.Countries.Country")).alias("country"),
            col("_source_file"),
            col("_ingested_at"),
        )
        .select(
            upper(trim(col("country.CountryCode"))).alias("country_code"),
            col("country.Names.Name").alias("country_name"),
            col("_source_file"),
            col("_ingested_at"),
            current_timestamp().alias("silver_processed_at"),
        )
        .dropDuplicates(["country_code"])
    )


@dp.table(
    name=config.silver_table_aircrafts,
    comment=config.silver_aircrafts_comment,
    table_properties={"quality": "silver"},
)
def aircraft_silver():
    return (
        spark.readStream.table("data_catalog.bronze.bronze_table_aircrafts")
        .select(

            explode_outer(col("AircraftResource.AircraftSummaries.AircraftSummary")).alias("aircraft"),
            col("_source_file"),
            col("_ingested_at"),
        )
        .select(
            upper(trim(col("aircraft.AircraftCode"))).alias("aircraft_code"),
            col("aircraft.AirlineEquipCode").alias("airline_equip_code"),
            col("aircraft.Names.Name.`$`").alias("aircraft_name"),
            col("aircraft.Names.Name.`@LanguageCode`").alias("language_code"),
            col("_source_file"),
            col("_ingested_at"),
            current_timestamp().alias("silver_processed_at"),
        )
        .dropDuplicates(["aircraft_code"])
    )

    # add country-specific transformations here
    # df = df.withColumn(...)

  
#not tested
