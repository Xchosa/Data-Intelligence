
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


@dp.table(
    name=config.silver_table_cities,
    comment="Cleaned city master data",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("valid_city_code", "city_code IS NOT NULL AND trim(city_code) <> ''")
@dp.expect_or_drop("valid_country_code", "country_code IS NOT NULL")
@dp.expect("valid_time_zone_id", "time_zone_id IS NOT NULL")
@dp.expect("valid_utc_offset_format", "utc_offset RLIKE '^[+-][0-9]{2}:[0-9]{2}$'")
def cities_silver():
    return (
        spark.readStream.table("data_catalog.bronze.bronze_table_cities")
        .select(
            explode_outer(col("CityResource.Cities.City")).alias("city"),
            col("_source_file"),
            col("_ingested_at"),
        )
        .select(
            upper(trim(col("city.CityCode"))).alias("city_code"),
            upper(trim(col("city.CountryCode"))).alias("country_code"),
            trim(col("city.Names.Name")).alias("city_name"),
            trim(col("city.TimeZoneId")).alias("time_zone_id"),
            trim(col("city.UtcOffset")).alias("utc_offset"),
            col("city.Airports").alias("airports"),
            col("_source_file"),
            col("_ingested_at"),
            current_timestamp().alias("silver_processed_at"),
        )
        .dropDuplicates(["city_code"])
    )