# from pyspark.sql import SparkSession
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
    lit,
    when
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
    df = spark.readStream.table("data_catalog.bronze.bronze_table_countries")
    
    # Filter out log files
    df = df.filter(~col("_source_file").contains("logs"))
    
    df = df.select(
        explode_outer(col("CountryResource.Countries.Country")).alias("country"),
        col("_source_file"),
        col("_ingested_at"),
    )
    
    df = df.withColumn(
        "country_code",
        upper(trim(col("country.CountryCode")))
    ).withColumn(
        "country_name",
        trim(col("country.Names.Name"))
    ).withColumn(
        "silver_processed_at",
        current_timestamp()
    )
    
    final_columns = [
        "country_code",
        "country_name",
        "_source_file",
        "_ingested_at",
        "silver_processed_at",
    ]
    
    return df.select(*final_columns).dropDuplicates(["country_code"])


@dp.table(
    name="quarantine_countries",
    comment="Quarantine table for countries records failing data quality checks",
    table_properties={"quality": "quarantine"},
)
def countries_quarantine():
    """Captures records that don't have valid country data"""
    df = spark.readStream.table("data_catalog.bronze.bronze_table_countries")
    
    # Filter out log files
    df = df.filter(~col("_source_file").contains("logs"))
    
    df = df.select(
        explode_outer(col("CountryResource.Countries.Country")).alias("country"),
        col("_source_file"),
        col("_ingested_at"),
    )
    
    df = df.withColumn(
        "country_code",
        upper(trim(col("country.CountryCode")))
    ).withColumn(
        "country_name",
        trim(col("country.Names.Name"))
    )
    
    # Filter for invalid records
    df = df.filter(
        (col("country").isNull()) |
        (col("country_code").isNull()) |
        (trim(col("country_code")) == "") |
        (col("country_name").isNull())
    )
    
    df = df.withColumn(
        "quarantine_reason",
        when(col("country").isNull(), "Missing country object")
        .when(col("country_code").isNull() | (trim(col("country_code")) == ""), "Missing or empty country_code")
        .when(col("country_name").isNull(), "Missing country_name")
        .otherwise("Invalid country data")
    ).withColumn(
        "quarantine_timestamp",
        current_timestamp()
    )
    
    final_columns = [
        "country_code",
        "country_name",
        "quarantine_reason",
        "_source_file",
        "_ingested_at",
        "quarantine_timestamp",
    ]
    
    return df.select(*final_columns).dropDuplicates(["country_code"])