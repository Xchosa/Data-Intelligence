
from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp



from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp

from pyspark.sql.functions import col, explode_outer, current_timestamp, trim, upper

import sys
import os
# This file defines a sample transformation.
# Edit the sample below or add new transformations
# using "+ Add" in the file browser.

# Get the absolute path of the directory containing THIS script (src/ingestion)
if "__file__" in globals():
    current_dir = os.path.dirname(os.path.abspath(__file__))
else:
    current_dir = os.getcwd()

# Go up one level to reach 'src'
# This ensures that 'import utils.helpers' will work correctly
src_root = os.path.abspath(os.path.join(current_dir, '..'))

if src_root not in sys.path:
    sys.path.append(src_root)



from extract_data.config import config




#handle dubliplate dropDuplicates 
@dp.table(
    name=config.silver_table_countries,
    comment=config.silver_countries_comment,
    table_properties={"quality": "silver"},
)
def countries_silver():
    df = (
        spark.readStream.table("countries_bronze")
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


    # add country-specific transformations here
    # df = df.withColumn(...)

    return df

    