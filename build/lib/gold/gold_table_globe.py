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
    name="gold_globe",
    comment="Departure delays enriched with airport and country names",
    table_properties={"quality": "gold"},
)
def gold_globe_delays():
    """ Airports in Globe """

    df_departures = spark.readStream.table(
        f"{config.catalog_name}.silver.{config.silver_table_dep_a}"
    )

    