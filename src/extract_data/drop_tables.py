

from pyspark import pipelines as dp
from pyspark.sql.functions import col
from pyspark.sql.functions import current_timestamp

import sys
import os


# if "__file__" in globals():
#     current_dir = os.path.dirname(os.path.abspath(__file__))
# else:
#     current_dir = os.getcwd()

# # Go up one level to reach 'src'
# # This ensures that 'import utils.helpers' will work correctly
# src_root = os.path.abspath(os.path.join(current_dir, '..'))

# if src_root not in sys.path:
#     sys.path.append(src_root)

from config import config


def drop_tables()->None:
    spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_aircrafts}")
    spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_airlines}")
    spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_countries}")
    spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_cities}")
    spark.sql(f"DROP TABLE IF EXISTS {config.bronze_table_airports}")


if __name__=="__main__":
    drop_tables()