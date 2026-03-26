from pyspark import pipelines as dp

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

extract_data_root = os.path.join(src_root, 'extract_data')
if extract_data_root not in sys.path:
    sys.path.append(extract_data_root)

from extract_data.config import config

# Import the modules to register their tables with DLT
import silver_tables_aircraft
# import silver_tables_country
# import silver_tables_airlines
# import silver_tables_cities
# import silver_tables_aircraft

# if __name__ == "__main__":
#     silver_tables_aircraft.aircraft_silver()
#     silver_tables_aircraft.aircraft_quarantine()
#     # silver_tables_country.countries_silver()
#     # silver_tables_country.countries_quarantine()
# #     silver_tables_country
# # 0   silver_tables_airlines
# #     silver_tables_cities
# #     silver_tables_aircraft
