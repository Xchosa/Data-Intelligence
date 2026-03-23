# all hard coded values try:
import os
import time
from datetime import datetime
from databricks.sdk import WorkspaceClient
from dataclasses import dataclass
from utilis import get_past_date

def get_lufthansa_secret() -> str:
    try:
        w = WorkspaceClient()
        return w.dbutils.secrets.get(scope="lufthansa-api", key="LUFTHANSA_SECRET")
    except Exception:
        print("No secret found")
        pass

def config_time() -> str:
    current_time = datetime.now()
    return current_time.strftime("%H:%M")

def config_depature_time_blocks() -> str:
    """
    Returns the current time rounded down to the nearest 4-hour block.
    Examples:
    - 03:45 AM -> 00:00
    - 07:30 AM -> 04:00
    - 08:15 AM -> 08:00
    - 15:45 PM -> 12:00
    - 23:30 PM -> 20:00
    """
    current_time = datetime.now()
    hour = current_time.hour
    
    # Round down to nearest 4-hour block
    block_hour = (hour // 4) * 4
    return f"{block_hour:02d}:00"


@dataclass(frozen=True)
class Config:
    catalog_name ="data_catalog"
    schema_name = "bronze"
    volume_name = "bronze_volume"
    table_name_1 = "sample_cities"


    base_url: str = "https://lh-proxy.onrender.com"
    record_limit: int = 100
    offset: int = 0
    mds_reference = ["countries", "cities", "airports","airlines", "aircraft"]
    meta_data_key = ["CountryResource", "CityResource", "AirportResource","AirlineResource", "AircraftResource"]
    local_folder = ["Countries", "Cities", "Airports","Airlines", "Aircrafts"]
    
    Date=get_past_date(from_time_block=0)
    Time=config_time()
    
    operations="operations"
    operation_type="flightstatus"
    operation_subtype="departures"
    airport_code=["FRA", "MUC"]
    meta_data_key_flight="FlightStatusResource"
    #analyzed airports a, b 
    airport_code=["FRA", "MUC"]
    serviceType="all"
    headers = {"password": get_lufthansa_secret()}


    path_cities = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/cities"
    path_countries = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/countries"
    path_airports = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/airports"
    path_airlines = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/airlines"
    path_aircraft = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/aircraft"

    bronze_table_cities=f"bronze_table_cities"
    bronze_table_countries=f"bronze_table_countries"
    bronze_table_airlines=f"bronze_table_airlines"
    bronze_table_airports=f"bronze_table_airports"
    bronze_table_aircrafts=f"bronze_table_aircrafts"

    bronze_table_dep_a=f"bronze_table_depatures_FRA" 
    bronze_table_dep_b=f"bronze_table_depatures_MUC" 
    blocktime=config_depature_time_blocks()
    airport_code_a="FRA"
    airport_code_b="MUC"
    # path_depature_airport_a=f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{airport_code_a}_{Date}_{blocktime}"
    path_depature_airport_a=f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{operations}/{operation_type}/{operation_subtype}/{airport_code_a}/{Date}/"
    path_depature_airport_b=f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{operations}/{operation_type}/{operation_subtype}/{airport_code_b}/{Date}/"


    silver_table_cities=f"silver_table_cities"
    silver_table_countries=f"silver_table_countries"
    silver_table_airlines=f"silver_table_airlines"
    silver_table_airports=f"silver_table_airports"
    silver_table_aircrafts=f"silver_table_aircrafts"

    silver_table_dep_a= f"silver_table_departures_FRA"
    silver_table_dep_b= f"silver_table_depatures_MUC"

config = Config()



# @dataclass(frozen=True)
# class Schema:
