# all hard coded values try:
import os
import time
from datetime import datetime
from databricks.sdk import WorkspaceClient
from dataclasses import dataclass
import utilis




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
    
    Date=utilis.get_current_date(utilis.config_time_blocks())
    Time=utilis.config_time()
    
    operations="operations"
    operation_type="flightstatus"
    operation_subtype="departures"
    airport_code=["FRA", "MUC"]
    meta_data_key_flight="FlightStatusResource"
    #analyzed airports a, b 
    airport_code=["FRA", "MUC"]
    serviceType="all"
    headers = {"password": utilis.get_lufthansa_secret()}


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
    blocktime=utilis.config_time_blocks()
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
