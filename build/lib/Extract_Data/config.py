# all hard coded values try:
import os
import sys
import time
from datetime import datetime
from databricks.sdk import WorkspaceClient
from dataclasses import dataclass, field

current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)


import utilis






@dataclass(frozen=True)
class Config:
    catalog_name ="data_catalog"
    schema_name = "bronze"
    volume_name = "bronze_volume"
    meta_valume= "autoload_metadata"


    base_url: str = "https://lh-proxy.onrender.com"
    record_limit: int = 100
    offset: int = 0
    mds_reference = ["countries", "cities", "airports","airlines", "aircraft"]
    meta_data_key = ["CountryResource", "CityResource", "AirportResource","AirlineResource", "AircraftResource"]
    local_folder = ["Countries", "Cities", "Airports","Airlines", "Aircrafts"]
    
    Date=utilis.get_current_date(utilis.config_time_blocks())
    departure_Date = utilis.get_flight_departure_date()
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

    bronze_table_cities=f"bronze_cities"
    bronze_table_countries=f"bronze_countries"
    bronze_table_airlines=f"bronze_airlines"
    bronze_table_airports=f"bronze_airports"
    bronze_table_aircrafts=f"bronze_aircrafts"

    bronze_table_dep_a=f"bronze_table_depatures_FRA" 
    bronze_table_dep_b=f"bronze_table_depatures_MUC" 
    blocktime=utilis.config_time_blocks()
    airport_code_a="FRA"
    airport_code_b="MUC"

    # wild card format for latest date
    path_depature_airport_a=f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{operations}/{operation_type}/{operation_subtype}/{airport_code_a}/{utilis.underscore_format(utilis.get_flight_departure_date())}/*/"
    path_depature_airport_b=f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{operations}/{operation_type}/{operation_subtype}/{airport_code_b}/{utilis.underscore_format(utilis.get_flight_departure_date())}/*/"


    silver_table_cities: str = "silver_cities"
    silver_table_countries: str = "silver_countries"
    silver_table_airlines: str = "silver_airlines"
    silver_table_airports: str = "silver_airports"
    silver_table_aircrafts: str = "silver_aircrafts"

    silver_cities_comment: str = "Cleaned cities dimension from bronze cities data"
    silver_countries_comment: str = "Cleaned countries dimension from bronze countries data"
    silver_airlines_comment: str = "Cleaned airlines dimension from bronze airlines data"
    silver_airports_comment: str = "Cleaned airports dimension from bronze airports data"
    silver_aircrafts_comment: str = "Cleaned aircraft dimension from bronze aircraft data"
    
    silver_table_dep_a: str = "silver_table_departures_FRA"
    silver_table_dep_b: str = "silver_table_depatures_MUC"
    silver_table_dep_a_comment: str = "Cleaned FRA departure flight status data for downstream delay analysis"
    silver_table_dep_b_comment: str = "Cleaned MUC departure flight status data for downstream delay analysis"

    table_specs: list = field(default_factory=lambda: [
        {
            "name": "countries_bronze",
            "config_name": "bronze_table_countries",
            "path": "/Volumes/data_catalog/bronze/bronze_volume/countries",
            "reference": "countries",
            "comment": "Raw countries JSON from Lufthansa landing volume",
        },
        {
            "name": "cities_bronze",
            "config_name": "bronze_table_cities",
            "path": "/Volumes/data_catalog/bronze/bronze_volume/cities",
            "reference": "cities",
            "comment": "Raw cities JSON from Lufthansa landing volume",
        },
        {
            "name": "airports_bronze",
            "config_name": "bronze_table_airports",
            "path": "/Volumes/data_catalog/bronze/bronze_volume/airports",
            "reference": "airports",
            "comment": "Raw airports JSON from Lufthansa landing volume",
        },
        {
            "name": "airlines_bronze",
            "config_name": "bronze_table_airlines",
            "path": "/Volumes/data_catalog/bronze/bronze_volume/airlines",
            "reference": "airlines",
            "comment": "Raw airlines JSON from Lufthansa landing volume",
        },
        {
            "name": "aircraft_bronze",
            "config_name": "bronze_table_aircrafts",
            "path": "/Volumes/data_catalog/bronze/bronze_volume/aircraft",
            "reference": "aircraft",
            "comment": "Raw aircraft JSON from Lufthansa landing volume",
        },
    ])

config = Config()



# @dataclass(frozen=True)
# class Schema:
