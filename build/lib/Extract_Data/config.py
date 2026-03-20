# all hard coded values try:
import os
from databricks.sdk import WorkspaceClient
from dataclasses import dataclass

def get_lufthansa_secret() -> str:
    try:
        w = WorkspaceClient()
        return w.dbutils.secrets.get(scope="lufthansa-api", key="LUFTHANSA_SECRET")
    except Exception:
        print("No secret found")
        pass

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
    
    
    
    operations="operations"
    operation_type="flightstatus"
    operation_subtype="departures"
    airport_code=["FRA", "MUC"]
    meta_data_key_flight="FlightStatusResource"
    airport_code=["FRA", "MUC"]
    serviceType="all"
    headers = {"password": get_lufthansa_secret()}


    path_cities = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/cities"
    path_countries = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/countries"
    path_airports = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/airports"
    path_airlines = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/airlines"
    path_aircraft = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/aircraft"



config = Config()

