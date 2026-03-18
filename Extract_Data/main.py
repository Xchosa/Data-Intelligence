import requests
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import uuid
import time
from dotenv import load_dotenv

from get_all_Recources import get_data_all_Reference
import get_flight_schedules
import get_flight_route
from utilis import is_databricks_notebook ,get_past_date
from get_flight_route import get_data_flight_route
from get_flight_departures import get_data_flight_depatures

# Load .env from parent directory
load_dotenv('../.env')

# password = dbutils.secrets.get(scope="lh-api", key="password")

def get_References()->None:
    base_url = "https://lh-proxy.onrender.com"
    if is_databricks_notebook():
        headers ={"password": dbutils.secrets.get(scope="lufthansa-api", key="LUFTHANSA_SECRET")}
    else:
        headers_env = os.getenv('headers_env')
        if not headers_env:
            raise ValueError("headers_env not found in .env file")
        headers = json.loads(headers_env)

    catalog_name ="data_catalog"
    schema_name = "bronze"
    volume_name = "bronze_volume"
        
        

    recordLimit = 100
    offset =0
    # save_on_Databricks=True
    save_on_Databricks=is_databricks_notebook()
    mds_reference = ["countries", "cities", "airports","airlines", "aircraft"]
    meta_data_key = ["CountryResource", "CityResource", "AirportResource","AirlineResource", "AircraftResource"]
    local_folder = ["Countries", "Cities", "Airports","Airlines", "Aircrafts"]



    for ref, key, folder in zip(mds_reference, meta_data_key, local_folder):
        get_data_all_Reference(
            base_Url=base_url,
            headers=headers,
                
            catalog_name=catalog_name,
            schema_name=schema_name,
            volume_name=volume_name,
            mds_reference=ref,
            recordLimit=recordLimit,
            offset=offset,

            save_on_Databricks=save_on_Databricks,
            meta_data_key=key,
            local_folder=folder
        )



def get_single_flight_route()->None:
        base_url = "https://lh-proxy.onrender.com"
        if is_databricks_notebook():
            headers ={"password": dbutils.secrets.get(scope="lufthansa-api", key="LUFTHANSA_SECRET")}
        else:
            headers_env = os.getenv('headers_env')
            if not headers_env:
                raise ValueError("headers_env not found in .env file")
        headers = json.loads(headers_env)
        catalog_name ="data_catalog"
        schema_name = "bronze"
        volume_name = "bronze_volume"
        operations="operations"
        operation_type="flightstatus"
        operation_subtype="route"
        origin="FRA"
        destination="MUC"
        #todays date
        Date="2026-03-12"
        meta_data_key="ScheduleResource"
        recordLimit=20
        offset=0
        #Date=datetime.now().strftime("%Y-%m-%d")

        serviceType="all"


        origin="FRA"
        destination ="MUC"

        get_data_flight_route(
            base_Url=base_url,
            headers=headers,
                
            catalog_name=catalog_name,
            schema_name=schema_name,
            volume_name=volume_name,

            operations= operations,
            operation_type=operation_type,

            recordLimit=recordLimit,
            offset=offset,

            save_on_Databricks=False,
            local_folder=f"flight_{operation_type}",
            
            meta_data_key=meta_data_key,
            origin=origin,
            destination=destination,
            Date=Date,
            
            serviceType=serviceType

    )
        

def get_flights()->None:
    
    base_url = "https://lh-proxy.onrender.com"
    if is_databricks_notebook():
        headers ={"password": dbutils.secrets.get(scope="lufthansa-api", key="LUFTHANSA_SECRET")}
    else:
        headers_env = os.getenv('headers_env')
        if not headers_env:
            raise ValueError("headers_env not found in .env file")
   
    catalog_name ="data_catalog"
    schema_name = "bronze"
    volume_name = "bronze_volume"
        
    operations="operations"
    operation_type="flightstatus"
    operation_subtype="departures"
    airport_code=["FRA", "MUC"]
    #todays date
    # 
    Date=get_past_date(time=0)
    meta_data_key="FlightStatusResource"
    recordLimit=20
    offset=0
    #Date=datetime.now().strftime("%Y-%m-%d")
    local_folder=f"{operation_subtype}_{airport_code}"
    serviceType="all"

    #for loop 
    for airport in airport_code:
        local_folder=f"{operation_subtype}_{airport}"
        get_data_flight_depatures(
            base_Url=base_url,
            headers=headers,
                
            catalog_name=catalog_name,
            schema_name=schema_name,
            volume_name=volume_name,

            operations= operations,
            operation_type=operation_type,
            operation_subtype= operation_subtype,
            recordLimit=recordLimit,
            offset=offset,

            save_on_Databricks=False,
            local_folder=local_folder,
            
            meta_data_key=meta_data_key,
            airport_code=airport,
            Date=Date,
            
            serviceType=serviceType

            )


if __name__ == "__main__":
    try:
        get_References()
        get_flights()

        get_single_flight_route()
    except Exception as e:
        print(f"error {e}")