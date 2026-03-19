import requests
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import uuid
import time



from get_all_Recources import get_data_all_Reference
from utilis import get_past_date
from get_flight_operations import get_flight_route 
from get_flight_departures import get_data_flight_depatures

import config


from databricks.sdk import WorkspaceClient




def get_References()->None:
    

    base_url = "https://lh-proxy.onrender.com"
    headers = {"password": config.get_lufthansa_secret()}
    #headers ={"password": dbutils.secrets.get(scope="lufthansa-api", key="LUFTHANSA_SECRET")}

    catalog_name ="data_catalog"
    schema_name = "bronze"
    volume_name = "bronze_volume"
        
        

    recordLimit = 100
    offset =0
    # save_on_Databricks=True
    save_on_Databricks=True
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