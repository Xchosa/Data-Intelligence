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



def get_flights()->None:
    
    base_url = "https://lh-proxy.onrender.com"
    headers = {"password": config.get_lufthansa_secret()}
    
   
    catalog_name ="data_catalog"
    schema_name = "bronze"
    volume_name = "bronze_volume"
        
    operations="operations"
    operation_type="flightstatus"
    operation_subtype="departures"
    airport_code=["FRA", "MUC"]
    #todays date
    # 
    Date=get_past_date(from_time_block=0)
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
            
            meta_data_key=meta_data_key,
            airport_code=airport,
            Date=Date,
            offset=offset,
            serviceType=serviceType,
           
            )
        