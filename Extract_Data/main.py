import requests
import json
import os
from datetime import datetime

from pathlib import Path
from typing import Dict, Any, List, Optional
import uuid
import time

import get_airports
import get_countries
import get_airlines
import get_cities
import get_aircrafts
import get_all_Recources
import get_flight_schedules

# password = dbutils.secrets.get(scope="lh-api", key="password")
base_url = "https://lh-proxy.onrender.com"
headers ={"password": "DataIntelligence2026"}

catalog_name ="data_catalog"
schema_name = "bronze"
volume_name = "bronze_volume"
    
    
# Option 1: Fetch all reference data

#reference_type = ["mds-reference", "Offers"]
#Offers = [ "SeatMaps", "Lounges"]

#SeatMaps = ["flightNumber", "origin", "destination", "departureDate", "cabinTypeCode"]
#lounges = ["code", "cabinClassCode", "tierCode", "languageCode"]

mds_reference = ["countries", "cities", "airports","airlines", "aircrafts"]
meta_data_key = ["CountryResource", "CityResource", "AirportResource","AirlineResource", "AircraftResource"]
local_folder = ["Countries", "Cities", "Airports","Airlines", "Aircrafts"]
#recordLimit = 100
#offset =11000

recordLimit = 100
offset =0


#wworking
#for ref, key, folder in zip(mds_reference, meta_data_key, local_folder):
#    get_all_Recources.get_data_all_Reference(
#        base_Url="https://lh-proxy.onrender.com",
#        headers=headers,
        
#        catalog_name=catalog_name,
#        schema_name=schema_name,
#        volume_name=volume_name,
#        mds_reference=ref,
#        recordLimit=recordLimit,
#        offset=offset,

#        save_on_Databricks=False,
#        meta_data_key=key,
#        local_folder=folder
#    )


#one domestic,Eu, international
operations="operations"
operation_type="schedules"
origin="FRA"
destination="MUC"
#todays date
Date="2026-03-12"
meta_data_key="ScheduleResource"
recordLimit=20
offset=0
#Date=datetime.now().strftime("%Y-%m-%d")

get_flight_schedules.get_data_flight_Schedules(
    base_Url="https://lh-proxy.onrender.com",
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

    )