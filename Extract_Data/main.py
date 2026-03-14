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

#11213
#test api call: 
# curl -v -H "password: DataIntelligence2026" "https://lh-proxy.onrender.com/v1/mds-references/airports?limit=100&offset=1100" --max-time 5



    
#get_countries.get_data_Reference_countries(
#    base_Url="https://lh-proxy.onrender.com",
#    headers=headers,
    
#    catalog_name = catalog_name,
#    schema_name = schema_name,
#    volume_name = volume_name,
#    mds_reference="countries",
#    recordLimit= recordLimit,
#    offset=offset,
	
#    save_on_Databricks = False,
#    meta_data_key = "CountryResource",
#    local_folder ="countries",

#    )

#mds_reference="cities",
#meta_data_key = "CityResource",
#local_folder ="cities",

#get_cities.get_data_Reference_cities(
#    base_Url="https://lh-proxy.onrender.com",
#    headers=headers,
    
#    catalog_name = catalog_name,
#    schema_name = schema_name,
#    volume_name = volume_name,
#    mds_reference="cities",
#    recordLimit= recordLimit,
#    offset=offset,

#    save_on_Databricks = False,
#    meta_data_key = "CityResource",
#    local_folder ="cities",
#    )

#get_airlines.get_data_Reference_airlines(
#    base_Url="https://lh-proxy.onrender.com",
#    headers=headers,
    
#    catalog_name = catalog_name,
#    schema_name = schema_name,
#    volume_name = volume_name,
#    mds_reference="airlines",
#    recordLimit= recordLimit,
#    offset=offset,

#    save_on_Databricks = False,
#    meta_data_key = "AirlineResource",
#    local_folder ="airlines",
#    )
    
#get_airports.get_data_Reference_airport(
#    base_Url="https://lh-proxy.onrender.com",
#    headers=headers,
    
#    catalog_name = catalog_name,
#    schema_name = schema_name,
#    volume_name = volume_name,
#    mds_reference="airports",
#    recordLimit= recordLimit,
#    offset=offset,

#    save_on_Databricks = False,
#    meta_data_key = "AirportResource",
#    local_folder ="airport",
#    )

#get_aircrafts.get_data_Reference_aircrafts(
#    base_Url="https://lh-proxy.onrender.com",
#    headers=headers,
    
#    catalog_name = catalog_name,
#    schema_name = schema_name,
#    volume_name = volume_name,
#    mds_reference="aircraft",
#    recordLimit= recordLimit,
#    offset=offset,

#    save_on_Databricks = False,
#    meta_data_key = "AircraftResource",
#    local_folder ="aircraft",
#    )



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
operation_type="schedule"
origin="FRA"
destination="POR"
#todays date
Date=datetime.now().strftime("%Y-%m-%d")

get_flight_schedules.get_data_flight_Schedules(
    base_Url="https://lh-proxy.onrender.com",
    headers=headers,
        
    catalog_name=catalog_name,
    schema_name=schema_name,
    volume_name=volume_name,

    operations= operations,
    operation_type="schedule",

    recordLimit=recordLimit,
    offset=offset,

    save_on_Databricks=False,
    local_folder=f"flight_{operation_type}",
    
    meta_data_key="ScheduleResource",
	origin=origin,
    destination=destination,
    Date=Date,

    )