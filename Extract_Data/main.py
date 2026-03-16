import requests
import json
import os
from datetime import datetime

from pathlib import Path
from typing import Dict, Any, List, Optional
import uuid
import time


import get_all_Recources
import get_flight_schedules
import get_flight_route
from utilis import is_databricks_notebook

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

#recordLimit = 100
#offset =11000

recordLimit = 100
offset =0
# save_on_Databricks=True
save_on_Databricks=is_databricks_notebook()
mds_reference = ["countries", "cities", "airports","airlines", "aircrafts"]
meta_data_key = ["CountryResource", "CityResource", "AirportResource","AirlineResource", "AircraftResource"]
local_folder = ["Countries", "Cities", "Airports","Airlines", "Aircrafts"]

#wworking
for ref, key, folder in zip(mds_reference, meta_data_key, local_folder):
   get_all_Recources.get_data_all_Reference(
       base_Url="https://lh-proxy.onrender.com",
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


#one domestic,Eu, international
#operations="operations"
#operation_type="schedules"
#origin="FRA"
#destination="MUC"
##todays date
#Date="2026-03-12"
#meta_data_key="ScheduleResource"
#recordLimit=20
#offset=0
#Date=datetime.now().strftime("%Y-%m-%d")



#get_flight_schedules.get_data_flight_Schedules(
#    base_Url="https://lh-proxy.onrender.com",
#    headers=headers,
        
#    catalog_name=catalog_name,
#    schema_name=schema_name,
#    volume_name=volume_name,

#    operations= operations,
#    operation_type=operation_type,

#    recordLimit=recordLimit,
#    offset=offset,

#    save_on_Databricks=False,
#    local_folder=f"flight_{operation_type}",
    
#    meta_data_key=meta_data_key,
#	origin=origin,
#    destination=destination,
#    Date=Date,

#    )


#how to everything here - not in notebooks
#only by route daily ( not offers...)
#GET /operations/flightstatus/route/{origin}/{destination}/{date}?serviceType={serviceType}


# operations="operations"
# operation_type="flightstatus"
# operation_subtype="route"
# origin="FRA"
# destination="MUC"
# #todays date
# Date="2026-03-12"
# meta_data_key="ScheduleResource"
# recordLimit=20
# offset=0
# #Date=datetime.now().strftime("%Y-%m-%d")

# serviceType="all"


# origin=["FRA", "FRA" , "FRA" "London", "NewYork"]
# destination =["MUC", "istanbul", "Bankok"]

# get_flight_route.get_data_flight_route(
#     base_Url="https://lh-proxy.onrender.com",
#     headers=headers,
        
#     catalog_name=catalog_name,
#     schema_name=schema_name,
#     volume_name=volume_name,

#     operations= operations,
#     operation_type=operation_type,

#     recordLimit=recordLimit,
#     offset=offset,

#     save_on_Databricks=False,
#     local_folder=f"flight_{operation_type}",
    
#     meta_data_key=meta_data_key,
# 	origin=origin,
#     destination=destination,
#     Date=Date,
	
#     serviceType=serviceTye

#     )