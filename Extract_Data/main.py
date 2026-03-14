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

#mds_reference = ["Countries", "Cities", "Airports", "NearestAirport", "Aircrafts"]
mds_reference ="airports"
#recordLimit = 100
#offset =11000

recordLimit = 100
offset =50

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
    
get_airports.get_data_Reference_airport(
    base_Url="https://lh-proxy.onrender.com",
    headers=headers,
    
    catalog_name = catalog_name,
    schema_name = schema_name,
    volume_name = volume_name,
    mds_reference="airports",
    recordLimit= recordLimit,
    offset=offset,

    save_on_Databricks = False,
    meta_data_key = "AirportResource",
    local_folder ="airport",
    )