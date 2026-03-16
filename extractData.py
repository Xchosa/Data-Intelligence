import requests
import json
import os
import datetime
#import UTC



    
def get_data(base_Url, 
             headers, 
             endpoint, 
             Catalog_name, 
             schema_name, 
             volume_name,
             Date, 
             Departure_Airport, 
             Destination_Airport):
    
    dic = f"/Volumes/{Catalog_name}/{schema_name}/{volume_name}/{Date}/{Departure_Airport}/{Destination_Airport}"
    FileName = f"{dic}/{Destination_Airport}{Date}.json"
   # print(Path)
    if not os.path.exists(dic):
        os.makedirs(dic)
    
    try:
        response = requests.get(base_Url + endpoint, headers=headers)
        if response.status_code == 200:
            json_data = response.json()
            # Flatten the AirportResource field if present
            
            with open (FileName, "w") as file:
                json.dump(json_data, file, indent=2)
            return json_data
        else:
            print(f"Request failed with status code {response.status_code}")
    except Exception as e:
        print(f"An error occurred: {e}")
        



result = get_data (
    base_Url="https://lh-proxy.onrender.com",
    headers=os.getenv("headers"),
    from_datetime="2026-03-06T10:00",
    service_type="passenger",
    Catalog_name="airportanalyze",
    schema_name="airporttoairport",
    volume_name="singleairport",
    Date="2026-03-06T10:00",
    Departure_Airport="FRA",
    Destination_Airport="LIS",
    endpoint=(f"/v1/operations/flightstatus/departures/{Departure_Airport}/{from_datetime}?serviceType={service_type}")
)
