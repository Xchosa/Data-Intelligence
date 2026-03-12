import requests
import json
import os
from datetime import datetime

from pathlib import Path
from typing import Dict, Any, List, Optional
import uuid
import time

#loop through list mds-reference 
def update_offset(recordLimit: int, offset: int, TotalCount: int)-> str: 
        if recordLimit < TotalCount:
            offset = offset + recordLimit
            return offset
        return offset 
    
def timeout_api_restriction(responseCode: int )->None:
        if responseCode == 503 or responseCode == 504 or responseCode == 429:
            print(f"api restriction: {responseCode}")
            time.sleep(4)
        
def versioning_fileNames(filename: str, offset:int ) -> str:
        """ differ by milliseconds , offset and unique key e.g. airports_2026-03-12-15-34-21-482_off200_a1f9c3.json"""
        unique_key = uuid.uuid4().hex[:6]
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f")[:-3]
        versioned_filename = f"{filename}_{timestamp}_{offset}_{unique_key}"
        return versioned_filename
    
def loop_until_data_pool_finished(Totaldata: int, recordLimit: int)->bool:
        if Totaldata > recordLimit:
            return True
        else:
            False


def save_json_locally(
    json_data: Any,
    base_filename: str,
    offset: int,
    local_folder: Optional[str] = None,
    ) -> None:
    """
    save json localy 
    """
    versioned_filename = versioning_fileNames(base_filename, offset)

    if local_folder is not None:
        os.makedirs(local_folder, exist_ok=True)
        file_path = os.path.join(local_folder, versioned_filename)
    else:
        file_path = versioned_filename

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(json_data, file, indent=2, ensure_ascii=False)

    print(file_path)
    print(f"saved locally: {file_path}")

def save_in_Notebooks(FileName_base,json_data)-> None:
    versioned_filename = versioning_fileNames(FileName_base)
    with open (versioned_filename, "w") as file:
        json.dump(json_data, file, indent=2)
    print(f"{versioned_filename} saved")


def get_next_endpoint_from_response(json_data: dict, proxy_base_url: str) -> Optional[str]:
    """
    Liest aus der API-Antwort den Link mit @Rel == 'next' aus
    und wandelt ihn in einen Endpoint für den Proxy um.

    Beispiel Rückgabe:
    /v1/mds-references/airports?limit=100&offset=100

    Falls kein next-Link vorhanden ist, wird None zurückgegeben.
    """
    airport_resource = json_data.get("AirportResource", {})
    meta = airport_resource.get("Meta", {})
    links = meta.get("Link", [])

    if isinstance(links, dict):
        links = [links]

    for link in links:
        if link.get("@Rel") == "next":
            next_href = link.get("@Href")

            if not next_href:
                return None

            # Falls die Lufthansa-API die volle URL liefert,
            # wird nur der Pfad + Query für den Proxy verwendet.
            if next_href.startswith("https://api.lufthansa.com"):
                return next_href.replace("https://api.lufthansa.com", "")

            # Falls die Antwort bereits nur einen relativen Pfad enthält
            if next_href.startswith("/"):
                return next_href

            # Falls der Proxy selbst als Basis enthalten ist
            if next_href.startswith(proxy_base_url):
                return next_href.replace(proxy_base_url, "")

    return None


def get_data_Reference_airport(
    base_Url: str,
    headers: Dict[str, str],
    catalog_name: str,
    schema_name: str,
    volume_name: str,

    mds_reference: str,

    recordLimit: int, 
    offset: int,


    ):
    """ Get Lufthansa operated Airports only """

    dic = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{mds_reference}/"
    FileName_base = f"{dic}/{mds_reference}.json"
    endpoint = f"/v1/mds-references/{mds_reference}?limit={recordLimit}&offset={offset}"
    
    dummy_count = 0
      
    while True:
        # get_next_endpoint_from_response(json_data: dict, proxy_base_url: str)

        # meta = json_data["AirportResource"]["Meta"]
        # links = meta["Link"]

        # next_link = None

        # for link in links:
        #     if link["@Rel"] == "next":
        #         next_link = link["@Href"]
        #         break
        
        endpoint = f"/v1/mds-references/{mds_reference}?limit={recordLimit}&offset={offset}"

        print(f"sending request {dummy_count}")
        print(f"{base_Url}{endpoint}")

        response = requests.get(
            base_Url + endpoint,
            headers=headers
        )
        print("response.url =", response.url)

        if response.status_code in (503, 504, 429):
            timeout_api_restriction(response.status_code)
            continue

        if response.status_code != 200:
            print(f"new error code: {response.status_code}")
            print(response.text)
            break

        json_data = response.json()

        total_count = json_data.get("AirportResource", {}).get("Meta", {}).get("TotalCount")
        airport_list = json_data.get("AirportResource", {}).get("Airports", {}).get("Airport", [])


        meta = json_data["AirportResource"]["Meta"]
        links = meta["Link"]

        next_link = None

        for link in links:
            if link["@Rel"] == "next":
                next_link = link["@Href"]
                break

        print(f"records in current page: {len(airport_list)}")
        print(f"total data: {total_count}")
        print(f"current offset: {offset}")

        save_json_locally(
            json_data=json_data,
            base_filename=f"{mds_reference}.json",
            local_folder="raw_data",
            offset=offset
        )
        

        #For saving in Notebooks Databricks
        #save_in_Notebooks(FileName_base,json_data)
        dummy_count += 1

        if total_count is None:
            print("TotalCount not found. Stop pagination.")
            break

        offset += recordLimit

        if offset >= total_count:
            print("all data collected")
            break
    


# password = dbutils.secrets.get(scope="lh-api", key="password")
base_url = "https://lh-proxy.onrender.com"
headers ={"password": "DataIntelligence2026"}
    
    
catalog_name = "data_catalog"
schema_name = "bronze"
volume_name = "bronze_volume"
    
    
    # Option 1: Fetch all reference data

#reference_type = ["mds-reference", "Offers"]
#Offers = [ "SeatMaps", "Lounges"]

#SeatMaps = ["flightNumber", "origin", "destination", "departureDate", "cabinTypeCode"]
#lounges = ["code", "cabinClassCode", "tierCode", "languageCode"]

#mds_reference = ["Countries", "Cities", "Airports", "NearestAirport", "Aircrafts"]
mds_reference ="airports"
recordLimit = 100
offset =0


print("hallo")
get_data_Reference_airport(
    base_Url="https://lh-proxy.onrender.com",
    headers=headers,
    
    catalog_name = catalog_name,
    schema_name = schema_name,
    volume_name = volume_name,

    mds_reference= mds_reference,
    
    recordLimit= recordLimit,
    offset=offset

    )
    