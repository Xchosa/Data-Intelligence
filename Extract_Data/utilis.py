import requests
import json
import os
from datetime import datetime

from pathlib import Path
from typing import Dict, Any, List, Optional
import uuid
import time
from urllib.parse import urlparse, parse_qs
import sys

def update_offset(recordLimit: int, offset: int, TotalCount: int)-> str: 
        if recordLimit < TotalCount:
            offset = offset + recordLimit
            return offset
        return offset 
    
def timeout_api_restriction(responseCode: int )->bool:
        if responseCode == 503 or responseCode == 504 or responseCode == 429:
            print(f"api restriction: {responseCode}")
            time.sleep(4)
            return True
        return False
        
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

def reset_timeout_rounds(timeout_rounds:int)->int:
    timeout_rounds = 0
    return timeout_rounds

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

def save_in_Notebooks(FileName_base,json_data, offset)-> None:
    versioned_filename = versioning_fileNames(FileName_base, offset)
    with open (versioned_filename, "w") as file:
        json.dump(json_data, file, indent=2)
    print(f"{versioned_filename} saved")


def get_next_endpoint_from_response(json_data: dict, meta_data_key: str) -> Optional[str]:
    """
    read api respond json file for link @Rel == 'next' and
    changes it to a Endpoint for proxy 
    """
    counter = 0
    airport_resource = json_data.get(meta_data_key, {})
    meta = airport_resource.get("Meta", {})
    links = meta.get("Link", [])

    # if Link is a object not an array 
    if isinstance(links, dict):
        links = [links]

    for link in links:
        counter +=1
        if link.get("@Rel") == "next":
            next_href = link.get("@Href")
            if next_href.startswith("https://api.lufthansa.com"):
                return next_href.replace("https://api.lufthansa.com", "")
            
        
        elif link.get("@Rel") == "last":
            last_href = link.get("@Href")
            print(f"found only last href {last_href}", file=sys.stderr)
            return None
    if counter == 4:
        return "Done"
        #if next and last is missing only 4 links are available
            #"@Href": "https://api.lufthansa.com/v1/mds-references/airports?limit=100&offset=1500",
            #"@Rel": "next"
            

    return None





def find_href(json_data: dict , meta_data_key:str) -> Optional[str]:
    
    airport_resource = json_data.get(meta_data_key, {})
    meta = airport_resource.get("Meta", {})
    links = meta.get("Link", [])

    if isinstance(links, dict):
         links = [links]

    for link in links:
        if link.get("@Rel") == "self":
              working_href = link.get("@Href")
        
        working_href.find("offset")
        if not working_href:
                print("there is no next_href")
                return None
        
def extract_offset_from_endpoint(endpoint: str) -> Optional[int]:
    parsed = urlparse(endpoint)
    qs = parse_qs(parsed.query)
    value = qs.get("offset")
    if value:
        return int(value[0])
    return None

def jump_offset(endpoint: str, skipped_values: int) -> Optional[str]:
    parsed = urlparse(endpoint)
    qs = parse_qs(parsed.query)

    limit_values = qs.get("limit")
    offset_values = qs.get("offset")

    if not limit_values or not offset_values:
        return None

    limit_value = int(limit_values[0])
    offset_value = int(offset_values[0])

    new_offset = offset_value + skipped_values

    return f"{parsed.path}?limit={limit_value}&offset={new_offset}"

def processing_Error(json_data: dict, meta_data_key: str) ->bool:
    
    if json_data.get(meta_data_key, {}) is None:
        return True
    if json_data.get("ProcessingErrors", {}):
        return True
    return False
