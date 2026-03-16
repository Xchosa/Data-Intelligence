
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

from utilis import (
    is_databricks_notebook,
    directory_exist,
    update_offset,
    timeout_api_restriction,
    versioning_fileNames,
    loop_until_data_pool_finished,
    reset_timeout_rounds,
    save_json_locally,
    save_in_Notebooks,
    get_next_endpoint_from_response,
    find_href,
    extract_offset_from_endpoint,
    jump_offset,
    processing_Error
)

#goal: get delays of lufthansa depatures  , to see parters between Airlines 
#GET /operations/flightstatus/departures/{airportCode}/{fromDateTime}?serviceType={serviceType}

# compare FRA with MUC in delays (differ later, passanger,and so on)

def get_data_flight_depatures(
    base_Url: str,
    headers: Dict[str, str],
    catalog_name: str,
    schema_name: str,
    volume_name: str,

    operations: str,
    operation_type: str,
    operation_subtype: str,

    recordLimit: int, 
    offset: int,

    save_on_Databricks: bool,
    local_folder: str,
    
	meta_data_key:str,
	airport_code:str,
    Date=str,
	
    serviceType=str

    ):



    """ Get FRA departures only """

    dic = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{operations}/{operation_type}/{operation_subtype}/{airport_code}/{Date}/"
    FileName_base = f"{operation_type}"
    endpoint = f"/v1/{operations}/{operation_type}/{operation_subtype}/{airport_code}/{Date}?serviceType={serviceType}"
    dummy_count = 0
    
    timeout_rounds = 0
    proxy_error = False
    Server_error = 0

    while True:
            print(f"sending request {dummy_count}")
            dummy_count += 1
            print(f"{base_Url}{endpoint}")
            try:
                response = requests.get(
                    base_Url + endpoint,
                    headers=headers,
                    timeout=10
                )

                if response.status_code in (503, 504, 429):
                    timeout_rounds = timeout_api_restriction(response.status_code)
                    if timeout_rounds == 5:
                        raise Exception("infinite Loop")
                    continue
                
                timeout_rounds = reset_timeout_rounds(timeout_rounds)
                if response.status_code != 200:
                    raise Exception(f"new error code: {response.status_code}")
                
                print("response.url =", response.url)
                json_data = response.json()
                
                if processing_Error(json_data, meta_data_key):
                    if proxy_error is True:
                        raise BrokenPipeError
                    time.sleep(10)
                    proxy_error = True
                    continue



                if(save_on_Databricks == False):
                    save_json_locally(
                        json_data=json_data,
                        base_filename=f"{operation_type}.json",
                        local_folder=local_folder,
                        offset=offset
                    )
                
                if(save_on_Databricks == True ):
                    save_in_Notebooks(
                        operation_type,
                        json_data,
                        offset,
                        dic)
                    
                #endpoint_backup = endpoint
                offset = extract_offset_from_endpoint(endpoint)
                endpoint =get_next_endpoint_from_response(json_data, meta_data_key)
                
                if endpoint == "Done":
                    return f"all files successfuly saved"
                
            except Exception as e:
                print(f"{operation_type}: api called failed \n \
                    last api endpoint {endpoint} {e}", file=sys.stderr)
                return