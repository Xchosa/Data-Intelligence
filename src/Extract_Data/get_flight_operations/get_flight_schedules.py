
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

import utilis




def get_data_flight_Schedules(
    base_Url: str,
    headers: Dict[str, str],
    catalog_name: str,
    schema_name: str,
    volume_name: str,

    operations: str,
    operation_type: str,

    recordLimit: int, 
    offset: int,

    save_on_Databricks: bool,
    local_folder: str,
    
	meta_data_key: str,
	origin:str,
    destination: str,
    Date:str,

    ):
    

    """ Get Lufthansa operated Contries only """

    dic = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{operations}{operation_type}{Date}/"
    FileName_base = f"{dic}/{origin}_{destination}.json"
    endpoint = f"/v1/{operations}/{operation_type}/{origin}/{destination}/{Date}?directFlights=true"
    
    dummy_count = 0
    
    timeout_rounds = 0
    proxy_error = False

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
                timeout_rounds = utilis.timeout_api_restriction(response.status_code)
                if timeout_rounds == 5:
                    raise Exception("infinite Loop")
                continue
            
            timeout_rounds = utilis.reset_timeout_rounds(timeout_rounds)
            if response.status_code != 200:
                raise Exception(f"new error code: {response.status_code}")
            
            print("response.url =", response.url)
            json_data = response.json()
            
            if utilis.processing_Error(json_data, meta_data_key):
                if proxy_error is True:
                    raise BrokenPipeError
                time.sleep(10)
                proxy_error = True
                continue



            if(save_on_Databricks == False):
                utilis.save_json_locally(
                    json_data=json_data,
                    base_filename=f"{operation_type}.json",
                    local_folder=local_folder,
                    offset=offset
                )
            
            if(save_on_Databricks == True ):
                utilis.save_in_Notebooks(
                    FileName_base,
                    json_data,
                    offset)
                
            #endpoint_backup = endpoint
            offset = utilis.extract_offset_from_endpoint(endpoint)
            endpoint =utilis.get_next_endpoint_from_response(json_data, meta_data_key)
            
            if endpoint == "Done":
                return f"all files successfuly saved"
            #if endpoint is None:
            #    time.sleep(5)
            #    endpoint = utilis.jump_offset(endpoint_backup, skipped_values=200)

        except Exception as e:
            print(f"{operation_type}: api called failed \n \
                   last api endpoint {endpoint} {e}", file=sys.stderr)
            return