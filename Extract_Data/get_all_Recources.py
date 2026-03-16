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

         

def get_data_all_Reference(
    base_Url: str,
    headers: Dict[str, str],
    catalog_name: str,
    schema_name: str,
    volume_name: str,

    mds_reference: str,

    recordLimit: int, 
    offset: int,

    save_on_Databricks:bool,
    meta_data_key =str,
    local_folder = str,
    ):
    
    language = "EN"
    dic = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{mds_reference}/"
    FileName_base = f"{dic}/{mds_reference}.json"
    endpoint = f"/v1/mds-references/{mds_reference}?limit={recordLimit}&offset={offset}"
    
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
                    base_filename=f"{mds_reference}.json",
                    local_folder=local_folder,
                    offset=offset
                )
            
            if(save_on_Databricks == True ):
                utilis.save_in_Notebooks(
                    FileName_base,
                    json_data,
                    offset,
                    dic)
                
            #endpoint_backup = endpoint
            offset = utilis.extract_offset_from_endpoint(endpoint)
            endpoint =utilis.get_next_endpoint_from_response(json_data, meta_data_key)
            
            if endpoint == "Done":
                return f"all files successfuly saved"
            
        except Exception as e:
            print(f"{mds_reference}: api called failed \n \
                   last api endpoint {endpoint} {e}", file=sys.stderr)
            return