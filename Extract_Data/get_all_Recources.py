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
    processing_Error,
    check_for_error_in_json,
    save_api_error,
    #create_logfile_path,
    #write_log
)
from utilis_logs import create_logfile_path, write_log
         

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
    
    
    dic = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{mds_reference}/"
    FileName_base = f"{mds_reference}"
    endpoint = f"/v1/mds-references/{mds_reference}?limit={recordLimit}&offset={offset}"

  
    log_file = create_logfile_path(catalog_name, schema_name, volume_name, mds_reference)
    write_log(log_file, f"START function call | mds_reference={mds_reference} | initial_endpoint={endpoint}")


    
    dummy_count = 0
    timeout_rounds = 0
    proxy_error = False

    while True:
        #print(f"sending request {dummy_count}")
        dummy_count += 1
        print(f"{base_Url}{endpoint}")
        try:
            response = requests.get(
                base_Url + endpoint,
                headers=headers,
                timeout=10
            )
            write_log(log_file, f"Response received | status_code={response.status_code} | url={response.url}")
            if response.status_code in (503, 504, 429):
                timeout_rounds = timeout_api_restriction(response.status_code)
                if timeout_rounds == 5:
                    write_log(log_file, "ERROR | infinite loop protection triggered after 5 timeout rounds")
                    raise Exception("infinite Loop")
                continue
            
            timeout_rounds = reset_timeout_rounds(timeout_rounds)
            if response.status_code != 200:
                write_log(log_file, f"ERROR | new error code: {response.status_code}")
                raise Exception(f"new error code: {response.status_code}")
            
            # print("response.url =", response.url)
            json_data = response.json()
            
            if check_for_error_in_json(json_data, meta_data_key):
                write_log(log_file, "API returned JSON error payload, Retry one time ")
                save_api_error(json_data,
                            meta_data_key,
                            dir)
                if proxy_error is True:
                    write_log(log_file, f"ERROR | missing meta key {meta_data_key}")
                    raise BrokenPipeError
                time.sleep(10)
                proxy_error = True
                continue

            if not save_on_Databricks:
                save_json_locally(
                    json_data=json_data,
                    base_filename=FileName_base,
                    local_folder=local_folder,
                    offset=offset
                )
            
            if save_on_Databricks:
                save_in_Notebooks(
                    mds_reference,
                    json_data,
                    offset,
                    dic)

            offset = extract_offset_from_endpoint(endpoint)
            endpoint =get_next_endpoint_from_response(json_data, meta_data_key)
            
            if endpoint == "Done":
                return f"all files successfuly saved"
            
        except Exception as e:
            print(f"{mds_reference}: api called failed \n \
                   last api endpoint {endpoint} {e}", file=sys.stderr)
            write_log(log_file, f"ERROR | {e}")
            return