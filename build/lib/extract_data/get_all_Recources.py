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

from databricks.sdk.runtime import dbutils


from utilis_logs import (
    add_log_line,
    write_log_event,
    write_final_log,
    create_log_run_paths,
    delete_tmp_logs
)
from utilis import (
    
    update_offset,
    timeout_api_restriction,
    versioning_fileNames,
    loop_until_data_pool_finished,
    reset_timeout_rounds,
    save_in_Notebooks,
    get_next_endpoint_from_response,
    find_href,
    extract_offset_from_endpoint,
    jump_offset,
    processing_Error,
    check_for_error_in_json,
)

         

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
    endpoint = f"/v1/mds-references/{mds_reference}?limit={recordLimit}&offset={offset}"

    if mds_reference == "airports":
        endpoint = f"/v1/mds-references/{mds_reference}?LHoperated=1"

    log_paths = create_log_run_paths(catalog_name, schema_name, volume_name, mds_reference)
    tmp_dir = log_paths["tmp_dir"]
    final_log_file = log_paths["final_log_file"]
    log_buffer: list[str] = []
    log_counter = 1

    def log(message: str) -> None:
        nonlocal log_counter
        line = add_log_line(log_buffer, message)
        write_log_event(tmp_dir, log_counter, line)
        log_counter += 1

    timeout_rounds = 0
    proxy_error = False

    while True:
        print(f" called api:{base_Url}{endpoint}")
        log(f"START function call | mds_reference={mds_reference} | initial_endpoint={endpoint}")
        try:
            response = requests.get(
                base_Url + endpoint,
                headers=headers,
                timeout=10
            )
            log(f"Response received | status_code={response.status_code} | url={response.url}")
            if response.status_code in (503, 504, 429):
                timeout_rounds = timeout_api_restriction(response.status_code)
                if timeout_rounds == 5:
                    raise Exception("ERROR | infinite loop protection triggered after 5 timeout rounds")
                continue
            
            timeout_rounds = reset_timeout_rounds(timeout_rounds)
            if response.status_code != 200:
                raise Exception(f"new error code: {response.status_code}")
            
            
            json_data = response.json()
            
            if check_for_error_in_json(json_data, meta_data_key):
                log(f"API returned JSON error payload, Retry one time , did not get {meta_data_key}")
                if proxy_error is True:
                    raise BrokenPipeError (f"ERROR | missing meta key {meta_data_key}")
                time.sleep(10)
                proxy_error = True
                continue
            
            save_in_Notebooks(mds_reference, json_data, offset, dic)

            offset = extract_offset_from_endpoint(endpoint)
            endpoint =get_next_endpoint_from_response(json_data, meta_data_key)
            
            if endpoint == "Done":
                return f"all files successfuly saved"
            
        except Exception as e:
            log(f"{mds_reference}: api called failed \n \
                   last api endpoint {endpoint} {e}")
            write_final_log(final_log_file, log_buffer)
            delete_tmp_logs(tmp_dir)
            return