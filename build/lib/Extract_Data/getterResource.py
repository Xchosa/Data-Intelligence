import requests
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import uuid
import time



from get_all_Recources import get_data_all_Reference
from utilis import get_past_date

from get_flight_departures import get_data_flight_depatures

from config import config


from databricks.sdk import WorkspaceClient




def get_References()->None:


    for ref, key, folder in zip(config.mds_reference, config.meta_data_key, config.local_folder):
        get_data_all_Reference(
            base_Url=config.base_url,
            headers=config.headers,
                
            catalog_name=config.catalog_name,
            schema_name=config.schema_name,
            volume_name=config.volume_name,
            mds_reference=ref,
            recordLimit=config.record_limit,
            offset=config.offset,

            save_on_Databricks=True,
            meta_data_key=key,
            local_folder=folder
        )



if __name__ == "__main__":
    try:
        get_References()

       # get_single_flight_route()
    except Exception as e:
        print(f"error {e}")