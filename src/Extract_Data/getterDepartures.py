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



def get_flights()->None:
    for airport in config.airport_code:
        get_data_flight_depatures(
            base_Url=config.base_url,
            headers=config.headers,
                
            catalog_name=config.catalog_name,
            schema_name=config.schema_name,
            volume_name=config.volume_name,

            operations=config.operations,
            operation_type=config.operation_type,
            operation_subtype= config.operation_subtype,
            
            meta_data_key=config.meta_data_key_flight,
            airport_code=airport,
            Date=get_past_date(from_time_block=0),
            offset=config.offset,
            serviceType=config.serviceType,
           
            )
        
if __name__ == "__main__":
    try:
        get_flights()

       # get_single_flight_route()
    except Exception as e:
        print(f"error {e}")

        