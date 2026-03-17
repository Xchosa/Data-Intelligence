
import json
import os
from datetime import datetime

from pathlib import Path
from typing import Dict, Any, List, Optional
import uuid
import time
from urllib.parse import urlparse, parse_qs

import logging 



def setup_logger(catalog_name: str, schema_name: str, volume_name: str, mds_reference: str, save_on_Databricks: bool) -> logging.Logger:
    
    if(save_on_Databricks is False):
        local_folder = f"{mds_reference}_logs"
        if local_folder is not None:
            os.makedirs(local_folder, exist_ok=True)
        
        with open(local_folder, "w", encoding="utf-8") as file:
            
        logger = logging.getLogger(mds_reference)
        logger.setLevel(logging.INFO)
    
        if not logger.handlers:
            file_handler = logging.FileHandler(log_file)
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
    
        return logger

    else:
        log_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{mds_reference}/logfile"
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f"{mds_reference}.log")

        logger = logging.getLogger(mds_reference)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            file_handler = logging.FileHandler(log_file)
            formatter = logging.Formatter(
                "%(asctime)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger
