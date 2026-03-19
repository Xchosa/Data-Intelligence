
import os
from datetime import datetime, timedelta
from utilis import is_databricks_notebook, directory_exist


def write_log(log_file: str, message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} | {message}\n")


def create_logfile_path(catalog_name: str, schema_name: str, volume_name: str, mds_reference: str) -> str:
    
    log_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{mds_reference}/logs"
    if not directory_exist(log_dir):
        dbutils.fs.mkdirs(log_dir)

    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    return f"{log_dir}/{mds_reference}_{timestamp}.log"

