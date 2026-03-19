from pathlib import Path

from datetime import datetime, timedelta


def write_log(log_file: str, message: str) -> None:
    Path(log_file).parent.mkdir(parent=True, exit_ok=True)
    timestamp = datetime.now().isoformat(sep='_', timespec='seconds')
    with open(log_file, "a", encoding="utf-8") as file:
        file.write(f"{timestamp} | {message}\n")


def create_logfile_path(catalog_name: str, schema_name: str, volume_name: str, mds_reference: str) -> str:
    
    log_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{mds_reference}/logs"
    timestamp = datetime.now().isoformat(sep='_', timespec='seconds')
    return f"{log_dir}/{mds_reference}_{timestamp}.log"

