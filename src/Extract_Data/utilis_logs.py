from pathlib import Path

from datetime import datetime, timedelta
from shutil import copyfile
import uuid

# def write_log(log_file: str, message: str) -> None:
#     Path(log_file).parent.mkdir(parents=True, exist_ok=True)
#     timestamp = datetime.now().isoformat(sep='_', timespec='seconds')
#     with open(log_file, "a", encoding="utf-8") as file:
#         file.write(f"{timestamp} | {message}\n")


# def create_logfile_path(catalog_name: str, schema_name: str, volume_name: str, mds_reference: str) -> str:
    
#     log_dir = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{mds_reference}/logs"
#     timestamp = datetime.now().isoformat(sep='_', timespec='seconds')
#     return f"{log_dir}/{mds_reference}_{timestamp}.log"


# #different approach save in tmplogs, append them in one file later
# def create_local_logfile_path(mds_reference: str) -> str:
#     log_dir = Path("/local_disk0/tmp/api_logs") / mds_reference
#     log_dir.mkdir(parents=True, exist_ok=True)
#     timestamp = datetime.now().isoformat(sep="_", timespec="seconds")
#     return str(log_dir / f"{mds_reference}_{timestamp}.log")


# create for every event lop a tmp_pogs 
# merge allo tmp_logs in eg. countries_final.log in log_bufer

def create_log_run_paths(
    catalog_name: str,
    schema_name: str,
    volume_name: str,
    mds_reference: str  
    ) -> dict:
    log_date = datetime.now().strftime("%Y-%m-%d")
    run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    base_dir = Path(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/{mds_reference}/logs")
    run_dir = base_dir / log_date / run_id
    tmp_dir = run_dir / "tmp_logs"
    final_log_file = run_dir / f"{mds_reference}_final.log"

    tmp_dir.mkdir(parents=True, exist_ok=True)

    return {
        "log_date": log_date,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "tmp_dir": str(tmp_dir),
        "final_log_file": str(final_log_file),
    }


def write_log_event(tmp_dir: str, counter: int, message: str) -> str:
    timestamp = datetime.now().isoformat(sep="_", timespec="seconds")
    file_name = f"{counter:04d}_{timestamp}.log"
    file_path = Path(tmp_dir) / file_name

    with open(file_path, "w", encoding="utf-8") as file:
        file.write(f"{timestamp} | {message}\n")

    return str(file_path)

def add_log_line(log_buffer: list[str], message: str) -> str:
    timestamp = datetime.now().isoformat(sep="_", timespec="seconds")
    line = f"{timestamp} | {message}"
    log_buffer.append(line)
    return line

def write_final_log(final_log_file: str, log_buffer: list[str]) -> None:
    Path(final_log_file).parent.mkdir(parents=True, exist_ok=True)

    with open(final_log_file, "w", encoding="utf-8") as file:
        file.write("\n".join(log_buffer) + "\n")

def delete_tmp_logs(tmp_dir: str) -> None:
    tmp_path = Path(tmp_dir)
    if not tmp_path.exists():
        return

    for child in tmp_path.iterdir():
        if child.is_file():
            child.unlink()

    tmp_path.rmdir()