# all hard coded values try:
import os
from databricks.sdk import WorkspaceClient
from dataclasses import dataclass

@dataclass(frozen=True)
class Config_obj:
    catalog_name ="data_catalog"
    schema_name = "bronze"
    volume_name = "bronze_volume"
    base_url: str = "https://lh-proxy.onrender.com"
    record_limit: int = 100
    offset = 0



def get_lufthansa_secret() -> str:
    try:
        w = WorkspaceClient()
        return w.dbutils.secrets.get(scope="lufthansa-api", key="LUFTHANSA_SECRET")
    except Exception:
        print("No secret found")
        pass

