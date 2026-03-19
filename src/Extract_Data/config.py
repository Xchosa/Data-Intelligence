# all hard coded values try:
import os
from databricks.sdk import WorkspaceClient

def get_lufthansa_secret() -> str:
    try:
        w = WorkspaceClient()
        return w.dbutils.secrets.get(scope="lufthansa-api", key="LUFTHANSA_SECRET")
    except Exception:
        pass