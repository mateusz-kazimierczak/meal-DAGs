"""
Environment config helper
=========================
All DAGs load their configuration from a single Airflow Variable named
'config.<env>' (e.g. 'config.prod', 'config.dev').

Expected Variable structure (stored as JSON):
{
    "MONGO_DB":       "test",
    "RESEND_API_KEY": "re_...",
    "GCP_AUTH":       { ...service account dict... },
    "GCP_PROJECT":    "ec-meals-462913",
    "BQ_DATASET":     "meal_history",
    "BQ_TABLE":       "HISTORY",
    "SPREADSHEET_ID": "1Qmq...",
    "NODE_BIN":       "/home/mateusz/.nvm/versions/node/v22.19.0/bin/node"
}

MongoDB connections are managed separately under Admin → Connections:
    mongoid_dev  →  dev MongoDB instance
    mongoid_prod →  prod MongoDB instance
"""

import json
from airflow.models import Variable


def get_config(env: str) -> dict:
    """Load environment-specific config from Airflow Variable 'config.<env>'."""
    return json.loads(Variable.get(f"config.{env}"))


def get_mongo_conn_id(env: str) -> str:
    """Return the Airflow connection ID for MongoDB for the given environment."""
    return f"mongoid_{env}"
