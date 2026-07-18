from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.models import Variable

import pandas as pd
import psycopg2
import requests
import tmdbsimple as tmdb




tmdb.API_KEY = 'YOUR_API_KEY_HERE'

default_args = {
    "owner": "egor",
    "depends_on_past": False,
}

with DAG(
    dag_id="movies_series_analytics",
    start_date=datetime(2026, 7, 18),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["movies", "etl"],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        postgres_conn_id="postgres_dwh",
        sql="sql/create_table.sql",
    )

    create_table