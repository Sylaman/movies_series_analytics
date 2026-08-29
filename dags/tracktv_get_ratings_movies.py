from datetime import datetime
import json

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
import requests

CLIENT_ID = Variable.get("track_client_id")
USERNAME = Variable.get("track_username")
SECRET = Variable.get("track_secret")


def fetch_and_load_movies_watch_history():
    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": CLIENT_ID,
    }

    # Запрашиваем историю просмотров с сервиса Track.TV
    url = f"https://api.trakt.tv/users/{USERNAME}/history/movies?extended=full"
    response = requests.get(url, headers=headers)

    print(f"Trakt History API Status: {response.status_code}")

    if response.status_code == 200:
        history_data = response.json()

        if not history_data:
            print("История просмотров пуста.")
            return

        print(f"Получено записей истории просмотров: {len(history_data)}")


        # Подключаемся к dwh и сохраняем в sa каждый элемент полученного json отдельной строкой
        pg_hook = PostgresHook(postgres_conn_id="postgres_dwh")

        rows_to_insert = [(json.dumps(item),) for item in history_data]

        pg_hook.insert_rows(
            table="sa.raw_trakt_watch_history",
            rows=rows_to_insert,
            target_fields=["trakt_history_json"],
            commit_every=500,
        )

        print(
            f"Успешно загружено записей в sa.raw_trakt_watch_history: {len(history_data)}"
        )
    else:
        raise RuntimeError(
            f"Ошибка Trakt API: {response.status_code} - {response.text}"
        )


with DAG(
    dag_id="trakt_movies_history_to_sa",
    start_date=datetime(2026, 8, 29),
    schedule=None,
    catchup=False,
    tags=['sa', 'trakt', 'watch_history'],
) as dag:

    load_history_task = PythonOperator(
        task_id = 'load_movies_history_to_postgres',
        python_callable = fetch_and_load_movies_watch_history,
    )