from datetime import datetime
import json
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator
import requests

# 1. Чтение переменных на верхнем уровне
CLIENT_ID = Variable.get('track_client_id')
USERNAME = Variable.get('track_username')
SECRET = Variable.get('track_secret') 	


OUTPUT_FILE_PATH = Path("/opt/airflow/dags/movies_history_response.json")


def fetch_and_save_movies_history():
    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": CLIENT_ID,
    }

    # limit=50 запросит последние 50 записей истории
    url = f"https://api.trakt.tv/users/{USERNAME}/history/movies?extended=full&limit=50"
    response = requests.get(url, headers=headers)

    print(f"Trakt History API Status: {response.status_code}")

    if response.status_code == 200:
        history_data = response.json()
        print(f"Получено записей истории просмотров: {len(history_data)}")

        with open(OUTPUT_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(history_data, f, ensure_ascii=False, indent=2)

        print(f"История успешно сохранена в: {OUTPUT_FILE_PATH}")
    else:
        raise RuntimeError(
            f"Ошибка Trakt API: {response.status_code} - {response.text}"
        )


with DAG(
    dag_id="test_trakt_movies_history",
    start_date=datetime(2026, 8, 29),
    schedule=None,
    catchup=False,
    tags=["test", "trakt", "history"],
) as dag:

    dump_history_task = PythonOperator(
        task_id="save_movies_history_json",
        python_callable=fetch_and_save_movies_history,
    )