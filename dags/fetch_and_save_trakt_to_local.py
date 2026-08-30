from datetime import datetime
import json
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

import requests


def fetch_and_save_trakt_to_local(endpoint, filename):
    CLIENT_ID = Variable.get("trakt_client_id")
    USERNAME = Variable.get("trakt_username")

    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": CLIENT_ID,
    }

    page = 1
    limit = 100
    all_data = []

    while True:
        separator = "&" if "?" in endpoint else "?"
        paged_url = f"https://api.trakt.tv/users/{USERNAME}/{endpoint}{separator}page={page}&limit={limit}"
        
        response = requests.get(paged_url, headers=headers)
        if response.status_code != 200:
            raise RuntimeError(
                f"Ошибка Trakt API: {response.status_code} - {response.text}"
            )

        page_data = response.json()
        if not page_data:
            break

        all_data.extend(page_data)

        if len(page_data) < limit:
            break

        page += 1

    # Сохраняем в папку data рядом с проектом
    data_dir = Path(__file__).resolve().parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    
    file_path = data_dir / filename
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    print(f"Успешно сохранено записей: {len(all_data)} в файл {file_path}")


with DAG(
    dag_id="trakt_episodes_to_local_json",
    start_date=datetime(2026, 8, 29),
    schedule=None,
    catchup=False,
    tags=["local", "trakt", "debug"],
) as dag:

    save_episodes_history = PythonOperator(
        task_id="save_episodes_history_to_file",
        python_callable=fetch_and_save_trakt_to_local,
        op_kwargs={
            "endpoint": "history/shows?extended=full",
            "filename": "episodes_history_response.json",
        },
    )

    save_episodes_ratings = PythonOperator(
        task_id="save_episodes_ratings_to_file",
        python_callable=fetch_and_save_trakt_to_local,
        op_kwargs={
            "endpoint": "ratings/episodes",
            "filename": "episodes_ratings_response.json",
        },
    )

    [save_episodes_history, save_episodes_ratings]