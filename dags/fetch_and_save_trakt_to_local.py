from datetime import datetime
import json
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.python import PythonOperator

import requests


def fetch_and_save_trakt_to_local(endpoint, filename, is_user_endpoint=True, base_url="https://api.trakt.tv"):
    CLIENT_ID = Variable.get("trakt_client_id")
    USERNAME = Variable.get("trakt_username")

    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": CLIENT_ID,
    }

    # Если эндпоинт пользовательский (начинается с history/ или ratings/), формируем путь через /users/{USERNAME}/
    # Иначе запрашиваем напрямую (например, для фильмов и сериалов)
    if is_user_endpoint:
        url_prefix = f"{base_url}/users/{USERNAME}/"
    else:
        url_prefix = f"{base_url}/"

    page = 1
    limit = 100
    all_data = []

    while True:
        separator = "&" if "?" in endpoint else "?"
        
        # Эндпоинты people обычно не используют пагинацию (отдают всё дерево целиком), 
        # но если пагинация нужна — оставляем логику страниц. Для людей делаем простой запрос.
        if "people" in endpoint:
            paged_url = f"{url_prefix}{endpoint}"
        else:
            paged_url = f"{url_prefix}{endpoint}{separator}page={page}&limit={limit}"
        
        response = requests.get(paged_url, headers=headers)
        if response.status_code != 200:
            raise RuntimeError(
                f"Ошибка Trakt API: {response.status_code} - {response.text}"
            )

        page_data = response.json()
        
        # Если эндпоинт вернул один объект (словарь), а не список, сохраняем его и выходим
        if isinstance(page_data, dict):
            all_data = page_data
            break

        if not page_data:
            break

        all_data.extend(page_data)

        if len(page_data) < limit or "people" in endpoint:
            break

        page += 1

    # Сохраняем в папку data рядом с проектом
    data_dir = Path(__file__).resolve().parent.parent / "data"
    data_dir.mkdir(exist_ok=True)
    
    file_path = data_dir / filename
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    total_count = len(all_data) if isinstance(all_data, list) else 1
    print(f"Успешно сохранено элементов: {total_count} в файл {file_path}")


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
            "is_user_endpoint": True,
        },
    )

    save_episodes_ratings = PythonOperator(
        task_id="save_episodes_ratings_to_file",
        python_callable=fetch_and_save_trakt_to_local,
        op_kwargs={
            "endpoint": "ratings/episodes",
            "filename": "episodes_ratings_response.json",
            "is_user_endpoint": True,
        },
    )

    # Примеры для людей (подставьте свои ID или slug)
    TEST_MOVIE_ID = "1049823"
    TEST_SHOW_ID = "180770"
    TEST_SEASON_NUM = 1

    save_movie_people = PythonOperator(
        task_id="save_movie_people_to_file",
        python_callable=fetch_and_save_trakt_to_local,
        op_kwargs={
            "endpoint": f"movies/{TEST_MOVIE_ID}/people",
            "filename": "movie_people_response.json",
            "is_user_endpoint": False,
        },
    )

    save_season_people = PythonOperator(
        task_id="save_season_people_to_file",
        python_callable=fetch_and_save_trakt_to_local,
        op_kwargs={
            "endpoint": f"shows/{TEST_SHOW_ID}/seasons/{TEST_SEASON_NUM}/people",
            "filename": "season_people_response.json",
            "is_user_endpoint": False,
        },
    )

    [save_episodes_history, save_episodes_ratings, save_movie_people, save_season_people]