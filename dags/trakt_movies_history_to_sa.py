from datetime import datetime
import json

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import requests
import tmdbsimple as tmdb



def fetch_and_load_movies_watch_history(endpoint, target_table, target_field):

    CLIENT_ID = Variable.get("track_client_id")
    USERNAME = Variable.get("track_username")
    SECRET = Variable.get("track_secret")

    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": CLIENT_ID,
    }

    # Запрашиваем историю просмотров с сервиса Track.TV
    url = f"https://api.trakt.tv/users/{USERNAME}/{endpoint}"
    response = requests.get(url, headers=headers)

    print(f"Trakt History API Status: {response.status_code}")

    if response.status_code == 200:
        history_data = response.json()

        if not history_data:
            print('История просмотров пуста.')
            return

        print(f'Получено записей истории просмотров: {len(history_data)}')


        # Подключаемся к dwh и сохраняем в sa каждый элемент полученного json отдельной строкой
        pg_hook = PostgresHook(postgres_conn_id = 'postgres_dwh')

        rows_to_insert = [(json.dumps(item),) for item in history_data]

        pg_hook.insert_rows(
            table = target_table,
            rows = rows_to_insert,
            target_fields = [target_field],
            commit_every = 500,
        )

        print(
            f'Успешно загружено записей в {target_table}: {len(history_data)}'
        )
    else:
        raise RuntimeError(
            f'Ошибка Trakt API: {response.status_code} - {response.text}'
        )


with DAG(
    dag_id = 'trakt_movies_history_to_sa',
    start_date = datetime(2026, 8, 29),
    schedule = None,
    catchup = False,
    tags = ['sa', 'trakt', 'watch_history'],
) as dag:

    truncate_sa_tables = SQLExecuteQueryOperator (
        task_id = 'truncate_sa_tables',
        conn_id = 'postgres_dwh',
        sql = 'TRUNCATE sa.raw_trakt_movies_history, sa.raw_trakt_movies_ratings, sa.raw_trakt_episodes_history, sa.raw_trakt_episodes_ratings'
    )

    load_movies_watch_history = PythonOperator(
        task_id = 'load_movies_watch_history_to_SA',
        python_callable = fetch_and_load_movies_watch_history,
        op_kwargs = {
            'endpoint': 'history/movies?extended=full',
            'target_table': 'sa.raw_trakt_watch_history',
            'target_field': 'trakt_history_json',
        },
    )

    load_movies_ratings = PythonOperator(
        task_id = 'load_movies_rating_to_SA',
        python_callable = fetch_and_load_movies_watch_history,
        op_kwargs = {
            'endpoint': 'ratings/movies',
            'target_table': 'sa.raw_trakt_ratings',
            'target_field': 'trakt_ratings_json',
        },
    )


    truncate_sa_tables >> load_movies_watch_history >> load_movies_ratings



# url = f"https://api.trakt.tv/users/{USERNAME}/history/shows?extended=full"
# url = f"https://api.trakt.tv/users/{USERNAME}/ratings/episodes"