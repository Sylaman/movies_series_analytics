from datetime import datetime
import json

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

import requests
import tmdbsimple as tmdb


def fetch_and_load_data_from_trakt(endpoint, target_table, target_field):
    CLIENT_ID = Variable.get('trakt_client_id')
    USERNAME = Variable.get('trakt_username')
    SECRET = Variable.get('trakt_secret')

    headers = {
        'Content-Type': 'application/json',
        'trakt-api-version': '2',
        'trakt-api-key': CLIENT_ID,
    }

    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    
    page = 1
    limit = 100  # Максимальный размер страницы, который поддерживает Trakt
    total_loaded = 0

    while True:
        # Корректно формируем URL с учетом того, есть ли уже параметры
        separator = '&' if '?' in endpoint else '?'
        paged_url = f'https://api.trakt.tv/users/{USERNAME}/{endpoint}{separator}page={page}&limit={limit}'
        
        response = requests.get(paged_url, headers=headers)
        print(f'Trakt API Status (Page {page}): {response.status_code}')

        if response.status_code != 200:
            raise RuntimeError(
                f'Ошибка Trakt API: {response.status_code} - {response.text}'
            )

        page_data = response.json()

        # Если страница пустая — значит, выгрузили всё
        if not page_data:
            break

        rows_to_insert = [(json.dumps(item),) for item in page_data]

        pg_hook.insert_rows(
            table=target_table,
            rows=rows_to_insert,
            target_fields=[target_field],
            commit_every=500,
        )

        total_loaded += len(page_data)
        print(f'Загружено записей со страницы {page}: {len(page_data)}')

        # Если пришло меньше элементов, чем лимит, это последняя страница
        if len(page_data) < limit:
            break

        page += 1

    print(f'Успешно загружено всего записей в {target_table}: {total_loaded}')


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

    load_movies_history = PythonOperator(
        task_id = 'load_movies_history_to_SA',
        python_callable = fetch_and_load_data_from_trakt,
        op_kwargs = {
            'endpoint': 'history/movies?extended=full',
            'target_table': 'sa.raw_trakt_movies_history',
            'target_field': 'trakt_movies_history_json',
        },
    )

    load_movies_ratings = PythonOperator(
        task_id = 'load_movies_rating_to_SA',
        python_callable = fetch_and_load_data_from_trakt,
        op_kwargs = {
            'endpoint': 'ratings/movies',
            'target_table': 'sa.raw_trakt_movies_ratings',
            'target_field': 'trakt_movies_ratings_json',
        },
    )

    load_episodes_history = PythonOperator(
        task_id = 'load_episodes_history_to_SA',
        python_callable = fetch_and_load_data_from_trakt,
        op_kwargs = {
            'endpoint': 'history/shows?extended=full',
            'target_table': 'sa.raw_trakt_episodes_history',
            'target_field': 'trakt_episodes_history_json',
        },
    )

    load_episodes_ratings = PythonOperator(
        task_id = 'load_episodes_ratings_to_SA',
        python_callable = fetch_and_load_data_from_trakt,
        op_kwargs = {
            'endpoint': 'ratings/episodes',
            'target_table': 'sa.raw_trakt_episodes_ratings',
            'target_field': 'trakt_episodes_ratings_json',
        },
    )

    truncate_ods_tables = SQLExecuteQueryOperator (
        task_id = 'truncate_ods_tables',
        conn_id = 'postgres_dwh',
        sql = 'TRUNCATE ods.trakt_movies_history'
    )

    load_trakt_data_from_sa_to_ods = SQLExecuteQueryOperator (
        task_id = 'load_trakt_data_from_sa_to_ods',
        conn_id = 'postgres_dwh',
        sql = 'sql/dml_from_sa_to_ods.sql'
    )


    truncate_sa_tables >> [load_movies_history, load_movies_ratings, load_episodes_history, load_episodes_ratings] >> truncate_ods_tables >> load_trakt_data_from_sa_to_ods
