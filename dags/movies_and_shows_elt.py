from datetime import datetime
import requests
import json

from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


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


def fetch_and_save_people_to_sa(target_type: str):
    CLIENT_ID = Variable.get("trakt_client_id")
    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": CLIENT_ID,
    }

    pg_hook = PostgresHook(postgres_conn_id= 'postgres_dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    if target_type == 'movies':
        cursor.execute('SELECT DISTINCT trakt_id FROM ods.trakt_movies_history WHERE trakt_id IS NOT NULL;')
        records = cursor.fetchall()
        print(f'Найдено уникальных фильмов для выгрузки people: {len(records)}')

        for (movie_trakt_id,) in records:
            url = f'https://api.trakt.tv/movies/{movie_trakt_id}/people'
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                people_data = response.json()
                cursor.execute("""
                    INSERT INTO sa.raw_trakt_movies_people (movie_trakt_id, movie_people_json, loaded_at)
                    VALUES (%s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (movie_trakt_id) DO UPDATE 
                    SET movie_people_json = EXCLUDED.movie_people_json, 
                        loaded_at = CURRENT_TIMESTAMP;
                """, (str(movie_trakt_id), json.dumps(people_data)))
                conn.commit()
            else:
                print(f'Ошибка получения people для фильма {movie_trakt_id}: {response.status_code} - {response.text}')

    elif target_type == 'seasons':
        cursor.execute('SELECT DISTINCT trakt_show_id, season_number FROM ods.trakt_episodes_history WHERE trakt_show_id IS NOT NULL AND season_number IS NOT NULL;')
        records = cursor.fetchall()
        print(f'Найдено уникальных сезонов сериалов для выгрузки people: {len(records)}')

        for show_trakt_id, season_num in records:
            url = f'https://api.trakt.tv/shows/{show_trakt_id}/seasons/{season_num}/people'
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                people_data = response.json()
                season_id = f'{show_trakt_id}_s{season_num}'
                cursor.execute("""
                    INSERT INTO sa.raw_trakt_seasons_people (season_id, show_trakt_id, season_number, season_people_json, loaded_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (season_id) DO UPDATE 
                    SET season_people_json = EXCLUDED.season_people_json, 
                        loaded_at = CURRENT_TIMESTAMP;
                """, (season_id, str(show_trakt_id), int(season_num), json.dumps(people_data)))
                conn.commit()
            else:
                print(f'Ошибка получения people для шоу {show_trakt_id} сезон {season_num}: {response.status_code} - {response.text}')

    cursor.close()
    conn.close()
    print(f"Загрузка people для типа '{target_type}' успешно завершена.")


def fetch_and_save_tmdb_movies_to_sa():
    TMDB_API_KEY = Variable.get('tmdb_api_token')
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Забираем и trakt_id, и tmdb_id из ODS
    cursor.execute('SELECT DISTINCT trakt_id, tmdb_id FROM ods.trakt_movies_history WHERE tmdb_id IS NOT NULL AND trakt_id IS NOT NULL;')
    records = cursor.fetchall()
    print(f'Найдено уникальных фильмов в ODS для выгрузки из TMDB: {len(records)}')

    success_count = 0
    error_count = 0

    for (trakt_id, tmdb_id) in records:
        url = f'https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_API_KEY}&language=en-US'
        
        response = requests.get(url)
        
        if response.status_code == 200:
            movie_data = response.json()
            cursor.execute("""
                INSERT INTO sa.raw_tmdb_movies_details (tmdb_id, trakt_id, tmdb_movies_details_json, loaded_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (tmdb_id) DO UPDATE 
                SET trakt_id = EXCLUDED.trakt_id,
                    tmdb_movies_details_json = EXCLUDED.tmdb_movies_details_json, 
                    loaded_at = CURRENT_TIMESTAMP;
            """, (str(tmdb_id), str(trakt_id), json.dumps(movie_data)))
            conn.commit()
            success_count += 1
        else:
            error_count += 1
            print(f'Ошибка получения деталей TMDB для фильма (Trakt ID: {trakt_id}, TMDB ID: {tmdb_id}): {response.status_code} - {response.text}')

    cursor.close()
    conn.close()
    print(f'Загрузка деталей фильмов из TMDB завершена. Успешно: {success_count}, Ошибок: {error_count}')


def fetch_and_save_tmdb_seasons_to_sa():
    TMDB_API_KEY = Variable.get('tmdb_api_token')
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Забираем уникальные связки show_trakt_id, tmdb_show_id и номера сезона из ODS
    cursor.execute("""
        SELECT DISTINCT trakt_show_id, tmdb_show_id, season_number 
        FROM ods.trakt_episodes_history 
        WHERE trakt_show_id IS NOT NULL 
          AND tmdb_show_id IS NOT NULL 
          AND season_number IS NOT NULL;
    """)
    records = cursor.fetchall()
    print(f'Найдено уникальных сезонов сериалов в ODS для выгрузки из TMDB: {len(records)}')

    success_count = 0
    error_count = 0

    for (show_trakt_id, tmdb_show_id, season_number) in records:
        url = f'https://api.themoviedb.org/3/tv/{tmdb_show_id}/season/{season_number}?api_key={TMDB_API_KEY}&language=en-US'
        
        response = requests.get(url)
        
        if response.status_code == 200:
            season_data = response.json()
            # Формируем уникальный season_id (например, через trakt_show_id или tmdb_show_id)
            season_id = f'{show_trakt_id}_s{season_number}'
            
            cursor.execute("""
                INSERT INTO sa.raw_tmdb_seasons_details (
                    season_id, 
                    show_trakt_id, 
                    tmdb_show_id, 
                    season_number, 
                    tmdb_seasons_details_json, 
                    loaded_at
                )
                VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (season_id) DO UPDATE 
                SET show_trakt_id = EXCLUDED.show_trakt_id,
                    tmdb_show_id = EXCLUDED.tmdb_show_id,
                    season_number = EXCLUDED.season_number,
                    tmdb_seasons_details_json = EXCLUDED.tmdb_seasons_details_json, 
                    loaded_at = CURRENT_TIMESTAMP;
            """, (
                str(season_id), 
                str(show_trakt_id), 
                str(tmdb_show_id), 
                int(season_number), 
                json.dumps(season_data)
            ))
            conn.commit()
            success_count += 1
        else:
            error_count += 1
            print(f'Ошибка TMDB для сериала (Trakt ID: {show_trakt_id}, TMDB ID: {tmdb_show_id}), сезон {season_number}: {response.status_code} - {response.text}')

    cursor.close()
    conn.close()
    print(f'Загрузка деталей сезонов из TMDB завершена. Успешно: {success_count}, Ошибок: {error_count}')


def fetch_and_save_tmdb_shows_to_sa():
    TMDB_API_KEY = Variable.get('tmdb_api_token')
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_dwh')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Забираем уникальные ID сериалов из истории эпизодов в ODS
    cursor.execute("""
        SELECT DISTINCT trakt_show_id, tmdb_show_id 
        FROM ods.trakt_episodes_history 
        WHERE trakt_show_id IS NOT NULL 
          AND tmdb_show_id IS NOT NULL;
    """)
    records = cursor.fetchall()
    print(f'Найдено уникальных сериалов в ODS для выгрузки из TMDB: {len(records)}')

    success_count = 0
    error_count = 0

    for (trakt_show_id, tmdb_show_id) in records:
        url = f'https://api.themoviedb.org/3/tv/{tmdb_show_id}?api_key={TMDB_API_KEY}&language=en-US'
        
        response = requests.get(url)
        
        if response.status_code == 200:
            show_data = response.json()
            
            cursor.execute("""
                INSERT INTO sa.raw_tmdb_shows_details (
                    tmdb_show_id, 
                    trakt_id, 
                    tmdb_shows_details_json, 
                    loaded_at
                )
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (tmdb_show_id) DO UPDATE 
                SET trakt_id = EXCLUDED.trakt_id,
                    tmdb_shows_details_json = EXCLUDED.tmdb_shows_details_json, 
                    loaded_at = CURRENT_TIMESTAMP;
            """, (
                str(tmdb_show_id), 
                str(trakt_show_id), 
                json.dumps(show_data)
            ))
            conn.commit()
            success_count += 1
        else:
            error_count += 1
            print(f'Ошибка TMDB для сериала (Trakt ID: {trakt_show_id}, TMDB ID: {tmdb_show_id}): {response.status_code} - {response.text}')

    cursor.close()
    conn.close()
    print(f'Загрузка деталей сериалов из TMDB завершена. Успешно: {success_count}, Ошибок: {error_count}')


with DAG(
    dag_id = 'movies_and_shows_elt',
    start_date = datetime(2026, 8, 29),
    schedule = None,
    catchup = False,
    tags = ['trakt', 'tmdb', 'movies', 'shows'],
) as dag:

    truncate_sa_tables = SQLExecuteQueryOperator (
        task_id = 'truncate_sa_tables',
        conn_id = 'postgres_dwh',
        sql = 'TRUNCATE sa.raw_trakt_movies_history, sa.raw_trakt_movies_ratings, sa.raw_trakt_episodes_history, sa.raw_trakt_episodes_ratings, sa.raw_trakt_movies_people, sa.raw_trakt_seasons_people, sa.raw_tmdb_movies_details, sa.raw_tmdb_seasons_details, sa.raw_tmdb_seasons_details'
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
        sql = 'TRUNCATE ods.trakt_movies_history, ods.trakt_movies_ratings, ods.trakt_episodes_history, ods.trakt_episodes_ratings, ods.trakt_movies_people, ods.trakt_seasons_people, ods.tmdb_movies_detail, ods.tmdb_show_details'
    )

    load_trakt_history_and_ratings_to_ods = SQLExecuteQueryOperator (
        task_id = 'load_trakt_history_and_ratings_to_ods',
        conn_id = 'postgres_dwh',
        sql = 'sql/trakt_history_and_ratings_to_ods.sql'
    )

    load_movies_people_to_sa = PythonOperator(
        task_id='load_movies_people_to_sa',
        python_callable=fetch_and_save_people_to_sa,
        op_kwargs={'target_type': 'movies'},
    )

    load_seasons_people_to_sa = PythonOperator(
        task_id='load_seasons_people_to_sa',
        python_callable=fetch_and_save_people_to_sa,
        op_kwargs={'target_type': 'seasons'},
    )

    load_tmdb_movies_details_to_sa = PythonOperator(
        task_id='load_tmdb_movies_details_to_sa',
        python_callable=fetch_and_save_tmdb_movies_to_sa,
    )

    load_tmdb_seasons_details_to_sa = PythonOperator(
        task_id='load_tmdb_seasons_details_to_sa',
        python_callable=fetch_and_save_tmdb_seasons_to_sa,
    )

    load_tmdb_shows_details_to_sa = PythonOperator(
        task_id='load_tmdb_shows_details_to_sa',
        python_callable=fetch_and_save_tmdb_shows_to_sa,
    )

    load_trakt_people_to_ods = SQLExecuteQueryOperator (
        task_id = 'load_trakt_people_to_ods',
        conn_id = 'postgres_dwh',
        sql = 'sql/trakt_people_to_ods.sql'
    )

    load_tmdb_details_to_ods = SQLExecuteQueryOperator (
            task_id = 'load_tmdb_details_to_ods',
            conn_id = 'postgres_dwh',
            sql = 'sql/tmdb_details_to_ods.sql'
        )

    chain_sequence = truncate_sa_tables >> [load_movies_history, load_movies_ratings, load_episodes_history, load_episodes_ratings] >> truncate_ods_tables >> load_trakt_history_and_ratings_to_ods
    chain_sequence >> load_movies_people_to_sa
    chain_sequence >> load_seasons_people_to_sa
    chain_sequence >> load_tmdb_movies_details_to_sa
    chain_sequence >> load_tmdb_seasons_details_to_sa
    chain_sequence >> load_tmdb_shows_details_to_sa
    [load_movies_people_to_sa, load_seasons_people_to_sa, load_tmdb_movies_details_to_sa, load_tmdb_seasons_details_to_sa, load_tmdb_shows_details_to_sa] >> load_trakt_people_to_ods
    [load_movies_people_to_sa, load_seasons_people_to_sa, load_tmdb_movies_details_to_sa, load_tmdb_seasons_details_to_sa, load_tmdb_shows_details_to_sa] >> load_tmdb_details_to_ods