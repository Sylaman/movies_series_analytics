INSERT INTO ods.trakt_movies_history (
	watch_id
	, title
	, release_date
	, watched_at
	, runtime
	, country
	, genres
	, subgenres
	, certification
	, media_type
	, trakt_id
	, imdb_id
	, tmdb_id
	, poster
	, updated_at
)
SELECT DISTINCT ON ((trakt_movies_history_json->>'id')::varchar) 
	(trakt_movies_history_json->>'id')::varchar AS watch_id
	, trakt_movies_history_json->'movie'->>'title' AS title
	, (trakt_movies_history_json->'movie'->>'released')::date AS release_date
	, (trakt_movies_history_json->>'watched_at')::timestamp AS watched_at
	, (trakt_movies_history_json->'movie'->>'runtime')::int AS runtime
	, trakt_movies_history_json->'movie'->>'country' AS country
	, ARRAY(SELECT jsonb_array_elements_text(trakt_movies_history_json->'movie'->'genres')) AS genres
	, ARRAY(SELECT jsonb_array_elements_text(trakt_movies_history_json->'movie'->'subgenres')) AS subgenres
	, COALESCE(trakt_movies_history_json->'movie'->>'certification', 'NR') AS certification
    , trakt_movies_history_json->>'type' AS media_type
	, (trakt_movies_history_json->'movie'->'ids'->>'trakt')::varchar AS trakt_id
	, (trakt_movies_history_json->'movie'->'ids'->>'imdb')::varchar AS imdb_id
    , (trakt_movies_history_json->'movie'->'ids'->>'tmdb')::varchar AS tmdb_id
	, (trakt_movies_history_json->'movie'->'images'->'poster'->> 0)::varchar AS poster
	, (trakt_movies_history_json->'movie'->>'updated_at')::timestamp AS updated_at
FROM sa.raw_trakt_movies_history;



INSERT INTO ods.trakt_movies_ratings (
	trakt_id
	, rating
	, rated_date
)
SELECT 
	(trakt_movies_ratings_json->'movie'->'ids'->>'trakt')::varchar AS trakt_id
	, (trakt_movies_ratings_json->>'rating')::numeric(3,1) AS rating
	, (trakt_movies_ratings_json->>'rated_at')::date AS rated_date
FROM sa.raw_trakt_movies_ratings;



INSERT INTO ods.trakt_episodes_history (
	watch_id
	, episode_title
	, show_title
	, release_date
	, watched_at
	, media_type
	, season_number
	, episode_number
	, runtime
	, show_total_runtime
	, country
	, genres
	, subgenres
	, trakt_episode_id
	, imdb_episode_id
	, tmdb_episode_id
	, season_id
	, trakt_show_id
	, imdb_show_id
	, tmdb_show_id
	, show_status
	, poster
	, episode_updated_at
	, show_updated_at
	, certification
)
SELECT DISTINCT ON ((trakt_episodes_history_json->>'id')::varchar) 
	(trakt_episodes_history_json->>'id')::varchar AS watch_id
	, trakt_episodes_history_json->'episode'->>'title' AS episode_title
	, trakt_episodes_history_json->'show'->>'title' AS show_title
	, COALESCE((trakt_episodes_history_json->'episode'->>'released')::date, (trakt_episodes_history_json->'show'->>'first_aired')::date) AS release_date
	, (trakt_episodes_history_json->>'watched_at')::timestamp AS watched_at
	, trakt_episodes_history_json->>'type' AS media_type
	, (trakt_episodes_history_json->'episode'->>'season')::int AS season_number
    , (trakt_episodes_history_json->'episode'->>'number')::int AS episode_number
    , (trakt_episodes_history_json->'episode'->>'runtime')::int AS runtime
    , (trakt_episodes_history_json->'show'->>'total_runtime')::int AS show_total_runtime
    , trakt_episodes_history_json->'show'->>'country' AS country
    , ARRAY(SELECT jsonb_array_elements_text(trakt_episodes_history_json->'show'->'genres')) AS genres
	, ARRAY(SELECT jsonb_array_elements_text(trakt_episodes_history_json->'show'->'subgenres')) AS subgenres
	, (trakt_episodes_history_json->'episode'->'ids'->>'trakt')::varchar AS trakt_episode_id
	, (trakt_episodes_history_json->'episode'->'ids'->>'imdb')::varchar AS imdb_episode_id
    , (trakt_episodes_history_json->'episode'->'ids'->>'tmdb')::varchar AS tmdb_episode_id
	, (trakt_episodes_history_json->'show'->'ids'->>'trakt') || '_s' || (trakt_episodes_history_json->'episode'->>'season') AS season_id
    , (trakt_episodes_history_json->'show'->'ids'->>'trakt')::varchar AS trakt_show_id
	, (trakt_episodes_history_json->'show'->'ids'->>'imdb')::varchar AS imdb_show_id
    , (trakt_episodes_history_json->'show'->'ids'->>'tmdb')::varchar AS tmdb_show_id
    , (trakt_episodes_history_json->'show'->>'status')::varchar AS show_status
    , (trakt_episodes_history_json->'show'->'images'->'poster'->> 0)::varchar AS poster
    , (trakt_episodes_history_json->'episode'->>'updated_at')::timestamp AS episode_updated_at
	, (trakt_episodes_history_json->'show'->>'updated_at')::timestamp AS show_updated_at
	, trakt_episodes_history_json->'show'->>'certification' AS certification
FROM sa.raw_trakt_episodes_history;


INSERT INTO ods.trakt_episodes_ratings (
	trakt_episode_id
	, trakt_show_id
	, rating
	, rated_date
)
SELECT 
	(trakt_episodes_ratings_json->'episode'->'ids'->>'trakt')::varchar AS trakt_id
	, (trakt_episodes_ratings_json->'show'->'ids'->>'trakt')::varchar AS trakt_id
	, (trakt_episodes_ratings_json->>'rating')::numeric(3,1) AS rating
	, (trakt_episodes_ratings_json->>'rated_at')::date AS rated_date
FROM sa.raw_trakt_episodes_ratings;


INSERT INTO ods.trakt_movies_people (
    movie_trakt_id,
    person_trakt_id,
    person_name,
    role,
    gender,
    birthday,
    birthplace,
    person_updated_at
)
-- 1. Топ-5 главных актеров
SELECT 
    s.movie_trakt_id,
    (actor_elem->'person'->'ids'->>'trakt')::varchar AS person_trakt_id,
    (actor_elem->'person'->>'name')::varchar AS person_name,
    'actor'::varchar AS role,
    (actor_elem->'person'->>'gender')::varchar AS gender,
    NULLIF(actor_elem->'person'->>'birthday', '')::date AS birthday,
    (actor_elem->'person'->>'birthplace')::text AS birthplace,
    (actor_elem->'person'->>'updated_at')::timestamp AS person_updated_at
FROM sa.raw_trakt_movies_people s,
LATERAL (
    SELECT value AS actor_elem
    FROM jsonb_array_elements(COALESCE(s.movie_people_json->'cast', '[]'::jsonb)) WITH ORDINALITY AS arr(value, idx)
    WHERE idx <= 5
) c

UNION ALL

-- 2. Режиссеры (все с job = 'Director')
SELECT 
    s.movie_trakt_id,
    (crew_elem->'person'->'ids'->>'trakt')::varchar AS person_trakt_id,
    (crew_elem->'person'->>'name')::varchar AS person_name,
    'director'::varchar AS role,
    (crew_elem->'person'->>'gender')::varchar AS gender,
    NULLIF(crew_elem->'person'->>'birthday', '')::date AS birthday,
    (crew_elem->'person'->>'birthplace')::text AS birthplace,
    (crew_elem->'person'->>'updated_at')::timestamp AS person_updated_at
FROM sa.raw_trakt_movies_people s,
LATERAL (
    SELECT value AS crew_elem
    FROM jsonb_array_elements(COALESCE(s.movie_people_json->'crew'->'directing', '[]'::jsonb))
    WHERE value->>'job' = 'Director'
) d

UNION ALL

-- 3. Сценаристы (все с job = 'Writer')
SELECT 
    s.movie_trakt_id,
    (crew_elem->'person'->'ids'->>'trakt')::varchar AS person_trakt_id,
    (crew_elem->'person'->>'name')::varchar AS person_name,
    'writer'::varchar AS role,
    (crew_elem->'person'->>'gender')::varchar AS gender,
    NULLIF(crew_elem->'person'->>'birthday', '')::date AS birthday,
    (crew_elem->'person'->>'birthplace')::text AS birthplace,
    (crew_elem->'person'->>'updated_at')::timestamp AS person_updated_at
FROM sa.raw_trakt_movies_people s,
LATERAL (
    SELECT value AS crew_elem
    FROM jsonb_array_elements(COALESCE(s.movie_people_json->'crew'->'writing', '[]'::jsonb))
    WHERE value->>'job' = 'Writer'
) w

UNION ALL

-- 4. Композиторы (все с job = 'Original Music Composer')
SELECT 
    s.movie_trakt_id,
    (crew_elem->'person'->'ids'->>'trakt')::varchar AS person_trakt_id,
    (crew_elem->'person'->>'name')::varchar AS person_name,
    'composer'::varchar AS role,
    (crew_elem->'person'->>'gender')::varchar AS gender,
    NULLIF(crew_elem->'person'->>'birthday', '')::date AS birthday,
    (crew_elem->'person'->>'birthplace')::text AS birthplace,
    (crew_elem->'person'->>'updated_at')::timestamp AS person_updated_at
FROM sa.raw_trakt_movies_people s,
LATERAL (
    SELECT value AS crew_elem
    FROM jsonb_array_elements(COALESCE(s.movie_people_json->'crew'->'sound', '[]'::jsonb))
    WHERE value->>'job' = 'Original Music Composer'
) comp;