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
SELECT 
	(trakt_movies_history_json->>'id')::varchar AS watch_id
	, trakt_movies_history_json->'movie'->>'title' AS title
	, (trakt_movies_history_json->'movie'->>'released')::date AS release_date
	, (trakt_movies_history_json->>'watched_at')::timestamp AS watched_at
	, (trakt_movies_history_json->'movie'->>'runtime')::int AS runtime
	, trakt_movies_history_json->'movie'->>'country' AS country
	, ARRAY(SELECT jsonb_array_elements_text(trakt_movies_history_json->'movie'->'genres')) AS genres
	, ARRAY(SELECT jsonb_array_elements_text(trakt_movies_history_json->'movie'->'subgenres')) AS subgenres
	, trakt_movies_history_json->'movie'->>'certification' AS certification
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




SELECT 
	(trakt_episodes_history_json->>'id')::varchar AS watch_id
	, trakt_episodes_history_json->'episode'->>'title' AS episode_title
	, trakt_episodes_history_json->'show'->>'title' AS show_title
	, (trakt_episodes_history_json->'episode'->>'released')::date AS release_date
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
    , (trakt_episodes_history_json->'show'->'ids'->>'trakt')::varchar AS trakt_show_id
	, (trakt_episodes_history_json->'show'->'ids'->>'imdb')::varchar AS imdb_show_id
    , (trakt_episodes_history_json->'show'->'ids'->>'tmdb')::varchar AS tmdb_show_id
    , (trakt_episodes_history_json->'show'->>'status')::varchar AS show_status
    , (trakt_episodes_history_json->'show'->'images'->'poster'->> 0)::varchar AS poster
    , (trakt_episodes_history_json->'episode'->>'updated_at')::timestamp AS episode_updated_at
	, (trakt_episodes_history_json->'show'->>'updated_at')::timestamp AS show_updated_at
	, trakt_episodes_history_json->'show'->>'certification' AS certification
FROM sa.raw_trakt_episodes_history;