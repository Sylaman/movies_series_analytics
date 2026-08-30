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