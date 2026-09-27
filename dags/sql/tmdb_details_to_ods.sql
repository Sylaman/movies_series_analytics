INSERT INTO ods.tmdb_movies_detail (
    tmdb_id,
    trakt_id,
    budget,
    revenue,
    production_companies,
    loaded_at
)
SELECT 
    tmdb_id,
    trakt_id,
    (tmdb_movies_details_json->>'budget')::bigint AS budget,
    (tmdb_movies_details_json->>'revenue')::bigint AS revenue,
    tmdb_movies_details_json->'production_companies' AS production_companies,
    loaded_at
FROM sa.raw_tmdb_movies_details;


INSERT INTO ods.tmdb_show_details (
    tmdb_show_id,
    trakt_show_id,
    created_by,
    production_companies,
    loaded_at
)
SELECT 
    tmdb_show_id,
    trakt_id AS trakt_show_id,
    tmdb_shows_details_json->'created_by' AS created_by,
    tmdb_shows_details_json->'production_companies' AS production_companies,
    loaded_at
FROM sa.raw_tmdb_shows_details;