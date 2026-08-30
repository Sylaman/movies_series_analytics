CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.trakt_movies_history (
    watch_id VARCHAR(64) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    release_date DATE,
    watched_at TIMESTAMPTZ NOT NULL,
    runtime INT,
    country VARCHAR(16),
    genres TEXT[],
    subgenres TEXT[],
    certification VARCHAR(32),
    media_type VARCHAR(32),
    trakt_id VARCHAR(32),
    imdb_id VARCHAR(32),
    tmdb_id VARCHAR(32),
    poster TEXT,
    updated_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);