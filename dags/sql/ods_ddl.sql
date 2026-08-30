CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.trakt_movies_history (
    watch_id VARCHAR(64) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    release_date DATE,
    watched_at timestamp NOT NULL,
    runtime INT,
    country VARCHAR(16),
    genres TEXT[],
    subgenres TEXT[],
    certification VARCHAR(64),
    media_type VARCHAR(64),
    trakt_id VARCHAR(64),
    imdb_id VARCHAR(64),
    tmdb_id VARCHAR(64),
    poster TEXT,
    updated_at timestamp,
    loaded_at timestamp DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS ods.trakt_movies_ratings (
    trakt_id VARCHAR(64) NOT NULL,
    rating numeric(3,1) NOT NULL CHECK (rating BETWEEN 0 AND 10),
    rated_date date NOT NULL,
    loaded_at timestamp DEFAULT CURRENT_TIMESTAMP
);