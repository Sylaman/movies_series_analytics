CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.trakt_movies_history (
    watch_id VARCHAR(64) PRIMARY KEY
    , title VARCHAR(255) NOT NULL
    , release_date date NOT NULL
    , watched_at timestamp NOT NULL CHECK (watched_at >= release_date)
    , runtime integer NOT NULL CHECK (runtime >= 0)
    , country VARCHAR(16) NOT NULL 
    , genres TEXT[]
    , subgenres TEXT[]
    , certification VARCHAR(64) NOT NULL
    , media_type VARCHAR(64) NOT NULL
    , trakt_id VARCHAR(64) NOT NULL
    , imdb_id VARCHAR(64) NOT NULL
    , tmdb_id VARCHAR(64) NOT NULL
    , poster TEXT
    , updated_at timestamp NOT NULL
    , loaded_at timestamp DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS ods.trakt_movies_ratings (
    trakt_id VARCHAR(64) NOT NULL
    , rating numeric(3,1) NOT NULL CHECK (rating BETWEEN 0 AND 10)
    , rated_date date NOT NULL
    , loaded_at timestamp DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS ods.trakt_episodes_history (
    watch_id VARCHAR(64) PRIMARY KEY
	, episode_title VARCHAR(255) NOT NULL
	, show_title VARCHAR(255) NOT NULL
	, release_date date NOT NULL
	, watched_at timestamp NOT NULL
	, media_type VARCHAR(64)
	, season_number integer NOT NULL
    , episode_number integer NOT NULL
    , runtime integer NOT NULL
    , show_total_runtime integer NOT NULL
    , country VARCHAR(16) NOT NULL
    , genres TEXT[]
	, subgenres TEXT[]
	, trakt_episode_id VARCHAR(64) NOT NULL
	, imdb_episode_id VARCHAR(64) NOT NULL
    , tmdb_episode_id VARCHAR(64) NOT NULL
    , season_id VARCHAR(64) NOT NULL
    , trakt_show_id VARCHAR(64) NOT NULL
	, imdb_show_id VARCHAR(64) NOT NULL
    , tmdb_show_id VARCHAR(64) NOT NULL
    , show_status VARCHAR(64)
    , poster text
    , episode_updated_at timestamp
	, show_updated_at timestamp
	, certification VARCHAR(64)
	, loaded_at timestamp DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS ods.trakt_episodes_ratings (
    trakt_episode_id VARCHAR(64) NOT NULL
    , trakt_show_id VARCHAR(64) NOT NULL
    , rating numeric(3,1) NOT NULL CHECK (rating BETWEEN 0 AND 10)
    , rated_date date NOT NULL
    , loaded_at timestamp DEFAULT CURRENT_TIMESTAMP
);