
CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE ods.trakt_movies_history (
	watch_id varchar(64) NOT NULL,
	title varchar(255) NOT NULL,
	release_date date NOT NULL,
	watched_at timestamp NOT NULL,
	runtime int4 NOT NULL,
	country varchar(16) NOT NULL,
	genres _text NULL,
	subgenres _text NULL,
	certification varchar(64) NOT NULL,
	media_type varchar(64) NOT NULL,
	trakt_id varchar(64) NOT NULL,
	imdb_id varchar(64) NOT NULL,
	tmdb_id varchar(64) NOT NULL,
	poster text NULL,
	updated_at timestamp NOT NULL,
	loaded_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT trakt_movies_history_check CHECK ((watched_at >= release_date)),
	CONSTRAINT trakt_movies_history_pkey PRIMARY KEY (watch_id),
	CONSTRAINT trakt_movies_history_runtime_check CHECK ((runtime >= 0))
);


CREATE TABLE ods.trakt_movies_ratings (
	trakt_id varchar(64) NOT NULL,
	rating numeric(3, 1) NOT NULL,
	rated_date date NOT NULL,
	loaded_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT trakt_movies_ratings_rating_check CHECK (((rating >= (0)::numeric) AND (rating <= (10)::numeric)))
);


CREATE TABLE ods.trakt_episodes_history (
	watch_id varchar(64) NOT NULL,
	episode_title varchar(255) NOT NULL,
	show_title varchar(255) NOT NULL,
	release_date date NOT NULL,
	watched_at timestamp NOT NULL,
	media_type varchar(64) NULL,
	season_number int4 NOT NULL,
	episode_number int4 NOT NULL,
	runtime int4 NOT NULL,
	show_total_runtime int4 NOT NULL,
	country varchar(16) NOT NULL,
	genres _text NULL,
	subgenres _text NULL,
	trakt_episode_id varchar(64) NOT NULL,
	imdb_episode_id varchar(64) NOT NULL,
	tmdb_episode_id varchar(64) NOT NULL,
	season_id varchar(64) NOT NULL,
	trakt_show_id varchar(64) NOT NULL,
	imdb_show_id varchar(64) NOT NULL,
	tmdb_show_id varchar(64) NOT NULL,
	show_status varchar(64) NULL,
	poster text NULL,
	episode_updated_at timestamp NULL,
	show_updated_at timestamp NULL,
	certification varchar(64) NULL,
	loaded_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT trakt_episodes_history_pkey PRIMARY KEY (watch_id)
);


REATE TABLE ods.trakt_episodes_ratings (
	trakt_episode_id varchar(64) NOT NULL,
	trakt_show_id varchar(64) NOT NULL,
	rating numeric(3, 1) NOT NULL,
	rated_date date NOT NULL,
	loaded_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT trakt_episodes_ratings_rating_check CHECK (((rating >= (0)::numeric) AND (rating <= (10)::numeric)))
);


CREATE TABLE ods.trakt_movies_people (
	id serial4 NOT NULL,
	movie_trakt_id varchar(64) NOT NULL,
	person_trakt_id varchar(64) NOT NULL,
	person_name varchar(255) NOT NULL,
	"role" varchar(64) NOT NULL,
	gender varchar(32) NULL,
	birthday date NULL,
	birthplace text NULL,
	person_updated_at timestamp NULL,
	loaded_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT trakt_movies_people_pkey PRIMARY KEY (id)
);


CREATE TABLE ods.trakt_seasons_people (
	id serial4 NOT NULL,
	season_id varchar(64) NOT NULL,
	show_trakt_id varchar(64) NOT NULL,
	season_number int4 NOT NULL,
	person_trakt_id varchar(64) NOT NULL,
	person_name varchar(255) NOT NULL,
	"role" varchar(64) NOT NULL,
	gender varchar(32) NULL,
	birthday date NULL,
	birthplace text NULL,
	person_updated_at timestamp NULL,
	loaded_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT trakt_seasons_people_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS ods.tmdb_movies_detail (
    tmdb_id VARCHAR(64) NOT NULL,
    trakt_id VARCHAR(64) NOT NULL,
    budget BIGINT,
    revenue BIGINT,
    production_companies JSONB,
    loaded_at TIMESTAMP,
    ods_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT tmdb_movies_detail_pkey PRIMARY KEY (tmdb_id)
);
CREATE INDEX IF NOT EXISTS idx_ods_tmdb_movies_trakt_id ON ods.tmdb_movies_detail (trakt_id);


CREATE TABLE IF NOT EXISTS ods.tmdb_show_details (
    tmdb_show_id VARCHAR(64) NOT NULL,
    trakt_show_id VARCHAR(64) NOT NULL,
    created_by JSONB,
    production_companies JSONB,
    loaded_at TIMESTAMP,
    ods_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT tmdb_show_details_pkey PRIMARY KEY (tmdb_show_id)
);
CREATE INDEX IF NOT EXISTS idx_ods_tmdb_shows_trakt_id ON ods.tmdb_show_details (trakt_show_id);