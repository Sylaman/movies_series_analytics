CREATE SCHEMA IF NOT EXISTS sa;

-- Таблица для сохранения сырого json c историей просмотров из Track.TV
CREATE TABLE IF NOT EXISTS sa.raw_trakt_movies_history (
    id serial PRIMARY KEY,
    trakt_movies_history_json JSONB NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для сохранения сырого json c оценками из Track.TV
CREATE TABLE IF NOT EXISTS sa.raw_trakt_movies_ratings (
    id serial PRIMARY KEY,
    trakt_movies_ratings_json JSONB NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sa.raw_trakt_episodes_history (
    id serial PRIMARY KEY,
    trakt_episodes_history_json JSONB NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для сохранения сырого json c оценками из Track.TV
CREATE TABLE IF NOT EXISTS sa.raw_trakt_episodes_ratings (
    id serial PRIMARY KEY,
    trakt_episodes_ratings_json JSONB NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для людей (актеров, съемочной группы) фильмов
CREATE TABLE IF NOT EXISTS sa.raw_trakt_movies_people (
    movie_trakt_id VARCHAR(64) PRIMARY KEY,
    movie_people_json JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для людей сезонов сериалов
CREATE TABLE IF NOT EXISTS sa.raw_trakt_seasons_people (
    season_id VARCHAR(128) PRIMARY KEY,
    show_trakt_id VARCHAR(64) NOT NULL,
    season_number INTEGER NOT NULL,
    season_people_json JSONB NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);