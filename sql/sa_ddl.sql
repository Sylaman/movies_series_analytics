CREATE SCHEMA IF NOT EXISTS sa;

-- Таблица для сохранения сырого json c историей просмотров из Track.TV
CREATE TABLE IF NOT EXISTS sa.raw_trakt_watch_history (
    id serial PRIMARY KEY,
    trakt_history_json JSONB NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для сохранения сырого json c оценками из Track.TV
CREATE TABLE IF NOT EXISTS sa.raw_trakt_ratings (
    id serial PRIMARY KEY,
    trakt_ratings_json JSONB NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);