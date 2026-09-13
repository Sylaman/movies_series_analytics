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