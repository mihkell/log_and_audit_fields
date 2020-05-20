DROP TABLE IF EXISTS public.person;
DROP TABLE IF EXISTS public_log.person_log;
CREATE TABLE public.person
(
    id   serial PRIMARY KEY,
    name text,
    created_by_db varchar,
    created_at varchar,
    modified_at varchar,
    modified_by_db varchar
);