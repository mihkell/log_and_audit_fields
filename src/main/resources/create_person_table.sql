create schema if not exists loging_test_schema;
DROP TABLE IF EXISTS loging_test_schema.person;
DROP TABLE IF EXISTS loging_test_schema_log.person_log;
CREATE TABLE loging_test_schema.person
(
    id   serial PRIMARY KEY,
    name text,
    created_at varchar,
    created_by varchar,
    modified_at varchar,
    created_by_db varchar,
    modified_by_db varchar
);