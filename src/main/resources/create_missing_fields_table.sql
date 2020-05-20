create schema if not exists loging_test_schema;
drop table IF EXISTS loging_test_schema.transaction;
drop table IF EXISTS loging_test_schema_log.transaction_log;
create TABLE loging_test_schema.transaction
(
    id   serial PRIMARY KEY,
    account varchar,
    amount numeric(10, 4),
    created_by varchar,
    modified_at varchar,
    created_by_db varchar,
    modified_by_db varchar
);