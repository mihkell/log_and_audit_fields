drop table IF EXISTS public.transaction;
drop table IF EXISTS public_log.transaction_log;
create TABLE public.transaction
(
    id   serial PRIMARY KEY,
    account varchar,
    amount numeric(10, 4),
    created_by_db varchar,
    created_at varchar,
    modified_at varchar,
    modified_by_db varchar
);