create schema if not exists loging_test_schema_log;
SET search_path TO loging_test_schema;

create or replace FUNCTION create_or_update(schema_name_val varchar, table_name_val varchar) RETURNS void AS
$$
DECLARE
    full_table_name         varchar = schema_name_val || '.' || table_name_val;
    log_trigger_name        varchar = 'log_trigger_' || table_name_val;
    log_table_name          varchar = table_name_val || '_log';
    log_schema_name         varchar = schema_name_val || '_log';
    full_log_table_name     varchar = log_schema_name || '.' || log_table_name;
    create_log_table        varchar = '';
    attach_trigger          varchar = '';
    create_procedure        varchar = '';
    missing_columns_clauses varchar = '';
    create_log_table_index  varchar = '';
    block_update_trigger    varchar = '';
    drop_trigger            varchar = '';
    drop_prodcedure         varchar = '';
    create_audit_trigger    varchar = '';
    columns                 varchar[] ;
    procedure_name          varchar = table_name_val || '_change_trigger';
BEGIN
    columns = columns_array(schema_name_val, table_name_val);

    create_audit_trigger = create_audit_fields_trigger(full_table_name, schema_name_val, table_name_val);
    create_log_table = create_missing_log_table(log_schema_name, log_table_name, full_log_table_name);
    block_update_trigger = block_update_on_log_table_trigger(log_table_name, full_log_table_name);
    create_log_table_index = log_table_index(full_log_table_name, log_table_name, full_table_name);
    missing_columns_clauses = add_missing_columns(log_schema_name, log_table_name, schema_name_val, table_name_val, columns);
    create_procedure = create_change_procedure(full_log_table_name, procedure_name, columns);
    drop_trigger = format('DROP TRIGGER IF EXISTS ' || log_trigger_name || ' ON ' || full_table_name || ' CASCADE; ');
    drop_prodcedure = format('DROP FUNCTION IF EXISTS ' || procedure_name || ' CASCADE; ');
    attach_trigger = format(' CREATE TRIGGER %I
        BEFORE INSERT OR UPDATE OR DELETE
        ON %I
        FOR EACH ROW
    EXECUTE PROCEDURE %I(); ', log_trigger_name, table_name_val, procedure_name);

    EXECUTE (create_audit_trigger
                 || create_log_table
                 || block_update_trigger
                 || missing_columns_clauses
                 || create_log_table_index
                 || drop_trigger
                 || drop_prodcedure
                 || create_procedure
        || attach_trigger);
END;
$$ LANGUAGE PLPGSQL;

create or replace FUNCTION create_change_procedure(full_log_table_name varchar, procedure_name varchar, columns varchar[])
    RETURNS varchar AS
$createchangetrigger$
DECLARE
    column_value_string varchar = 'operation,';
    value_string        varchar = 'TG_OP';
    insert_into_log     varchar;
    delete_log          varchar = 'TG_OP';
    placeholders        varchar = '';
BEGIN
    DECLARE
        delimiter_val varchar = '';
        column_name   varchar;
        index         int     = 0;
    BEGIN
        FOREACH column_name IN array columns
            LOOP
                index = index + 1;
                delimiter_val = delimiter(value_string);
                delete_log = delete_log || delimiter_val || 'OLD.' || column_name;
                value_string = value_string || delimiter_val || 'NEW.' || column_name;
                placeholders = placeholders || delimiter_val || '%L';
            END LOOP;
        column_value_string = column_value_string || array_to_string(columns, ',');
    END;

    insert_into_log = format('INSERT INTO %s ( %s ) VALUES ( %s );', full_log_table_name, column_value_string, value_string);
    delete_log = format('INSERT INTO %s ( %s ) VALUES ( %s );', full_log_table_name, column_value_string, delete_log);

    RETURN $$
    create or replace FUNCTION $$ || procedure_name || $$() RETURNS trigger AS
    $changetrigger$
    BEGIN
        IF TG_OP IN ('INSERT', 'UPDATE')
        THEN
            $$ || insert_into_log || $$
            RETURN NEW;
        ELSIF TG_OP in ('DELETE')
        THEN
            $$ || delete_log || $$
            RETURN OLD;
        END IF;

    END;
    $changetrigger$ LANGUAGE PLPGSQL SECURITY DEFINER; $$;

END;
$createchangetrigger$ LANGUAGE PLPGSQL SECURITY DEFINER;

create or replace FUNCTION log_table_index(full_log_table_name varchar, log_table_name varchar, full_table_name varchar)
    RETURNS varchar AS
$logtablepresent$
DECLARE
    primary_key_column varchar;
BEGIN
    SELECT a.attname
    FROM pg_index i
             JOIN pg_attribute a ON a.attrelid = i.indrelid
        AND a.attnum = ANY (i.indkey)
    WHERE i.indrelid = (full_table_name)::regclass
      AND i.indisprimary
    into primary_key_column;
    return $$CREATE INDEX IF NOT EXISTS index_$$ || log_table_name || $$ ON $$ || full_log_table_name || $$($$ || primary_key_column ||
           $$); $$;
END;
$logtablepresent$ LANGUAGE PLPGSQL;

create or replace FUNCTION log_table_present(schema_name_val varchar, table_name_val varchar) RETURNS bool AS
$$
BEGIN
    RETURN (select exists(
                           select information_schema.columns.column_name
                           from information_schema.columns
                           where table_name = table_name_val
                             and table_schema = schema_name_val
                           limit 1));
END;
$$ LANGUAGE PLPGSQL;

create or replace FUNCTION columns_array(schema_name_val varchar, table_name_val varchar) RETURNS varchar[] AS
$$
BEGIN
    RETURN (select array_agg(c.column_name::varchar)
            from information_schema.columns as c
            where table_name = table_name_val
              and table_schema = schema_name_val
            group by table_name);
END;
$$ LANGUAGE PLPGSQL;

create or replace FUNCTION create_missing_log_table(log_schema_name varchar, log_table_name varchar,
                                                    full_log_table_name varchar) RETURNS varchar AS
$$
BEGIN
    IF NOT log_table_present(log_schema_name, log_table_name)
    THEN
        RETURN 'CREATE TABLE ' || full_log_table_name ||
               '(log_id serial PRIMARY KEY,
                log_created_by varchar default session_user,
                log_created_at timestamptz default CURRENT_TIMESTAMP,
                operation varchar NOT NULL
                );
               ';
    END IF;
    RETURN '';
END;
$$ LANGUAGE PLPGSQL;

create or replace FUNCTION block_update_on_log_table_trigger(log_table_name varchar,
                                                             full_log_table_name varchar) RETURNS varchar AS
$$
DECLARE
    trigger_name varchar = log_table_name || '_block_update';
BEGIN
    RETURN ' DROP TRIGGER IF EXISTS ' || trigger_name || ' ON ' || full_log_table_name || ' CASCADE;
           CREATE TRIGGER ' || trigger_name || '
                    BEFORE UPDATE OR DELETE
                    ON ' || full_log_table_name || '
                    FOR EACH ROW
                EXECUTE PROCEDURE change_log_table_trigger(); ';
END;
$$ LANGUAGE PLPGSQL;

create or replace function add_missing_columns(log_schema_name varchar, log_table_name varchar, schema_name_val varchar,
                                               table_name_val varchar, columns varchar[])
    RETURNS varchar AS
$$
declare
    alter_columns_clauses varchar = '';
begin
    declare
        column_name text;
        column_type varchar;
    begin
        FOREACH column_name IN array columns
            LOOP
                column_type = column_type(schema_name_val, table_name_val, column_name);
                alter_columns_clauses = alter_columns_clauses ||
                                        add_columns_clause_if_missing(log_schema_name, log_table_name, column_name,
                                                                      column_type);
            END LOOP;
    end;
    RETURN alter_columns_clauses;
END;

$$ LANGUAGE PLPGSQL SECURITY DEFINER;
create or replace FUNCTION add_columns_clause_if_missing(schema_name_val varchar, table_name_val varchar, column_name varchar,
                                                         column_type varchar) RETURNS varchar AS
$$
DECLARE
    full_log_table varchar = schema_name_val || '.' || table_name_val;
BEGIN
    IF NOT history_table_does_have_column(schema_name_val, table_name_val, column_name)
    THEN
        RETURN add_column(full_log_table, column_name, column_type);
    END IF;
    RETURN '';
END;

$$ LANGUAGE PLPGSQL;

create or replace FUNCTION add_column(full_log_table_name varchar, column_name varchar, column_type varchar) RETURNS varchar AS
$$
BEGIN
    return ' ALTER TABLE ' || full_log_table_name || ' ADD COLUMN ' || column_name || ' ' || column_type || '; ';
END;

$$ LANGUAGE PLPGSQL;

create or replace FUNCTION history_table_does_have_column(schema_name_val varchar, table_name_val name, column_name_val varchar) RETURNS boolean AS
$$
BEGIN
    RETURN (select exists(
                           select
                           from information_schema.columns
                           where column_name = column_name_val
                             and table_name = table_name_val
                             and table_schema = schema_name_val));
END;

$$ LANGUAGE PLPGSQL;
create or replace FUNCTION delimiter(columns varchar) RETURNS varchar AS
$$
BEGIN
    IF (columns = '') THEN
        RETURN '';
    ELSE
        RETURN ', ';
    END IF;
END;

$$ LANGUAGE PLPGSQL;

create or replace FUNCTION column_type(schema_name_val name, table_name_val name, column_name_val varchar) RETURNS varchar AS
$$
BEGIN
    RETURN (select udt_name
            from information_schema.columns
            where column_name = column_name_val
              and table_name = table_name_val
              and table_schema = schema_name_val);
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION change_log_table_trigger() RETURNS trigger AS
$$
BEGIN
    RAISE EXCEPTION 'Update not allowed on log tables - table name: %', TG_TABLE_NAME;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION create_audit_fields_trigger(full_table_name_val varchar, schema_name_val varchar, table_name_val varchar) RETURNS varchar AS
$create_audit_fields_trigger$
DECLARE
    trigger_name      varchar = 'enforce_' || table_name_val || '_audit_triggers';
    full_trigger_name varchar = schema_name_val || '.' || 'enforce_' || table_name_val || '_audit_triggers';
BEGIN

    IF audit_fields_missing_present(schema_name_val, table_name_val) THEN
      return $$CREATE OR REPLACE FUNCTION $$ || full_trigger_name || $$()
              RETURNS trigger
              LANGUAGE plpgsql
              SECURITY DEFINER
          AS $function$
          BEGIN
              IF TG_OP = 'INSERT'
              THEN
                  NEW.created_by_db=SESSION_USER;
                  NEW.created_at=CURRENT_TIMESTAMP;
                  NEW.modified_at=CURRENT_TIMESTAMP;
                  NEW.modified_by_db=SESSION_USER;
              ELSIF TG_OP = 'UPDATE'
              THEN
                  NEW.modified_by_db=SESSION_USER;
                  NEW.modified_at=CURRENT_TIMESTAMP;
                  NEW.created_by_db=OLD.created_by_db;
                  NEW.created_at=OLD.created_at;
                  NEW.created_by=OLD.created_by;
              END IF;
              RETURN NEW;
          END;
          $function$
          ;
          DROP TRIGGER IF EXISTS $$ || trigger_name || $$ ON $$ || full_table_name_val || $$ ;
          CREATE TRIGGER $$ || trigger_name || $$ BEFORE INSERT OR UPDATE
              ON $$ || full_table_name_val || $$ FOR EACH ROW EXECUTE PROCEDURE public.$$ || trigger_name || $$();$$;
      END IF;
      RAISE EXCEPTION 'All auditing fields not present!';
END;
$create_audit_fields_trigger$ LANGUAGE PLPGSQL;

create or replace FUNCTION audit_fields_missing_present(schema_name_val varchar, table_name_val name) RETURNS boolean AS
$$
BEGIN
    RETURN (select 5 =(
                       select count(column_name)
                       from information_schema.columns
                       where table_name = table_name_val
                         and table_schema = schema_name_val
                         and (
                               column_name = 'created_at'
                               or column_name = 'created_by'
                               or column_name = 'modified_at'
                               or column_name = 'created_by_db'
                               or column_name = 'modified_by_db'
                           )));
END;

$$ LANGUAGE PLPGSQL;