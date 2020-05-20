DO
$$
BEGIN
    RAISE NOTICE 'Starting insertions to person table...';
    FOR tempseq IN 1..10000
        LOOP
            INSERT INTO public.person (name)
            values ('some name');
        END LOOP;
    RAISE NOTICE 'Done insertions to person table.';
END;
$$ LANGUAGE plpgsql;