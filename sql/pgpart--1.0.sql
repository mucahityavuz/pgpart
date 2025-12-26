CREATE TABLE pgpart.part_config
( 
    id                      bigserial PRIMARY KEY NOT NULL,
    parent                  text                  NOT NULL,
    period                  text,
    range_function          text,
    start_from              text                  NOT NULL,
    child_naming            text,
    create_older_partitions bool                           DEFAULT FALSE NOT NULL,
    template_table          text,
    precreate               text,
    detach_retention        text,
    drop_retention          text,
    keep_partitions         text[]                NOT NULL DEFAULT '{}',
    ret_check               jsonb                          DEFAULT '{
      "detach_retention": null,
      "drop_retention": null,
      "keep_partitions": []
    }'::jsonb,
    is_active               bool                           DEFAULT TRUE
);
CREATE UNIQUE INDEX ix_part_config_parent on pgpart.part_config(parent);

CREATE TABLE pgpart.retention
(
    id                   bigserial
        PRIMARY KEY,
    parent               text      NOT NULL,
    child                text      NOT NULL,
    time_range_start     timestamp,
    time_range_end       timestamp,
    is_parent_also_child boolean   NOT NULL,
    is_child_partitioned boolean   NOT NULL,
    range_start          text,
    range_end            text,
    detach_after         timestamp,
    drop_after           timestamp,
    created_at           timestamp NOT NULL,
    detached_at          timestamp,
    dropped_at           timestamp,
    status               text      NOT NULL --opts:created/detached/dropped/not detachable/not droppable/locked
);
CREATE INDEX ix_retention_parent_child ON pgpart.retention (parent, child);
CREATE INDEX ix_retention_status_detached_at_dropped_at ON pgpart.retention (status, detached_at, dropped_at);

CREATE TYPE pgpart.partition_row AS
(
    time_range_start              timestamp,
    time_range_end                timestamp,
    range_start                   text,
    range_end                     text,
    partition_name                text,
    partition_creation_sql        text,
    partition_attach_sql          text,
    partition_detach_time         timestamptz,
    partition_drop_time           timestamptz,
    partition_name_already_exists bool,
    partition_overlap             bool,
    pk_natts                      int2,
    pk_strat                      text,
    pk_type                       regtype,
    pk_type_cat                   text
);

CREATE FUNCTION pgpart.validate_part_config(part_config_id bigint, as_of_time timestamptz DEFAULT NOW())
    RETURNS bool
AS
$$
DECLARE
    v_part_config pgpart.part_config%ROWTYPE;
    v_pk_natts    int2;
    v_pk_strat    text;
    v_pk_type     regtype;
    v_pk_type_cat text;
    v_interval    interval;
    v_numeric     numeric;
    v_timestamptz timestamptz;
    v_text        text;
BEGIN
    --ID
    SELECT *
    INTO v_part_config
    FROM pgpart.part_config
    WHERE id = part_config_id
      AND is_active;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Config ID % not found in pgpart.part_config', part_config_id;
    END IF;

    --parent
    SELECT --format('%I.%I', n.nspname, c.relname) AS table_name,
           pt.partnatts,
           pt.partstrat,
           --(pt.partattrs::int2[])[0],
           a.atttypid::regtype,
           t.typcategory
    INTO v_pk_natts,v_pk_strat,v_pk_type,v_pk_type_cat
    FROM pg_partitioned_table pt
             JOIN pg_class c ON pt.partrelid = c.oid
             JOIN pg_namespace n ON c.relnamespace = n.oid
             JOIN pg_attribute a ON a.attrelid = c.oid
        AND a.attnum = (pt.partattrs::int2[])[0]
             JOIN pg_type t ON a.atttypid = t.oid
    WHERE FORMAT('%I.%I', n.nspname, c.relname) = v_part_config.parent;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table not found or not partitioned: %', v_part_config.parent;
    END IF;

    IF v_pk_natts > 1 THEN
        RAISE EXCEPTION 'Has more than 1 partition key column, found: %',v_pk_natts;
    END IF;

    IF v_pk_strat <> 'r' THEN
        RAISE EXCEPTION 'Not a range partition, found: %',v_pk_strat;
    END IF;

    IF v_pk_type_cat NOT IN ('D', 'N') THEN
        RAISE EXCEPTION 'not a date/time nor numeric partition key type, found: %',v_pk_type_cat;
    END IF;

    --period
    IF (TRIM(COALESCE(v_part_config.period, '')) = '') THEN
        RAISE EXCEPTION 'period cannot be empty';
    END IF;
    BEGIN
        PERFORM v_part_config.period::interval;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'invalid interval format for v_part_config.period: "%"', v_part_config.period;
    END;

    --range_function
    BEGIN
        IF v_pk_type_cat = 'D' THEN
            IF TRIM(COALESCE(v_part_config.range_function)) = '' THEN
                EXECUTE FORMAT($SQL$SELECT EXTRACT(EPOCH FROM '%s'::timestamp)::numeric$SQL$,
                               as_of_time) INTO v_numeric;
            ELSE
                EXECUTE 'SELECT ' ||
                        REPLACE(REPLACE(v_part_config.range_function, '#calc_time#', as_of_time::text), '#range_for#',
                                'from') INTO v_timestamptz;
            END IF;
        ELSE
            IF TRIM(COALESCE(v_part_config.range_function)) = '' THEN
                EXECUTE FORMAT($SQL$SELECT EXTRACT(EPOCH FROM '%s'::timestamp)::numeric$SQL$,
                               as_of_time) INTO v_numeric;
            ELSE
                --RAISE NOTICE 'SELECT %' ,REPLACE(v_part_config.range_function, '#calc_time#', as_of_time::text) ;
                EXECUTE 'SELECT ' ||
                        REPLACE(REPLACE(v_part_config.range_function, '#calc_time#', as_of_time::text), '#range_for#',
                                'from') INTO v_numeric;
            END IF;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Invalid range_function syntax for v_part_config.range_function: "%"', v_part_config.range_function;
    END;

    --start_from
    BEGIN
        EXECUTE 'SELECT ' || v_part_config.start_from INTO v_timestamptz;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'invalid date/time format for v_part_config.start_from: "%"', v_part_config.start_from;
    END;

    --child_naming
    BEGIN
        EXECUTE 'SELECT ' || REPLACE(
                REPLACE(v_part_config.child_naming, '#parent#', v_part_config.parent),
                '#calc_time#', as_of_time::text
                             ) INTO v_text;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'invalid child_naming syntax for v_part_config.child_naming: "%"', v_part_config.child_naming;
    END;

    --precreate
    IF (TRIM(COALESCE(v_part_config.precreate, '')) = '') THEN
        RAISE EXCEPTION 'precreate cannot be empty';
    END IF;
    BEGIN
        PERFORM v_part_config.precreate::interval;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'invalid interval format for v_part_config.precreate: "%"', v_part_config.precreate;
    END;

    --detach_retention, drop_retention
    IF (TRIM(COALESCE(v_part_config.detach_retention, '')) = '') AND
       (TRIM(COALESCE(v_part_config.drop_retention, '')) <> '') THEN
        RAISE EXCEPTION 'if drop_retention is set, then detach_retention cannot be empty';
    END IF;

    BEGIN
        IF (TRIM(COALESCE(v_part_config.detach_retention, '')) <> '') THEN
            PERFORM v_part_config.detach_retention::interval;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'invalid interval format for v_part_config.detach_retention: "%"', v_part_config.detach_retention;
    END;

    BEGIN
        IF (TRIM(COALESCE(v_part_config.drop_retention, '')) <> '') THEN
            PERFORM v_part_config.drop_retention::interval;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'invalid interval format for v_part_config.drop_retention: "%"', v_part_config.drop_retention;
    END;

    RETURN TRUE;
END;
$$ LANGUAGE plpgsql;

CREATE PROCEDURE pgpart.sp_get_partition_specs(
    part_config_id bigint,
    OUT v_pk_natts int2,
    OUT v_pk_strat text,
    OUT v_pk_type regtype,
    OUT v_pk_type_cat text)
AS
$$
DECLARE
    v_part_config pgpart.part_config%ROWTYPE;
BEGIN
    SELECT *
    INTO v_part_config
    FROM pgpart.part_config
    WHERE id = part_config_id
      AND is_active;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Config ID % not found in pgpart.part_config', part_config_id;
    END IF;

    RAISE NOTICE 'Loaded config: %', part_config_id;

    PERFORM pgpart.validate_part_config(part_config_id := part_config_id);

    SELECT --format('%I.%I', n.nspname, c.relname) AS table_name,
           pt.partnatts,
           pt.partstrat,
           --(pt.partattrs::int2[])[0],
           a.atttypid::regtype,
           t.typcategory
    INTO v_pk_natts,v_pk_strat,v_pk_type,v_pk_type_cat
    FROM pg_partitioned_table pt
             JOIN pg_class c ON pt.partrelid = c.oid
             JOIN pg_namespace n ON c.relnamespace = n.oid
             JOIN pg_attribute a ON a.attrelid = c.oid
        AND a.attnum = (pt.partattrs::int2[])[0]
             JOIN pg_type t ON a.atttypid = t.oid
    WHERE FORMAT('%I.%I', n.nspname, c.relname) = v_part_config.parent;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgpart.get_range_from_time(
    type_cat text,
    range_function text,
    time_to_convert timestamptz,
    ret_type regtype DEFAULT 'numeric'::regtype)
    RETURNS text
AS
$$
DECLARE
    v_ret_value text;
BEGIN
    IF type_cat = 'D' THEN
        IF TRIM(COALESCE(range_function)) = '' THEN
            RETURN time_to_convert;
        ELSE
            --RAISE NOTICE 'SELECT %' , REPLACE(range_function, '#calc_time#', time_to_convert::text);
            EXECUTE 'SELECT ' || REPLACE(range_function, '#calc_time#', time_to_convert::text) INTO v_ret_value;
        END IF;
    ELSE
        IF TRIM(COALESCE(range_function)) = '' THEN
            EXECUTE FORMAT($SQL$SELECT EXTRACT(EPOCH FROM '%s'::timestamp)::%s$SQL$, time_to_convert,
                           ret_type::text) INTO v_ret_value;
        ELSE
            --RAISE NOTICE 'SELECT %' ,REPLACE(range_function, '#calc_time#', time_to_convert::text);
            EXECUTE 'SELECT ' || REPLACE(range_function, '#calc_time#', time_to_convert::text) INTO v_ret_value;
        END IF;
    END IF;
    RETURN v_ret_value;
END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pgpart.find_nearest_time(start_from text, period text, as_of_time timestamptz DEFAULT NOW())
    RETURNS timestamp AS
$$
DECLARE
    v_sql          text;
    v_nearest_time timestamptz;
BEGIN
    v_sql := FORMAT($sql$
            SELECT ((%1$s)::timestamp)
                 + floor(EXTRACT(EPOCH FROM (('%2$s'::timestamp) - (%1$s)::timestamp)) / EXTRACT(EPOCH FROM %3$L::interval))
                 * %3$L::interval;
        $sql$, start_from, as_of_time, period);

    EXECUTE v_sql INTO v_nearest_time;
    RETURN v_nearest_time;
END;
$$ LANGUAGE plpgsql;

CREATE PROCEDURE pgpart.sp_process_retention(
    part_config_id bigint,
    dry_run bool DEFAULT FALSE
)
AS
$$
DECLARE
    rec               record;
    v_part_config     pgpart.part_config%ROWTYPE;
    v_ret_detach      text;
    v_ret_drop        text;
    v_keep_partitions text[];
    v_old_ver         text;
    v_new_ver         text;
    v_attempts        int;
    v_success         bool;
BEGIN
    --------------------------------------------
    -- 1. Retrieve configuration
    --------------------------------------------
    SELECT *
    INTO v_part_config
    FROM pgpart.part_config
    WHERE id = part_config_id
      AND is_active;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Config ID % not found in pgpart.part_config', part_config_id;
    END IF;

    PERFORM pgpart.validate_part_config(part_config_id := part_config_id);
    --check if retention policy is changed
    SELECT (v_part_config.ret_check ->> 'detach_retention'),
           (v_part_config.ret_check ->> 'drop_retention'),
           ARRAY(
                   SELECT JSONB_ARRAY_ELEMENTS_TEXT(v_part_config.ret_check -> 'keep_partitions')
           )
    INTO v_ret_detach,v_ret_drop,v_keep_partitions;
    RAISE NOTICE 'DETACH % -> % | DROP % -> % | keep_partitions % -> %',
        v_ret_detach,v_part_config.detach_retention,v_ret_drop,v_part_config.drop_retention,v_keep_partitions,v_part_config.keep_partitions;
    IF (COALESCE(v_ret_detach, '') <> COALESCE(v_part_config.detach_retention, '')) OR
       (COALESCE(v_ret_drop, '') <> COALESCE(v_part_config.drop_retention, '')) OR
       v_keep_partitions IS DISTINCT FROM COALESCE(v_part_config.keep_partitions, '{}'::text[]) THEN
        RAISE NOTICE 'detach, drop or keep_partitions changed, regenerating..';
        FOR rec IN
            SELECT *
            FROM pgpart.retention
            WHERE parent = v_part_config.parent
              AND status IN ('created', 'locked')
            LOOP
                SELECT FORMAT('old : %s ', (SELECT FORMAT('%s detach_after : %s , drop_after : %s , status : %s', child,
                                                          detach_after,
                                                          drop_after, status)))
                INTO v_old_ver
                FROM pgpart.retention r
                WHERE id = rec.id;
                RAISE NOTICE ' %',v_old_ver;

                UPDATE pgpart.retention r
                SET detach_after=
                        CASE
                            WHEN (TRIM(COALESCE(v_part_config.detach_retention)) <> '') THEN
                                r.time_range_end + v_part_config.detach_retention::interval
                            ELSE NULL END,
                    drop_after= CASE
                                    WHEN (TRIM(COALESCE(v_part_config.drop_retention)) <> '') AND
                                         (TRIM(COALESCE(v_part_config.detach_retention)) <> '')
                                        THEN
                                        r.time_range_end + v_part_config.detach_retention::interval +
                                        v_part_config.drop_retention::interval
                                    ELSE NULL END,
                    status= CASE
                                WHEN r.child = ANY (COALESCE(v_part_config.keep_partitions, '{}'::text[])) THEN 'locked'
                                WHEN r.child <> ALL (COALESCE(v_part_config.keep_partitions, '{}'::text[])) AND
                                     r.status = 'locked' THEN 'created'
                                ELSE r.status
                        END
                WHERE r.id = rec.id;


                SELECT FORMAT('new : %s ',
                              (SELECT FORMAT('%s detach_after : %s , drop_after : %s , status : %s', child,
                                             detach_after,
                                             drop_after, status)))
                INTO v_new_ver
                FROM pgpart.retention r
                WHERE id = rec.id;
                RAISE NOTICE '% %',v_new_ver,CASE
                                                 WHEN SUBSTR(v_old_ver, 4) <> SUBSTR(v_new_ver, 4) THEN '*'
                                                 ELSE '' END;

            END LOOP;
        UPDATE pgpart.part_config p
        SET ret_check = JSONB_BUILD_OBJECT(
                'detach_retention', v_part_config.detach_retention,
                'drop_retention', v_part_config.drop_retention,
                'keep_partitions', TO_JSONB(v_part_config.keep_partitions)
                        )
        WHERE p.id = part_config_id
          AND is_active;

    END IF;
    --UPDATE_DETACHABLE_PARTITION_LIST
    WITH workset AS (SELECT id, parent, child
                     FROM pgpart.retention
                     WHERE detached_at IS NULL
                       AND status IN ('created')
                       AND detach_after <= NOW()),
         all_partitions AS (SELECT c1.relnamespace::regnamespace || '.' || c1.relname::text AS parent,
                                   c2.relnamespace::regnamespace || '.' || c2.relname::text AS child
                            FROM pg_inherits i
                                     JOIN pg_class c1 ON c1.oid = i.inhparent
                                     JOIN pg_class c2 ON c2.oid = i.inhrelid)
    UPDATE pgpart.retention
    SET status = 'not detachable'
    WHERE id IN (SELECT w.id
                 FROM workset w
                          LEFT JOIN all_partitions i
                                    ON i.child = w.child AND i.parent = w.parent
                 WHERE i.parent IS NULL);

    --SQL_UPDATE_DROPPABLE_TABLE_LIST
    WITH workset AS (SELECT id, parent, child
                     FROM pgpart.retention
                     WHERE detached_at IS NOT NULL
                       AND dropped_at IS NULL
                       AND status IN ('detached')
                       AND drop_after <= NOW()),
         all_tables AS (SELECT schemaname || '.' || tablename AS child
                        FROM pg_tables)
    UPDATE pgpart.retention
    SET status = 'not droppable'
    WHERE id IN (SELECT w.id
                 FROM workset w
                          LEFT JOIN all_tables a ON a.child = w.child
                 WHERE a.child IS NULL);
    --SQL_GET_PARTITIONS_TO_DETACH
    FOR rec IN SELECT id, parent, child
               FROM pgpart.retention
               WHERE detached_at IS NULL
                 AND status IN ('created')
                 AND detach_after <= NOW()
               ORDER BY parent, child
        LOOP
            RAISE NOTICE '% is going to be detached',rec.child;
            IF dry_run THEN
                UPDATE pgpart.retention
                SET detached_at = NOW(),
                    status      = 'detached'
                WHERE id = rec.id;
            ELSE
                PERFORM SET_CONFIG('lock_timeout', '100', FALSE);

                v_attempts := 0;
                v_success := FALSE;

                WHILE v_attempts < 10 AND NOT v_success
                    LOOP
                        v_attempts := v_attempts + 1;

                        BEGIN
                            EXECUTE FORMAT('ALTER TABLE %s DETACH PARTITION %s',
                                           rec.parent, rec.child);

                            v_success := TRUE;
                            RAISE NOTICE 'Detached % on attempt %', rec.child, v_attempts;

                        EXCEPTION
                            WHEN lock_not_available THEN
                                RAISE NOTICE 'Lock timeout on %. Attempt % of 10',
                                    rec.child, v_attempts;

                                PERFORM PG_SLEEP(2);
                            WHEN OTHERS THEN
                                RAISE;
                        END;
                    END LOOP;

                IF v_success THEN
                    UPDATE pgpart.retention
                    SET detached_at = NOW(),
                        status      = 'detached'
                    WHERE id = rec.id;
                    COMMIT;
                ELSE
                    RAISE NOTICE 'Could not detach % after 10 attempts', rec.child;
                END IF;
            END IF;
        END LOOP;
    --SQL_GET_PARTITIONS_TO_DROP
    FOR rec IN SELECT id, child
               FROM pgpart.retention
               WHERE detached_at IS NOT NULL
                 AND dropped_at IS NULL
                 AND status IN ('detached')
                 AND drop_after <= NOW()
               ORDER BY parent, child
        LOOP
            RAISE NOTICE '% is going to be dropped',rec.child;
            IF NOT dry_run THEN
                PERFORM SET_CONFIG('lock_timeout', '100', FALSE);

                v_attempts := 0;
                v_success := FALSE;

                WHILE v_attempts < 10 AND NOT v_success
                    LOOP
                        v_attempts := v_attempts + 1;

                        BEGIN
                            EXECUTE FORMAT('DROP TABLE %s;', rec.child);

                            v_success := TRUE;
                            RAISE NOTICE 'Dropped % on attempt %', rec.child, v_attempts;

                        EXCEPTION
                            WHEN lock_not_available THEN
                                RAISE NOTICE 'Lock timeout on %. Attempt % of 10',
                                    rec.child, v_attempts;

                                PERFORM PG_SLEEP(2);
                            WHEN OTHERS THEN
                                RAISE;
                        END;
                    END LOOP;

                IF v_success THEN
                    UPDATE pgpart.retention
                    SET dropped_at = NOW(),
                        status     = 'dropped'
                    WHERE id = rec.id;
                    COMMIT;
                ELSE
                    RAISE NOTICE 'Could not detach % after 10 attempts', rec.child;
                END IF;
            END IF;
        END LOOP;

    IF dry_run THEN


    ELSE
    END IF;
    IF dry_run THEN ROLLBACK; END IF;
END;
$$ LANGUAGE plpgsql;

CREATE PROCEDURE pgpart.sp_get_new_partition_metadata(
    OUT new_parts pgpart.partition_row[],
    OUT success boolean,
    IN part_config_id bigint DEFAULT 1,
    IN as_of_time timestamptz DEFAULT NOW()
)
AS
$$
DECLARE
    v_part_config  pgpart.part_config%ROWTYPE;
    v_start_from   timestamp;
    v_nearest_time timestamp;
    v_sql          text;
    new_part       pgpart.partition_row;
    v_pk_natts     int2;
    v_pk_strat     text;
    v_pk_type      regtype;
    v_pk_type_cat  text;
BEGIN
    success := FALSE;
    BEGIN
        --------------------------------------------
        -- 1. Retrieve configuration
        --------------------------------------------
        SELECT *
        INTO v_part_config
        FROM pgpart.part_config
        WHERE id = part_config_id
          AND is_active;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'Config ID % not found in pgpart.part_config', part_config_id;
        END IF;

        RAISE NOTICE 'Loaded config: %', part_config_id;

        PERFORM pgpart.validate_part_config(part_config_id := part_config_id);

        CALL pgpart.sp_get_partition_specs(part_config_id := part_config_id, v_pk_natts := v_pk_natts,
                                           v_pk_strat := v_pk_strat, v_pk_type := v_pk_type,
                                           v_pk_type_cat := v_pk_type_cat);
        --------------------------------------------
        -- 2. Find nearest time
        --------------------------------------------

        SELECT pgpart.find_nearest_time(start_from := v_part_config.start_from, period := v_part_config.period,
                                        as_of_time := as_of_time)
        INTO v_nearest_time;
        RAISE NOTICE 'Nearest time calculated: %', v_nearest_time;

        --------------------------------------------
        -- 3. Determine start time
        --------------------------------------------
        IF v_part_config.create_older_partitions THEN
            v_sql := FORMAT($sql$
                SELECT %s::timestamp;
            $sql$, v_part_config.start_from);
            EXECUTE v_sql INTO v_start_from;
        ELSE
            v_start_from := v_nearest_time;
        END IF;

        RAISE NOTICE 'Start from: %', v_start_from;

        --------------------------------------------
        -- 4. Create new partition array
        --------------------------------------------
        SELECT ARRAY_AGG(
                       ROW (
                           gen,
                           gen + v_part_config.period::interval,
                           NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                           FALSE, FALSE,NULL,NULL,NULL,NULL
                           )::pgpart.partition_row
               )
        INTO new_parts
        FROM GENERATE_SERIES(
                     v_start_from,
                     v_nearest_time + v_part_config.precreate::interval,
                     v_part_config.period::interval
             ) gen;
        RAISE NOTICE 'Generated % partitions.', COALESCE(ARRAY_LENGTH(new_parts, 1), 0);
        --------------------------------------------
        --5. Generate metadata for each partition
        --------------------------------------------
        FOR i IN 1..ARRAY_LENGTH(new_parts, 1)
            LOOP
                new_part := new_parts[i];
                --set pk specs
                new_part.pk_natts = v_pk_natts;
                new_part.pk_strat = v_pk_strat;
                new_part.pk_type = v_pk_type;
                new_part.pk_type_cat = v_pk_type_cat;
                -- Generate partition names
                v_sql := REPLACE(
                        REPLACE(v_part_config.child_naming, '#parent#', v_part_config.parent),
                        '#calc_time#', new_part.time_range_start::text
                         );
                EXECUTE 'SELECT ' || v_sql INTO new_part.partition_name;

                -- Generate ranges
                SELECT pgpart.get_range_from_time(type_cat := v_pk_type_cat,
                                                  range_function := REPLACE(v_part_config.range_function, '#range_for#', 'from'),
                                                  time_to_convert := new_part.time_range_start, ret_type := v_pk_type)
                INTO new_part.range_start;

                SELECT pgpart.get_range_from_time(type_cat := v_pk_type_cat,
                                                  range_function := REPLACE(v_part_config.range_function, '#range_for#', 'to'),
                                                  time_to_convert := new_part.time_range_end, ret_type := v_pk_type)
                INTO new_part.range_end;
                -- Generate SQL commands
                new_part.partition_creation_sql :=
                        FORMAT(
                                'CREATE TABLE %s (LIKE %s INCLUDING ALL);',
                                new_part.partition_name, v_part_config.template_table
                        );
                IF v_pk_type_cat = 'D' THEN
                    new_part.partition_attach_sql :=
                            FORMAT(
                                    $sql$ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM ('%s') TO ('%s');$sql$,
                                    v_part_config.parent, new_part.partition_name,
                                    new_part.range_start, new_part.range_end
                            );
                ELSE
                    new_part.partition_attach_sql :=
                            FORMAT( --EXTRACT(EPOCH FROM '%s'::timestamp)
                                    $sql$ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (%s) TO (%s);$sql$,
                                    v_part_config.parent, new_part.partition_name,
                                    new_part.range_start, new_part.range_end
                            );
                END IF;

                -- Generate retentions
                IF (TRIM(COALESCE(v_part_config.detach_retention)) <> '') THEN
                    new_part.partition_detach_time :=
                            new_part.time_range_end + v_part_config.detach_retention::interval;
                    IF (TRIM(COALESCE(v_part_config.drop_retention)) <> '') THEN
                        new_part.partition_drop_time :=
                                new_part.time_range_end + v_part_config.detach_retention::interval +
                                v_part_config.drop_retention::interval;
                    END IF;
                END IF;

                -- Check if relation already exists
                SELECT EXISTS (SELECT 1
                               FROM pg_tables t
                               WHERE t.schemaname || '.' || t.tablename = new_part.partition_name)
                INTO new_part.partition_name_already_exists;

                -- Check for overlap
                WITH existing_parts AS (SELECT c.relnamespace::regnamespace || '.' || c.relname::text AS partition,
                                               PG_GET_EXPR(c.relpartbound, c.oid)                     AS bounds,
                                               SUBSTRING(PG_GET_EXPR(c.relpartbound, c.oid) FROM
                                                         $filter$\('*(.*?)'*\) TO$filter$)            AS starts,
                                               SUBSTRING(PG_GET_EXPR(c.relpartbound, c.oid) FROM
                                                         $filter$TO \('*(.*?)'*\)$filter$)            AS ends
                                        FROM pg_inherits AS i
                                                 JOIN pg_class AS c ON c.oid = i.inhrelid
                                        WHERE (c.relnamespace::regnamespace || '.' || c.relname::text) =
                                              new_part.partition_name)
                SELECT EXISTS (SELECT 1
                               FROM existing_parts
                               WHERE CASE
                                         WHEN v_pk_type_cat = 'D' THEN
                                             TSRANGE(new_part.range_start::timestamp, new_part.range_end::timestamp)
                                                 && TSRANGE(starts::timestamp, ends::timestamp)
                                         ELSE
                                             NUMRANGE(new_part.range_start::numeric, new_part.range_end::numeric)
                                                 && NUMRANGE(starts::numeric, ends::numeric)
                                         END)
                INTO new_part.partition_overlap;
                RAISE NOTICE 'Partition % -> overlap: %, exists: %, range from: %, range to: % %',
                    new_part.partition_name,
                    new_part.partition_overlap,
                    new_part.partition_name_already_exists,
                    new_part.range_start,
                    new_part.range_end,CASE
                                           WHEN (NOT new_part.partition_overlap AND
                                                 NOT new_part.partition_name_already_exists) THEN 'will be created'
                                           ELSE 'will be skiped (retention records might be created)' END;


                new_parts[i] := new_part;
            END LOOP;

        success := TRUE;
        RAISE NOTICE 'Procedure completed successfully.';

    EXCEPTION
        WHEN OTHERS THEN
            RAISE WARNING 'Error in get_new_partition_metadata: % [%]', SQLERRM, SQLSTATE;
            success := FALSE;
    END;
END;
$$ LANGUAGE plpgsql;

CREATE PROCEDURE pgpart.sp_process_creation(part_config_id bigint)
AS
$$
DECLARE
    v_part_config pgpart.part_config%ROWTYPE;
    success       bool;
    new_parts     pgpart.partition_row[];
    new_part      pgpart.partition_row;
BEGIN
    SELECT *
    INTO v_part_config
    FROM pgpart.part_config
    WHERE id = part_config_id
      AND is_active;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Config ID % not found in pgpart.part_config', part_config_id;
    END IF;
    PERFORM pgpart.validate_part_config(part_config_id);

    IF (TRIM(COALESCE(v_part_config.template_table, '')) = '') THEN
        UPDATE pgpart.part_config
        SET template_table='pgpart.template_' || SPLIT_PART(part_config.parent, '.', 2)
        WHERE part_config.id = part_config_id
          AND is_active
        RETURNING part_config.template_table INTO v_part_config.template_table;
    END IF;


    IF TRIM(COALESCE(v_part_config.template_table, '')) <> '' AND (SELECT NOT EXISTS(SELECT *
                                                                                     FROM pg_tables
                                                                                     WHERE schemaname || '.' || tablename = v_part_config.template_table)) THEN
        EXECUTE FORMAT('CREATE TABLE %s (LIKE %s INCLUDING ALL);', v_part_config.template_table, v_part_config.parent);
        EXECUTE FORMAT('ALTER TABLE %s OWNER TO %s;', v_part_config.template_table,
                       (SELECT tableowner FROM pg_tables WHERE schemaname || '.' || tablename = v_part_config.parent));
    END IF;

    CALL pgpart.sp_get_new_partition_metadata(new_parts := new_parts, success := success,
                                              part_config_id := part_config_id,
                                              as_of_time := NOW());
    RAISE NOTICE '%', (SELECT ARRAY_LENGTH(new_parts, 1));
    FOR i IN 1..ARRAY_LENGTH(new_parts, 1)
        LOOP
            new_part := new_parts[i];
            /*  To add already existing partitions before pgpart, check
                *if a table with that name already exists
                *if create_older_partitions=true
                *if partition is child of parent
                *range.from and .to matches
                *no retention record exists with status in created or locked
                */
            IF new_part.partition_name_already_exists THEN
                IF (v_part_config.create_older_partitions) AND
                   (SELECT NOT EXISTS(SELECT 1
                                      FROM pgpart.retention
                                      WHERE parent = v_part_config.parent
                                        AND child = new_part.partition_name
                                        AND status IN ('created', 'locked', 'detached')))
                    AND EXISTS(WITH existing_parts
                                        AS (SELECT c.relnamespace::regnamespace || '.' || c.relname::text AS partition,
                                                   SUBSTRING(PG_GET_EXPR(c.relpartbound, c.oid) FROM
                                                             $filter$\('*(.*?)'*\) TO$filter$)            AS starts,
                                                   SUBSTRING(PG_GET_EXPR(c.relpartbound, c.oid) FROM
                                                             $filter$TO \('*(.*?)'*\)$filter$)            AS ends
                                            FROM pg_inherits AS i
                                                     JOIN pg_class AS c ON c.oid = i.inhrelid
                                            WHERE (c.relnamespace::regnamespace || '.' || c.relname::text) =
                                                  new_part.partition_name)
                               SELECT EXISTS (SELECT 1
                                              FROM existing_parts
                                              WHERE CASE
                                                        WHEN new_part.pk_type_cat = 'D' THEN
                                                            starts = new_part.range_start AND ends = new_part.range_end
                                                        ELSE
                                                            starts = new_part.range_start AND ends = new_part.range_end
                                                        END))
                THEN
                    INSERT INTO pgpart.retention (parent, child, time_range_start, time_range_end, is_parent_also_child,
                                                  is_child_partitioned, range_start, range_end, detach_after,
                                                  drop_after, created_at, detached_at, dropped_at, status)
                    SELECT v_part_config.parent,
                           new_part.partition_name,
                           new_part.time_range_start,
                           new_part.time_range_end,
                           FALSE,
                           FALSE,
                           new_part.range_start,
                           new_part.range_end,
                           new_part.partition_detach_time,
                           new_part.partition_drop_time,
                           NOW(),
                           NULL,
                           NULL,
                           CASE
                               WHEN new_part.partition_name = ANY
                                    (COALESCE(v_part_config.keep_partitions, '{}'::text[]))
                                   THEN 'locked'
                               ELSE 'created' END;
                    COMMIT;
                ELSE
                    RAISE NOTICE 'A relation with name % already exists, skipping.', new_part.partition_name;

                END IF;
            ELSIF new_part.partition_overlap THEN
                RAISE NOTICE 'Partition % overlaps with existing partition. Attempted range: from % to %, skipping.', new_part.partition_name,new_part.range_start,new_part.range_end;
            ELSE
                EXECUTE new_part.partition_creation_sql;
                EXECUTE FORMAT('ALTER TABLE %s OWNER TO %s;', new_part.partition_name, (SELECT tableowner
                                                                                        FROM pg_tables
                                                                                        WHERE schemaname || '.' || tablename = v_part_config.template_table));
                EXECUTE new_part.partition_attach_sql;
                RAISE NOTICE 'before insert';
                INSERT INTO pgpart.retention (parent, child, time_range_start, time_range_end, is_parent_also_child,
                                              is_child_partitioned, range_start, range_end, detach_after,
                                              drop_after, created_at, detached_at, dropped_at, status)
                SELECT v_part_config.parent,
                       new_part.partition_name,
                       new_part.time_range_start,
                       new_part.time_range_end,
                       FALSE,
                       FALSE,
                       new_part.range_start,
                       new_part.range_end,
                       new_part.partition_detach_time,
                       new_part.partition_drop_time,
                       NOW(),
                       NULL,
                       NULL,
                       CASE
                           WHEN new_part.partition_name = ANY (COALESCE(v_part_config.keep_partitions, '{}'::text[]))
                               THEN 'locked'
                           ELSE 'created' END
                WHERE NOT EXISTS(SELECT 1
                                 FROM pgpart.retention
                                 WHERE parent = v_part_config.parent
                                   AND new_part.partition_name = child
                                   AND status IN ('created', 'detached', 'locked'));
                RAISE NOTICE 'Partition % range: from % to %, created.', new_part.partition_name,new_part.range_start,new_part.range_end;
                COMMIT;
            END IF;
        END LOOP;
    IF v_part_config.create_older_partitions THEN
        UPDATE pgpart.part_config SET create_older_partitions= FALSE WHERE id = v_part_config.id AND is_active;
        RAISE NOTICE 'v_part_config.create_older_partitions set to FALSE';
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE PROCEDURE pgpart.sp_process_all()
AS
$$
DECLARE
    rec record;
BEGIN
    FOR rec IN SELECT id FROM pgpart.part_config WHERE is_active
        LOOP
            RAISE NOTICE '%' , rec.id;
            CALL pgpart.sp_process_creation(rec.id);
            CALL pgpart.sp_process_retention(part_config_id := rec.id, dry_run := FALSE);
        END LOOP;
END;
$$ LANGUAGE plpgsql;
