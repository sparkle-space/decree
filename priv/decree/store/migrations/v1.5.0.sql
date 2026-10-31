DO $migration$
DECLARE
  v_data_type text;
  v_causation_id_type text;
  v_correlation_id_type text;
  v_body text := $body$
-- Appends events to one stream, in one round trip and in one lock order: the
-- stream's row first (FOR NO KEY UPDATE, or the insert of a new row), then $all.
-- $all is always the last lock an append takes, so appends cannot cycle on these
-- two locks. Batches sharing event ids in another order can still meet on
-- events_pkey, as they could in the statements this replaces.
--
-- Name resolution: the body names tables unqualified, and the set_config below
-- points search_path at p_schema for this call. The attached SET search_path
-- clause is what scopes that: Postgres restores the session's value when the
-- function exits, on success and on a raise, also inside a caller's transaction.
-- So no store schema name is frozen in the catalog, and ALTER SCHEMA ... RENAME
-- needs nothing but the config change. SET search_path FROM CURRENT, or a schema
-- baked into this text, would freeze the name the function was created under.
--
-- The _v2 suffix is a contract: a later change adds a new version and never
-- replaces or drops this one, because an old pod still calls this signature.
--
-- Tags travel as two parallel flat arrays, because Postgres arrays are rectangular:
-- p_tag_event_indexes[n] is the 1-based ordinal, within this batch, of the event
-- p_tags[n] belongs to.
DECLARE
  v_count bigint := cardinality(p_event_ids);
  v_stream_id bigint := p_stream_id;
  v_stream_version bigint := 0;
  v_all_version bigint;
  v_stale boolean := false;
  v_stamp timestamptz;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'append_events_v2 needs READ COMMITTED, not %',
      current_setting('transaction_isolation')
      USING ERRCODE = 'invalid_transaction_state';
  END IF;

  PERFORM set_config('search_path', quote_ident(p_schema) || ', pg_temp', true);

  -- Both arms write the stream's row here, so its event_notification NOTIFY still
  -- fires before the one for $all. A stale append writes no row and fires neither.
  IF p_stream_id IS NULL THEN
    INSERT INTO streams (stream_uuid, stream_version, created_at)
    VALUES (p_stream_uuid, v_count, COALESCE(p_stream_created_at, now()))
    RETURNING stream_id INTO v_stream_id;
  ELSE
    SELECT stream_version INTO v_stream_version
    FROM streams
    WHERE stream_id = p_stream_id
    FOR NO KEY UPDATE;

    IF NOT FOUND THEN
      RETURN NULL;
    END IF;

    -- Decided here, on the locked row, and answered after the events insert, whose
    -- unique index answers a duplicate event id first, stored or in flight.
    v_stale := p_expected_version IS NOT NULL AND p_expected_version <> v_stream_version;

    IF NOT v_stale THEN
      UPDATE streams
      SET stream_version = stream_version + v_count
      WHERE stream_id = p_stream_id;
    END IF;
  END IF;

  -- These rows carry no position, so they stay out of the $all lock.
  INSERT INTO events
    (event_id, event_type, causation_id, correlation_id, data, metadata, created_at)
  SELECT *
  FROM unnest(
    p_event_ids,
    p_event_types,
    p_causation_ids,
    p_correlation_ids,
    p_data,
    p_metadata,
    p_created_at
  );

  -- A concurrent writer used to surface as this unique violation from the index.
  -- Raising it keeps the error's shape and the abort of an enclosing transaction,
  -- and rolls the rows above back. A stale append never takes $all.
  IF v_stale THEN
    RAISE EXCEPTION 'duplicate key value violates unique constraint "ix_stream_events"'
      USING ERRCODE = 'unique_violation', CONSTRAINT = 'ix_stream_events';
  END IF;

  UPDATE streams
  SET stream_version = stream_version + v_count
  WHERE stream_id = 0
  RETURNING stream_version - v_count INTO v_all_version;

  INSERT INTO stream_events
    (event_id, stream_id, stream_version, original_stream_id, original_stream_version)
  SELECT
    new_events.event_id,
    target.stream_id,
    target.base_version + new_events.index,
    v_stream_id,
    v_stream_version + new_events.index
  FROM unnest(p_event_ids) WITH ORDINALITY AS new_events (event_id, index)
  CROSS JOIN (
    VALUES (v_stream_id, v_stream_version), (0, v_all_version)
  ) AS target (stream_id, base_version);

  -- These rows carry the position, so they wait for $all. Every event gets its
  -- event_types row, tagged or not: that is what an item with types and no tags reads.
  INSERT INTO event_types (event_type, position)
  SELECT new_events.event_type, v_all_version + new_events.index
  FROM unnest(p_event_types) WITH ORDINALITY AS new_events (event_type, index);

  -- Read here, after $all is granted, so stamps are monotone in position order. now()
  -- is the transaction's start, and events.created_at is written before the lock and
  -- cannot be updated. Only the command: row needs a stamp (Q-3).
  v_stamp := clock_timestamp();

  INSERT INTO event_tags (position, tag, event_type, stamped_at)
  SELECT
    v_all_version + new_tags.index,
    new_tags.tag,
    p_event_types[new_tags.index],
    CASE WHEN starts_with(new_tags.tag, 'command:') THEN v_stamp END
  FROM unnest(p_tag_event_indexes, p_tags) AS new_tags (index, tag);

  RETURN v_stream_id;
END;
$body$;
BEGIN
  -- Neither table has a foreign key: one from a tag row would make a hard delete fail
  -- or force a tag-row CTE into hard_delete_stream.
  -- Their text columns hold machine identifiers matched by equality, so bytewise
  -- comparison keeps index work under $all cheap and independent of the host's locale.
  CREATE TABLE event_tags
  (
      position bigint NOT NULL,
      tag text COLLATE "C" NOT NULL,
      event_type text COLLATE "C" NOT NULL,
      stamped_at timestamp with time zone NULL,
      PRIMARY KEY(tag, position),
      CONSTRAINT event_tags_tag_length CHECK (octet_length(tag) <= 512)
  );

  CREATE INDEX ix_event_tags_tag_event_type_position ON event_tags (tag, event_type, position);

  CREATE TABLE event_types
  (
      event_type text COLLATE "C" NOT NULL,
      position bigint NOT NULL,
      PRIMARY KEY(event_type, position)
  );

  -- prevent updates and deletions, as on `stream_events`
  CREATE TRIGGER no_update_event_tags
  BEFORE UPDATE ON event_tags
  FOR EACH STATEMENT
  EXECUTE PROCEDURE event_store_exception('Cannot update event tags');

  CREATE TRIGGER no_delete_event_tags
  BEFORE DELETE ON event_tags
  FOR EACH STATEMENT
  EXECUTE PROCEDURE event_store_delete('Cannot delete event tags');

  CREATE TRIGGER no_update_event_types
  BEFORE UPDATE ON event_types
  FOR EACH STATEMENT
  EXECUTE PROCEDURE event_store_exception('Cannot update event types');

  CREATE TRIGGER no_delete_event_types
  BEFORE DELETE ON event_types
  FOR EACH STATEMENT
  EXECUTE PROCEDURE event_store_delete('Cannot delete event types');

  -- `events` resolves through the `SET LOCAL search_path` Migrate runs. Its column
  -- types differ per store and Postgrex encodes from the declared types, so the
  -- signature takes them from the table.
  SELECT format_type(atttypid, atttypmod) INTO STRICT v_data_type
  FROM pg_attribute
  WHERE attrelid = 'events'::regclass AND attname = 'data';

  SELECT format_type(atttypid, atttypmod) INTO STRICT v_causation_id_type
  FROM pg_attribute
  WHERE attrelid = 'events'::regclass AND attname = 'causation_id';

  SELECT format_type(atttypid, atttypmod) INTO STRICT v_correlation_id_type
  FROM pg_attribute
  WHERE attrelid = 'events'::regclass AND attname = 'correlation_id';

  EXECUTE
    'CREATE FUNCTION append_events_v2('
    || 'p_schema text, '
    || 'p_stream_id bigint, '
    || 'p_stream_uuid text, '
    || 'p_expected_version bigint, '
    || 'p_stream_created_at timestamptz, '
    || 'p_event_ids uuid[], '
    || 'p_event_types text[], '
    || 'p_causation_ids ' || v_causation_id_type || '[], '
    || 'p_correlation_ids ' || v_correlation_id_type || '[], '
    || 'p_data ' || v_data_type || '[], '
    || 'p_metadata ' || v_data_type || '[], '
    || 'p_created_at timestamptz[], '
    || 'p_tag_event_indexes bigint[], '
    || 'p_tags text[]'
    || ') RETURNS bigint LANGUAGE plpgsql VOLATILE '
    || 'SET search_path TO pg_catalog, pg_temp AS $fn$' || v_body || '$fn$';

  -- Record schema migration
  INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (1, 5, 0);
END;
$migration$ LANGUAGE plpgsql;
