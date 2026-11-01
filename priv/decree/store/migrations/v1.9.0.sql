DO $migration$
DECLARE
  v_data_type text;
  v_causation_id_type text;
  v_correlation_id_type text;
  v_body text := $body$
-- Appends events to one or more member streams, atomically, in one round trip and in
-- one lock order: every member's row, sorted by stream_uuid (FOR NO KEY UPDATE, or the
-- insert of a new row), then $all. $all is always the last lock an append takes, and
-- members are always taken in one order, so appends cannot cycle on these locks: a
-- holder of $all never waits on a stream's row. Batches sharing event ids in another
-- order can still meet on events_pkey, as they could in the statements this replaces.
--
-- The members are locked one statement at a time, because a single
-- WHERE ... = ANY(...) FOR NO KEY UPDATE has no documented lock order and this loop
-- has. The sort is bytewise and lives only here, so it does not depend on a store's
-- collation, and no pod can disagree with another about it. It covers new and existing
-- members alike: an insert's wait on the unique index of stream_uuid sits at the same
-- sorted position a row lock would, so racing creators keep the one order.
--
-- Name resolution: the body names tables unqualified, and the set_config below
-- points search_path at p_schema for this call. The attached SET search_path
-- clause is what scopes that: Postgres restores the session's value when the
-- function exits, on success and on a raise, also inside a caller's transaction.
-- So no store schema name is frozen in the catalog, and ALTER SCHEMA ... RENAME
-- needs nothing but the config change. SET search_path FROM CURRENT, or a schema
-- baked into this text, would freeze the name the function was created under.
--
-- The _v5 suffix is a contract: a later change adds a new version and never
-- replaces or drops this one, because an old pod still calls this signature.
--
-- Three OUT parameters say what this call wrote. o_stream_ids[n] and
-- o_stream_versions[n] are member n's stream id and its version after this call, in
-- the caller's order, and o_position is the last $all position this call wrote. The
-- members' two are set before $all, so what they add under the lock is nothing:
-- only o_position's assignment is new there.
--
-- The p_stream_* arrays are parallel, one entry per member in the caller's order:
-- p_stream_ids[n] NULL creates that stream, p_expected_versions[n] NULL means any
-- version, and p_stream_counts[n] is that member's events in this call, at least 1.
-- p_event_streams[k] is the 1-based member ordinal of event k, and p_event_offsets[k]
-- its 1-based ordinal within that member in this call. Positions in $all follow the
-- order of the events here, not the sort.
--
-- Tags travel as three parallel flat arrays, because Postgres arrays are rectangular:
-- p_tag_event_indexes[n] is the 1-based ordinal, within this call, of the event
-- p_tags[n] belongs to, and p_tag_event_types[n] is that event's type. The type comes
-- with the tag because a call has no bound on its events, and subscripting a text
-- array is linear in the array.
--
-- A condition is a compiled query and a position, p_after, whose NULL means a plain
-- append. It fails the append if any event after p_after matches. It is a statement
-- of its own right after the $all UPDATE, because each statement here takes a new
-- snapshot: its snapshot starts after $all is granted, so it sees every event
-- committed by an earlier holder of $all. Inside that UPDATE it would read a snapshot
-- from before the wait. It runs before this append's own rows exist, because its
-- live-event check reads stream_events.
--
-- The attached SET enable_seqscan pins the condition's plan. PL/pgSQL caches a generic
-- plan per session, and on a small analysed store that plan seq-scans stream_events
-- and event_tags inside the OFFSET 0 subplans. It outlives the tables' growth until an
-- ANALYZE invalidates it, and a store starts empty, so every pod would cache it and
-- run it under $all. Attached, the setting scopes to this function and is restored
-- on success and on a raise, also inside a caller's transaction, as search_path is.
DECLARE
  v_count bigint := cardinality(p_event_ids);
  v_stream_ids bigint[] := p_stream_ids;
  v_versions bigint[] := array_fill(0::bigint, ARRAY[cardinality(p_stream_uuids)]);
  v_member integer;
  v_stream_id bigint;
  v_version bigint;
  v_all_version bigint;
  v_stale boolean := false;
  v_stamp timestamptz;
  v_conflict boolean;
BEGIN
  IF current_setting('transaction_isolation') <> 'read committed' THEN
    RAISE EXCEPTION 'append_events_v5 needs READ COMMITTED, not %',
      current_setting('transaction_isolation')
      USING ERRCODE = 'invalid_transaction_state';
  END IF;

  PERFORM set_config('search_path', quote_ident(p_schema) || ', pg_temp', true);

  o_stream_versions := array_fill(0::bigint, ARRAY[cardinality(p_stream_uuids)]);

  -- Every arm writes its member's row here, so the members' event_notification NOTIFYs
  -- fire in this order, before the one for $all. A stale member writes no row, and the
  -- raise below takes back any NOTIFY an earlier member queued.
  FOR v_member IN
    SELECT member.index
    FROM unnest(p_stream_uuids) WITH ORDINALITY AS member (stream_uuid, index)
    ORDER BY member.stream_uuid COLLATE "C"
  LOOP
    IF p_stream_ids[v_member] IS NULL THEN
      INSERT INTO streams (stream_uuid, stream_version, created_at)
      VALUES (
        p_stream_uuids[v_member],
        p_stream_counts[v_member],
        COALESCE(p_stream_created_at, now())
      )
      RETURNING stream_id INTO v_stream_id;

      v_stream_ids[v_member] := v_stream_id;
    ELSE
      SELECT stream_version INTO v_version
      FROM streams
      WHERE stream_id = p_stream_ids[v_member]
      FOR NO KEY UPDATE;

      -- A raise, because RETURN NULL would be a successful statement, and in
      -- autocommit it would commit a new member's row inserted earlier in this loop.
      -- So this race inside a caller's transaction aborts it, as a stale append does.
      IF NOT FOUND THEN
        RAISE EXCEPTION 'stream row is gone' USING ERRCODE = 'ZD002';
      END IF;

      v_versions[v_member] := v_version;

      -- Decided here, on the locked row, and answered after the events insert, whose
      -- unique index answers a duplicate event id first, stored or in flight.
      v_stale := p_expected_versions[v_member] IS NOT NULL
        AND p_expected_versions[v_member] <> v_version;

      EXIT WHEN v_stale;

      UPDATE streams
      SET stream_version = stream_version + p_stream_counts[v_member]
      WHERE stream_id = p_stream_ids[v_member];
    END IF;

    o_stream_versions[v_member] := v_versions[v_member] + p_stream_counts[v_member];
  END LOOP;

  o_stream_ids := v_stream_ids;

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

  IF p_after IS NOT NULL THEN
    SELECT EXISTS (

SELECT probe.item, hit.position
FROM unnest(p_probe_items, p_probe_tags, p_probe_types) AS probe (item, tag, event_type)
CROSS JOIN LATERAL (
  (
    SELECT t1.position
    FROM event_tags t1
    WHERE probe.tag IS NOT NULL AND probe.event_type IS NOT NULL
      AND t1.tag = probe.tag COLLATE "C"
      AND t1.event_type = probe.event_type COLLATE "C"
      AND t1.position >= p_after + 1
      AND EXISTS (
        SELECT 1
        FROM stream_events se
        WHERE se.stream_id = 0 AND se.stream_version = t1.position
        OFFSET 0
      )
      AND NOT EXISTS (
        SELECT 1
        FROM unnest(p_rest_items, p_rest_tags) AS rest (item, tag)
        WHERE rest.item = probe.item
          AND NOT EXISTS (
            SELECT 1
            FROM event_tags t2
            WHERE t2.tag = rest.tag COLLATE "C" AND t2.position = t1.position
            OFFSET 0
          )
      )
    ORDER BY t1.position ASC
    LIMIT 1
  )
  UNION ALL
  (
    SELECT t1.position
    FROM event_tags t1
    WHERE probe.tag IS NOT NULL AND probe.event_type IS NULL
      AND t1.tag = probe.tag COLLATE "C"
      AND t1.position >= p_after + 1
      AND EXISTS (
        SELECT 1
        FROM stream_events se
        WHERE se.stream_id = 0 AND se.stream_version = t1.position
        OFFSET 0
      )
      AND NOT EXISTS (
        SELECT 1
        FROM unnest(p_rest_items, p_rest_tags) AS rest (item, tag)
        WHERE rest.item = probe.item
          AND NOT EXISTS (
            SELECT 1
            FROM event_tags t2
            WHERE t2.tag = rest.tag COLLATE "C" AND t2.position = t1.position
            OFFSET 0
          )
      )
    ORDER BY t1.position ASC
    LIMIT 1
  )
  UNION ALL
  (
    SELECT e1.position
    FROM event_types e1
    WHERE probe.tag IS NULL
      AND e1.event_type = probe.event_type COLLATE "C"
      AND e1.position >= p_after + 1
      AND EXISTS (
        SELECT 1
        FROM stream_events se
        WHERE se.stream_id = 0 AND se.stream_version = e1.position
        OFFSET 0
      )
    ORDER BY e1.position ASC
    LIMIT 1
  )
) AS hit

    ) INTO v_conflict;

    IF v_conflict THEN
      RAISE EXCEPTION 'append condition failed' USING ERRCODE = 'ZD001';
    END IF;
  END IF;

  INSERT INTO stream_events
    (event_id, stream_id, stream_version, original_stream_id, original_stream_version)
  SELECT
    new_events.event_id,
    target.stream_id,
    target.stream_version,
    v_stream_ids[new_events.member],
    v_versions[new_events.member] + new_events.member_offset
  FROM unnest(p_event_ids, p_event_streams, p_event_offsets) WITH ORDINALITY
    AS new_events (event_id, member, member_offset, index)
  CROSS JOIN LATERAL (
    VALUES
      (
        v_stream_ids[new_events.member],
        v_versions[new_events.member] + new_events.member_offset
      ),
      (0, v_all_version + new_events.index)
  ) AS target (stream_id, stream_version);

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
    new_tags.event_type,
    CASE WHEN starts_with(new_tags.tag, 'command:') THEN v_stamp END
  FROM unnest(p_tag_event_indexes, p_tags, p_tag_event_types)
    AS new_tags (index, tag, event_type);

  o_position := v_all_version + v_count;

  RETURN;
END;
$body$;
BEGIN
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

  -- Adds a version and drops none: an old pod still calls v1, v2, v3 and v4.
  EXECUTE
    'CREATE FUNCTION append_events_v5('
    || 'p_schema text, '
    || 'p_stream_ids bigint[], '
    || 'p_stream_uuids text[], '
    || 'p_expected_versions bigint[], '
    || 'p_stream_counts bigint[], '
    || 'p_stream_created_at timestamptz, '
    || 'p_event_streams bigint[], '
    || 'p_event_offsets bigint[], '
    || 'p_event_ids uuid[], '
    || 'p_event_types text[], '
    || 'p_causation_ids ' || v_causation_id_type || '[], '
    || 'p_correlation_ids ' || v_correlation_id_type || '[], '
    || 'p_data ' || v_data_type || '[], '
    || 'p_metadata ' || v_data_type || '[], '
    || 'p_created_at timestamptz[], '
    || 'p_tag_event_indexes bigint[], '
    || 'p_tags text[], '
    || 'p_tag_event_types text[], '
    || 'p_probe_items bigint[], '
    || 'p_probe_tags text[], '
    || 'p_probe_types text[], '
    || 'p_rest_items bigint[], '
    || 'p_rest_tags text[], '
    || 'p_after bigint, '
    || 'OUT o_stream_ids bigint[], '
    || 'OUT o_stream_versions bigint[], '
    || 'OUT o_position bigint'
    || ') RETURNS record LANGUAGE plpgsql VOLATILE '
    || 'SET search_path TO pg_catalog, pg_temp '
    || 'SET enable_seqscan TO off AS $fn$' || v_body || '$fn$';

  -- Record schema migration
  INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (1, 9, 0);
END;
$migration$ LANGUAGE plpgsql;
