defmodule Decree.Store.Sql.Init do
  @moduledoc false

  alias Decree.Store.Sql.Statements

  # PostgreSQL statements to intialize an event store schema.

  def statements(config) do
    column_data_type = Keyword.fetch!(config, :column_data_type)
    correlation_id_type = Keyword.fetch!(config, :correlation_id_type)
    causation_id_type = Keyword.fetch!(config, :causation_id_type)
    schema = Keyword.fetch!(config, :schema)

    [
      ~s(SET LOCAL search_path TO "#{schema}";),
      create_streams_table(),
      create_stream_uuid_index(),
      create_events_table(column_data_type, correlation_id_type, causation_id_type),
      create_stream_events_table(),
      create_stream_events_index(),
      create_event_tags_table(),
      create_event_tags_index(),
      create_event_tags_position_index(),
      create_event_types_table(),
      create_event_store_exception_function(),
      create_event_store_delete_function(),
      prevent_streams_delete(),
      prevent_event_delete(),
      prevent_event_update(),
      prevent_stream_events_delete(),
      prevent_stream_events_update(),
      prevent_event_tags_delete(),
      prevent_event_tags_update(),
      prevent_event_types_delete(),
      prevent_event_types_update(),
      create_notify_events_function(),
      create_append_events_function(column_data_type, correlation_id_type, causation_id_type),
      seed_all_stream(),
      create_event_notification_trigger(),
      create_subscriptions_table(),
      create_subscription_index(),
      create_snapshots_table(column_data_type),
      create_schema_migrations_table(),
      record_event_store_schema_version()
    ]
  end

  defp create_streams_table do
    """
    CREATE TABLE streams
    (
        stream_id bigserial PRIMARY KEY NOT NULL,
        stream_uuid text NOT NULL,
        stream_version bigint default 0 NOT NULL,
        created_at timestamp with time zone DEFAULT NOW() NOT NULL,
        deleted_at timestamp with time zone
    );
    """
  end

  defp create_stream_uuid_index do
    """
    CREATE UNIQUE INDEX ix_streams_stream_uuid ON streams (stream_uuid);
    """
  end

  # Create `$all` stream
  defp seed_all_stream do
    """
    INSERT INTO streams (stream_id, stream_uuid, stream_version) VALUES (0, '$all', 0);
    """
  end

  defp create_events_table(column_data_type, correlation_id_type, causation_id_type) do
    """
    CREATE TABLE events
    (
        event_id uuid PRIMARY KEY NOT NULL,
        event_type text NOT NULL,
        causation_id #{causation_id_type} NULL,
        correlation_id #{correlation_id_type} NULL,
        data #{column_data_type} NOT NULL,
        metadata #{column_data_type} NULL,
        created_at timestamp with time zone DEFAULT NOW() NOT NULL
    );
    """
  end

  defp create_event_store_exception_function do
    """
    CREATE OR REPLACE FUNCTION event_store_exception()
      RETURNS trigger AS $$
    DECLARE
      message text;
    BEGIN
      message := 'EventStore: ' || TG_ARGV[0];

      RAISE EXCEPTION USING MESSAGE = message;
    END;
    $$ LANGUAGE plpgsql;
    """
  end

  # Prevent DELETE operations unless hard deletes have been enabled.
  defp create_event_store_delete_function do
    """
    CREATE OR REPLACE FUNCTION event_store_delete()
      RETURNS trigger AS $$
    DECLARE
      message text;
    BEGIN
      IF current_setting('eventstore.enable_hard_deletes', true) = 'on' OR
        current_setting('eventstore.reset', true) = 'on'
      THEN
        -- Allow DELETE
        RETURN OLD;
      ELSE
        -- Prevent DELETE
        message := 'EventStore: ' || TG_ARGV[0];

        RAISE EXCEPTION USING MESSAGE = message, ERRCODE = 'feature_not_supported';
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
  end

  # prevent updates to `events` table
  defp prevent_event_update do
    """
    CREATE TRIGGER no_update_events
    BEFORE UPDATE ON events
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_exception('Cannot update events');
    """
  end

  # prevent deletion from `events` table
  defp prevent_event_delete do
    """
    CREATE TRIGGER no_delete_events
    BEFORE DELETE ON events
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_delete('Cannot delete events');
    """
  end

  defp create_stream_events_table do
    """
    CREATE TABLE stream_events
    (
      event_id uuid NOT NULL REFERENCES events (event_id),
      stream_id bigint NOT NULL REFERENCES streams (stream_id),
      stream_version bigint NOT NULL,
      original_stream_id bigint REFERENCES streams (stream_id),
      original_stream_version bigint,
      PRIMARY KEY(event_id, stream_id)
    );
    """
  end

  defp create_stream_events_index do
    """
    CREATE UNIQUE INDEX ix_stream_events ON stream_events (stream_id, stream_version);
    """
  end

  # prevent updates to `stream_events` table
  defp prevent_stream_events_update do
    """
    CREATE TRIGGER no_update_stream_events
    BEFORE UPDATE ON stream_events
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_exception('Cannot update stream events');
    """
  end

  # prevent deletion from `stream_events` table
  def prevent_stream_events_delete do
    """
    CREATE TRIGGER no_delete_stream_events
    BEFORE DELETE ON stream_events
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_delete('Cannot delete stream events');
    """
  end

  # Neither `event_tags` nor `event_types` has a foreign key: a row of either carries a
  # position, and `hard_delete_stream` removes it by that position in the statement that
  # removes the event. Their text columns hold machine identifiers matched by equality, so
  # bytewise comparison keeps index work under `$all` cheap and independent of the host's
  # locale.
  defp create_event_tags_table do
    """
    CREATE TABLE event_tags
    (
        position bigint NOT NULL,
        tag text COLLATE "C" NOT NULL,
        event_type text COLLATE "C" NOT NULL,
        stamped_at timestamp with time zone NULL,
        PRIMARY KEY(tag, position),
        CONSTRAINT event_tags_tag_length CHECK (octet_length(tag) <= 512)
    );
    """
  end

  defp create_event_tags_index do
    """
    CREATE INDEX ix_event_tags_tag_event_type_position ON event_tags (tag, event_type, position);
    """
  end

  # Finds a stream's tag rows by position, for `hard_delete_stream` and
  # `delete_stream_tags`. The primary key leads with the tag, so it cannot.
  defp create_event_tags_position_index do
    """
    CREATE INDEX ix_event_tags_position ON event_tags (position);
    """
  end

  defp create_event_types_table do
    """
    CREATE TABLE event_types
    (
        event_type text COLLATE "C" NOT NULL,
        position bigint NOT NULL,
        PRIMARY KEY(event_type, position)
    );
    """
  end

  # prevent updates to `event_tags` table
  defp prevent_event_tags_update do
    """
    CREATE TRIGGER no_update_event_tags
    BEFORE UPDATE ON event_tags
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_exception('Cannot update event tags');
    """
  end

  # prevent deletion from `event_tags` table
  defp prevent_event_tags_delete do
    """
    CREATE TRIGGER no_delete_event_tags
    BEFORE DELETE ON event_tags
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_delete('Cannot delete event tags');
    """
  end

  # prevent updates to `event_types` table
  defp prevent_event_types_update do
    """
    CREATE TRIGGER no_update_event_types
    BEFORE UPDATE ON event_types
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_exception('Cannot update event types');
    """
  end

  # prevent deletion from `event_types` table
  defp prevent_event_types_delete do
    """
    CREATE TRIGGER no_delete_event_types
    BEFORE DELETE ON event_types
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_delete('Cannot delete event types');
    """
  end

  def prevent_streams_delete do
    """
    CREATE TRIGGER no_delete_streams
    BEFORE DELETE ON streams
    FOR EACH STATEMENT
    EXECUTE PROCEDURE event_store_delete('Cannot delete streams');
    """
  end

  defp create_notify_events_function do
    """
    CREATE OR REPLACE FUNCTION notify_events()
      RETURNS trigger AS $$
    DECLARE
      old_stream_version bigint;
      channel text;
      payload text;
    BEGIN
        -- Payload text contains:
        --  * `stream_uuid`
        --  * `stream_id`
        --  * first `stream_version`
        --  * last `stream_version`
        -- Each separated by a comma (e.g. 'stream-12345,1,1,5')

        IF TG_OP = 'UPDATE' THEN
          old_stream_version := OLD.stream_version + 1;
        ELSE
          old_stream_version := 1;
        END IF;

        channel := TG_TABLE_SCHEMA || '.events';
        payload := NEW.stream_uuid || ',' || NEW.stream_id || ',' || old_stream_version || ',' || NEW.stream_version;

        -- Notify events to listeners
        PERFORM pg_notify(channel, payload);

        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    """
  end

  # The append function, in its latest version only: a fresh store carries no older
  # one. The signature follows the `events` columns, because Postgrex encodes from
  # the declared types. The body must stay byte-identical to the migration that
  # adds this version (priv/decree/store/migrations/v1.9.0.sql); the comment at its
  # top states the name resolution and the versioning contract. The condition is
  # rendered from the one text that says what a query matches, and the migration
  # embeds the same bytes, so a change to that template fails their comparison.
  defp create_append_events_function(column_data_type, correlation_id_type, causation_id_type) do
    condition =
      Statements.query_positions(
        "",
        "p_probe_items",
        "p_probe_tags",
        "p_probe_types",
        "p_rest_items",
        "p_rest_tags",
        "p_after + 1",
        "1",
        ">=",
        "ASC"
      )
      |> IO.iodata_to_binary()

    """
    CREATE FUNCTION append_events_v5(
      p_schema text,
      p_stream_ids bigint[],
      p_stream_uuids text[],
      p_expected_versions bigint[],
      p_stream_counts bigint[],
      p_stream_created_at timestamptz,
      p_event_streams bigint[],
      p_event_offsets bigint[],
      p_event_ids uuid[],
      p_event_types text[],
      p_causation_ids #{causation_id_type}[],
      p_correlation_ids #{correlation_id_type}[],
      p_data #{column_data_type}[],
      p_metadata #{column_data_type}[],
      p_created_at timestamptz[],
      p_tag_event_indexes bigint[],
      p_tags text[],
      p_tag_event_types text[],
      p_probe_items bigint[],
      p_probe_tags text[],
      p_probe_types text[],
      p_rest_items bigint[],
      p_rest_tags text[],
      p_after bigint,
      OUT o_stream_ids bigint[],
      OUT o_stream_versions bigint[],
      OUT o_position bigint
    ) RETURNS record
    LANGUAGE plpgsql VOLATILE
    SET search_path TO pg_catalog, pg_temp
    SET enable_seqscan TO off
    AS $$
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
    #{condition}
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
    $$;
    """
  end

  defp create_event_notification_trigger do
    """
    CREATE TRIGGER event_notification
    AFTER INSERT OR UPDATE ON streams
    FOR EACH ROW EXECUTE PROCEDURE notify_events();
    """
  end

  defp create_subscriptions_table do
    """
    CREATE TABLE subscriptions
    (
        subscription_id bigserial PRIMARY KEY NOT NULL,
        stream_uuid text NOT NULL,
        subscription_name text NOT NULL,
        last_seen bigint NULL,
        created_at timestamp with time zone DEFAULT NOW() NOT NULL
    );
    """
  end

  defp create_subscription_index do
    """
    CREATE UNIQUE INDEX ix_subscriptions_stream_uuid_subscription_name ON subscriptions (stream_uuid, subscription_name);
    """
  end

  defp create_snapshots_table(column_data_type) do
    """
    CREATE TABLE snapshots
    (
        source_uuid text PRIMARY KEY NOT NULL,
        source_version bigint NOT NULL,
        source_type text NOT NULL,
        data #{column_data_type} NOT NULL,
        metadata #{column_data_type} NULL,
        created_at timestamp with time zone DEFAULT NOW() NOT NULL
    );
    """
  end

  # record execution of upgrade scripts
  defp create_schema_migrations_table do
    """
    CREATE TABLE schema_migrations
    (
        major_version int NOT NULL,
        minor_version int NOT NULL,
        patch_version int NOT NULL,
        migrated_at timestamp with time zone DEFAULT NOW() NOT NULL,
        PRIMARY KEY(major_version, minor_version, patch_version)
    );
    """
  end

  # record current event store schema version
  defp record_event_store_schema_version do
    """
    INSERT INTO schema_migrations (major_version, minor_version, patch_version)
    VALUES (1, 9, 0);
    """
  end
end
