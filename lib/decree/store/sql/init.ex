defmodule Decree.Store.Sql.Init do
  @moduledoc false

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
      create_event_store_exception_function(),
      create_event_store_delete_function(),
      prevent_streams_delete(),
      prevent_event_delete(),
      prevent_event_update(),
      prevent_stream_events_delete(),
      prevent_stream_events_update(),
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
  # adds this version (priv/decree/store/migrations/v1.4.0.sql); the comment at its
  # top states the name resolution and the versioning contract.
  defp create_append_events_function(column_data_type, correlation_id_type, causation_id_type) do
    """
    CREATE FUNCTION append_events_v1(
      p_schema text,
      p_stream_id bigint,
      p_stream_uuid text,
      p_expected_version bigint,
      p_stream_created_at timestamptz,
      p_event_ids uuid[],
      p_event_types text[],
      p_causation_ids #{causation_id_type}[],
      p_correlation_ids #{correlation_id_type}[],
      p_data #{column_data_type}[],
      p_metadata #{column_data_type}[],
      p_created_at timestamptz[]
    ) RETURNS bigint
    LANGUAGE plpgsql VOLATILE
    SET search_path TO pg_catalog, pg_temp
    AS $$
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
    -- The _v1 suffix is a contract: a later change adds a new version and never
    -- replaces or drops this one, because an old pod still calls this signature.
    DECLARE
      v_count bigint := cardinality(p_event_ids);
      v_stream_id bigint := p_stream_id;
      v_stream_version bigint := 0;
      v_all_version bigint;
      v_stale boolean := false;
    BEGIN
      IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'append_events_v1 needs READ COMMITTED, not %',
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

      RETURN v_stream_id;
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
    VALUES (1, 4, 0);
    """
  end
end
