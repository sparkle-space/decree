DO $migration$
DECLARE
  v_data_type text;
BEGIN
  -- One snapshot row per source and snapshot version, beside the one-row `snapshots` table.
  -- Expand only: an old pod keeps reading and writing `snapshots`, and this release reads
  -- and writes only `versioned_snapshots`, so neither overwrites the other's row during a
  -- rollout. `snapshots` is not altered here; a later release contracts it.
  --
  -- `Migrate` sends a file as one statement, so this is one block. `snapshots` resolves
  -- through the `SET LOCAL search_path` Migrate runs. Its column type differs per store, so
  -- the new table takes it from there, as `Decree.Store.Sql.Init` takes it from the config.
  --
  -- A row belongs to its stream: the foreign key's cascade deletes it with the stream's row,
  -- so an old pod's hard delete, which knows nothing of this table, leaves none behind.
  SELECT format_type(atttypid, atttypmod) INTO STRICT v_data_type
  FROM pg_attribute
  WHERE attrelid = 'snapshots'::regclass AND attname = 'data';

  EXECUTE
    'CREATE TABLE versioned_snapshots'
    || '('
    || 'source_uuid text NOT NULL, '
    || 'snapshot_version bigint NOT NULL, '
    || 'source_version bigint NOT NULL, '
    || 'source_type text NOT NULL, '
    || 'data ' || v_data_type || ' NOT NULL, '
    || 'metadata ' || v_data_type || ' NULL, '
    || 'created_at timestamp with time zone DEFAULT NOW() NOT NULL, '
    || 'PRIMARY KEY (source_uuid, snapshot_version), '
    || 'CONSTRAINT fk_versioned_snapshots_stream FOREIGN KEY (source_uuid) '
    || 'REFERENCES streams (stream_uuid) ON DELETE CASCADE'
    || ')';

  -- Record schema migration
  INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (1, 10, 0);
END;
$migration$ LANGUAGE plpgsql;
