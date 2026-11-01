DO $migration$
BEGIN
  -- `Migrate` sends a file as one statement, so both of these sit in one block. Finding a
  -- stream's tag rows by position is what a hard delete and `delete_stream_tags` need, and
  -- the tag-first primary key cannot serve it. No function changes: nothing here touches an
  -- append, and an old pod runs on against this index.
  CREATE INDEX ix_event_tags_position ON event_tags (position);

  -- Record schema migration
  INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (1, 8, 0);
END;
$migration$ LANGUAGE plpgsql;
