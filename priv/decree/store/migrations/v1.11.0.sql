DO $migration$
BEGIN
  -- Contracts the one-row `snapshots` table that 1.10.0 left beside `versioned_snapshots`.
  -- Safe now: since release 1 of sparkle-space/sparklespace#8200 no code path reads, writes
  -- or deletes from it, so neither the pod of the previous release nor this one touches it
  -- during a rollout. Any rows still in it go with it; nothing reads them.
  --
  -- `Migrate` sends a file as one statement, so this is one block. `snapshots` resolves
  -- through the `SET LOCAL search_path` Migrate runs, which names the store's schema only,
  -- so a same-named table in another schema is out of reach. No CASCADE: nothing depends on
  -- the table, and a dependency would fail the migration rather than go with it.
  DROP TABLE IF EXISTS snapshots;

  -- Record schema migration
  INSERT INTO schema_migrations (major_version, minor_version, patch_version) VALUES (1, 11, 0);
END;
$migration$ LANGUAGE plpgsql;
