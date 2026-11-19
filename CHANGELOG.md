# Changelog

decree follows [Semantic Versioning](https://semver.org). Until 1.0, a minor version (0.2.0) may change the API or the store schema, and its entry here says what changed and how to upgrade. A patch version (0.1.1) does neither. Depend on `~> 0.1.0` to take patches only.

## 0.1.0

The first release on Hex. decree is pre-1.0 and has one production user, sparkle.space.

### What is in it

- **The store** (`Decree.Store`): a Postgres event store. Streams, append with expected version, snapshots, persistent and transient subscriptions, schemas (stream prefixes), tags on events, queries over tags and types, and the conditional append. The Mix tasks `decree.store.create`, `decree.store.init`, `decree.store.drop`, `decree.store.migrate` and `decree.store.migrations`.
- **The framework** (`Decree`): applications, command routing and dispatch, aggregates and snapshots, decision rules (`Decree.Decision.Rule`), decisions that own their streams (`Decree.Decision`), event handlers, middleware, upcasting, personal fields with per-subject keys, telemetry, Ecto read-model projections (`Decree.Projections.Ecto`), and the Mix task `decree.verify.eventless`.

### What is proven, and what is not

sparkle.space runs these in production: the store and its migrations, aggregates and snapshots, the command router and middleware, event handlers, Ecto projections, upcasting, personal fields, and decision rules on an aggregate's own stream (`Decree.Decision.Rule`).

These are built and tested, and no production application uses them yet:

- decisions that own their streams (`Decree.Decision`);
- the multi-stream conditional append (`append_to_streams`);
- the command window (`Decree.Aggregate`, "Command window");
- `mix decree.verify.eventless`;
- `:strong` dispatch consistency.

### Changed from earlier commits

For an application that pinned a decree commit before this release:

- **The tag index's named families are the application's.** `Decree.Store.TagIndex.named_families/0` is now `command`, `command_kind` and `correlation`, then the kinds in `config :decree, :tag_index_named_families`. It used to name four more kinds, `tenant_slug`, `plan_slug`, `email` and `oauth_uid`. An application whose decisions rely on those as families of their own keeps them with one config line:

  ```elixir
  config :decree, :tag_index_named_families, ["tenant_slug", "plan_slug", "email", "oauth_uid"]
  ```

  Without that line, a tag of one of those kinds falls into the `entity` family, and a decision requires `entity`'s fact in place of the kind's own. Where `entity` is not enabled, the decision refuses with `{:error, :tag_index_not_enabled}`. Where it is enabled, the decision goes on, and a later disable of the kind's own family no longer stops it, with no error.

- **Elixir 1.20 or later.** `mix.exs` required `~> 1.18`; it now requires `~> 1.20`, the version CI tests.
