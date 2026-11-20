# Changelog

decree follows [Semantic Versioning](https://semver.org). Until 1.0, a minor version (0.2.0) may change the API or the store schema, and its entry here says what changed and how to upgrade. A patch version (0.1.1) does neither. Depend on `~> 0.1.0` to take patches only.

## Unreleased

Documentation and tests only: no API, behaviour or store schema change.

- **The examples name the decree application `MyApp.App`.** `Decree.Application`, `Decree.Aggregates.Aggregate`, `Decree.Event.Handler` and `Decree.PersonalFields.KeySource` called it `MyApp.Application`, the name of your OTP application module. Their module and config examples now compile as written.
- **The `project/2` and `project/3` examples run as written.** `Decree.Projections.Ecto` shows each as a complete projector on the moduledoc's read model, in place of a fragment that was not valid Elixir.
- **`Decree.Event.Handler`'s moduledoc examples compile as written.** They name the decree application `MyApp.App` and the README's `MyApp.AccountRegistered` event, in place of upstream placeholders such as `ExampleApp` and `%AnEvent{..}`. Three of them were a `CompileError` in decree: the stateful and the concurrent handler now name their event in `handle/2`'s head, and the batching handler declares `personal_fields: :opaque`, which a `handle_batch/1` handler needs.
- **Testing your application.** The README says what to use in place of test helpers, which decree does not ship: a test store on Postgres, dropped, created and initialised in your `test` alias, and each test's own streams.
- **decree's own test build fails on a warning.** Its test files compiled with 10 warnings on Elixir 1.20; they compile with none, and a warning in decree's own code now fails `mix test`. A build of decree as your dependency is unchanged.

## 0.1.1 (2026-11-20)

Documentation only: no API, behaviour or store schema change.

- **The `projection_versions` table.** `Decree.Projections.Ecto` now says that decree ships no migration for it, and gives one: the table a projector records its last projected event in, created in your repo's database before any projector starts. With `schema_prefix:`, each schema needs the table.
- **The projector example runs as written.** It is a complete projector for the README's minimal application, with its Ecto repo and config, its read model, its migration and the OTP application that starts it. It names the decree application `MyApp.App`, apart from the OTP application module `MyApp.Application`.
- **Check for an empty store before enabling the tag index.** The README and `Decree.Store.TagIndex` ("The lifecycle") give the call, `MyApp.EventStore.stream_info(:all)`, whose `stream_version` is 0 on an empty store, and say why it matters: decree accepts an enable on a store with history, and that enable vouches for tag rows nothing has checked.

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
- `mix decree.verify.eventless`.

### Changed from earlier commits

For an application that pinned a decree commit before this release:

- **The tag index's named families are the application's.** `Decree.Store.TagIndex.named_families/0` is now `command`, `command_kind` and `correlation`, then the kinds in `config :decree, :tag_index_named_families`. It used to name four more kinds, `tenant_slug`, `plan_slug`, `email` and `oauth_uid`. An application whose decisions rely on those as families of their own keeps them with one config line:

  ```elixir
  config :decree, :tag_index_named_families, ["tenant_slug", "plan_slug", "email", "oauth_uid"]
  ```

  Without that line, a tag of one of those kinds falls into the `entity` family, and a decision requires `entity`'s fact in place of the kind's own. Where `entity` is not enabled, the decision refuses with `{:error, :tag_index_not_enabled}`. Where it is enabled, the decision goes on, and a later disable of the kind's own family no longer stops it, with no error.

- **`consistency:` is removed.** decree is eventually consistent: a dispatch returns once its events are written, and an event handler catches up on its own, so the command path never waits on a handler's health. `Decree.Middleware.ConsistencyGuarantee` is gone from the default middleware, with the framework's subscription registry that tracked handler acks, `%Decree.Middleware.Pipeline{}`'s `consistency` field and the `{:error, :consistency_timeout}` answer. Every door that took the option now refuses it, whatever its value, with an `ArgumentError`:

  - at build: the `consistency:` option on `use Decree.Event.Handler` (and `use Decree.Projections.Ecto`), on a `dispatch` or `decide` route, and in `default_dispatch_opts:` on `use Decree.Application` or `use Decree.Commands.CompositeRouter`, and `default_consistency:` on `use Decree.Commands.Router`;
  - at run time: `consistency:` given to `dispatch/2`, or to an event handler's `start_link/1` or returned from its `init/1`;
  - at start: `config :decree, :default_consistency` or `:dispatch_consistency_timeout`.

  To upgrade, delete the option and the config keys. A caller that needs its own write back reads it from the dispatch's answer (`returning:`), and a view that needs a handler's result is pushed it from the handler's event.

- **The application's `pubsub:` option is removed.** Nothing in decree published on it once `consistency:` went. `Decree.PubSub`, its `Adapter` behaviour, `Decree.PubSub.LocalPubSub` and `Decree.PubSub.PhoenixPubSub` are gone, with the pub-sub child an application started and the optional `phoenix_pubsub` dependency. The store's `Decree.Store.PubSub` is unchanged. `pubsub:`, whatever its value, now raises an `ArgumentError`:

  - at build: on `use Decree.Application`, or in the application's config (`config :my_app, MyApp.App, pubsub: ...`);
  - at start: given to the application's `start_link/1`, or returned from its `init/1`.

  To upgrade, delete the option. An application that broadcasts its own messages starts its own `Phoenix.PubSub` and depends on `phoenix_pubsub` itself.

- **Elixir 1.20 or later.** `mix.exs` required `~> 1.18`; it now requires `~> 1.20`, the version CI tests.
