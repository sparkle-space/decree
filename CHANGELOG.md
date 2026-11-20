# Changelog

decree follows [Semantic Versioning](https://semver.org). Until 1.0, a minor version (0.2.0) may change the API or the store schema, and its entry here says what changed and how to upgrade. A patch version (0.1.1) does neither. Depend on `~> 0.1.0` to take patches only.

## Unreleased

New public API in `Decree.Store.TagIndex` and `Decree.Decision.Index`, so this is a minor release. Nothing is removed or changed: an application on 0.1.x compiles and runs as before, and no store schema changes.

- **The tag-index fold is public, and shared.** `Decree.Store.TagIndex.fold/1` is the fold every decision and rule dispatch runs over the facts, moved out of decree's internals. An application's release check now reads the facts exactly as a dispatch does, where a copied fold could disagree: one application's copy answered `:verified` on a fact naming one family both ways, where every dispatch refuses.
- **`states/2` and `missing/2`.** `states/2` reads every fact on a store and folds it: `{:ok, %{{family, key} => :enabled | :disabled}}`, or `{:error, :unattributable_tag_index_fact}` for a fact whose state tags the read leaves out, which a dispatch refuses too. `missing/2` answers the requirements, `family` or `{family, key}`, that are not enabled.
- **`bootstrap/3`, the release step.** On an empty store it calls your `enable` once with the families you list and answers `:enabled` once they read as enabled. On a store with history it never calls `enable`: it answers `:verified`, or `{:error, {:tag_index_not_enabled, missing}}` for a family that is missing. It refuses an empty, repeated or malformed list with `{:error, :invalid_tag_index_requirements}` before it reads anything. Its answer covers the families you list, not every family your rules read; `enable` must make one atomic append; and no other writer may run until it answers. `bootstrap/4` takes the store's `name:` too.
- **The README's minimal application uses it.** Its ledger enables every family in one fact (`MyApp.EnableTagIndexes`), a resend answers `{:error, :duplicate_command}` and appends nothing, and its release step is one `bootstrap/3` call. The README's ledger enabled one family per fact and had no duplicate clause, so a resend appended a second fact.
- **`Decree.Decision.Index.requirements/3` is public.** It answers the families and keys a rule's or a decision's items require, in `bootstrap/3`'s grammar: `command` first for a dispatch that reads a command item, then each family once, a keyless one bare and a digest one as `{family, key}`. An application's test can now check its rules against the list its release step gives `bootstrap/3`, with no store, where before a rule reading a family the list left out was found only when its dispatch refused. The README's "Testing your application" has that test, and its ledger names the list once, `MyApp.TagIndexLedger.families/0`, which the release step and the test both read.
- **The README's Status names the series, "(version 0.1.x)".** It said "version 0.1.0" at 0.1.2. A minor or major release now moves it, the README's install requirement and this file's `~> X.Y.0` with the version (`scripts/release.exs bump`), so a release PR passes CI as generated.

### Upgrading from 0.1.x

Move to `~> 0.2.0`; nothing else is required. To adopt the check, replace your own empty-store check, enable and history check with one `bootstrap/3` call, as the README's [minimal application](README.md#a-minimal-application) does in its release step. Keep your own fact, command and ledger: decree still ships none. Your `enable` must enable every family in one append, as one command whose events land in one batch. Delete any fold of the facts you copied; `states/2` and `missing/2` replace it. On a store with history, a step that did nothing there before now checks the families you list and fails on a missing one, so list only the families your store has enabled, or repair the missing one first ("The lifecycle" in `Decree.Store.TagIndex`). To catch a rule that reads a family your list leaves out, add a test like the README's, with `Decree.Decision.Index.requirements/3`.

## 0.1.2 (2026-11-20)

Documentation and tests only: no API, behaviour or store schema change.

- **The examples name the decree application `MyApp.App`.** `Decree.Application`, `Decree.Aggregates.Aggregate`, `Decree.Event.Handler` and `Decree.PersonalFields.KeySource` called it `MyApp.Application`, the name of your OTP application module. Their module and config examples now compile as written.
- **The `project/2` and `project/3` examples run as written.** `Decree.Projections.Ecto` shows each as a complete projector on the moduledoc's read model, in place of a fragment that was not valid Elixir.
- **`Decree.Event.Handler`'s moduledoc examples compile as written.** They name the decree application `MyApp.App` and the README's `MyApp.AccountRegistered` event, in place of upstream placeholders such as `ExampleApp` and `%AnEvent{..}`. Three of them were a `CompileError` in decree: the stateful and the concurrent handler now name their event in `handle/2`'s head, and the batching handler declares `personal_fields: :opaque`, which a `handle_batch/1` handler needs.
- **`Decree.Event.Handler`'s callback examples compile as written too.** The `after_start/1`, `init/1` and `error/3` examples, and the `child_spec/1` example every handler's docs carry, name `MyApp.ExampleHandler`, `MyApp.App` and `MyApp.AccountRegistered`. The `init/1` example sets the handler's application to the name a tenant's application was started under, as "Dynamic application" does, in place of a module per tenant. The moduledoc now says state can be updated from `handle_batch/1`, where it said `handle_batch/2`.
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
