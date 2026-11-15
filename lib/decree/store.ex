defmodule Decree.Store do
  @moduledoc """
  Decree.Store allows you to define one or more event store modules to append,
  read, and subscribe to streams of events.

  It uses PostgreSQL (v18) as the underlying storage engine.

  ## Defining an event store

  An event store module is defined in your own application as follows:

      defmodule MyApp.EventStore do
        use Decree.Store, otp_app: :my_app

        # Optional `init/1` function to modify config at runtime.
        def init(config) do
          {:ok, config}
        end
      end

  Where the configuration for the event store must be in your application
  environment, usually defined in `config/config.exs`:

      config :my_app, MyApp.EventStore,
        serializer: Decree.Store.JsonSerializer,
        username: "postgres",
        password: "postgres",
        database: "eventstore",
        hostname: "localhost"

  Or use a URL to connect instead:

      config :my_app, MyApp.EventStore,
        serializer: Decree.Store.JsonSerializer,
        url: "postgres://postgres:postgres@localhost/eventstore"

  **Note:** To use a Decree.Store with Decree you should configure the event
  store to use Decree's JSON serializer which provides additional support for
  JSON decoding:

      config :my_app, MyApp.EventStore,
        serializer: Decree.Serialization.JsonSerializer

  The event store module defines a `start_link/1` function that needs to be
  invoked before using the event store. In general, this function is not
  called directly, but included as part of your application supervision tree.

  If your application was generated with a supervisor (by passing `--sup`
  to `mix new`) you will have a `lib/my_app/application.ex` file
  containing the application start callback that defines and starts your
  supervisor. You just need to edit the `start/2` function to start the event
  store in your application's supervisor:

        def start(_type, _args) do
          children = [
            MyApp.EventStore
          ]

          opts = [strategy: :one_for_one, name: MyApp.Supervisor]
          Supervisor.start_link(children, opts)
        end

  Each event store module (e.g. `MyApp.EventStore`) provides a public API to
  read events from and write events to an event stream, and subscribe to event
  notifications.

  ## Postgres schema

  By default the `public` schema will be used for event store tables. An event
  store can be configured to use an alternate Postgres schema:

      defmodule MyApp.EventStore do
        use Decree.Store, otp_app: :my_app, schema: "schema_name"
      end

  Or provide the schema as an option in the `init/1` callback function:

      defmodule MyApp.EventStore do
        use Decree.Store, otp_app: :my_app

        def init(config) do
          {:ok, Keyword.put(config, :schema, "schema_name")}
        end
      end

  Or define it in environment config when configuring the database connection
  settings:

      # config/config.exs
      config :my_app, MyApp.EventStore, schema: "schema_name"

  This feature allows you to define and start multiple event stores sharing a
  single Postgres database, but with their data isolated and segregated by
  schema.

  Note the `mix event_store.<task>` tasks to create, initialize, and drop an
  event store database will also handle creating and/or dropping the schema.

  ## Dynamic named event store

  An event store can be started multiple times by providing a name when
  starting. The name must be provided as an option to all event store operations
  to identify the correct instance.

  ### Example

  Define an event store:

      defmodule MyApp.EventStore do
        use Decree.Store, otp_app: :my_app
      end

  Start multiple instances of the event store, each with a unique name:

      {:ok, _pid} = Decree.Store.start_link(name: :eventstore1)
      {:ok, _pid} = Decree.Store.start_link(name: :eventstore2)
      {:ok, _pid} = Decree.Store.start_link(name: :eventstore3)

  Use a dynamic event store by providing its name as an option to each function:

      :ok = Decree.Store.append_to_stream(stream_uuid, expected_version, events, name: :eventstore1)

      {:ok, events} = Decree.Store.read_stream_forward(stream_uuid, 0, 1_000, name: :eventstore1)

  ## Dynamic schemas

  This feature also allows you to start each event store instance using a
  different schema:

      {:ok, _pid} = Decree.Store.start_link(name: :tenant1, schema: "tenant1")
      {:ok, _pid} = Decree.Store.start_link(name: :tenant2, schema: "tenant2")

  Or start supervised:

      children =
        for tenant <- [:tenant1, :tenant2, :tenant3] do
          {MyApp.EventStore, name: tenant, schema: "#\{tenant\}"}
        end

      opts = [strategy: :one_for_one, name: MyApp.Supervisor]

      Supervisor.start_link(children, opts)

  The above can be used for multi-tenancy where the data for each tenant is
  stored in a separate, isolated schema.

  ## Using an existing database connection or transaction

  In some situations you might want to execute the event store operations using
  an existing Postgres database connection or transaction. For instance, if you
  want to persist changes to one or more other tables, such as a read-model
  projection.

  To do this you can provide a Postgrex connection process or transaction as a
  `:conn` option to any of the supported `Decree.Store` functions.

      {:ok, pid} = Postgrex.start_link(config)

      Postgrex.transaction(pid, fn conn ->
        :ok = Decree.Store.append_to_stream(stream_uuid, expected_version, events, conn: conn)
      end)

  This can also be used with an Ecto `Repo` which is configured to use the
  Postgres SQL adapter. The connection process may be looked up as follows:

      Repo.transaction(fn ->
        %{pid: pool} = Ecto.Adapter.lookup_meta(Repo)

        conn = Process.get({Ecto.Adapters.SQL, pool})

        :ok = Decree.Store.append_to_stream(stream_uuid, expected_version, events, conn: conn)
      end)

  Two appends to different streams in one such transaction take the second
  stream's row after `$all`, which breaks the lock order every other append
  follows, so that caller can see a `deadlock_detected` `Decree.Store.AppendError`.
  `c:append_to_streams/2` takes both in one call and keeps the order.

  An append refuses any isolation level but READ COMMITTED: in a `REPEATABLE READ`
  or `SERIALIZABLE` transaction it answers a `Decree.Store.AppendError` with class
  `:rejected` and code `:invalid_transaction_state`.

  ## Reading by query

  `c:read_query/2` reads the events matching a `Decree.Store.Query` across every
  stream, in one SQL statement, and answers a `Decree.Store.ReadResult`:

      items = [[tags: ["order:1"]], [types: ["OrderPlaced"], tags: ["tenant:a"]]]

      {:ok, %Decree.Store.ReadResult{events: events, after: head}} =
        MyApp.EventStore.read_query(items)

  One statement has one snapshot, so the events, `after` and `read_at` agree with each
  other. The read takes no lock and blocks no writer.

  `after` is the `$all` head at the read's snapshot, not the last match. Over a complete
  read it says that no event up to `after` matches other than those returned, which
  makes it safe as the position of a later condition. A read is partial, and its
  `after` is not safe for that, when `count` cut it (it returned `count` events) or when
  it ran with any `from` but the direction's default.

  A position with no event is a permanent hole, never one still pending. The `$all` bump
  and every row carrying a position commit together while holding `$all`, so one
  snapshot never sees a position past one that later fills.

  What a read sees is `$all`:

    - A hard-deleted event is gone, and a hard delete removes its tag and type rows. A
      row left by an old pod's hard delete, or by a raw delete of events, is ignored, so
      it uses up no `count`. `after` does not fall.
    - A soft-deleted stream's events are still returned.
    - A link row a store still holds from before links were removed is not seen. The
      original event is, once, with its original stream.

  **The coverage gap.** A query matches rows of the `event_tags` and `event_types`
  tables. An event from before store migration 1.5.0, or one an old pod appended through
  `append_events_v1`, has no such rows, so a query silently misses it, and the read
  cannot cheaply detect that: `after` still counts it. A store that starts empty after
  1.5.0 has no such event; any other store needs its events backfilled into both tables
  before a query's answer can be trusted to be complete. W-1 starts from an empty store,
  and the plan's §5.6 fallback backfills.

  ## Reading the tags of events

  `matched_items` says which items matched an event, and an item cannot name a value it does
  not know. A `c:read_query/2` with `tag_items:` answers, in its result's `tags`, the tags of
  each match one of those items matched, in the same statement and snapshot as its events:

      {:ok, %Decree.Store.ReadResult{tags: %{12 => ["command:1", "user:7"]}}} =
        MyApp.EventStore.read_query([[tags: ["user:7"]], [tags: ["command:1"]]],
          tag_items: [1]
        )

  `c:read_tags/2` answers what tags the events at some positions carry, in a statement of its
  own:

      {:ok, %{12 => ["command:1", "user:7"]}} = MyApp.EventStore.read_tags([12, 40])

  Both answer alike. A position with no row is absent, as position 40 is here, and a caller
  reads that as an event that does not carry the tag. Tags of a kind the store declares
  deletable are left out in the statement, and so is every tag of an event of a type it
  declares, the `command:` tag included, so neither is a way round
  `{:error, :query_names_deletable}` (see "Deletable streams"), even at a position a
  `command:` item reached. A position whose every tag is left out is absent too and reads the
  same way, which fails closed: no tag of it reaches a caller, and a decision still refuses
  that command match, by the item that matched and not by the tag. `c:read_tags/2` is a read
  of its own, in its own snapshot. That is sound for what a tag row can do: a row takes no
  update, so a read after another read can see one go and never see one change.

  ## Conditional appends

  A conditional append writes its events only if no event after a position matches a
  query:

      items = [[types: ["EmailClaimed"], tags: ["email:digest"]]]

      {:ok, %Decree.Store.ReadResult{events: [], after: head}} =
        MyApp.EventStore.read_query(items)

      :ok =
        MyApp.EventStore.append_to_stream("decision-1", :no_stream, events,
          condition: [items: items, after: head]
        )

  `items` is a `Decree.Store.Query` list, and `after` comes from the `after` of a complete
  `c:read_query/2`. The append does not write and answers
  `{:error, :append_condition_failed}` if any event past `after` matches. It is decided
  while holding `$all`, in a statement of its own, so an event a concurrent append commits
  first is seen and fails it, and no event can land between the check and the write.

  A condition that cannot run is refused before any statement is sent, so a caller's
  transaction stays usable: `{:error, :invalid_condition}` for a malformed condition,
  whose `after` must be an integer and never `nil`, `{:error, :invalid_query}` and
  `{:error, :tag_too_long}` for its `items`, `{:error, :query_names_deletable}` for items
  naming something deletable (see "Deletable streams"), and
  `{:error, :undeclared_conditional_target}` for an append the store does not declare
  (see `c:append_to_stream/4`). An `expected_version` is checked first, so a stale append
  answers `{:error, :wrong_expected_version}` whatever its condition says.

  A failed condition inside a caller's transaction aborts that transaction like any failed
  statement, and a retry there is the caller's. A condition sees that transaction's own
  earlier events, so one of them past `after` fails it. Outside a transaction, a failed
  condition is an answer and is not retried. A `deadlock_detected` retry evaluates the
  condition again, with the same `after`.

  **Nothing else writes to a declared prefix.** A stream starting with a prefix of
  `conditional_appends` holds what a condition let through, and only that. A plain
  `c:append_to_stream/4` or `c:append_to_streams/2` to one answers
  `{:error, :plain_append_to_conditional_prefix}`, one such member refusing the whole batch.
  This is answered before any statement is sent, at every expected version. A condition is what
  makes an append conditional, so one passing on a declared `target` is allowed on such a
  stream.

  Those events are what later decisions rest on, their `command:` tags included, so a hard
  delete of a stream starting with such a prefix answers
  `{:error, :hard_delete_of_conditional_prefix}`, before any statement is sent and at every
  expected version, unless the stream also starts with a prefix of `deletable`: a store that
  declares both has said so, and "Deletable streams" states that cost. A store without
  `enable_hard_deletes` keeps its own answer, and a soft delete is unchanged. The refusal goes
  by the stream's name, and no other stream can hold such an event, because no link exists or
  can be made. A link row a store still holds from before links were removed is not found by
  this: a hard delete of the stream holding it removes the event, as "Deletable streams" says.

  **What the append wrote.** With `returning: :append_result`, an append answers
  `{:ok, %Decree.Store.AppendResult{}}` in place of `:ok`, with the last position it wrote and
  each stream's version after it:

      {:ok, %Decree.Store.AppendResult{position: position, stream_versions: versions}} =
        MyApp.EventStore.append_to_stream("decision-1", :no_stream, events,
          condition: [items: items, after: head],
          returning: :append_result
        )

  Both come from the function that wrote them, so no read can race them. Any other value
  answers `{:error, :invalid_returning}` before any statement is sent.

  **The coverage gap, on a write path.** A condition matches rows of the `event_tags` and
  `event_types` tables. An event from before store migration 1.5.0, or one an old pod
  appended through `append_events_v1`, has no such rows, so a condition silently misses
  it, and the append cannot cheaply detect that. A store that starts empty after 1.5.0 has
  no such event; any other store needs its events backfilled into both tables before a
  condition can be trusted to be complete.

  **The cost under the lock.** A condition runs while holding `$all`, so every append
  waits for it. It costs in proportion to each item's first-tag hits after `after`, not to
  history. With an `after` from a read, that is a few events. `after: 0` over a broad
  first tag scans them all under `$all`: 2.3 s for 200,000 hits in a spike. See "The first
  tag" in `Decree.Store.Query`.

  ## Appending to several streams

  `c:append_to_streams/2` writes to several streams atomically, each with its own expected
  version, under at most one condition:

      :ok =
        MyApp.EventStore.append_to_streams(
          [
            {"account-1", 4, debit_events},
            {"account-2", :any_version, credit_events},
            {"transfer-7", :no_stream, transfer_events}
          ],
          condition: [items: items, after: head, target: :transfer]
        )

  **All or none.** It is one database statement, so a stale member, a failed condition or
  a duplicate event id leaves no stream row, no event and no position on any member.

  **Positions.** Events take their `$all` positions in the order given, contiguous and
  with nothing interleaved. Within a stream they keep their order.

  **Locks.** The append takes every member's row and then `$all`, whatever order the
  list is in: the function sorts the members by the bytes of their names, so a multi-stream
  append cannot deadlock against another one or against a plain append. That holds for a
  call that takes its own locks: a caller's transaction that has already appended holds
  `$all` before the call's members, as "Using an existing database connection or
  transaction" says. Elixir sorts nothing, so no pod can disagree with another about the
  order. Prefer this to two appends in one transaction, which take the second stream's row
  after `$all`.

  **Notifications.** One call fires one notification per member, sorted by name, then one
  for `$all`. A stream's subscribers get their stream's events, and a `$all` subscriber
  gets the batch in position order.

  **The one-call rule.** A multi-stream append is one function call at any size, with no
  split into batches of 1,000, because the next batch would take a stream's row while
  holding `$all`. It holds `$all` no longer than a split would, which holds it from its
  first batch to the commit, but its `timeout` applies to that one call, so a very large
  batch needs a larger one. Only `c:append_to_stream/4` splits.

  **The declared rule.** A condition here needs what `c:append_to_stream/4` needs: a
  condition with a `target` needs that target declared, whatever its members are named, and
  one with none needs every member's name to start with a declared prefix: one member
  outside them refuses the batch.

  ## Deletable streams

  A hard delete removes events, so no decision may rest on one that can vanish. A store
  declares what can, and refuses any query over it:

      defmodule MyApp.EventStore do
        use Decree.Store,
          otp_app: :my_app,
          enable_hard_deletes: true,
          deletable: [
            stream_prefixes: ["XPost-"],
            event_types: ["Elixir.MyApp.XPostCaptured"],
            tag_kinds: ["content_ref"]
          ]
      end

  Each key is optional and each is a list of non-empty strings. A tag is `<kind>:<value>`:
  the kind has no colon, the value may, and a tag with no colon is its own kind. A malformed
  declaration, such as a misspelt key or a module where its name belongs, raises an
  `ArgumentError` when the config is parsed, because it would refuse nothing. So does a tag
  kind of `Decree.Store.TagIndex.reserved_kinds/0`, because a fact whose state tag is gone
  could have been a disable. Only the config declares: `deletable` is no option of any call. A store that declares nothing behaves as
  if none of this existed. Declaring does not narrow what `enable_hard_deletes` allows: a
  hard delete can still delete a stream outside the prefixes. The one stream a hard delete
  refuses starts with a `conditional_appends` prefix and with no prefix declared here: see
  "Conditional appends".

  **The refusal.** A `c:read_query/2`, a `c:stream_query/2` or a condition with a query naming
  a declared type, or a tag of a declared kind, answers `{:error, :query_names_deletable}`
  before any statement is sent, so a caller's transaction stays usable. A condition's refusal
  comes before `{:error, :undeclared_conditional_target}`.

  **The `command:` exception.** A `command:` item over a deletable stream's events passes,
  because a decision has to find its own earlier command, and `command` cannot be declared.
  The refusal is per name, so an item naming a declared type or kind beside its `command:` tag
  is refused. What this costs: a deleted stream's command tags vanish with it, which ends
  their deduplication early. A condition on such a tag fails before the delete and passes
  after it, and a read agrees with it both times. The item finds an event and not its tags:
  neither a read's `tags` nor `c:read_tags/2` answers any tag of an event of a declared type.

  **Completeness.** An append to a stream starting with a declared prefix, of an event whose
  type is not in `event_types`, answers `{:error, :undeclared_deletable_event_type}` before
  any statement is sent. Without this a forgotten type would stay queryable and vanish under
  a decision.

  **Links.** No link exists or can be made, so a stream that can be deleted holds nothing but
  its own events. A store can still hold link rows written before links were removed, and a
  hard delete of a stream holding one removes that event from every stream, the stream it was
  appended to included.

  **Tag index facts.** A stream holding an event tagged `tag_index:fact`, its own or through a
  link row written before links were removed, can never be hard-deleted, whatever its prefix
  or its declarations, the stream an application keeps its facts on, a `Platform` singleton's
  included. The delete answers `{:error, :hard_delete_of_index_fact}` from inside its
  transaction, which rolls back with every row standing, a caller's transaction with it. A
  hard delete takes the stream's events from every stream, so a deleted disable would leave
  an earlier enable standing: see `Decree.Store.TagIndex`. The check reads after the stream's
  row is locked, so no append into the stream can slip past it.

  **The limit.** The refusal is by name. An item of tags alone, naming a kind that is not
  declared, can still match a deletable event. So the declaration must list every kind a
  deletable event carries.

  **The race.** A hard delete takes no `$all`, so it can land while a condition runs. That is
  harmless because of the refusal: no condition but a `command:` item can see the change, and
  that one is the cost above.

  **Erasing a tag.** `c:delete_stream_tags/3` removes one stream's own tag rows of the given
  kinds and leaves its events standing, which is how an erased subject's value tags go. It
  does so while holding `$all`, so it changes the tag index only under the append lock.
  Afterwards a query matches as if those tags had never been written: an item naming one
  matches nothing there, and an item of the event's type alone still matches, in a read and
  in a condition alike.

  """

  @type t :: module
  @type option ::
          {:name, atom}
          | {:conn, Postgrex.conn() | DBConnection.t()}
          | {:timeout, timeout()}
  @type options :: [option]
  @type read_query_option ::
          option
          | {:from, integer()}
          | {:direction, :forward | :backward}
          | {:count, non_neg_integer()}
          | {:command_tag, String.t()}
          | {:tag_items, [non_neg_integer()]}
          | {:position_items, [non_neg_integer()]}
  @type stream_query_option ::
          option
          | {:from, integer()}
          | {:direction, :forward | :backward}
          | {:read_batch_size, pos_integer()}
  @type transient_subscribe_option ::
          {:name, atom}
          | {:selector, (Decree.Store.RecordedEvent.t() -> any())}
          | {:mapper, (Decree.Store.RecordedEvent.t() -> any())}
  @type transient_subscribe_options :: [transient_subscribe_option]
  @type persistent_subscription_option ::
          transient_subscribe_option
          | {:buffer_size, pos_integer()}
          | {:checkpoint_after, non_neg_integer()}
          | {:checkpoint_threshold, pos_integer()}
          | {:concurrency_limit, pos_integer()}
          | {:max_size, pos_integer()}
          | {:partition_by, (Decree.Store.RecordedEvent.t() -> any())}
          | {:start_from, :origin | :current | non_neg_integer()}
          | {:timeout, timeout()}
          | {:transient, boolean()}
  @type persistent_subscription_options :: [persistent_subscription_option]
  @type expected_version :: :any_version | :no_stream | :stream_exists | non_neg_integer
  @type start_from :: :origin | :current | non_neg_integer
  @typedoc "A running subscription, as `c:local_subscriptions/1` describes it."
  @type subscription_info :: %{
          subscription_name: String.t(),
          stream_uuid: String.t(),
          subscription_id: pos_integer() | nil,
          pid: pid(),
          transient: boolean(),
          status: :subscribed | :parked | :unsubscribed,
          last_seen: non_neg_integer()
        }

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Decree.Store

      alias Decree.Store.{Config, EventData, PubSub, Subscriptions}
      alias Decree.Store.Snapshots.{SnapshotData, Snapshotter}
      alias Decree.Store.Subscriptions.Subscription
      alias Decree.Store.Streams.Stream

      @otp_app Keyword.fetch!(opts, :otp_app)

      @all_stream "$all"
      @default_batch_size 1_000
      @default_count 1_000

      def config(opts \\ []) do
        opts = Keyword.merge(unquote(opts), opts)

        with {:ok, config} <- Decree.Store.Supervisor.runtime_config(__MODULE__, @otp_app, opts) do
          config
        end
      end

      def child_spec(opts) do
        name = name(opts)

        %{
          id: name,
          start: {__MODULE__, :start_link, [opts]},
          type: :supervisor
        }
      end

      def start_link(opts \\ []) do
        opts = Keyword.merge(unquote(opts), opts)
        name = name(opts)

        Decree.Store.Supervisor.start_link(__MODULE__, @otp_app, name, opts)
      end

      def stop(supervisor, timeout \\ 5000) do
        Supervisor.stop(supervisor, :normal, timeout)
      end

      @accepted_overrides_append_to_stream [:created_at_override, :condition, :returning]

      def append_to_stream(stream_uuid, expected_version, events, opts \\ [])

      def append_to_stream(@all_stream, _expected_version, _events, _opts),
        do: {:error, :cannot_append_to_all_stream}

      def append_to_stream(stream_uuid, expected_version, events, opts) do
        overrides = Keyword.take(opts, @accepted_overrides_append_to_stream)
        {conn, opts} = parse_opts(opts)
        opts = Keyword.merge(opts, overrides)

        Stream.append_to_stream(conn, stream_uuid, expected_version, events, opts)
      end

      def append_to_streams(appends, opts \\ []) do
        overrides = Keyword.take(opts, @accepted_overrides_append_to_stream)
        {conn, opts} = parse_opts(opts)
        opts = Keyword.merge(opts, overrides)

        Stream.append_to_streams(conn, appends, opts)
      end

      def read_stream_forward(
            stream_uuid,
            start_version \\ 0,
            count \\ @default_count,
            opts \\ []
          )

      def read_stream_forward(stream_uuid, start_version, count, opts) do
        {conn, opts} = parse_opts(opts)

        Stream.read_stream_forward(conn, stream_uuid, start_version, count, opts)
      end

      def read_all_streams_forward(
            start_version \\ 0,
            count \\ @default_count,
            opts \\ []
          )

      def read_all_streams_forward(start_version, count, opts),
        do: read_stream_forward(@all_stream, start_version, count, opts)

      def read_stream_backward(
            stream_uuid,
            start_version \\ -1,
            count \\ @default_count,
            opts \\ []
          )

      def read_stream_backward(stream_uuid, start_version, count, opts) do
        {conn, opts} = parse_opts(opts)

        Stream.read_stream_backward(conn, stream_uuid, start_version, count, opts)
      end

      def read_all_streams_backward(
            start_version \\ -1,
            count \\ @default_count,
            opts \\ []
          )

      def read_all_streams_backward(start_version, count, opts),
        do: read_stream_backward(@all_stream, start_version, count, opts)

      def stream_forward(stream_uuid, start_version \\ 0, opts \\ [])

      def stream_forward(stream_uuid, start_version, read_batch_size)
          when is_integer(start_version) and is_integer(read_batch_size) do
        stream_forward(stream_uuid, start_version, read_batch_size: read_batch_size)
      end

      def stream_forward(stream_uuid, start_version, opts) do
        {conn, opts} = parse_opts(opts)

        opts = Keyword.put_new(opts, :read_batch_size, @default_batch_size)

        Stream.stream_forward(conn, stream_uuid, start_version, opts)
      end

      def stream_all_forward(start_version \\ 0, opts \\ [])

      def stream_all_forward(start_version, opts),
        do: stream_forward(@all_stream, start_version, opts)

      def stream_backward(stream_uuid, start_version \\ -1, opts \\ [])

      def stream_backward(stream_uuid, start_version, read_batch_size)
          when is_integer(start_version) and is_integer(read_batch_size) do
        stream_backward(stream_uuid, start_version, read_batch_size: read_batch_size)
      end

      def stream_backward(stream_uuid, start_version, opts) do
        {conn, opts} = parse_opts(opts)

        opts = Keyword.put_new(opts, :read_batch_size, @default_batch_size)

        Stream.stream_backward(conn, stream_uuid, start_version, opts)
      end

      def stream_all_backward(start_version \\ -1, opts \\ [])

      def stream_all_backward(start_version, opts),
        do: stream_backward(@all_stream, start_version, opts)

      @accepted_overrides_read_query [
        :command_tag,
        :count,
        :direction,
        :from,
        :tag_items,
        :position_items
      ]
      @accepted_overrides_stream_query [:direction, :from]

      def read_query(items, opts \\ []) do
        overrides = Keyword.take(opts, @accepted_overrides_read_query)
        {conn, opts} = parse_opts(opts)

        opts = opts |> Keyword.merge(overrides) |> Keyword.put_new(:count, @default_count)

        Stream.read_query(conn, items, opts)
      end

      def read_tags(positions, opts \\ []) do
        {conn, opts} = parse_opts(opts)

        Stream.read_tags(conn, positions, opts)
      end

      def stream_query(items, opts \\ []) do
        overrides = Keyword.take(opts, @accepted_overrides_stream_query)
        {conn, opts} = parse_opts(opts)

        opts =
          opts
          |> Keyword.merge(overrides)
          |> Keyword.put_new(:read_batch_size, @default_batch_size)

        Stream.stream_query(conn, items, opts)
      end

      def delete_stream(stream_uuid, expected_version, type \\ :soft, opts \\ [])

      def delete_stream(@all_stream, _expected_version, _type, _opts),
        do: {:error, :cannot_delete_all_stream}

      def delete_stream(stream_uuid, expected_version, type, opts) when type in [:soft, :hard] do
        {conn, opts} = parse_opts(opts)

        Stream.delete(conn, stream_uuid, expected_version, type, opts)
      end

      def delete_stream_tags(stream_uuid, tag_kinds, opts \\ [])

      def delete_stream_tags(@all_stream, _tag_kinds, _opts),
        do: {:error, :cannot_delete_all_stream}

      def delete_stream_tags(stream_uuid, tag_kinds, opts) do
        {conn, opts} = parse_opts(opts)

        Stream.delete_tags(conn, stream_uuid, tag_kinds, opts)
      end

      def stream_info(stream_uuid, opts \\ [])
      def stream_info(:all, opts), do: stream_info(@all_stream, opts)

      def stream_info(stream_uuid, opts) do
        {conn, opts} = parse_opts(opts)

        Stream.stream_info(conn, stream_uuid, :stream_exists, opts)
      end

      def subscribe(stream_uuid, opts \\ []) do
        name = name(opts)

        # `:notices` registers a subscription process for the publisher's position notices,
        # which a transient subscriber is never sent.
        PubSub.subscribe(name, stream_uuid, Keyword.drop(opts, [:name, :notices]))
      end

      def subscribe_to_stream(stream_uuid, subscription_name, subscriber, opts \\ []) do
        name = name(opts)
        config = Config.lookup(name)
        conn = Keyword.fetch!(config, :conn)
        schema = Keyword.fetch!(config, :schema)
        serializer = Keyword.fetch!(config, :serializer)
        correlation_id_type = Keyword.get(config, :correlation_id_type, "uuid")
        causation_id_type = Keyword.get(config, :causation_id_type, "uuid")

        query_timeout = timeout(opts, config)

        {start_from, opts} = Keyword.pop(opts, :start_from, :origin)

        with {:ok, start_from} <-
               Stream.start_from(conn, stream_uuid, start_from,
                 schema: schema,
                 timeout: query_timeout
               ) do
          opts =
            opts
            |> Keyword.delete(:timeout)
            |> Keyword.merge(
              conn: conn,
              event_store: name,
              query_timeout: query_timeout,
              schema: schema,
              serializer: serializer,
              correlation_id_type: correlation_id_type,
              causation_id_type: causation_id_type,
              stream_uuid: stream_uuid,
              subscription_name: subscription_name,
              start_from: start_from
            )
            |> Keyword.put_new_lazy(:hibernate_after, fn ->
              Keyword.fetch!(config, :subscription_hibernate_after)
            end)
            |> Keyword.put_new_lazy(:retry_interval, fn ->
              Keyword.fetch!(config, :subscription_retry_interval)
            end)
            |> Keyword.put_new_lazy(:head_check_interval, fn ->
              Keyword.fetch!(config, :subscription_head_check_interval)
            end)

          # The event store's setting is validated at start; a subscription's own option
          # is checked here, before a subscription process starts.
          max_head_check_interval = Subscriptions.max_head_check_interval()

          case Keyword.fetch!(opts, :head_check_interval) do
            interval
            when is_integer(interval) and interval > 0 and interval <= max_head_check_interval ->
              Subscriptions.subscribe_to_stream(subscriber, opts)

            _invalid ->
              {:error, :invalid_head_check_interval}
          end
        end
      end

      def subscribe_to_all_streams(subscription_name, subscriber, opts \\ []),
        do: subscribe_to_stream(@all_stream, subscription_name, subscriber, opts)

      defdelegate ack(subscription, ack), to: Subscription

      def unsubscribe_from_stream(stream_uuid, subscription_name, opts \\ []) do
        name = name(opts)

        Subscriptions.unsubscribe_from_stream(name, stream_uuid, subscription_name)
      end

      def unsubscribe_from_all_streams(subscription_name, opts \\ []),
        do: unsubscribe_from_stream(@all_stream, subscription_name, opts)

      def delete_subscription(stream_uuid, subscription_name, opts \\ []) do
        name = name(opts)

        with :ok <- Subscriptions.stop_subscription(name, stream_uuid, subscription_name) do
          {conn, opts} = parse_opts(opts)

          Subscriptions.delete_subscription(conn, stream_uuid, subscription_name, opts)
        end
      end

      def delete_all_streams_subscription(subscription_name, opts \\ []),
        do: delete_subscription(@all_stream, subscription_name, opts)

      @default_local_subscriptions_timeout 1_000

      def local_subscriptions(opts \\ []) do
        name = name(opts)
        timeout = Keyword.get(opts, :timeout, @default_local_subscriptions_timeout)

        Subscriptions.local_subscriptions(name, timeout)
      end

      def subscription_locks(subscription_ids, opts \\ []) when is_list(subscription_ids) do
        name = name(opts)
        timeout = timeout(opts, Config.lookup(name))

        Subscriptions.subscription_locks(name, subscription_ids, timeout)
      end

      def stop_subscription(subscription, opts \\ []) when is_pid(subscription) do
        name = name(opts)

        Subscriptions.stop_subscription(name, subscription)
      end

      def read_snapshot(source_uuid, opts \\ []) do
        {snapshot_version, opts} = Keyword.pop(opts, :snapshot_version)
        {conn, opts} = parse_opts(opts)

        Snapshotter.read_snapshot(
          conn,
          source_uuid,
          Keyword.put(opts, :snapshot_version, snapshot_version)
        )
      end

      def record_snapshot(%SnapshotData{} = snapshot, opts \\ []) do
        {conn, opts} = parse_opts(opts)

        Snapshotter.record_snapshot(conn, snapshot, opts)
      end

      def delete_snapshot(source_uuid, opts \\ []) do
        {conn, opts} = parse_opts(opts)

        Snapshotter.delete_snapshot(conn, source_uuid, opts)
      end

      def prune_snapshots(source_type, snapshot_version, opts \\ []) do
        {conn, opts} = parse_opts(opts)

        Snapshotter.prune_snapshots(conn, source_type, snapshot_version, opts)
      end

      defp parse_opts(opts) do
        name = name(opts)
        config = Config.lookup(name)
        conn = Keyword.get(opts, :conn) || Keyword.fetch!(config, :conn)
        timeout = timeout(opts, config)

        config =
          case Keyword.fetch(opts, :read_batch_size) do
            {:ok, read_batch_size} -> Keyword.put(config, :read_batch_size, read_batch_size)
            :error -> config
          end

        {conn, Keyword.put(config, :timeout, timeout)}
      end

      defp name(opts) do
        case Keyword.get(opts, :name) do
          nil ->
            __MODULE__

          name when is_atom(name) ->
            name

          invalid ->
            raise ArgumentError,
              message: "expected `:name` to be an atom, got: " <> inspect(invalid)
        end
      end

      defp timeout(opts, config) do
        case Keyword.get(opts, :timeout) do
          nil ->
            # Use event store default timeout, or 15 seconds if not configured
            Keyword.get(config, :timeout, 15_000)

          timeout when is_integer(timeout) ->
            timeout

          :infinity ->
            :infinity

          invalid ->
            raise ArgumentError,
              message:
                "expected `:timeout` to be an integer or `:infinity`, got: " <>
                  inspect(invalid)
        end
      end
    end
  end

  alias Decree.Store.{Config, EventData}
  alias Decree.Store.Snapshots.SnapshotData
  alias Decree.Store.Streams.StreamInfo

  ## User callbacks
  @optional_callbacks init: 1

  @doc """
  A callback executed when the event store starts or when configuration is read.

  It must return `{:ok, keyword}` with the updated list of configuration.
  """
  @callback init(config :: Keyword.t()) :: {:ok, Keyword.t()}

  @doc """
  Returns the event store configuration stored in the `:otp_app` environment.
  """
  @callback config() :: Keyword.t()

  @doc """
  Starts any connection pooling or supervision and return `{:ok, pid}`
  or just `:ok` if nothing needs to be done.

  Returns `{:error, {:already_started, pid}}` if the event store is already
  started or `{:error, term}` in case anything else goes wrong.
  """
  @callback start_link(opts :: Keyword.t()) ::
              {:ok, pid}
              | {:error, {:already_started, pid}}
              | {:error, term}

  @doc """
  Shuts down the event store.
  """
  @callback stop(Supervisor.supervisor(), timeout) :: :ok

  @doc """
  Append one or more events to a stream atomically.

    - `stream_uuid` is used to uniquely identify a stream.

    - `expected_version` is used for optimistic concurrency checks.
      You can provide a non-negative integer to specify the expected stream
      version. This is used to ensure you can only append to the stream if it is
      at exactly that version.

      You can also provide one of the following values to alter the concurrency
      check behaviour:

      - `:any_version` - No concurrency checking and allow any stream version
        (including no stream).
      - `:no_stream` - Ensure the stream does not exist.
      - `:stream_exists` - Ensure the stream exists.

    - `events` is a list of `%Decree.Store.EventData{}` structs.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database transaction, in
        milliseconds. Defaults to 15,000ms.
      - `condition` a `Decree.Store.AppendCondition` keyword list, for a conditional
        append. See "Conditional appends" in `Decree.Store`, and below.
      - `returning` `:append_result` to answer `{:ok, %Decree.Store.AppendResult{}}` on
        success. Without it success answers `:ok`. An append that splits into batches of
        1,000 answers its last batch's position and version, which are the whole append's.

  #### Declaring a conditional append

  Since a condition takes time while holding `$all`, a store refuses every conditional
  append until its config declares it: by the condition's `target` when it names one, and
  otherwise by the stream's prefix:

      defmodule MyApp.EventStore do
        use Decree.Store,
          otp_app: :my_app,
          conditional_appends: [stream_prefixes: ["decision-"], targets: [MyApp.User]]
      end

  Or via config:

      # config/config.exs
      config :my_app, MyApp.EventStore,
        conditional_appends: [stream_prefixes: ["decision-"], targets: [MyApp.User]]

  The condition's shape says which declaration it needs. One with a `target` is declared
  when that target is one of `targets`, whatever its `stream_uuid`, which suits a stream
  whose name has no prefix. One with no `target` is declared when its `stream_uuid` starts
  with a declared prefix. A prefix never declares a target: a condition naming an undeclared
  one is refused on a prefixed stream too. Only the config declares: `conditional_appends`
  is no `append_to_stream/4` option.

  A declared prefix also keeps every other writer out: an append with no condition to a
  stream starting with one is refused. A target keeps nobody out. It is compared on a
  conditional append and nowhere else, so a stream it covers takes plain appends and hard
  deletes as any other does. A `Decree.Decision.Rule` route's condition names its
  aggregate as the target, and an application will not start while its store leaves one out:
  see "Two declarations" there.

  Returns `:ok` on success, `{:ok, %Decree.Store.AppendResult{}}` with
  `returning: :append_result`, or an `{:error, reason}` tagged tuple:

    * `{:error, :wrong_expected_version}` when the actual stream version differs
      from the provided expected version, or another writer appended at the same
      stream version first.
    * `{:error, :stream_exists}` when the stream exists, but expected version
      was `:no_stream`.
    * `{:error, :stream_not_found}` when the stream does not exist, but
      expected version was `:stream_exists`.
    * `{:error, :stream_deleted}` when the stream was soft deleted.
    * `{:error, :cannot_append_to_all_stream}` for the `$all` stream.
    * `{:error, :duplicate_event}` when an event id was already appended.
    * `{:error, :not_found}` when the stream was removed (hard deleted) between
      reading its version and appending. Inside a caller's transaction this aborts
      that transaction, as a stale append does.
    * `{:error, :append_condition_failed}` when an event past the condition's
      `after` matches its `items`. A stale append answers
      `{:error, :wrong_expected_version}` instead: it never evaluates a condition.
    * `{:error, :invalid_tag}` when a tag of an event is not a binary, is not valid
      UTF-8 or holds a NUL byte, and `{:error, :tag_too_long}` when a tag that is none of
      these is over 512 bytes. Each is answered before any statement is sent, so it wins
      over every answer above that reads the stream, and a caller's transaction stays
      usable.
    * `{:error, :invalid_tag_index_fact}` when an event carries a `tag_index` tag that is not
      exactly `tag_index:fact`, a `tag_index_state` tag that does not parse, or one of the two
      without the other. See `Decree.Store.TagIndex`. It is answered as a tag that is too long
      is, and after one.
    * `{:error, :invalid_required_tag_kinds}` when an event's `required_tag_kinds` are not
      kinds, and `{:error, :missing_required_tag}` when an event lacks a kind on that list.
      See `Decree.Store.EventData`. Each is answered as a tag that is too long is, after
      one, at every expected version. An event answering what is not a list never gets this
      far: preparing it answers the first, and `Decree.Event.RequiredTags` says so.
    * `{:error, :undeclared_deletable_event_type}` when the stream starts with a
      declared deletable prefix and an event's type is not declared. See "Deletable
      streams" in `Decree.Store`. It is answered as a tag that is too long is, and after
      one.
    * `{:error, :invalid_returning}` when `returning` is anything but `:append_result`, or
      is given twice. It is answered after that one and before any statement is sent.
    * `{:error, :invalid_condition}`, `{:error, :invalid_query}`,
      `{:error, :query_names_deletable}` and `{:error, :undeclared_conditional_target}`
      when the `condition` is malformed, when its `items` are, when they name something
      deletable, and when the store does not declare it. Each is answered after a tag that
      is too long and before any statement is sent, as that one is.
    * `{:error, :plain_append_to_conditional_prefix}` when there is no `condition` and the
      stream starts with a declared prefix. It is answered where a condition's refusals are.
    * `{:error, :duplicate_stream_uuid}` when the append lost the race to create a
      new stream inside a caller's transaction. That aborts the transaction, so it
      is answered and not retried.
    * `{:error, {:already_retried_once, :duplicate_stream_uuid}}` when the
      append lost the race to create a new stream twice, outside a caller's
      transaction.
    * `{:error, %Decree.Store.AppendError{}}` for any other failure of the
      append statement, classified `:unavailable`, `:retryable` or `:rejected`
      (see `Decree.Store.AppendError`).

  A failure of the query that reads the stream's version first is returned as
  the raw error `{:error, reason}` (for example a `Postgrex.Error` or a
  `DBConnection.ConnectionError`), not as an `AppendError`. An
  append of 1,000 or more events runs in a transaction; when that transaction
  cannot begin or commit, the call raises (for example a
  `DBConnection.ConnectionError`), or returns `{:error, :rollback}` when the
  connection reports a transaction state error at begin or commit.
  """
  @callback append_to_stream(
              stream_uuid :: String.t(),
              expected_version,
              events :: list(EventData.t()),
              opts :: [option | {:condition, keyword()} | {:returning, :append_result}]
            ) ::
              :ok
              | {:ok, Decree.Store.AppendResult.t()}
              | {:error, :cannot_append_to_all_stream}
              | {:error, :stream_exists}
              | {:error, :stream_not_found}
              | {:error, :wrong_expected_version}
              | {:error, :stream_deleted}
              | {:error, :duplicate_event}
              | {:error, :not_found}
              | {:error, :append_condition_failed}
              | {:error, :invalid_tag}
              | {:error, :tag_too_long}
              | {:error, :invalid_tag_index_fact}
              | {:error, :invalid_required_tag_kinds}
              | {:error, :missing_required_tag}
              | {:error, :query_names_deletable}
              | {:error, :undeclared_deletable_event_type}
              | {:error, :invalid_condition}
              | {:error, :invalid_query}
              | {:error, :undeclared_conditional_target}
              | {:error, :plain_append_to_conditional_prefix}
              | {:error, :invalid_returning}
              | {:error, :duplicate_stream_uuid}
              | {:error, {:already_retried_once, :duplicate_stream_uuid}}
              | {:error, Decree.Store.AppendError.t()}
              | {:error, :rollback}
              | {:error, reason :: term}

  @doc """
  Append events to several streams atomically, in one database statement. See "Appending
  to several streams" in the module documentation for the guarantees, the order of
  positions and notifications, and the rule that it is one call at any size.

    - `appends` is a non-empty list of `{stream_uuid, expected_version, events}`, one per
      stream. `expected_version` and `events` are those of `c:append_to_stream/4`, and
      `events` has at least one event. Events take their `$all` positions in list order.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `conn` an existing Postgrex connection or transaction to append through.
      - `timeout` an optional timeout for the database statement, in milliseconds.
        Defaults to 15,000ms. It applies to the one statement, at any size.
      - `condition` a `Decree.Store.AppendCondition` keyword list, for a conditional
        append. See "Conditional appends" in `Decree.Store`. It is declared, as
        `c:append_to_stream/4` says, when its `target` is declared, or, with no `target`,
        when every member starts with a declared prefix.
      - `created_at_override` dates the new streams and every event.
      - `returning` `:append_result` to answer `{:ok, %Decree.Store.AppendResult{}}` on
        success, with a version for every member. Without it success answers `:ok`.

  Returns `:ok` on success, `{:ok, %Decree.Store.AppendResult{}}` with
  `returning: :append_result`, or the errors of `c:append_to_stream/4`, for any member, and:

    * `{:error, :cannot_append_to_all_stream}` when a member is the `$all` stream.
    * `{:error, :invalid_append}` when `appends` is not a non-empty list of those tuples.
    * `{:error, :repeated_stream}` when a stream appears twice, whatever its expected
      versions. A repeat cannot name one base version.

  With no `condition`, one member starting with a declared prefix refuses the batch with
  `{:error, :plain_append_to_conditional_prefix}`.

  These, like a tag that is invalid or too long, a required tag kind one member's event lacks
  and a condition that cannot run, are answered before any statement is sent, so a caller's
  transaction stays usable.

  A member's row gone by the time the function locks it answers `{:error, :not_found}`,
  and aborts a caller's transaction, as a stale append does. Outside a caller's
  transaction, losing a race to create a member and a `deadlock_detected` are each retried
  once, reading every member again and evaluating the condition again, with the same
  `after`. Inside one, either aborts it and is answered, a lost race to create a member as
  `{:error, :duplicate_stream_uuid}`.
  """
  @callback append_to_streams(
              appends :: [{stream_uuid :: String.t(), expected_version, list(EventData.t())}],
              opts :: [
                option
                | {:condition, keyword()}
                | {:created_at_override, DateTime.t()}
                | {:returning, :append_result}
              ]
            ) ::
              :ok
              | {:ok, Decree.Store.AppendResult.t()}
              | {:error, :cannot_append_to_all_stream}
              | {:error, :invalid_append}
              | {:error, :repeated_stream}
              | {:error, :stream_exists}
              | {:error, :stream_not_found}
              | {:error, :wrong_expected_version}
              | {:error, :stream_deleted}
              | {:error, :duplicate_event}
              | {:error, :not_found}
              | {:error, :append_condition_failed}
              | {:error, :invalid_tag}
              | {:error, :tag_too_long}
              | {:error, :invalid_tag_index_fact}
              | {:error, :invalid_required_tag_kinds}
              | {:error, :missing_required_tag}
              | {:error, :query_names_deletable}
              | {:error, :undeclared_deletable_event_type}
              | {:error, :invalid_condition}
              | {:error, :invalid_query}
              | {:error, :undeclared_conditional_target}
              | {:error, :plain_append_to_conditional_prefix}
              | {:error, :invalid_returning}
              | {:error, :duplicate_stream_uuid}
              | {:error, {:already_retried_once, :duplicate_stream_uuid}}
              | {:error, Decree.Store.AppendError.t()}
              | {:error, reason :: term}

  @doc """
  Reads the requested number of events from the given stream in the order in
  which they were originally written.

    - `stream_uuid` is used to uniquely identify a stream.

    - `start_version` optionally, the stream version of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      Defaults to to returning 1,000 events from the stream.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
  """
  @callback read_stream_forward(
              stream_uuid :: String.t(),
              start_version :: non_neg_integer,
              count :: non_neg_integer,
              opts :: options
            ) ::
              {:ok, list(Decree.Store.RecordedEvent.t())}
              | {:error, :stream_deleted}
              | {:error, reason :: term}

  @doc """
  Reads the requested number of events from all streams in the order in which
  they were originally written.

    - `start_version` optionally, the stream version of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      Defaults to returning 1,000 events from all streams.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
  """
  @callback read_all_streams_forward(
              start_version :: non_neg_integer,
              count :: non_neg_integer,
              opts :: options
            ) :: {:ok, list(Decree.Store.RecordedEvent.t())} | {:error, reason :: term}

  @doc """
  Reads the requested number of events from the given stream in the reverse
  order from which they were originally written.

    - `stream_uuid` is used to uniquely identify a stream.

    - `start_version` optionally, the stream version of the first event to read.
      Use `-1` to indicate starting from the end of the stream. Defaults to the
      end of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      Defaults to to returning 1,000 events from the stream.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
  """
  @callback read_stream_backward(
              stream_uuid :: String.t(),
              start_version :: non_neg_integer,
              count :: non_neg_integer,
              opts :: options
            ) ::
              {:ok, list(Decree.Store.RecordedEvent.t())}
              | {:error, :stream_deleted}
              | {:error, reason :: term}

  @doc """
  Reads the requested number of events from all streams in the reverse order
  from which they were originally written.

    - `start_version` optionally, the stream version of the first event to read.
      Use `-1` to indicate starting from the end of the stream. Defaults to the
      end of the stream if not set.

    - `count` optionally, the maximum number of events to read.
      Defaults to returning 1,000 events from all streams.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
  """
  @callback read_all_streams_backward(
              start_version :: integer,
              count :: non_neg_integer,
              opts :: options
            ) :: {:ok, list(Decree.Store.RecordedEvent.t())} | {:error, reason :: term}

  @doc """
  Streams events from the given stream in the order in which they were
  originally written.

    - `start_version` optionally, the stream version of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
      - `read_batch_size` optionally, the number of events to read at a time
        from storage. Defaults to reading 1,000 events per batch.
  """
  @callback stream_forward(
              stream_uuid :: String.t(),
              start_version :: integer,
              opts :: [options | {:read_batch_size, non_neg_integer}]
            ) :: Enumerable.t() | {:error, :stream_deleted} | {:error, reason :: term}

  @doc """
  Streams events from all streams in the order in which they were originally
  written.

    - `start_version` optionally, the stream version of the first event to read.
      Defaults to the beginning of the stream if not set.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
      - `read_batch_size` optionally, the number of events to read at a time from
        storage. Defaults to reading 1,000 events per batch.
  """
  @callback stream_all_forward(
              start_version :: non_neg_integer,
              opts :: [options | {:read_batch_size, non_neg_integer}]
            ) :: Enumerable.t() | {:error, :stream_deleted} | {:error, reason :: term}

  @doc """
  Streams events from the given stream in the reverse order from which they
  were originally written.

    - `start_version` optionally, the stream version of the first event to read.
      Use `-1` to indicate starting from the end of the stream. Defaults to the
      end of the stream if not set.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
      - `read_batch_size` optionally, the number of events to read at a time
        from storage. Defaults to reading 1,000 events per batch.
  """
  @callback stream_backward(
              stream_uuid :: String.t(),
              start_version :: integer,
              opts :: [options | {:read_batch_size, non_neg_integer}]
            ) :: Enumerable.t() | {:error, :stream_deleted} | {:error, reason :: term}

  @doc """
  Streams events from all streams in the reverse order from which they were
  originally written.

    - `start_version` optionally, the stream version of the first event to read.
      Use `-1` to indicate starting from the end of the stream. Defaults to the
      end of the stream if not set.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
      - `read_batch_size` optionally, the number of events to read at a time from
        storage. Defaults to reading 1,000 events per batch.
  """
  @callback stream_all_backward(
              start_version :: non_neg_integer,
              opts :: [options | {:read_batch_size, non_neg_integer}]
            ) :: Enumerable.t() | {:error, :stream_deleted} | {:error, reason :: term}

  @doc """
  Reads the events matching a query across all streams, in one SQL statement, and
  answers them with the `$all` head of the same snapshot. See "Reading by query" in the
  module documentation for what `after` promises and for what a query cannot see.

    - `items` the query: a list of items as described in `Decree.Store.Query`.

    - `opts` an optional keyword list containing:
      - `from` the `$all` position to start at, inclusive. Forward it defaults to 0.
        Backward it defaults to `-1`, meaning the head.
      - `direction` `:forward` (the default) or `:backward`.
      - `count` the maximum number of events to read, counted after an event matching
        two items has become one. Defaults to returning 1,000 events.
      - `command_tag` a tag named by one of the query's items. The result's
        `command_stamps` then has an entry for each match carrying it.
      - `tag_items` a list of 0-based indexes of the query's items. The result's `tags`
        then has the tags of each match one of them matched, read in the same statement
        (see "Reading the tags of events" in `Decree.Store`). Defaults to `[]`, which reads
        no tag.
      - `position_items` a list of 0-based indexes of the query's items. A match only these
        items matched is answered by position alone: it is in `matched_items`, in `tags` when
        a `tag_items` item matched it, and in `command_stamps` when it carries `command_tag`,
        but not in `events`. `count` neither cuts nor counts such a match, so every one of
        them is answered, however many there are. A match any other item matched too is
        answered with its event and counted, exactly as without the option. The read's
        statement and snapshot are the same. One caveat: a read that `count` cut answers a
        match past the cut that a `position_items` item matched as if only those items had,
        since the other items' probes stop at the cut; such a read answered `count` events
        and is partial anyway. Defaults to `[]`, which answers every match with its event.
      - `name` the name of the event store if provided to `start_link/1`.
      - `conn` an existing Postgrex connection or transaction to read through.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.

  Returns `{:ok, %Decree.Store.ReadResult{}}`. A query `Decree.Store.Query.compile/1`
  refuses answers its error, a query naming something deletable answers
  `{:error, :query_names_deletable}` (see "Deletable streams" in `Decree.Store`), and a
  `command_tag` no item names, or `tag_items` or `position_items` that is anything but a
  list of indexes of the query's items, answers `{:error, :invalid_query}`: none of these
  sends anything to the database. An invalid
  `direction`, `from` or `count` raises an `ArgumentError`.
  """
  @callback read_query(items :: [Decree.Store.Query.item()], opts :: [read_query_option]) ::
              {:ok, Decree.Store.ReadResult.t()}
              | {:error, :invalid_query}
              | {:error, :tag_too_long}
              | {:error, :query_names_deletable}
              | {:error, reason :: term}

  @doc """
  Reads the tags of the events at the given `$all` positions, in one SQL statement. See
  "Reading the tags of events" in the module documentation.

    - `positions` a list of non-negative `$all` positions. A position may repeat.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `conn` an existing Postgrex connection or transaction to read through.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.

  Returns `{:ok, %{position => tags}}`, with each position's tags sorted bytewise. A position
  with no row is absent, and so is one whose every tag is left out: tags of a kind the store
  declares deletable, and every tag of an event of a type it declares. `[]` answers
  `{:ok, %{}}` and sends nothing to the database.
  `{:error, :invalid_positions}` answers anything but a list of non-negative bigints, and
  sends nothing either, so a caller's transaction stays usable.
  """
  @callback read_tags(positions :: [non_neg_integer()], opts :: options) ::
              {:ok, %{non_neg_integer() => [String.t()]}}
              | {:error, :invalid_positions}
              | {:error, reason :: term}

  @doc """
  Streams the events matching a query across all streams, in order of their `$all`
  position, reading a page at a time.

  Each page is its own read, and so its own snapshot. There is no result envelope,
  because no one head, read time or complete answer belongs to the whole stream. Use it
  to walk events, not to decide anything: that is what `c:read_query/2` is for.

    - `items` the query: a list of items as described in `Decree.Store.Query`.

    - `opts` an optional keyword list containing:
      - `from` the `$all` position to start at, inclusive. Forward it defaults to 0.
        Backward it defaults to `-1`, meaning the head.
      - `direction` `:forward` (the default) or `:backward`.
      - `name` the name of the event store if provided to `start_link/1`.
      - `conn` an existing Postgrex connection or transaction to read through.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.
      - `read_batch_size` optionally, the number of events to read at a time from
        storage. Defaults to reading 1,000 events per batch.

  `count` and `command_tag` do not apply to a stream and are ignored.

  Returns an `Enumerable` of `Decree.Store.RecordedEvent`, the error
  `Decree.Store.Query.compile/1` answers, or `{:error, :query_names_deletable}` for a query
  naming something deletable (see "Deletable streams" in `Decree.Store`). An invalid
  `direction`, `from` or
  `read_batch_size` raises an `ArgumentError`. A page that fails to read raises while
  the stream is being consumed.
  """
  @callback stream_query(items :: [Decree.Store.Query.item()], opts :: [stream_query_option]) ::
              Enumerable.t(Decree.Store.RecordedEvent.t())
              | {:error, :invalid_query}
              | {:error, :tag_too_long}
              | {:error, :query_names_deletable}

  @doc """
  Delete an existing stream.

    - `stream_uuid` identity of the stream to be deleted.

    - `expected_version` is used for optimistic concurrency checking.
      You can provide a non-negative integer to specify the expected stream
      version. This is used to ensure you can only delete a stream if it is
      at exactly that version.

      You can also provide one of the following values to alter the concurrency
      checking behaviour:

      - `:any_version` - No concurrency check, allow any stream version.
      - `:stream_exists` - Ensure the stream exists, at any version.

    - `type` - used to indicate how the stream is deleted:

      - `:soft` - the stream is marked as deleted, but no events are removed.
      - `:hard` - the stream and its events are permanently deleted from the
        database.

      Soft deletion is the default if the type is not provided.

  Returns `:ok` on success or an error tagged tuple on failure.

  ### Soft delete

  Will mark the stream as deleted, but will not delete its events. Events from
  soft deleted streams will still appear in the globally ordered all events
  (`$all`) stream.

  A soft deleted stream cannot be read nor appended to. Subscriptions to the
  deleted stream will not receive any events, but a subscription to the global
  all events stream will still receive events from the deleted stream.

  ### Hard delete

  Will permanently delete the stream and its events. **This is irreversible and
  will remove data**. Events will be removed from the globally ordered all
  events stream (`$all`).

  The events' tag and type rows go in the same statement, so a query finds nothing of
  them. A store can still hold link rows written before links were removed: a hard delete
  of a stream holding one deletes that event everywhere, from the stream it was appended to
  as well, tags included. Hard deletes take no `$all`: see "Deletable streams" in
  `Decree.Store`.

  The expected version is checked again under the stream's row lock, so an append that
  lands after the first read answers `{:error, :wrong_expected_version}` to an integer
  expected version, and is deleted with the rest under `:any_version` or `:stream_exists`.
  An append into the stream waiting on that lock answers `{:error, :not_found}`.

  Last, the stream's row goes, and its snapshot rows at every version in
  `versioned_snapshots` go with it, by their foreign key's cascade. A snapshot write takes
  the stream's row first (see `c:record_snapshot/2`), so one that commits before the delete
  is deleted with the rest, and one that comes after it finds no stream and writes nothing.

  After being hard deleted, a stream can later be appended to and read as if it
  had never existed.

  ### Examples

  #### Soft delete a stream

  Delete a stream at any version:

      :ok = MyApp.EventStore.delete_stream("stream1", :any_version, :soft)

  Delete a stream at an expected version:

      :ok = MyApp.EventStore.delete_stream("stream2", 3, :soft)

  Delete stream will use soft delete by default so you can omit the type:

      :ok = MyApp.EventStore.delete_stream("stream1", :any_version)

  #### Hard delete a stream

  Since hard deletes are destructive and irreversible they are disabled by
  default. To use hard deletes you must first enable them for the event store:

      defmodule MyApp.EventStore do
        use Decree.Store, otp_app: :my_app, enable_hard_deletes: true
      end

  Or via config:

      # config/config.exs
      config :my_app, MyApp.EventStore, enable_hard_deletes: true

  The value is a boolean, or `"true"` or `"false"` as an environment variable gives them, with
  `{:system, "VAR"}`. Nothing set is off. Anything else raises an `ArgumentError` when the
  config is parsed.

  Hard delete a stream at any version:

      :ok = MyApp.EventStore.delete_stream("stream1", :any_version, :hard)

  Hard delete a stream that should exist:

      :ok = MyApp.EventStore.delete_stream("stream2", :stream_exists, :hard)

  A hard delete of a stream starting with a declared `conditional_appends` prefix, and with
  no declared `deletable` prefix, answers `{:error, :hard_delete_of_conditional_prefix}`
  before any statement is sent, at every expected version. See "Conditional appends" in
  `Decree.Store`.

  A hard delete of a stream holding an event tagged `tag_index:fact` answers
  `{:error, :hard_delete_of_index_fact}` from inside its transaction, which rolls back with
  every row standing. See "Deletable streams" in `Decree.Store`.

  """
  @callback delete_stream(
              stream_uuid :: String.t(),
              expected_version :: :any_version | :stream_exists | non_neg_integer(),
              type :: :soft | :hard,
              opts :: Keyword.t()
            ) ::
              :ok
              | {:error, :stream_not_found}
              | {:error, :stream_deleted}
              | {:error, :wrong_expected_version}
              | {:error, :hard_delete_of_conditional_prefix}
              | {:error, :hard_delete_of_index_fact}
              | {:error, term}

  @doc """
  Delete a stream's own tag rows of the given kinds, leaving its events standing. For
  erasing a subject's value tags: see "Deletable streams" in `Decree.Store`.

    - `stream_uuid` identity of the stream. A soft-deleted stream is not refused, because
      erasure must still reach it.

    - `tag_kinds` a non-empty list of tag kinds. A tag's kind is what precedes its first
      `:`, so `email` does not match `email_verified:x`, and it does match `email:a:b`.
      `command` is refused, alone or among others: those rows are what a decision reads to
      refuse a resend, so erasing them would let one `command_id` commit twice. So are
      `tag_index` and `tag_index_state`: a tag index fact whose state tag is gone could have
      been a disable. See `Decree.Store.TagIndex`.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `conn` an existing Postgrex connection or transaction to delete through.
      - `timeout` an optional timeout for the database transaction, in milliseconds.
        Defaults to 15,000ms.

  Only rows of events appended to this stream go. Rows of another stream holding the same
  tag, an event this stream holds through a link row written before links were removed, and
  an event appended to it later all stay.
  The events keep their `event_types` rows.

  It needs `enable_hard_deletes`. It takes the stream's row and then `$all`, so appends wait
  for it, and it broadcasts nothing.

  Returns `{:ok, deleted_row_count}`, which a second call makes `{:ok, 0}`, or:

    * `{:error, :invalid_tag_kinds}` when `tag_kinds` is not a non-empty list of non-empty
      strings without a `:`, or holds `command`, `tag_index` or `tag_index_state`, before any
      statement is sent.
    * `{:error, :not_supported}` when hard deletes are not enabled, also before any
      statement is sent.
    * `{:error, :cannot_delete_all_stream}` for the `$all` stream.
    * `{:error, :stream_not_found}` when the stream does not exist.
  """
  @callback delete_stream_tags(
              stream_uuid :: String.t(),
              tag_kinds :: [String.t()],
              opts :: options()
            ) ::
              {:ok, non_neg_integer()}
              | {:error, :invalid_tag_kinds}
              | {:error, :not_supported}
              | {:error, :cannot_delete_all_stream}
              | {:error, :stream_not_found}
              | {:error, term}

  @doc """
  Get basic information about a stream, including its version, status, and
  created date.

    - `opts` an optional keyword list containing:
      - `name` the name of the event store if provided to `start_link/1`.
      - `timeout` an optional timeout for the database query, in milliseconds.
        Defaults to 15,000ms.

  Returns `{:ok, StreamInfo.t()}` on success, or an `{:error, reason}` tagged
  tuple. The returned error may be due to one of the following reasons:

    * `{:error, :stream_not_found}` when the stream does not exist.
    * `{:error, :stream_deleted}` when the stream was soft deleted.

  ### Example

      alias Decree.Store.Streams.StreamInfo

      {:ok, %StreamInfo{stream_version: stream_version}} =
        MyApp.EventStore.stream_info("stream-1234")

  """
  @callback stream_info(stream_uuid :: String.t() | :all, opts :: options()) ::
              {:ok, StreamInfo.t()}
              | {:error, :stream_not_found}
              | {:error, :stream_deleted}
              | {:error, reason :: term}

  @doc """
  Create a transient subscription to a given stream.

    - `stream_uuid` is the stream to subscribe to.
      Use the `$all` identifier to subscribe to events from all streams.

    - `opts` is an optional keyword list providing additional subscription
      configuration:
      - `name` the name of the event store if provided to `start_link/1`.
      - `selector` to define a function to filter each event, i.e. returns
        only those elements for which fun returns a truthy value
      - `mapper` to define a function to map each recorded event before sending
        to the subscriber.

  The calling process will be notified whenever new events are appended to
  the given `stream_uuid`.

  As the subscription is transient you do not need to acknowledge receipt of
  each event. The subscriber process will miss any events if it is restarted
  and resubscribes. If you need a persistent subscription with guaranteed
  at-least-once event delivery and back-pressure you should use
  `c:Decree.Store.subscribe_to_stream/4`.

  ## Notification message

  Events will be sent to the subscriber, in batches, as `{:events, events}`
  where events is a collection of `Decree.Store.RecordedEvent` structs.

  ## Example

      {:ok, subscription} = Decree.Store.subscribe(stream_uuid)

      # receive first batch of events
      receive do
        {:events, events} ->
          IO.puts "Received events: " <> inspect(events)
      end

  """
  @callback subscribe(stream_uuid :: String.t(), opts :: transient_subscribe_options) ::
              :ok | {:error, term}

  @doc """
  Create a subscription to a single stream. By default the subscription is persistent.

  The `subscriber` process will be notified of each batch of events appended to
  the single stream identified by `stream_uuid`.

    - `stream_uuid` is the stream to subscribe to.
      Use the `$all` identifier to subscribe to events from all streams.

    - `subscription_name` is used to uniquely identify the subscription.

    - `subscriber` is a process that will be sent `{:events, events}`
      notification messages.

    - `opts` is an optional keyword list providing additional subscription
      configuration:

      - `name` the name of the event store if provided to `start_link/1`.

      - `start_from` is a pointer to the first event to receive.
        It must be one of:
          - `:origin` for all events from the start of the stream (default).
          - `:current` for any new events appended to the stream after the
            subscription has been created.
          - any positive integer for a stream version to receive events after.

      - `selector` to define a function to filter each event, i.e. returns
        only those elements for which the function returns a truthy value.

      - `mapper` to define a function to map each recorded event before sending
        to the subscriber.

      - `concurrency_limit` defines the maximum number of concurrent subscribers
        allowed to connect to the subscription. By default only one subscriber
        may connect. If too many subscribers attempt to connect to the
        subscription an `{:error, :too_many_subscribers}` is returned.

      - `buffer_size` limits how many in-flight events will be sent to the
        subscriber process before acknowledgement of successful processing. This
        limits the number of messages sent to the subscriber and stops their
        message queue from getting filled with events. Defaults to one in-flight
        event.

      - `checkpoint_threshold` determines how frequently a checkpoint is written
        to the database for the subscription after events are acknowledged.
        Increasing the threshold will reduce the number of database writes for
        busy subscriptions, but means that events might be replayed when the
        subscription resumes if the checkpoint cannot be written.
        The default is to persist the checkpoint after each acknowledgement.

      - `checkpoint_after` (milliseconds) used to ensure a checkpoint is written
        after a period of inactivity even if the checkpoint threshold has not
        been met. This ensures checkpoints are consistently written during
        less busy periods. It is only applicable when a checkpoint threshold has
        been set as the default subscription behaviour is to checkpoint after
        each acknowledgement.

      - `max_size` limits the number of events queued in memory by the
        subscription process to prevent excessive memory usage. If the in-memory
        queue exceeds the max size - because the subscriber cannot keep up -
        then events will not be queued in memory, but instead will be read from
        the database on demand once the subscriber process has processed the
        queue. This limit also determines how many events are read from the
        database at a time during catch-up. Defaults to 1,000 events.

      - `partition_by` is an optional function used to partition events to
        subscribers. It can be used to guarantee processing order when multiple
        subscribers have subscribed to a single subscription. The function is
        passed a single argument (an `Decree.Store.RecordedEvent` struct) and must
        return the partition key. As an example to guarantee events for a single
        stream are processed serially, but different streams are processed
        concurrently, you could use the `stream_uuid` as the partition key.

          ```
          by_stream = fn %Decree.Store.RecordedEvent{stream_uuid: stream_uuid} -> stream_uuid end

          {:ok, _subscription} =
            Decree.Store.subscribe_to_stream(stream_uuid, "example", self(),
              concurrency_limit: 10,
              partition_by: by_stream
            )
          ```

      - `timeout` an optional timeout for database queries, in milliseconds.
        Defaults to 15,000ms.

      - `transient` is an optional boolean flag to create a transient subscription.
        By default this is set to `false`. If you want to create a transient
        subscription set this flag to true. Your subscription will not be
        persisted, so if the subscription is restarted, you will receive the events
        again starting from `start_from`.

        An example usage are short lived event handlers that keep their state in
        memory but still want to have the guarantee to have received all events.

        It's possible to create a persistent subscription with some name,
        stop it and later create a transient subscription with the same name. The
        transient subscription will now receive all events starting from `start_from`.
        If you later stop this `transient` subscription and start a persistent
        subscription again with the same name, you will receive the events again
        as if the transient subscription never existed.

  The subscription will resume from the last acknowledged event if it already
  exists. It will ignore the `start_from` argument in this case.

  Returns `{:ok, subscription}` when subscription succeeds.

  ## Notification messages

  Subscribers will initially receive a `{:subscribed, subscription}` message
  once the subscription has successfully subscribed.

  After this message events will be sent to the subscriber, in batches, as
  `{:events, events}` where events is a collection of `Decree.Store.RecordedEvent`
  structs.

  A persistent subscription delivers events only while it holds its advisory lock,
  which keeps one subscription of a given name delivering at a time across every
  instance of the event store on the database. When the lock is lost (its
  database session ended, for example when the lock connection reconnects or the
  database restarts), subscribers receive `{:subscription_lock_lost, subscription}`.
  From then on another instance may take the lock and deliver the same events, so
  a subscriber treats this as the end of its ownership: work it still holds for
  events it has not acknowledged belongs to whichever instance takes the lock, and
  acknowledgements it sends are ignored. The subscription tries the lock again after
  `subscription_retry_interval` and, on taking it, sends `{:subscribed,
  subscription}` again and resumes from the last acknowledged event, so events
  sent but not acknowledged before the loss are sent again. A subscriber that
  connects while the subscription is waiting for its lock receives `{:subscribed,
  subscription}` only once the lock is taken.

  A subscription that finds the lock taken by another instance tries again after the
  full `subscription_retry_interval`, without logging, so idle standby instances do
  not poll. Any other failed attempt to subscribe, such as a lock connection that is
  not up yet, is retried after a wait drawn at random between half of and the whole
  of a backoff from one second, doubling up to that interval, so subscriptions that
  failed together do not retry in step; a warning names the reason and the wait once
  per run of failures, and a successful attempt resets the backoff.

  ## Example

      {:ok, subscription} = Decree.Store.subscribe_to_stream(stream_uuid, "example", self())

      # wait for the subscription confirmation
      receive do
        {:subscribed, ^subscription} ->
          IO.puts "Successfully subscribed to stream: " <> inspect(stream_uuid)
      end

      receive do
        {:events, events} ->
          IO.puts "Received events: " <> inspect(events)

          # acknowledge receipt
          Decree.Store.ack(subscription, events)
      end

  ## Subscription tuning

  Use the `checkpoint_threshold` and `checkpoint_after` options to configure how
  frequently checkpoints are written to the database. By default a subscription
  will persist a checkpoint after each acknowledgement. This can cause high
  write load on the database for busy subscriptions which receive a large number
  of events. This problem is known as write amplification where each event
  written to a stream causes many additional writes as subscriptions acknowledge
  processing of the event.

  The `checkpoint_threshold` controls how frequently checkpoints are persisted.
  Increasing the threshold reduces the number of database writes. For example
  using a threshold of 100 means that a checkpoint is written at most once for
  every 100 events processed. The `checkpoint_after` ensures that a checkpoint
  will still be written after a period of inactivity even when the threshold has
  not been met. This ensures bursts of event processing can be safely handled.

  ## Catch-up read errors

  When a subscription's read of events from storage returns an error, such as a
  query timeout, the subscription holds: it keeps its position and its subscribers,
  and retries the read after a jittered, capped exponential backoff. The backoff is
  one second after the first failed read, doubling after each further consecutive
  one, up to the event store's `subscription_retry_interval` (default one minute), or
  a subscription's own `retry_interval` option. Each wait is drawn at random between
  half of and the whole of that backoff, and is never under one millisecond, so
  subscriptions held on the same error do not retry in step: the first wait is
  between half a second and a second. An interval shorter than one second caps the
  backoff at that interval from the first read, and the wait is still drawn over it;
  only the `retry_interval` option can set one, since the configured
  `subscription_retry_interval` is raised to at least one second. The next successful
  read resets the backoff, and so does losing the subscription's advisory lock.

  An ack received while the read is held still writes the subscription's checkpoint.
  If that write fails, the failure is logged and the write is not retried; a later
  ack writes a later checkpoint, so delivery stays at-least-once.

  Each failed read logs a warning and emits the telemetry event
  `[:decree, :store, :subscription, :catch_up, :error]`, with the measurement
  `system_time` and the metadata `event_store`, `subscription_name`, `stream_uuid`,
  `reason`, `retry_interval` (the wait drawn and scheduled before the next read) and
  `attempt` (the number of consecutive failed reads). An event of a type the event
  store's serializer does not know is not a read error: see "Unknown event types"
  below. Any other event that fails to deserialize, such as a corrupt payload, raises in
  the read and still stops the subscription.

  ## Missed wake-ups

  A subscription that has caught up waits for the store to wake it when events are
  appended to its stream. A wake-up can be lost: the store's notifications connection
  reconnects without replaying what it missed, the read of a notification's events
  fails, or a notification is dropped by the connection. Left alone, the subscription
  would stay behind until the next append to its stream.

  So a caught-up subscription reads its stream's head, the `stream_version` that
  `stream_info/2` returns, every quarter to half of the event store's
  `subscription_head_check_interval` (default one minute, at least one second), or of a
  subscription's own `head_check_interval` option (which can be shorter than one
  second), drawn at random per check. A `head_check_interval` that is not a positive
  integer, in milliseconds, or is above 4_294_967_295 (about 49.7 days, the longest
  timer `Process.send_after/3` can arm; see
  `Decree.Store.Subscriptions.max_head_check_interval/0`), is refused with
  `{:error, :invalid_head_check_interval}` and starts no subscription; the event store's
  setting has the same limit. A head above the last event the subscription has
  received is a missed wake-up only when it was seen at one check and is still above it
  at the next, since a read can see an append before its wake-up arrives. The
  subscription then catches up from storage, as after a gap in the events it received,
  so a lost wake-up is repaired within the interval. Each head is repaired once: a head
  with no event left to read under it, such as `$all`'s after a hard delete, does not
  repair again at every check.

  Each repair logs a warning and emits the telemetry event
  `[:decree, :store, :subscription, :missed_wake_up]`, with the measurement
  `system_time` and the metadata `event_store`, `subscription_name`, `stream_uuid`,
  `head` (the head read now), `seen_head` (the head seen at the previous check, now
  repaired), `last_received` (the last event number the subscription had received),
  `last_seen` (the last event number its subscribers acknowledged) and
  `head_check_interval`. A head read that fails is logged at debug level and retried at
  the next check; it emits nothing and leaves the subscription as it is.

  ## Unknown event types

  An event appended by a newer release of the application can have a type this
  instance's serializer does not know (`c:Decree.Store.Serializer.known_type?/1`). A
  subscription never skips such an event and never crashes on it: it holds before it.

  The subscription delivers the events before it as usual. Once those are acknowledged
  (or filtered out by its `selector`), it writes its checkpoint at once, whatever its
  `checkpoint_threshold` or `checkpoint_after`, sends each subscriber
  `{:subscription_lock_lost, subscription}`, and releases its advisory lock, so another
  instance, one that knows the type, can take the lock and deliver the event. The
  subscription process lives on: `local_subscriptions/1` reports it `:parked`, and it
  delivers nothing and holds no lock. A subscriber that never acknowledges an event
  before the unknown one keeps the subscription where it is.

  While held, the subscription probes, without taking the lock, after a wait drawn as a
  failed catch-up read's is: at random between half of and the whole of a capped
  doubling backoff (one second, doubling per probe, up to `subscription_retry_interval`
  or the subscription's `retry_interval`). If its row has reached the held event, or
  is gone, another instance has delivered it: the subscription waits for its lock as
  after losing it, silently. If the event's type is known now, it takes its lock again
  and delivers the event. Otherwise it stays held.

  Each hold, and each probe that leaves the subscription held, logs a warning naming the
  event's type and number (never its data) and emits the telemetry event
  `[:decree, :store, :subscription, :held]`, with the measurement `system_time` and the
  metadata `event_store`, `subscription_name`, `stream_uuid`, `event_type`,
  `event_number`, `retry_interval` (the wait scheduled before the next probe, `nil` for a
  transient subscription), `attempt` (the number of this alarm in the hold, from one) and
  `transient`.

  A transient subscription has no lock to release and its position lives in its
  process: once the events before the unknown one are acknowledged it alarms and stops,
  with reason `{:shutdown, {:unknown_event_type, event_type}}`.

  A read (`read_*`, `stream_*`, `read_query`) that meets an event of an unknown type
  raises `Decree.Store.UnknownEventTypeError`, naming its type and number. A transient
  `subscribe/2` subscriber is sent the events of its append before it, never it or the rest
  of that append. An event from a later append is still sent, so such a subscriber checks
  `stream_version` or `event_number` continuity itself.

  """
  @callback subscribe_to_stream(
              stream_uuid :: String.t(),
              subscription_name :: String.t(),
              subscriber :: pid,
              opts :: persistent_subscription_options
            ) ::
              {:ok, subscription :: pid}
              | {:error, :already_subscribed}
              | {:error, :subscription_already_exists}
              | {:error, :too_many_subscribers}
              | {:error, :invalid_head_check_interval}
              | {:error, reason :: term}

  @doc """
  Create a subscription to all streams. By default the subscription is persistent.

  See `c:Decree.Store.subscribe_to_stream/4` for options.

  ## Example

      {:ok, subscription} = Decree.Store.subscribe_to_all_streams("all_subscription", self())

      # wait for the subscription confirmation
      receive do
        {:subscribed, ^subscription} ->
          IO.puts "Successfully subscribed to all streams"
      end

      receive do
        {:events, events} ->
          IO.puts "Received events: " <> inspect(events)

          # acknowledge receipt
          Decree.Store.ack(subscription, events)
      end

  """
  @callback subscribe_to_all_streams(
              subscription_name :: String.t(),
              subscriber :: pid,
              opts :: persistent_subscription_options
            ) ::
              {:ok, subscription :: pid}
              | {:error, :already_subscribed}
              | {:error, :subscription_already_exists}
              | {:error, :too_many_subscribers}
              | {:error, :invalid_head_check_interval}
              | {:error, reason :: term}

  @doc """
  List this event store instance's running subscriptions (on this node only).

  Each subscription is described by a map with:

    - `subscription_name` and `stream_uuid`;
    - `subscription_id`, the persistent subscription's row id, or `nil` until
      the subscription first takes its advisory lock (and for a transient
      subscription);
    - `pid`, the subscription process;
    - `transient`, whether the subscription is transient;
    - `status`: `:subscribed` while delivering events (for a persistent
      subscription, holding its advisory lock), `:parked` while waiting to
      subscribe or to take its lock again after losing it, or while held before an
      event of an unknown type (see `c:subscribe_to_stream/4`), or `:unsubscribed`;
    - `last_seen`, the last event number its subscribers acknowledged. It is the
      subscription's delivery position only while `status` is `:subscribed`: a
      parked subscription's value is stale, as another instance may be delivering.
      Compare it with the stream's head, the `stream_version` of `stream_info/2`
      (`stream_info(:all)` for a subscription to all streams), to read its lag.

  Returns `{infos, omitted}`. Each subscription is read concurrently, waiting at
  most `timeout` milliseconds (default 1,000). A subscription that does not
  answer in time is left out of `infos` and counted in `omitted`, so
  `omitted > 0` means the list is partial: some running subscriptions were not
  read. A subscription that stops during its read is no longer running, so it is
  left out of `infos` and not counted.

  ## Options

    - `name` the event store instance (defaults to the event store module).
    - `timeout` the wait for each subscription, in milliseconds.

  """
  @callback local_subscriptions(opts :: options) ::
              {infos :: [subscription_info], omitted :: non_neg_integer()}

  @doc """
  For each persistent subscription id, whether its subscription row exists and
  whether this event store instance's own advisory lock session holds the
  subscription's lock.

  The query runs on the instance's advisory lock connection, the session its
  subscriptions take their locks on. `lock_held: false` for a subscription that
  `local_subscriptions/1` reports as `:subscribed` means the subscription is
  delivering without its lock.

  Returns `{:ok, %{subscription_id => %{row_exists: boolean, lock_held:
  boolean}}}`, or `{:error, reason}` when the query fails or does not answer
  within the `timeout` option (defaults to the event store's query timeout).
  """
  @callback subscription_locks(subscription_ids :: [pos_integer()], opts :: options) ::
              {:ok, %{pos_integer() => %{row_exists: boolean(), lock_held: boolean()}}}
              | {:error, reason :: term}

  @doc """
  Stop a running subscription of this event store instance, given its pid (see
  `local_subscriptions/1`).

  The subscription process exits with reason `:shutdown`; its subscribers that
  monitor it see it stop, and the subscription's lock is released. A subscriber
  can subscribe again to resume from the last acknowledged event.

  Returns `:ok`, or `{:error, :not_found}` when the pid is not a running
  subscription of this instance.
  """
  @callback stop_subscription(subscription :: pid, opts :: options) ::
              :ok | {:error, :not_found}

  @doc """
  Acknowledge receipt of the given events received from a subscription.

  Accepts a single `Decree.Store.RecordedEvent` struct, a list of
  `Decree.Store.RecordedEvent`s, or the event number of the recorded event to
  acknowledge.
  """
  @callback ack(
              subscription :: pid,
              Decree.Store.RecordedEvent.t()
              | list(Decree.Store.RecordedEvent.t())
              | non_neg_integer()
            ) :: :ok | {:error, reason :: term}

  @doc """
  Unsubscribe an existing subscriber from event notifications.

    - `stream_uuid` is the stream to unsubscribe from.

    - `subscription_name` is used to identify the existing subscription process
      to stop.

  Returns `:ok` on success.
  """
  @callback unsubscribe_from_stream(
              stream_uuid :: String.t(),
              subscription_name :: String.t(),
              opts :: options
            ) ::
              :ok

  @doc """
  Unsubscribe an existing subscriber from all event notifications.

    - `subscription_name` is used to identify the existing subscription process
      to stop.

  Returns `:ok` on success.
  """
  @callback unsubscribe_from_all_streams(subscription_name :: String.t(), opts :: options) :: :ok

  @doc """
  Delete an existing persistent subscription.

    - `stream_uuid` is the stream the subscription is subscribed to.

    - `subscription_name` is used to identify the existing subscription to
      remove.

  Returns `:ok` on success.
  """
  @callback delete_subscription(
              stream_uuid :: String.t(),
              subscription_name :: String.t(),
              opts :: options
            ) ::
              :ok | {:error, term}

  @doc """
  Delete an existing persistent subscription to all streams.

    - `subscription_name` is used to identify the existing subscription to
      remove.

  Returns `:ok` on success.
  """
  @callback delete_all_streams_subscription(subscription_name :: String.t(), opts :: options) ::
              :ok | {:error, term}

  @doc """
  Read a snapshot, if available, for a given source.

  `snapshot_version: version` (a non-negative integer) in `opts` is required: reads that
  version's row of `versioned_snapshots`, where a source has one row per version. A nil,
  absent or invalid `snapshot_version` is refused with `{:error, :invalid_snapshot_version}`.

  Returns `{:ok, %Decree.Store.Snapshots.SnapshotData{}}` on success, or
  `{:error, :snapshot_not_found}` when unavailable.
  """
  @callback read_snapshot(source_uuid :: String.t(), opts :: options) ::
              {:ok, SnapshotData.t()}
              | {:error, :snapshot_not_found}
              | {:error, :invalid_snapshot_version}

  @doc """
  Record a snapshot of the data and metadata for a given source.

  The snapshot is recorded as its `snapshot_version`'s row of `versioned_snapshots`,
  replacing only that version's. It is written in one transaction that first takes the row
  of the stream named `source_uuid`: with no such stream it answers
  `{:error, :stream_not_found}` and writes nothing. The row's foreign key to its stream
  cascades the stream's hard delete, so a snapshot never outlives its stream. A snapshot
  whose `snapshot_version` is nil or not a non-negative integer is refused with
  `{:error, :invalid_snapshot_version}` and writes nothing.

  Returns `:ok` on success.
  """
  @callback record_snapshot(snapshot :: SnapshotData.t(), opts :: options) ::
              :ok
              | {:error, :stream_not_found}
              | {:error, :invalid_snapshot_version}
              | {:error, reason :: term}

  @doc """
  Delete a previously recorded snapshot for a given source: every version's row.

  Returns `:ok` on success, or when the snapshot does not exist.
  """
  @callback delete_snapshot(source_uuid :: String.t(), opts :: options) ::
              :ok | {:error, reason :: term}

  @doc """
  Delete the rows of `versioned_snapshots` with the given `source_type` whose
  `snapshot_version` is below the given one. For an operator, once no running code reads an
  older version.

  Returns `{:ok, count}`, the number of rows deleted, or `{:error, :invalid_snapshot_version}`
  when `snapshot_version` is not a non-negative integer.
  """
  @callback prune_snapshots(
              source_type :: String.t(),
              snapshot_version :: non_neg_integer,
              opts :: options
            ) ::
              {:ok, non_neg_integer}
              | {:error, :invalid_snapshot_version}
              | {:error, reason :: term}

  @doc """
  Returns all running Decree.Store instances.

  Note that order is not guaranteed.
  """
  @spec all_instances :: list({event_store :: module(), [{:name, atom()}]})
  def all_instances do
    for {event_store, name} <- Config.all(), Process.whereis(name) do
      {event_store, [name: name]}
    end
  end
end
