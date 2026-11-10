defmodule Decree.Aggregate do
  @moduledoc """
  What an aggregate module declares about itself, read where the aggregate is defined.

  ## Snapshots

      defmodule MyApp.ExampleAggregate do
        use Decree.Aggregate, snapshot: [every: 10, version: 1]

        defstruct [:id, items: []]

        # execute/2 and apply/2 as for any aggregate
      end

  `every` is how many events the aggregate takes between two snapshots, a positive integer.
  `version` is the version of the snapshot's shape, a non-negative integer, 1 when left out.
  Both are integer literals, checked when the module compiles: a missing or bad `every`, a
  bad `version`, an unknown key under `snapshot:` or an unknown option fails the build.
  `use Decree.Aggregate` with no `snapshot:` declares nothing.

  A module that declares `snapshot:` takes its options from here and not from the
  application's `snapshotting:` config, which remains the fallback for a module that does
  not, until a later release removes it. A module in both logs a warning when the
  application starts, and its config entry is ignored. `snapshot_options/2` answers the
  options in force. See "Snapshotting" in `Decree.Aggregates.Aggregate` for how a snapshot
  is taken and read.

  ## Snapshot versions

  The store keeps one row per stream and snapshot version. Code at version 1 and code at
  version 2 read and write their own rows, so during a rollout neither overwrites the
  other's, and a version bump folds the stream from its events for the new version until a
  row exists for it. `warm_snapshot/3` records that row ahead of the first command.
  `prune_snapshots/2` deletes the rows of the versions below the declared one: run it once
  no running code reads an older version.

  ## Command window

      defmodule MyApp.Account do
        use Decree.Aggregate, command_window: [ms: 300_000]

        defstruct [:account_id, :owner]

        def authorize(%__MODULE__{owner: owner}, %{actor: actor}) when actor != owner,
          do: {:error, :unauthorized}

        def authorize(%__MODULE__{}, _command), do: :ok

        # execute/2 and apply/2 as for any aggregate
      end

  An aggregate that declares a command window answers a repeat of a `command_id` from the
  store's command index, as a `Decree.Decision` does, and not from its state. `ms` is the
  window's length in milliseconds, a positive integer literal, checked when the module
  compiles: a missing or bad `ms`, an unknown key under `command_window:`, or a module with no
  public `authorize/2` fails the build. The length lives on the definition alone: no config is
  read and there is no default. `command_window_ms/1` answers it. A module with no
  `command_window:` has no window: none of what follows runs for it, and `authorize/2` is never
  called.

  `c:authorize/2` holds the aggregate's admission, ownership and authorization guards, and
  answers `:ok` or `{:error, reason}`. In the aggregate's process each attempt at a command runs,
  in this order:

    1. The route's `before_execute`, as on any route.
    2. `authorize/2` on the state, only when the stream is not empty. Its refusal is the
       answer as it stands, with nothing read: an actor who may not ask never learns that a
       `command_id` is live. An empty stream has nothing to own yet: a create's own guards stay
       in `execute/2`. The emptiness is this process's state, so a process that has not yet
       received an event another pod appended to its stream reads the window before
       `authorize/2`, and can answer a duplicate or a reused id ahead of a refusal, appending
       nothing. An append on that stale state loses its expected version and appends nothing:
       after a rebuild that succeeds, and while attempts remain, the next attempt runs
       `authorize/2`; otherwise the dispatch answers the rebuild's or the retry's error.
    3. One read of the command item (`command:<command_id>`) and the tag index item, with the
       route's rule's items in the same read when the route has a rule, and the classification
       of a decision's step 7 with this window, on an empty stream too: a reuse on another
       stream needs no state to see.
    4. `execute/2`, only for a command the window found open.
    5. The append, always under the condition the read built, whose `target` is the aggregate.

  The answers:

    - `{:error, :duplicate_command}` - a match inside the window, from this command's kind on
      this stream. Nothing is appended and `execute/2` is not called.
    - `{:error, :reused_command_id}` - a match inside the window from another command kind, or
      on another stream (the route's stream tag is the scope). Nothing is appended.
    - `{:error, :tag_index_not_enabled}` - the `command` family, or a family the route's rule
      requires, is not enabled by the tag index facts, as "Index facts" in `Decree.Decision`
      says: a resend refuses as a new command does.
    - `{:error, :command_id_required}`, `{:error, :invalid_command_id}` or
      `{:error, :invalid_aggregate_identity}` for a direct call that bypassed the router and
      carries no usable `command_id` or stream tag.
    - otherwise what the command answers on any route.

  The window's semantics are a decision's: the stamp and `read_at` are both the database's
  clock, the edge is outside, a negative age is inside, and after the window the same id is a
  new command and appends. Two writers racing one `command_id` both read the window open, and
  one commits: on the same stream the other loses its expected version, rebuilds and reads
  again, and is the duplicate; on another stream of the same kind its condition fails, it reads
  again, and the id is reused. Neither answers a store's atom while attempts remain. A route
  with a rule counts the command item against the rule's `max_items`; a route with none takes a
  rule's default bounds (16 items, 48 probes, 1,000 matched events, 100 new events per batch).

  A host moving an aggregate onto this window from one timed by its own pods' clocks runs both
  through a rolling deploy. A new pod refuses every resend an old pod would when `ms` is at
  least the old window plus twice the largest offset between a pod's clock and the database's:
  the old pod stamps before its commit, so the time between only shortens the age the database
  measures. The host knows the old window, so the host's test holds that floor.

  An application checks a windowed aggregate as it starts: every one it routes must be a target
  of its event store's `conditional_appends: [targets: [...]]`, and every route to one must
  give its stream an entity tag, so an opaque identity, or a function identity with no
  `stream_tag:`, raises an `ArgumentError` and the application does not start. A declared
  target that declares a window draws no warning. A router outside the application is not
  checked, and its dispatch answers the store's `{:error, :undeclared_conditional_target}`.

  `check_command_window/2` is the same read and classification for a host's facade, before its
  business proofs: the probe reads the index, and the dispatch's condition still decides under
  the store's lock.

  `[:decree, :decision, :read]` fires once per attempt that read and passed the index check, and
  `[:decree, :decision, :retry]` for a failed condition or a command match only a rule's second
  read has (`:new_command_match`), as on a rule route. Their `decision` is
  the route's rule, or the aggregate's module on a route with none. See
  `Decree.Decision.Rule`.
  """

  alias Decree.Aggregates.Aggregate, as: AggregateProcess
  alias Decree.Aggregates.{AggregateStateBuilder, ExecutionContext}
  alias Decree.Application.Config
  alias Decree.Command
  alias Decree.Decision.Dispatch
  alias Decree.EventStore
  alias Decree.EventStore.{SnapshotData, TypeProvider}
  alias Decree.Middleware.{ExtractAggregateIdentity, Pipeline}
  alias Decree.Snapshotting

  @snapshot_keys [:every, :version]
  @command_window_keys [:ms]

  @doc """
  An aggregate's admission, ownership and authorization guards, required of a module that
  declares `command_window:`. See "Command window". Called on an existing stream only, before
  the window is read, and never for a module that declares no window.
  """
  @callback authorize(state :: struct(), command :: struct()) :: :ok | {:error, term()}

  @optional_callbacks authorize: 2

  @doc false
  defmacro __using__(opts) do
    {snapshot, command_window} = definition!(__CALLER__.module, opts)

    check_authorize =
      if command_window do
        quote do: @before_compile({Decree.Aggregate, :__check_authorize__})
      end

    quote do
      unquote(check_authorize)

      @doc false
      def __decree_aggregate__(:snapshot), do: unquote(snapshot)
      def __decree_aggregate__(:command_window), do: unquote(command_window)
    end
  end

  # A window is decided after `authorize/2`, so a module that declares one and defines no public
  # `authorize/2` would answer a duplicate to whoever asked.
  @doc false
  defmacro __check_authorize__(%Macro.Env{module: module}) do
    if not Module.defines?(module, {:authorize, 2}, :def) do
      invalid!(
        module,
        "`command_window:` requires a public `authorize/2`, the aggregate's admission, " <>
          "ownership and authorization guards, answering `:ok` or `{:error, reason}`"
      )
    end

    :ok
  end

  # The options are read as written: a value that is not an integer literal is refused,
  # which keeps the declaration readable at the line that makes it.
  defp definition!(module, opts) do
    unless Keyword.keyword?(opts) do
      invalid!(
        module,
        "expected `use Decree.Aggregate` options to be a keyword list, got: #{Macro.to_string(opts)}"
      )
    end

    case Keyword.keys(opts) -- [:snapshot, :command_window] do
      [] -> :ok
      [option | _] -> invalid!(module, "unknown option #{inspect(option)}")
    end

    snapshot =
      case Keyword.fetch(opts, :snapshot) do
        :error -> nil
        {:ok, snapshot} -> snapshot!(module, snapshot)
      end

    command_window =
      case Keyword.fetch(opts, :command_window) do
        :error -> nil
        {:ok, command_window} -> command_window!(module, command_window)
      end

    {snapshot, command_window}
  end

  defp command_window!(module, command_window) do
    unless Keyword.keyword?(command_window) do
      invalid!(
        module,
        "expected `command_window:` to be a keyword list, got: " <>
          Macro.to_string(command_window)
      )
    end

    case Keyword.keys(command_window) -- @command_window_keys do
      [] -> :ok
      [key | _] -> invalid!(module, "unknown `command_window:` key #{inspect(key)}")
    end

    case Keyword.fetch(command_window, :ms) do
      {:ok, ms} when is_integer(ms) and ms > 0 ->
        [ms: ms]

      {:ok, ms} ->
        invalid!(
          module,
          "expected `command_window: [ms: ...]` to be a positive integer, got: " <>
            Macro.to_string(ms)
        )

      :error ->
        invalid!(module, "`command_window:` requires `ms:`, a positive integer")
    end
  end

  defp snapshot!(module, snapshot) do
    unless Keyword.keyword?(snapshot) do
      invalid!(
        module,
        "expected `snapshot:` to be a keyword list, got: #{Macro.to_string(snapshot)}"
      )
    end

    case Keyword.keys(snapshot) -- @snapshot_keys do
      [] -> :ok
      [key | _] -> invalid!(module, "unknown `snapshot:` key #{inspect(key)}")
    end

    every =
      case Keyword.fetch(snapshot, :every) do
        {:ok, every} when is_integer(every) and every > 0 ->
          every

        {:ok, every} ->
          invalid!(
            module,
            "expected `snapshot: [every: ...]` to be a positive integer, got: " <>
              Macro.to_string(every)
          )

        :error ->
          invalid!(module, "`snapshot:` requires `every:`, a positive integer")
      end

    version =
      case Keyword.get(snapshot, :version, 1) do
        version when is_integer(version) and version >= 0 ->
          version

        version ->
          invalid!(
            module,
            "expected `snapshot: [version: ...]` to be a non-negative integer, got: " <>
              Macro.to_string(version)
          )
      end

    [snapshot_every: every, snapshot_version: version]
  end

  defp invalid!(module, message) do
    raise ArgumentError, "#{inspect(module)} uses Decree.Aggregate: " <> message
  end

  @doc """
  The snapshot options in force for `aggregate_module` in `application`.

  The definition's (`use Decree.Aggregate, snapshot: ...`) when the module declares them;
  otherwise the application's `snapshotting:` config entry for the module; `[]` when neither
  enables snapshots. Answers `[snapshot_every: every, snapshot_version: version]`, with the
  version 1 when the config entry leaves it out.
  """
  @spec snapshot_options(Decree.Application.t(), module()) :: keyword()
  def snapshot_options(application, aggregate_module) when is_atom(aggregate_module) do
    resolve_snapshot_options(aggregate_module, Config.get(application, :snapshotting))
  end

  @doc false
  # `snapshot_options/2` for a caller that has already read the `snapshotting:` config map.
  def resolve_snapshot_options(aggregate_module, snapshotting) do
    case declared(aggregate_module) do
      nil -> configured(aggregate_module, snapshotting)
      options -> options
    end
  end

  @doc false
  # The definition's options, or nil for a module that declares none or does not
  # `use Decree.Aggregate`.
  def declared(aggregate_module), do: definition(aggregate_module, :snapshot)

  @doc """
  The length of `aggregate_module`'s command window in milliseconds, as its definition declares
  it (`use Decree.Aggregate, command_window: [ms: n]`), or nil for a module that declares none
  or does not `use Decree.Aggregate`. No config is read, and there is no default length. See
  "Command window".
  """
  @spec command_window_ms(module()) :: pos_integer() | nil
  def command_window_ms(aggregate_module) when is_atom(aggregate_module) do
    case definition(aggregate_module, :command_window) do
      [ms: ms] -> ms
      nil -> nil
    end
  end

  @doc """
  What a dispatch of `command` would answer from its aggregate's command window now, read before
  a host's own business proofs: `:ok`, `{:error, :duplicate_command}`,
  `{:error, :reused_command_id}`, `{:error, :tag_index_not_enabled}`, or the refusal a dispatch
  gives first, such as `{:error, :command_id_required}`, `{:error, :invalid_command_id}` or
  `{:error, :invalid_aggregate_identity}`.

  It resolves the command's route through the application's router, computes the stream tag
  as that route's dispatch does, and reads and classifies through the code the aggregate's
  process runs, the window's items alone: on a rule route the rule's items are the dispatch's.
  It reads the command index only. It does not fold the aggregate's state or call
  `authorize/2`: a host that needs that order runs its own fold and `authorize/2` first. And it
  decides nothing: the dispatch's condition still decides under the store's lock, so a racing
  writer can commit between the probe and the dispatch.

  The command is read as sent, before any middleware of the router's own. Raises an
  `ArgumentError` for a command the application routes nowhere, and for one whose route reaches
  a decision, or an aggregate that declares no `command_window:`: a probe that sees no window is
  a wiring bug.
  """
  @spec check_command_window(Decree.Application.t(), struct()) :: :ok | {:error, term()}
  def check_command_window(application, %command_module{} = command) when is_atom(application) do
    module = Config.get(application, :application)

    route =
      module.__route__(command_module) ||
        raise ArgumentError, "#{inspect(command_module)} has no route in #{inspect(module)}"

    aggregate =
      route[:aggregate] ||
        raise ArgumentError,
              "#{inspect(command_module)} is decided by #{inspect(route[:decision])}, which " <>
                "has a command window of its own"

    window_ms =
      command_window_ms(aggregate) ||
        raise ArgumentError,
              "#{inspect(command_module)} is routed to #{inspect(aggregate)}, which declares " <>
                "no `command_window:`"

    with :ok <- Command.check_id(Map.get(command, :command_id)),
         {:ok, identity} <- route_identity(command, route) do
      context = %ExecutionContext{
        command: command,
        identity: route[:identity],
        aggregate_identity: identity,
        stream_tag: route[:stream_tag]
      }

      case Dispatch.read_aggregate(application, aggregate, window_ms, context) do
        {:ok, _result, _read} -> :ok
        {:error, _reason} = error -> error
      end
    end
  end

  # The identity the route's dispatch resolves, through the middleware that resolves it.
  defp route_identity(command, route) do
    pipeline = %Pipeline{
      command: command,
      identity: route[:identity],
      identity_prefix: route[:identity_prefix],
      identity_opaque: route[:identity_opaque]
    }

    case ExtractAggregateIdentity.before_dispatch(pipeline) do
      %Pipeline{halted: true, response: {:error, _reason} = error} -> error
      %Pipeline{assigns: %{aggregate_identity: identity}} -> {:ok, identity}
    end
  end

  defp definition(aggregate_module, key) do
    if Code.ensure_loaded?(aggregate_module) and
         function_exported?(aggregate_module, :__decree_aggregate__, 1) do
      aggregate_module.__decree_aggregate__(key)
    end
  end

  defp configured(aggregate_module, snapshotting) when is_map(snapshotting) do
    options = Map.get(snapshotting, aggregate_module) || []

    case Keyword.get(options, :snapshot_every) do
      every when is_integer(every) and every > 0 ->
        [snapshot_every: every, snapshot_version: Keyword.get(options, :snapshot_version, 1)]

      _disabled ->
        []
    end
  end

  defp configured(_aggregate_module, _snapshotting), do: []

  @doc """
  Records a snapshot of an aggregate's stream at its head, for the declared version.

  Folds the stream as an aggregate starting would (from this version's row, if there is
  one, and the events after it) without starting an aggregate process, then:

    - `{:ok, :empty}` - the stream has no events, and nothing is recorded;
    - `{:ok, :current}` - this version's row is already at the head;
    - `{:ok, :recorded}` - a row at the head is recorded, through the same path an
      aggregate's own snapshot takes, so personal state is encrypted.

  It records whatever `every` says: the caller chooses which streams to warm. Answers
  `{:error, :snapshotting_not_configured}` when snapshots are not enabled for the module,
  `{:error, :key_unknown}` or `{:error, :unknown_event_type}` when the fold refuses (see
  `Decree.Aggregates.Aggregate`), or the error that stopped the record.
  """
  @spec warm_snapshot(Decree.Application.t(), module(), String.t()) ::
          {:ok, :recorded | :current | :empty} | {:error, term()}
  def warm_snapshot(application, aggregate_module, aggregate_uuid)
      when is_atom(aggregate_module) and is_binary(aggregate_uuid) do
    case snapshot_options(application, aggregate_module) do
      [] ->
        {:error, :snapshotting_not_configured}

      options ->
        snapshotting = Snapshotting.new(application, aggregate_uuid, options)

        # Read here as well as in the fold, which does not say what it started from. A row
        # recorded between the two reads costs at most one more write of the same head.
        row_version =
          case Snapshotting.read_snapshot(snapshotting) do
            {:ok, %SnapshotData{source_version: source_version}} -> source_version
            {:error, _reason} -> nil
          end

        %AggregateProcess{
          application: application,
          aggregate_module: aggregate_module,
          aggregate_uuid: aggregate_uuid,
          snapshotting: snapshotting
        }
        |> AggregateStateBuilder.populate()
        |> warm(row_version)
    end
  end

  defp warm({:error, _reason} = error, _row_version), do: error
  defp warm(%AggregateProcess{aggregate_version: 0}, _row_version), do: {:ok, :empty}
  defp warm(%AggregateProcess{aggregate_version: version}, version), do: {:ok, :current}

  defp warm(%AggregateProcess{} = aggregate, _row_version) do
    %AggregateProcess{
      aggregate_state: aggregate_state,
      aggregate_version: aggregate_version,
      snapshotting: snapshotting
    } = aggregate

    case Snapshotting.take_snapshot(snapshotting, aggregate_version, aggregate_state) do
      {:ok, %Snapshotting{}} -> {:ok, :recorded}
      {:error, _reason} = error -> error
    end
  end

  @doc """
  Deletes the module's snapshot rows whose version is below its declared one.

  The rows are found by the module's source type, as the configured
  `Decree.EventStore.TypeProvider` names its state. Run it once no running code reads an
  older version: a pod still at an older version would fold from events on every start.
  Answers `{:ok, count}`, or `{:error, :snapshotting_not_configured}` when snapshots are not
  enabled for the module.
  """
  @spec prune_snapshots(Decree.Application.t(), module()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def prune_snapshots(application, aggregate_module) when is_atom(aggregate_module) do
    case snapshot_options(application, aggregate_module) do
      [] ->
        {:error, :snapshotting_not_configured}

      options ->
        source_type = TypeProvider.to_string(struct(aggregate_module))
        snapshot_version = Keyword.fetch!(options, :snapshot_version)

        EventStore.prune_snapshots(application, source_type, snapshot_version)
    end
  end
end
