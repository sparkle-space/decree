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
  """

  alias Decree.Aggregates.Aggregate, as: AggregateProcess
  alias Decree.Aggregates.AggregateStateBuilder
  alias Decree.Application.Config
  alias Decree.EventStore
  alias Decree.EventStore.{SnapshotData, TypeProvider}
  alias Decree.Snapshotting

  @snapshot_keys [:every, :version]

  @doc false
  defmacro __using__(opts) do
    snapshot = definition!(__CALLER__.module, opts)

    quote do
      @doc false
      def __decree_aggregate__(:snapshot), do: unquote(snapshot)
    end
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

    case Keyword.keys(opts) -- [:snapshot] do
      [] -> :ok
      [option | _] -> invalid!(module, "unknown option #{inspect(option)}")
    end

    case Keyword.fetch(opts, :snapshot) do
      :error -> nil
      {:ok, snapshot} -> snapshot!(module, snapshot)
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
  def declared(aggregate_module) do
    if Code.ensure_loaded?(aggregate_module) and
         function_exported?(aggregate_module, :__decree_aggregate__, 1) do
      aggregate_module.__decree_aggregate__(:snapshot)
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
