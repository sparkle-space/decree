defmodule Decree.Application.Supervisor do
  @moduledoc false

  use Supervisor

  alias Decree.Aggregates.ExecutionContext
  alias Decree.Application.Config
  alias Decree.Event.Append
  alias Decree.UUID

  require Logger

  # Options read from the `use` line and nowhere else: they name what protects personal
  # data, so neither the environment, nor start options, nor `init/1` can change them.
  @source_opts [:key_source, :personal_metadata_keys]

  @doc """
  Retrieves the compile time configuration.
  """
  def compile_config(application, opts) do
    otp_app = Keyword.fetch!(opts, :otp_app)
    env_opts = Application.get_env(otp_app, application, [])

    config = opts |> Keyword.merge(env_opts) |> Keyword.drop(@source_opts)

    {otp_app, config}
  end

  @doc """
  Retrieves the source-level options from the `use` line, and only from there.
  """
  def source_opts(opts) do
    key_source = Keyword.get(opts, :key_source)
    personal_metadata_keys = Keyword.get(opts, :personal_metadata_keys, [])

    unless is_atom(key_source) do
      raise ArgumentError,
            "expected :key_source to be a module but got: " <> inspect(key_source)
    end

    unless is_list(personal_metadata_keys) and Enum.all?(personal_metadata_keys, &is_atom/1) do
      raise ArgumentError,
            "expected :personal_metadata_keys to be a list of atoms but got: " <>
              inspect(personal_metadata_keys)
    end

    {key_source, personal_metadata_keys}
  end

  @doc """
  Retrieves the runtime configuration.
  """
  def runtime_config(application, otp_app, config, opts) do
    config =
      config
      |> Keyword.merge(opts)
      |> Keyword.put(:otp_app, otp_app)
      |> Keyword.put(:application, application)

    case application_init(application, config) do
      {:ok, config} -> {:ok, config}
      :ignore -> :ignore
    end
  end

  @doc """
  Starts the application supervisor.
  """
  def start_link(application, otp_app, config, name, opts) do
    Supervisor.start_link(
      __MODULE__,
      {application, otp_app, config, name, opts},
      name: name
    )
  end

  def init({application, otp_app, config, name, opts}) do
    case runtime_config(application, otp_app, config, opts) do
      {:ok, config} ->
        {event_store_child_spec, config} = event_store_child_spec(name, config)
        {pubsub_child_spec, config} = pubsub_child_spec(name, config)
        {registry_child_spec, config} = registry_child_spec(name, config)

        :ok = Config.associate(self(), name, config)

        children =
          event_store_child_spec ++
            conditional_targets_child_spec(application, name) ++
            pubsub_child_spec ++
            registry_child_spec ++
            app_child_spec(name, config)

        Supervisor.init(children, strategy: :one_for_one)

      :ignore ->
        :ignore
    end
  end

  # A rule route's half and the store's half of one declaration, checked by a child started right
  # after the store's, which has parsed its config and registered it by then. A routed aggregate
  # that declares a command window appends under a condition on every command, so it is a target
  # too, read from the routes and the definitions here at start: the router names no aggregate's
  # definition. An application with neither gets no such child, and asks its store nothing. See
  # "Two declarations" in `Decree.Decision.Rule` and "Command window" in `Decree.Aggregate`.
  defp conditional_targets_child_spec(application, name) do
    case {application.__conditional_targets__(), windowed_routes(application)} do
      {[], []} ->
        []

      {routed, windowed} ->
        [
          %{
            id: {__MODULE__, :conditional_targets},
            start:
              {__MODULE__, :check_conditional_targets, [application, routed, windowed, name]},
            restart: :temporary
          }
        ]
    end
  end

  # Each route to an aggregate that declares a command window, as `{command, route}`.
  defp windowed_routes(application) do
    for command <- application.__registered_commands__(),
        route = application.__route__(command),
        aggregate = route[:aggregate],
        Decree.Aggregate.command_window_ms(aggregate) != nil,
        do: {command, route}
  end

  @doc false
  # Raises at start for a rule route's aggregate or a windowed aggregate the running store does
  # not declare, and for a windowed route whose identity gives no stream tag, and otherwise
  # starts nothing.
  def check_conditional_targets(application, routed, windowed, name) do
    Enum.each(windowed, &check_window_identity!(application, &1))

    declared = Decree.EventStore.conditional_targets(name)

    with [aggregate | _] <- routed -- declared do
      raise ArgumentError,
            "#{inspect(application)} routes a rule to #{inspect(aggregate)}, which its event " <>
              "store does not declare: add it to `conditional_appends: [targets: [...]]`"
    end

    windowed_aggregates = windowed |> Enum.map(fn {_command, route} -> route[:aggregate] end)

    with [aggregate | _] <- Enum.uniq(windowed_aggregates) -- declared do
      raise ArgumentError,
            "#{inspect(application)} routes commands to #{inspect(aggregate)}, which declares " <>
              "a command window, and its event store does not declare it: add it to " <>
              "`conditional_appends: [targets: [...]]`"
    end

    for target <- declared -- routed, Decree.Aggregate.command_window_ms(target) == nil do
      Logger.warning(fn ->
        "#{inspect(application)}'s event store declares the conditional target " <>
          "#{inspect(target)}, and no route in it names a rule for that aggregate"
      end)
    end

    :ignore
  end

  # A window's scope is the stream's entity tag. With none, the same kind's id on another stream
  # would read as this stream's duplicate, so such a route does not start.
  defp check_window_identity!(application, {command, route}) do
    named =
      "#{inspect(application)} routes #{inspect(command)} to #{inspect(route[:aggregate])}, " <>
        "which declares a command window,"

    cond do
      route[:identity_opaque] ->
        raise ArgumentError,
              "#{named} by an opaque identity, which gives its stream no entity tag"

      stream_tag(route) == nil ->
        raise ArgumentError,
              "#{named} by an identity that gives its stream no entity tag: name its kind " <>
                "with `stream_tag:`"

      true ->
        :ok
    end
  end

  # The route's tag as an append computes it, for any canonical identity.
  defp stream_tag(route) do
    Append.stream_tag(%ExecutionContext{
      identity: route[:identity],
      stream_tag: route[:stream_tag],
      aggregate_identity: UUID.uuid4()
    })
  end

  defp app_child_spec(name, config) do
    task_dispatcher_name = Module.concat([name, Decree.Commands.TaskDispatcher])
    aggregates_supervisor_name = Module.concat([name, Decree.Aggregates.Supervisor])
    subscriptions_name = Module.concat([name, Decree.Subscriptions])
    registry_name = Module.concat([name, Decree.Subscriptions.Registry])
    snapshotting = Keyword.get(config, :snapshotting, %{})
    hibernate_after = Keyword.get(config, :hibernate_after, :infinity)

    warn_declared_snapshots(name, snapshotting)

    [
      {Task.Supervisor, name: task_dispatcher_name, hibernate_after: hibernate_after},
      {Decree.Aggregates.Supervisor,
       name: aggregates_supervisor_name,
       application: name,
       snapshotting: snapshotting,
       hibernate_after: hibernate_after},
      {Decree.Subscriptions.Registry,
       application: name, name: registry_name, hibernate_after: hibernate_after},
      {Decree.Subscriptions,
       application: name, name: subscriptions_name, hibernate_after: hibernate_after}
    ]
  end

  # A module that declares `snapshot:` on its definition takes its options from there, so its
  # entry here is ignored: said once per module, at start.
  defp warn_declared_snapshots(name, snapshotting) when is_map(snapshotting) do
    for {aggregate_module, _options} <- snapshotting,
        is_atom(aggregate_module),
        Decree.Aggregate.declared(aggregate_module) != nil do
      Logger.warning(fn ->
        "#{inspect(aggregate_module)} declares `snapshot:` on its definition and is also in " <>
          "the `snapshotting:` config of #{inspect(name)}: the definition wins, and the " <>
          "config entry is ignored"
      end)
    end

    :ok
  end

  defp warn_declared_snapshots(_name, _snapshotting), do: :ok

  # The application's `Decree.Store` module, started as its child under the config's `name:`,
  # by default the module's own. `Decree.EventStore` calls it with that name.
  defp event_store_child_spec(name, config) do
    store_config = event_store_config!(name, Keyword.get(config, :event_store))

    {event_store, store_config} = Keyword.pop(store_config, :event_store)

    verify_event_store!(name, event_store)

    store_name = Keyword.get(store_config, :name, event_store)

    # Rename `prefix` config to `schema`
    store_config =
      case Keyword.pop(store_config, :prefix) do
        {nil, store_config} -> store_config
        {prefix, store_config} -> Keyword.put(store_config, :schema, prefix)
      end

    config = Keyword.put(config, :event_store, %{event_store: event_store, name: store_name})

    {[{event_store, store_config}], config}
  end

  defp event_store_config!(application, nil) do
    raise ArgumentError, "missing :event_store config for application " <> inspect(application)
  end

  defp event_store_config!(application, config) do
    if Keyword.has_key?(config, :adapter) do
      raise ArgumentError,
            "the :adapter option in the :event_store config of application " <>
              inspect(application) <>
              " is no longer supported: the event store adapter layer was removed. " <>
              "Name the Decree.Store module in :event_store instead, " <>
              "e.g. `event_store: [event_store: MyApp.EventStore]`"
    end

    config
  end

  defp verify_event_store!(application, event_store) do
    unless event_store do
      raise ArgumentError,
            "missing :event_store option in the :event_store config of application " <>
              inspect(application)
    end

    unless Code.ensure_loaded?(event_store) and implements?(event_store, Decree.Store) do
      raise ArgumentError,
            "module " <>
              inspect(event_store) <>
              " is not a Decree.Store, " <>
              "ensure you pass an event store module to the :event_store config in application " <>
              inspect(application)
    end
  end

  # Returns `true` if module implements behaviour.
  defp implements?(module, behaviour) do
    behaviours = Keyword.take(module.__info__(:attributes), [:behaviour])

    [behaviour] in Keyword.values(behaviours)
  end

  defp pubsub_child_spec(name, config) do
    {adapter, adapter_config} =
      Decree.PubSub.adapter(name, Keyword.get(config, :pubsub, :local))

    {:ok, child_spec, adapter_meta} = adapter.child_spec(name, adapter_config)

    config = Keyword.put(config, :pubsub, {adapter, adapter_meta})

    {child_spec, config}
  end

  defp registry_child_spec(name, config) do
    {adapter, adapter_config} =
      Decree.Registration.adapter(name, Keyword.get(config, :registry, :local))

    {:ok, child_spec, adapter_meta} = adapter.child_spec(name, adapter_config)

    config = Keyword.put(config, :registry, {adapter, adapter_meta})

    {child_spec, config}
  end

  defp application_init(application, config) do
    if Code.ensure_loaded?(application) and function_exported?(application, :init, 1) do
      application.init(config)
    else
      {:ok, config}
    end
  end
end
