defmodule Decree.Application.Supervisor do
  @moduledoc false

  use Supervisor

  alias Decree.Application.Config

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
            conditional_targets_child_spec(application, config) ++
            pubsub_child_spec ++
            registry_child_spec ++
            app_child_spec(name, config)

        Supervisor.init(children, strategy: :one_for_one)

      :ignore ->
        :ignore
    end
  end

  # A rule route's half and the store's half of one declaration, checked by a child started right
  # after the store's, which has parsed its config and registered it by then. An application with
  # no rule route gets no such child, and asks its store nothing. See "Two declarations" in
  # `Decree.Decision.Rule`.
  defp conditional_targets_child_spec(application, config) do
    case application.__conditional_targets__() do
      [] ->
        []

      routed ->
        event_store = Keyword.fetch!(config, :event_store)

        [
          %{
            id: {__MODULE__, :conditional_targets},
            start: {__MODULE__, :check_conditional_targets, [application, routed, event_store]},
            restart: :temporary
          }
        ]
    end
  end

  @doc false
  # Raises at start for a rule route's aggregate the running store does not declare, and
  # otherwise starts nothing.
  def check_conditional_targets(application, routed, {adapter, adapter_meta}) do
    declared = adapter.conditional_targets(adapter_meta)

    with [aggregate | _] <- routed -- declared do
      raise ArgumentError,
            "#{inspect(application)} routes a rule to #{inspect(aggregate)}, which its event " <>
              "store does not declare: add it to `conditional_appends: [targets: [...]]`"
    end

    for target <- declared -- routed do
      Logger.warning(fn ->
        "#{inspect(application)}'s event store declares the conditional target " <>
          "#{inspect(target)}, and no route in it names a rule for that aggregate"
      end)
    end

    :ignore
  end

  defp app_child_spec(name, config) do
    task_dispatcher_name = Module.concat([name, Decree.Commands.TaskDispatcher])
    aggregates_supervisor_name = Module.concat([name, Decree.Aggregates.Supervisor])
    subscriptions_name = Module.concat([name, Decree.Subscriptions])
    registry_name = Module.concat([name, Decree.Subscriptions.Registry])
    snapshotting = Keyword.get(config, :snapshotting, %{})
    hibernate_after = Keyword.get(config, :hibernate_after, :infinity)

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

  defp event_store_child_spec(name, config) do
    {adapter, adapter_config} =
      Decree.EventStore.adapter(name, Keyword.get(config, :event_store))

    {:ok, child_spec, adapter_meta} = adapter.child_spec(name, adapter_config)

    config = Keyword.put(config, :event_store, {adapter, adapter_meta})

    {child_spec, config}
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
