defmodule Decree.Store.Notifications.Supervisor do
  @moduledoc false

  # Supervises the individual `GenStage` stages used to listen to, read, and
  # publish all events appended to storage.

  use Supervisor

  alias Decree.Store.{Config, MonitoredServer, Subscriptions}
  alias Decree.Store.Notifications.{Connection, Listener, Publisher}

  def child_spec({event_store, _config} = init_arg) do
    %{id: Module.concat(event_store, __MODULE__), start: {__MODULE__, :start_link, [init_arg]}}
  end

  def start_link({_event_store, _config} = init_arg) do
    Supervisor.start_link(__MODULE__, init_arg)
  end

  @impl Supervisor
  def init({event_store, config}) do
    conn = Keyword.fetch!(config, :conn)
    schema = Keyword.fetch!(config, :schema)
    serializer = Keyword.fetch!(config, :serializer)
    query_timeout = Keyword.fetch!(config, :timeout)
    correlation_id_type = Keyword.get(config, :correlation_id_type, "uuid")
    causation_id_type = Keyword.get(config, :causation_id_type, "uuid")

    listener_name = Module.concat([event_store, Listener])
    publisher_name = Module.concat([event_store, Publisher])
    postgrex_notifications_conn = postgrex_notifications_conn(event_store)

    postgrex_notifications_config =
      Config.postgrex_notifications_opts(config, postgrex_notifications_conn) ++
        [listener: listener_name, channel: schema <> ".events"]

    hibernate_after = Subscriptions.hibernate_after(event_store, config)

    Supervisor.init(
      [
        # The connection ends itself on a failed query and starts again here. Its start
        # never fails (it connects after it starts), so the backoff resets on every start,
        # and each restart waits `backoff_min`: half a second, as Postgrex waits between
        # reconnects, rather than a reconnect loop against a server that refuses every
        # LISTEN.
        Supervisor.child_spec(
          {MonitoredServer,
           mfa: {Connection, :start_link, [postgrex_notifications_config]},
           name: Module.concat([event_store, Postgrex, Notifications, MonitoredServer]),
           backoff_min: 500},
          id: Module.concat([postgrex_notifications_conn, MonitoredServer])
        ),
        {Listener,
         listen_to: postgrex_notifications_conn,
         query_timeout: query_timeout,
         name: listener_name,
         hibernate_after: hibernate_after},
        {Publisher,
         conn: conn,
         query_timeout: query_timeout,
         event_store: event_store,
         schema: schema,
         serializer: serializer,
         correlation_id_type: correlation_id_type,
         causation_id_type: causation_id_type,
         subscribe_to: listener_name,
         name: publisher_name,
         hibernate_after: hibernate_after}
      ],
      strategy: :one_for_all
    )
  end

  # Each event store instance starts its own notifications connection, named after it.
  defp postgrex_notifications_conn(name), do: Module.concat([name, Postgrex, Notifications])
end
