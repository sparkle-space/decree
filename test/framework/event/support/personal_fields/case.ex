defmodule Decree.PersonalFields.Case do
  @moduledoc false

  # Helpers for the personal field tests. Every test starts `App` on an empty store, with one
  # minted subject, and reads the store directly when a test is about what was stored.

  use ExUnit.CaseTemplate

  alias Decree.Event.Append
  alias Decree.Event.Mapper
  alias Decree.EventStore
  alias Decree.EventStore.RecordedEvent
  alias Decree.FrameworkStorage
  alias Decree.PersonalFields.Fixtures.{App, Keys}
  alias Decree.PersonalFields.Fixtures.Commands.Emit
  alias Decree.Store.Config
  alias Decree.UUID

  using do
    quote do
      import Decree.PersonalFields.Case
      import Decree.PersonalFields.Fixtures, only: [email: 0, email: 1]

      alias Decree.EventStore
      alias Decree.EventStore.RecordedEvent
      alias Decree.Helpers.Wait
      alias Decree.PersonalFields
      alias Decree.PersonalFields.Fixtures.{App, Keys}
      alias Decree.UUID
    end
  end

  setup do
    FrameworkStorage.reset!()

    conn = start_supervised!({Postgrex, Config.default_postgrex_opts(FrameworkStorage.config())})

    start_supervised!(Keys)
    Keys.sink()

    [conn: conn, subject: Keys.mint(UUID.uuid4()), stream: UUID.uuid4()]
  end

  @doc "Stores events through a real dispatch."
  def dispatch!(stream, events, opts \\ []) do
    :ok =
      App.dispatch(
        %Emit{account_id: stream, command_id: UUID.uuid4(), events: List.wrap(events)},
        opts
      )
  end

  @doc "Reads a stream, upcast, as a consumer does."
  def read(stream) do
    App |> EventStore.stream_forward(stream) |> Enum.to_list()
  end

  @doc "The envelopes `events` become in `stream`, as `Append.prepare/4` writes them."
  def prepare!(stream, events, metadata \\ %{}) do
    context = %Decree.Aggregates.ExecutionContext{
      command: %Emit{account_id: stream},
      metadata: metadata
    }

    event_data = Mapper.map_to_event_data(List.wrap(events), metadata: metadata)
    {:ok, prepared} = Append.prepare(App, stream, event_data, context)

    prepared
  end

  @doc "Writes to the store with no framework in the way, at any version."
  def append_raw!(stream, event_data) do
    :ok = EventStore.append_to_stream(App, stream, :any_version, List.wrap(event_data))
  end

  @doc "Writes an event as stored, its fields and metadata exactly as given."
  def store_raw!(stream, event, metadata \\ %{}) do
    append_raw!(stream, Mapper.map_to_event_data(event, metadata: metadata))
  end

  @doc "Each stored row's bytes, as SQL returns them."
  def rows(conn, table, columns) do
    %{rows: rows} = Postgrex.query!(conn, "SELECT #{columns} FROM #{table}", [])

    Enum.map(rows, &Enum.join(&1, " "))
  end

  @doc "Decrypts one read event."
  def decrypt(%RecordedEvent{} = event) do
    Decree.PersonalFields.decrypt_recorded_event(App, event)
  end

  @doc "A subscription's stored position: nil or 0 means nothing was acked."
  def acked(conn, name) do
    %{rows: rows} =
      Postgrex.query!(
        conn,
        "SELECT last_seen FROM subscriptions WHERE subscription_name = $1",
        [name]
      )

    case rows do
      [[last_seen]] -> last_seen || 0
      [] -> 0
    end
  end

  @doc "Collects telemetry into the test process, until `detach/1`."
  def attach_telemetry(events) do
    id = make_ref()
    test = self()

    :telemetry.attach_many(
      id,
      events,
      fn event, measurements, metadata, _config ->
        send(test, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)

    id
  end

  @doc "Every telemetry message waiting in this process's mailbox."
  def flush_telemetry(collected \\ []) do
    receive do
      {:telemetry, _event, _measurements, _metadata} = message ->
        flush_telemetry([message | collected])
    after
      0 -> collected
    end
  end
end
