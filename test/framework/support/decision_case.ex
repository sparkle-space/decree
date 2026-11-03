defmodule Decree.DecisionCase do
  @moduledoc """
  This module defines the test case to be used by a decision's tests with no store.
  """

  use ExUnit.CaseTemplate

  using opts do
    quote do
      import Decree.Decision.Assertions

      @decision Keyword.fetch!(unquote(opts), :decision)

      # Assert that the expected events are decided after the given events.
      defp assert_events(initial_events \\ [], command, expected_events) do
        assert_decides(@decision, initial_events, command, expected_events)
      end

      # Assert that the state reached after deciding is the expected one.
      defp assert_state(initial_events \\ [], command, expected_state) do
        assert {:ok, ^expected_state} =
                 decision_result(@decision, initial_events, command, returning: :aggregate_state)
      end

      defp assert_error(initial_events \\ [], command, expected_error) do
        assert_refuses(@decision, initial_events, command, expected_error)
      end
    end
  end
end

defmodule Decree.DecisionStoreCase do
  @moduledoc """
  This module defines the test case for a decision dispatched against the store.
  """

  use ExUnit.CaseTemplate

  alias Decree.DecisionFixtures.{App, Claimed, Scripted}
  alias Decree.EventStore
  alias Decree.EventStore.EventData
  alias Decree.Store.{AppendResult, Config}
  alias Decree.TagIndexFixtures
  alias Decree.UUID

  using do
    quote do
      import Decree.DecisionStoreCase

      alias Decree.DecisionFixtures.{
        App,
        ByFunction,
        Claim,
        Claimed,
        Create,
        Released,
        Scripted,
        Tight
      }

      alias Decree.EventStore
      alias Decree.TagIndexFixtures
      alias Decree.UUID
    end
  end

  # Every family is enabled unless a test is tagged `:no_index_facts`, as a fresh store's
  # release step would leave it.
  setup context do
    Decree.FrameworkStorage.reset!()

    start_supervised!(App)

    if not Map.get(context, :no_index_facts, false), do: TagIndexFixtures.enable!(App)

    :ok
  end

  @doc """
  Disables the index of a family and key on the fixture ledger, and answers its id.
  """
  def disable_index!(family, key \\ nil, reason \\ "repair") do
    TagIndexFixtures.disable!(App, family, key, reason)
  end

  @doc """
  Enables a family and key again, under a new index id.
  """
  def enable_index!(family, key \\ nil), do: TagIndexFixtures.reenable!(App, family, key)

  @doc """
  A `Scripted` command, its script made of the options named after its callbacks.
  """
  def scripted(opts \\ [], module \\ Scripted) do
    {script, fields} = Keyword.split(opts, [:query, :followup, :admit, :decide, :scope_tags])

    struct!(
      module,
      [command_id: UUID.uuid4(), claim_id: UUID.uuid4(), scope: scope(), script: Map.new(script)]
      |> Keyword.merge(fields)
    )
  end

  def scope, do: "scope-#{System.unique_integer([:positive])}"

  @doc """
  How many facts the case's setup appends, at positions 1 and up.
  """
  def fact_count, do: length(TagIndexFixtures.families())

  @doc """
  The positions of the facts the case's setup appends.
  """
  def fact_positions, do: Enum.to_list(1..fact_count())

  def claimed_item(scope), do: [types: [Atom.to_string(Claimed)], tags: ["scope:" <> scope]]

  @doc """
  Appends an event to a stream no decision owns, and answers its `$all` position.
  """
  def seed(event, tags \\ [], application \\ App, stream_uuid \\ nil) do
    stream_uuid = stream_uuid || "other-" <> UUID.uuid4()

    data = %EventData{
      event_type: Atom.to_string(event.__struct__),
      data: event,
      metadata: %{},
      tags: tags
    }

    {:ok, %AppendResult{position: position}} =
      EventStore.append_to_stream(application, stream_uuid, :any_version, [data],
        returning: :append_result
      )

    position
  end

  @doc """
  The events of a stream, as recorded.
  """
  def stored(stream_uuid, application \\ App) do
    case EventStore.stream_forward(application, stream_uuid) do
      {:error, :stream_not_found} -> []
      stream -> Enum.to_list(stream)
    end
  end

  @doc """
  The events of a stream as `$all` holds them, so `event_number` is the `$all` position. A
  read of the stream gives its version there instead.
  """
  def positioned(stream_uuid) do
    store = Module.concat([App, "FrameworkEventStore"])

    {:ok, events} = Decree.FrameworkEventStore.read_all_streams_forward(0, 1_000, name: store)

    Enum.filter(events, &(&1.stream_uuid == stream_uuid))
  end

  @doc """
  Sets the window every fixture decision answers, for the rest of the test.
  """
  def with_window(window) do
    previous = Application.fetch_env(:decree, :decision_test_window_ms)

    Application.put_env(:decree, :decision_test_window_ms, window)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:decree, :decision_test_window_ms, value)
        :error -> Application.delete_env(:decree, :decision_test_window_ms)
      end
    end)
  end

  @doc """
  Moves a command's stamps `seconds` into the past, so a test leaves no clock to wait on. A
  stamp cannot be updated, so the one statement runs with that trigger off, in a transaction
  that puts it back.
  """
  def age_stamps(command_id, seconds) do
    update_past_trigger!(
      "event_tags",
      "no_update_event_tags",
      "UPDATE event_tags SET stamped_at = stamped_at - make_interval(secs => $2) WHERE tag = $1",
      ["command:" <> command_id, seconds * 1.0]
    )
  end

  @doc """
  Overwrites the stored data of every event of `event_type` with `bytes`, as a corrupt row, or
  one written by a serializer the reader does not share, would hold. An event cannot be
  updated, so the one statement runs with that trigger off, in a transaction that puts it back.
  """
  def corrupt_data(event_type, bytes) do
    update_past_trigger!(
      "events",
      "no_update_events",
      "UPDATE events SET data = $2 WHERE event_type = $1",
      [event_type, bytes]
    )
  end

  # One statement that changes at least one row, with `trigger` off on `table` for it alone.
  defp update_past_trigger!(table, trigger, statement, params) do
    {:ok, conn} =
      Decree.FrameworkStorage.config() |> Config.default_postgrex_opts() |> Postgrex.start_link()

    try do
      {:ok, rows} =
        Postgrex.transaction(conn, fn tx ->
          Postgrex.query!(tx, "ALTER TABLE #{table} DISABLE TRIGGER #{trigger}", [])

          %Postgrex.Result{num_rows: rows} = Postgrex.query!(tx, statement, params)

          Postgrex.query!(tx, "ALTER TABLE #{table} ENABLE TRIGGER #{trigger}", [])

          rows
        end)

      true = rows > 0
    after
      GenServer.stop(conn)
    end

    :ok
  end

  @doc """
  Sends the test each of these telemetry events as `{:telemetry, event, measurements,
  metadata}`, for the rest of the test.
  """
  def attach_telemetry(events) do
    test = self()
    id = make_ref()

    :telemetry.attach_many(
      id,
      events,
      fn event, measurements, metadata, _config ->
        send(test, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  @doc """
  Blocks a callback until the test sends `:go`, telling the test it is there.
  """
  def hold(test, label) do
    send(test, {label, self()})

    receive do
      :go -> :ok
    end
  end
end
