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
  alias Decree.Store.AppendResult
  alias Decree.UUID

  using do
    quote do
      import Decree.DecisionStoreCase

      alias Decree.DecisionFixtures.{
        App,
        ByFunction,
        Claim,
        Claimed,
        Released,
        Scripted,
        Tight
      }

      alias Decree.EventStore
      alias Decree.UUID
    end
  end

  setup do
    Decree.FrameworkStorage.reset!()

    start_supervised!(App)

    :ok
  end

  @doc """
  A `Scripted` command, its script made of the options named after its callbacks.
  """
  def scripted(opts \\ [], module \\ Scripted) do
    {script, fields} = Keyword.split(opts, [:query, :followup, :admit, :decide])

    struct!(
      module,
      [command_id: UUID.uuid4(), claim_id: UUID.uuid4(), scope: scope(), script: Map.new(script)]
      |> Keyword.merge(fields)
    )
  end

  def scope, do: "scope-#{System.unique_integer([:positive])}"

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
