defmodule Decree.Commands.DispatchConsistencyTest do
  use Decree.PostgresCase

  alias Decree.Commands.{
    ConsistencyAggregateRoot,
    ConsistencyPrefixRouter,
    EventuallyConsistentEventHandler,
    ExecutionResult,
    OptionalStronglyConsistentEventHandler,
    StronglyConsistentEventHandler
  }

  alias Decree.Commands.ConsistencyApp
  alias Decree.EventStore
  alias Decree.Helpers.Wait
  alias Decree.UUID
  alias ConsistencyAggregateRoot.ConsistencyCommand
  alias ConsistencyAggregateRoot.NoOpCommand
  alias ConsistencyAggregateRoot.RequestDispatchCommand

  setup do
    start_supervised!(ConsistencyApp)
    :ok
  end

  describe "event handlers" do
    setup [:start_event_handlers]

    test "should wait for strongly consistent event handler to handle event" do
      command = %ConsistencyCommand{uuid: UUID.uuid4(), delay: 0}

      assert :ok = ConsistencyApp.dispatch(command, consistency: :strong)
    end

    # default consistency timeout set to 1s test config
    test "should timeout waiting for strongly consistent event handler to handle event" do
      command = %ConsistencyCommand{uuid: UUID.uuid4(), delay: 5_000}

      assert {:error, :consistency_timeout} =
               ConsistencyApp.dispatch(command, consistency: :strong)
    end

    test "should not wait when command creates no events" do
      command = %NoOpCommand{uuid: UUID.uuid4()}

      assert :ok = ConsistencyApp.dispatch(command, consistency: :strong)
    end

    test "should allow strongly consistent event handler to dispatch a command" do
      command = %RequestDispatchCommand{uuid: UUID.uuid4(), delay: 0}

      assert :ok = ConsistencyApp.dispatch(command, consistency: :strong)
    end

    test "should timeout waiting for strongly consistent handler dispatching a command" do
      command = %RequestDispatchCommand{uuid: UUID.uuid4(), delay: 5_000}

      assert {:error, :consistency_timeout} =
               ConsistencyApp.dispatch(command, consistency: :strong)
    end
  end

  describe "optional `:strong` consistency" do
    setup [:start_event_handlers, :start_optional_handler]

    # The opt-in handler sleeps 300ms, inside the 1s timeout; the other strongly consistent
    # handler sleeps 3s, past it, so waiting for that one would answer a timeout.
    test "should only wait for opt-in strongly consistent event handler to handle event" do
      command = %ConsistencyCommand{uuid: UUID.uuid4(), delay: 3_000}
      opts = [application: ConsistencyApp, consistency: [OptionalStronglyConsistentEventHandler]]

      assert :ok = ConsistencyApp.dispatch(command, opts)
    end

    # Default consistency timeout set to 1s test config, and this handler sleeps a tenth
    # of the delay
    test "should timeout waiting for strongly consistent event handler to handle event" do
      command = %ConsistencyCommand{uuid: UUID.uuid4(), delay: 20_000}

      opts = [
        application: ConsistencyApp,
        consistency: ["OptionalStronglyConsistentEventHandler"]
      ]

      assert {:error, :consistency_timeout} = ConsistencyApp.dispatch(command, opts)
    end
  end

  describe "aggregate identity prefix" do
    setup :start_event_handlers

    test "should wait for strongly consistent event handler to handle event" do
      uuid = UUID.uuid4()
      command = %ConsistencyCommand{uuid: uuid, delay: 0}

      assert :ok =
               ConsistencyPrefixRouter.dispatch(command,
                 application: ConsistencyApp,
                 consistency: :strong
               )
    end

    test "should append events to stream using prefixed aggregate uuid" do
      uuid = UUID.uuid4()
      command = %ConsistencyCommand{uuid: uuid, delay: 0}

      assert {:ok, %ExecutionResult{aggregate_uuid: aggregate_uuid}} =
               ConsistencyPrefixRouter.dispatch(command,
                 application: ConsistencyApp,
                 consistency: :strong,
                 include_execution_result: true
               )

      assert aggregate_uuid == "example-prefix-" <> uuid

      recorded_events =
        EventStore.stream_forward(ConsistencyApp, aggregate_uuid) |> Enum.to_list()

      assert length(recorded_events) == 1
    end
  end

  def start_event_handlers(_context) do
    start_supervised!(StronglyConsistentEventHandler, shutdown: :brutal_kill)
    start_supervised!(EventuallyConsistentEventHandler, shutdown: :brutal_kill)

    await_subscribed(["StronglyConsistentEventHandler", "EventuallyConsistentEventHandler"])
  end

  def start_optional_handler(_context) do
    start_supervised!(OptionalStronglyConsistentEventHandler)

    await_subscribed(["OptionalStronglyConsistentEventHandler"])
  end

  # A handler subscribes on its own, after it starts, and a subscription whose first
  # attempt finds the store's lock connection not up yet retries a second later. The
  # dispatches here wait 1s, so a test dispatches only once its handlers' subscriptions
  # hold their locks; an event appended from then on reaches them.
  defp await_subscribed(subscription_names) do
    event_store = Module.concat([ConsistencyApp, "FrameworkEventStore"])

    Wait.until(15_000, fn ->
      assert {subscriptions, 0} =
               Decree.FrameworkEventStore.local_subscriptions(name: event_store)

      subscribed =
        for %{subscription_name: name, status: :subscribed} <- subscriptions, do: name

      assert subscription_names -- subscribed == []
    end)

    :ok
  end
end
