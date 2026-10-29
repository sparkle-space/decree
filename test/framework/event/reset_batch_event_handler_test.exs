defmodule Decree.Event.BatchResetEventHandlerTest do
  use ExUnit.Case

  import Decree.Assertions.EventAssertions

  alias Decree.Event.Mapper
  alias Decree.EventStore
  alias Decree.ExampleDomain.BankAccount.BankAccountBatchHandler
  alias Decree.ExampleDomain.BankAccount.Events.BankAccountOpened
  alias Decree.ExampleDomain.BankApp
  alias Decree.Helpers.Wait
  alias Decree.UUID

  describe "reset batch event handler" do
    setup do
      start_supervised!(BankApp)
      :ok
    end

    test "should be reset when starting from `:origin`" do
      stream_uuid = UUID.uuid4()
      initial_events = [%BankAccountOpened{account_number: "ACC123", initial_balance: 1_000}]

      :ok = EventStore.append_to_stream(BankApp, stream_uuid, 0, to_event_data(initial_events))

      handler = start_supervised!(BankAccountBatchHandler)

      Wait.until(fn ->
        assert BankAccountBatchHandler.current_accounts() == ["ACC123"]
      end)

      :ok = BankAccountBatchHandler.change_prefix("PREF_")

      send(handler, :reset)

      Wait.until(fn ->
        assert BankAccountBatchHandler.current_accounts() == ["PREF_ACC123"]
      end)
    end

    test "should be reset when starting from `:current`" do
      stream_uuid = UUID.uuid4()

      # Ignored initial events
      initial_events = [%BankAccountOpened{account_number: "ACC123", initial_balance: 1_000}]
      :ok = EventStore.append_to_stream(BankApp, stream_uuid, 0, to_event_data(initial_events))

      handler = start_supervised!({BankAccountBatchHandler, start_from: :current})

      Wait.until(fn ->
        assert BankAccountBatchHandler.current_accounts() == []
      end)

      :ok = BankAccountBatchHandler.change_prefix("PREF_")

      send(handler, :reset)

      # Wait for the :reset message to be handled, otherwise there is a risk the append_to_stream
      # below gets sent to the old subscription.
      _ = :sys.get_state(handler)

      new_event = [%BankAccountOpened{account_number: "ACC1234", initial_balance: 1_000}]
      :ok = EventStore.append_to_stream(BankApp, stream_uuid, 1, to_event_data(new_event))

      wait_for_event(BankApp, BankAccountOpened, fn event, recorded_event ->
        event.account_number == "ACC1234" and recorded_event.event_number == 2
      end)

      Wait.until(fn ->
        assert BankAccountBatchHandler.current_accounts() == ["PREF_ACC1234"]
      end)
    end
  end

  defp to_event_data(events) do
    Mapper.map_to_event_data(events,
      causation_id: UUID.uuid4(),
      correlation_id: UUID.uuid4(),
      metadata: %{}
    )
  end
end
