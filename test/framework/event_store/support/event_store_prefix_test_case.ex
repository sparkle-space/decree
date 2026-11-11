defmodule Decree.EventStore.EventStorePrefixTestCase do
  import Decree.SharedTestCase

  define_tests do
    alias Decree.EventStore
    alias Decree.EventStore.EventData
    alias Decree.UUID

    defmodule BankAccountOpened do
      @derive Jason.Encoder
      defstruct [:account_number, :initial_balance]
    end

    describe "event store prefix" do
      setup do
        {:ok, application1} = start_event_store(name: :prefix1, prefix: "prefix1")
        {:ok, application2} = start_event_store(name: :prefix2, prefix: "prefix2")

        [application1: application1, application2: application2]
      end

      test "should append events to named event store", %{
        application1: application1,
        application2: application2
      } do
        events = build_events(1)

        assert :ok == EventStore.append_to_stream(application1, "stream", 0, events)

        assert {:error, :stream_not_found} ==
                 EventStore.stream_forward(application2, "stream")
      end
    end

    defp build_events(count, correlation_id \\ UUID.uuid4(), causation_id \\ UUID.uuid4())

    defp build_events(count, correlation_id, causation_id) do
      for account_number <- 1..count,
          do: build_event(account_number, correlation_id, causation_id)
    end

    defp build_event(account_number, correlation_id, causation_id) do
      %EventData{
        correlation_id: correlation_id,
        causation_id: causation_id,
        event_type: "#{__MODULE__}.BankAccountOpened",
        data: %BankAccountOpened{account_number: account_number, initial_balance: 1_000},
        metadata: %{"metadata" => "value"}
      }
    end
  end
end
