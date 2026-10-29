defmodule Decree.Event.EventHandlerMacroTest do
  use Decree.MockEventStoreCase

  alias Decree.Event.IgnoredEvent
  alias Decree.ExampleDomain.BankAccount.AccountBalanceHandler
  alias Decree.ExampleDomain.BankAccount.Events.{BankAccountOpened, MoneyDeposited}
  alias Decree.Helpers.{EventFactory, Wait}
  alias Decree.MockedApp

  describe "event handler" do
    setup do
      handler = start_supervised!({AccountBalanceHandler, application: MockedApp})

      Wait.until(fn ->
        assert AccountBalanceHandler.subscribed?()
      end)

      [handler: handler]
    end

    test "should handle published events", %{handler: handler} do
      recorded_events =
        [
          %BankAccountOpened{account_number: "ACC123", initial_balance: 1_000},
          %MoneyDeposited{amount: 50, balance: 1_050},
          %IgnoredEvent{name: "ignored"}
        ]
        |> EventFactory.map_to_recorded_events()

      send(handler, {:events, recorded_events})

      Wait.until(fn ->
        assert AccountBalanceHandler.current_balance() == 1_050
      end)

      # Make sure the event handler doesn't continue running logic after the test is done
      Process.exit(handler, :kill)
    end

    test "cannot specify conflicting options" do
      assert_raise ArgumentError, fn ->
        __MODULE__.ConflictingOptions.start_link()
      end
    end

    # A bit of an odd duck test, but it works :)
    assert_raise CompileError, fn ->
      defmodule ConflictingHandlers do
        use Decree.Event.Handler,
          application: Decree.ExampleDomain.BankApp,
          name: __MODULE__

        @impl true
        def handle(_event, _meta) do
          :ok
        end

        @impl true
        def handle_batch(_events) do
          :ok
        end
      end
    end
  end
end

defmodule Decree.Event.EventHandlerMacroTest.ConflictingOptions do
  use Decree.Event.Handler,
    application: Decree.ExampleDomain.BankApp,
    name: __MODULE__,
    concurrency: 2,
    batch_size: 10
end
