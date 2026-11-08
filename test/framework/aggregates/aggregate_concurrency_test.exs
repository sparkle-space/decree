defmodule Decree.Aggregates.AggregateConcurrencyTest do
  use Decree.MockEventStoreCase

  alias Decree.MockedApp
  alias Decree.Aggregates.{Aggregate, ExecutionContext}
  alias Decree.EventStore.RecordedEvent
  alias Decree.ExampleDomain.{BankAccount, DepositMoneyHandler, OpenAccountHandler}
  alias Decree.ExampleDomain.BankAccount.Commands.{DepositMoney, OpenAccount}
  alias Decree.ExampleDomain.BankAccount.Events.MoneyDeposited
  alias Decree.UUID

  setup do
    expect(MockEventStore, :subscribe_to, fn
      _event_store_meta, stream_uuid, handler_name, handler, _subscribe_from, _opts ->
        assert is_binary(stream_uuid)
        assert is_binary(handler_name)

        {:ok, handler}
    end)

    expect(MockEventStore, :subscribe, fn _event_store_meta, aggregate_uuid ->
      assert is_binary(aggregate_uuid)

      :ok
    end)

    :ok
  end

  describe "concurrency error" do
    setup [:open_account]

    test "should retry command", context do
      %{account_number: account_number} = context

      command = %DepositMoney{
        command_id: UUID.uuid4(),
        account_number: account_number,
        transfer_uuid: UUID.uuid4(),
        amount: 100
      }

      context = %ExecutionContext{
        command: command,
        handler: DepositMoneyHandler,
        function: :handle,
        retry_attempts: 1
      }

      # Fail to append once
      expect(MockEventStore, :append_to_stream, fn
        _event_store_meta, ^account_number, 1, _event_data, _opts ->
          {:error, :wrong_expected_version}
      end)

      # Return "missing" event
      expect(MockEventStore, :stream_forward, fn
        _event_store_meta, ^account_number, 2, _batch_size ->
          [
            %RecordedEvent{
              event_id: UUID.uuid4(),
              event_number: 2,
              stream_id: account_number,
              stream_version: 2,
              event_type: "Elixir.Decree.ExampleDomain.BankAccount.Events.MoneyDeposited",
              data: %MoneyDeposited{
                account_number: account_number,
                transfer_uuid: UUID.uuid4(),
                amount: 500,
                balance: 1_500
              },
              metadata: %{}
            }
          ]
      end)

      # Succeed on second attempt
      expect(MockEventStore, :append_to_stream, fn
        _event_store_meta, ^account_number, 2, _event_data, _opts ->
          :ok
      end)

      assert {:ok, 3, _events, _aggregate_state} =
               Aggregate.execute(MockedApp, BankAccount, account_number, context)

      assert Aggregate.aggregate_version(MockedApp, BankAccount, account_number) == 3

      assert Aggregate.aggregate_state(MockedApp, BankAccount, account_number) == %BankAccount{
               account_number: account_number,
               balance: 1_600,
               state: :active
             }
    end

    test "should error after too many attempts", %{account_number: account_number} do
      # Fail to append to stream
      expect(MockEventStore, :append_to_stream, 6, fn
        _event_store_meta, ^account_number, 1, _event_data, _opts ->
          {:error, :wrong_expected_version}
      end)

      expect(MockEventStore, :stream_forward, 6, fn
        _event_store_meta, ^account_number, 2, _batch_size ->
          []
      end)

      command = %DepositMoney{
        command_id: UUID.uuid4(),
        account_number: account_number,
        transfer_uuid: UUID.uuid4(),
        amount: 100
      }

      context = %ExecutionContext{
        command: command,
        handler: DepositMoneyHandler,
        function: :handle,
        retry_attempts: 5
      }

      assert {:error, :too_many_attempts} =
               Aggregate.execute(MockedApp, BankAccount, account_number, context)
    end

    defp open_account(_context) do
      account_number = UUID.uuid4()

      expect(MockEventStore, :stream_forward, fn
        _event_store_meta, ^account_number, 1, _batch_size ->
          []
      end)

      expect(MockEventStore, :append_to_stream, fn
        _event_store_meta, ^account_number, 0, _event_data, _opts ->
          :ok
      end)

      {:ok, ^account_number} =
        Decree.Aggregates.Supervisor.open_aggregate(MockedApp, BankAccount, account_number)

      command = %OpenAccount{
        command_id: UUID.uuid4(),
        account_number: account_number,
        initial_balance: 1_000
      }

      context = %ExecutionContext{
        command: command,
        handler: OpenAccountHandler,
        function: :handle,
        retry_attempts: 1
      }

      {:ok, 1, _events, _aggregate_state} =
        Aggregate.execute(MockedApp, BankAccount, account_number, context)

      [
        account_number: account_number
      ]
    end
  end
end
