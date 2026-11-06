defmodule Decree.Serialization.JsonSerializerTest do
  use Decree.PostgresCase

  alias Decree.ExampleDomain.BankAccount.Events.BankAccountOpened
  alias Decree.Serialization.JsonSerializer
  alias Decree.Store.FiniteTypeProvider

  @serialized_event_json "{\"account_number\":\"ACC123\",\"initial_balance\":1000}"

  test "should serialize event to JSON" do
    account_opened = %BankAccountOpened{account_number: "ACC123", initial_balance: 1_000}

    assert JsonSerializer.serialize(account_opened) == @serialized_event_json
  end

  test "should deserialize event from JSON" do
    account_opened = %BankAccountOpened{account_number: "ACC123", initial_balance: 1_000}
    type = Atom.to_string(account_opened.__struct__)

    assert JsonSerializer.deserialize(@serialized_event_json, type: type) == account_opened
  end

  defmodule NamedEvent do
    defstruct [:data]
  end

  defmodule AnotherNamedEvent do
    defstruct [:data]
  end

  test "should deserialize to event type module name" do
    assert %NamedEvent{data: "data"} ==
             JsonSerializer.deserialize("{\"data\": \"data\"}",
               type: "#{__MODULE__}.NamedEvent"
             )

    assert %AnotherNamedEvent{data: "data"} ==
             JsonSerializer.deserialize("{\"data\": \"data\"}",
               type: "#{__MODULE__}.AnotherNamedEvent"
             )
  end

  describe "known_type?/1" do
    # The `:type_provider` app env is global: this module is not async.

    defmodule CrashingTypeProvider do
      @behaviour Decree.EventStore.TypeProvider

      @impl Decree.EventStore.TypeProvider
      def to_string(struct), do: Decree.Serialization.ModuleNameTypeProvider.to_string(struct)

      @impl Decree.EventStore.TypeProvider
      def to_struct(_type), do: raise("type provider crashed")
    end

    test "answers false for a name that is no module, under the default provider" do
      assert JsonSerializer.known_type?("#{__MODULE__}.NamedEvent")
      refute JsonSerializer.known_type?("Elixir.Decree.X16.NotYetReleasedEvent")
    end

    test "answers false for a type a custom provider has no clause for" do
      FiniteTypeProvider.configure!()

      # `NewReleaseEvent` is a loaded struct module: only the provider decides.
      unknown_type = "Elixir.Decree.Store.NewReleaseEvent"
      assert Code.ensure_loaded?(Decree.Store.NewReleaseEvent)

      assert_raise FunctionClauseError, fn ->
        Decree.EventStore.TypeProvider.to_struct(unknown_type)
      end

      refute JsonSerializer.known_type?(unknown_type)
      assert JsonSerializer.known_type?(FiniteTypeProvider.known_type())
    end

    test "lets any other provider crash through" do
      FiniteTypeProvider.configure!(CrashingTypeProvider)

      assert_raise RuntimeError, "type provider crashed", fn ->
        JsonSerializer.known_type?("#{__MODULE__}.NamedEvent")
      end
    end
  end
end
