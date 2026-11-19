defmodule Decree.Aggregates.AppendResultStoreTest do
  use ExUnit.Case

  alias Decree.EventStore
  alias Decree.Event.Mapper
  alias Decree.FrameworkStorage
  alias Decree.Store.AppendResult
  alias Decree.UUID

  defmodule Opened do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:id]
  end

  defmodule Open do
    @moduledoc false
    defstruct [:command_id, :id]
  end

  defmodule Claim do
    @moduledoc false
    defstruct [:command_id, :id]
  end

  defmodule Account do
    @moduledoc false
    defstruct [:id, opened: 0]

    def execute(%__MODULE__{}, %Open{id: id}), do: %Opened{id: id}

    def apply(%__MODULE__{opened: opened} = state, %Opened{id: id}) do
      %__MODULE__{state | id: id, opened: opened + 1}
    end
  end

  defmodule ClaimDecision do
    @moduledoc false
    use Decree.Decision, create: false

    def command_window_ms, do: 300_000
    def query(%Claim{id: id}, _tag_context), do: [[types: ["Opened"], tags: ["item:" <> id]]]
    def initial_state, do: nil
    def evolve(state, _event), do: state
    def decide(_state, %Claim{id: id}), do: %Opened{id: id}
  end

  defmodule Router do
    @moduledoc false
    use Decree.Commands.Router

    identify Account, by: :id, prefix: "append-result-"

    dispatch Open, to: Account

    decide(Claim, to: ClaimDecision, identity: :id, identity_prefix: "decided-result-")
  end

  defmodule App do
    @moduledoc false

    use Decree.Application,
      otp_app: :decree,
      event_store:
        FrameworkStorage.event_store_config(
          returning: :append_result,
          conditional_appends: [stream_prefixes: ["decided-result-"]]
        ),
      registry: :local

    router Router
    router Decree.TagIndexFixtures.Router

    def init(config), do: {:ok, FrameworkStorage.name_event_store(config)}
  end

  setup do
    FrameworkStorage.reset!()

    start_supervised!(App)

    Decree.TagIndexFixtures.enable!(App)

    :ok
  end

  defp stream(%{id: id}), do: "append-result-" <> id

  describe "a store configured with `returning: :append_result`" do
    # What the option does to every append, so a test below can't pass without it.
    test "answers it for an append that doesn't ask" do
      assert {:ok, %AppendResult{}} =
               EventStore.append_to_stream(App, "other-" <> UUID.uuid4(), :any_version, [
                 Mapper.map_to_event_data(%Opened{id: "plain"})
               ])
    end

    test "an aggregate reads a committed append as success" do
      command = %Open{command_id: UUID.uuid4(), id: UUID.uuid4()}

      assert App.dispatch(command, returning: :aggregate_version) == {:ok, 1}

      assert [%{data: %Opened{}}] = command |> stream() |> stored()
      assert App.aggregate_state(Account, stream(command)).opened == 1
    end

    test "an aggregate's next command appends at the next version" do
      command = %Open{command_id: UUID.uuid4(), id: UUID.uuid4()}

      assert :ok = App.dispatch(command)
      assert App.dispatch(command, returning: :aggregate_version) == {:ok, 2}

      assert [_first, _second] = command |> stream() |> stored()
    end

    test "a decision's append, which asks as well, gets one answer" do
      command = %Claim{command_id: UUID.uuid4(), id: UUID.uuid4()}

      assert App.dispatch(command, returning: :aggregate_version) == {:ok, 1}
    end
  end

  defp stored(stream_uuid), do: App |> EventStore.stream_forward(stream_uuid) |> Enum.to_list()
end
