defmodule Decree.Commands.CommandIdRefusalTest do
  use Decree.PostgresCase

  # The default refusal of a command whose `command_id` is not a canonical UUID, on an
  # aggregate route: answered before the identity is extracted, so the aggregate is never
  # opened and nothing is appended. Each router registers no middleware unless it says so.

  alias Decree.Aggregates.Aggregate
  alias Decree.{CommandIdCases, DefaultApp, EventStore, Registration, UUID}

  defmodule Touched do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:id, :command_id]
  end

  defmodule Touch do
    @moduledoc false
    defstruct [:id, :command_id, :test]
  end

  defmodule TouchWithNoId do
    @moduledoc false
    defstruct [:id, :test]
  end

  defmodule Unrouted do
    @moduledoc false
    defstruct [:id, :command_id]
  end

  defmodule Toucher do
    @moduledoc false
    defstruct [:id]

    def execute(%__MODULE__{}, %{id: id, test: test} = command) do
      send(test, {:executed, command})

      %Touched{id: id, command_id: Map.get(command, :command_id)}
    end

    def apply(%__MODULE__{} = state, %Touched{id: id}), do: %__MODULE__{state | id: id}
  end

  defmodule BareRouter do
    @moduledoc false
    use Decree.Commands.Router

    identify Toucher, by: :id, prefix: "toucher-"

    dispatch [Touch, TouchWithNoId], to: Toucher
  end

  defmodule HostRefusing do
    @moduledoc false
    @behaviour Decree.Middleware

    import Decree.Middleware.Pipeline

    def before_dispatch(pipeline) do
      pipeline
      |> respond({:error, :host_refused})
      |> halt()
    end

    def after_dispatch(pipeline), do: pipeline
    def after_failure(pipeline), do: pipeline
  end

  defmodule HostRecording do
    @moduledoc false
    @behaviour Decree.Middleware

    def before_dispatch(pipeline), do: record(:before_dispatch, pipeline)
    def after_dispatch(pipeline), do: record(:after_dispatch, pipeline)
    def after_failure(pipeline), do: record(:after_failure, pipeline)

    defp record(stage, pipeline) do
      send(self(), {:host, stage, pipeline.command, pipeline.response})

      pipeline
    end
  end

  defmodule RefusingRouter do
    @moduledoc false
    use Decree.Commands.Router

    middleware HostRefusing

    identify Toucher, by: :id, prefix: "toucher-"

    dispatch Touch, to: Toucher
  end

  defmodule RecordingRouter do
    @moduledoc false
    use Decree.Commands.Router

    middleware HostRecording

    identify Toucher, by: :id, prefix: "toucher-"

    dispatch Touch, to: Toucher
  end

  setup do
    start_supervised!(DefaultApp)

    :ok
  end

  defp touch(fields) do
    struct!(Touch, [id: UUID.uuid4(), command_id: UUID.uuid4(), test: self()] ++ fields)
  end

  defp dispatch(router, command, opts \\ []) do
    router.dispatch(command, Keyword.put(opts, :application, DefaultApp))
  end

  # Nothing ran: no aggregate process, no stream, and the aggregate saw no command.
  defp refute_touched(%{id: id}) do
    stream_uuid = "toucher-" <> id

    assert Registration.whereis_name(DefaultApp, Aggregate.name(DefaultApp, Toucher, stream_uuid)) ==
             :undefined

    assert {:error, :stream_not_found} = EventStore.stream_forward(DefaultApp, stream_uuid)
    refute_received {:executed, _command}
  end

  describe "an aggregate route refuses a `command_id`" do
    for {name, value, refusal} <- CommandIdCases.refusals() do
      test "that is #{name}, with `#{inspect(refusal)}`" do
        command = touch(command_id: unquote(Macro.escape(value)))

        assert {:error, unquote(refusal)} = dispatch(BareRouter, command)

        refute_touched(command)
      end
    end

    test "on a command with no `command_id` field, with `:command_id_required`" do
      command = %TouchWithNoId{id: UUID.uuid4(), test: self()}

      assert {:error, :command_id_required} = dispatch(BareRouter, command)

      refute_touched(command)
    end

    test "that is nil, though the dispatch minted a `command_uuid`" do
      command = touch(command_id: nil)

      assert {:error, :command_id_required} =
               dispatch(BareRouter, command, command_uuid: UUID.uuid4())

      assert {:error, :command_id_required} = dispatch(BareRouter, command)

      refute_touched(command)
    end
  end

  describe "a refused `command_id` and a host middleware" do
    test "a host middleware's own refusal is the answer" do
      command = touch(command_id: nil)

      assert {:error, :host_refused} = dispatch(RefusingRouter, command)

      refute_touched(command)
    end

    test "a host middleware sees the command before the refusal" do
      command = touch(command_id: "not-a-uuid")

      assert {:error, :invalid_command_id} = dispatch(RecordingRouter, command)

      assert_received {:host, :before_dispatch, ^command, nil}
      assert_received {:host, :after_failure, ^command, {:error, :invalid_command_id}}
      refute_received {:host, :after_dispatch, _command, _response}

      refute_touched(command)
    end
  end

  describe "a command that is not refused" do
    test "an unregistered command is still `:unregistered_command`" do
      assert {:error, :unregistered_command} =
               dispatch(BareRouter, %Unrouted{id: UUID.uuid4(), command_id: nil})

      assert {:error, :unregistered_command} =
               dispatch(BareRouter, %Unrouted{id: UUID.uuid4(), command_id: UUID.uuid4()})
    end

    test "a canonical `command_id` reaches the aggregate as it was sent" do
      command = touch([])
      %Touch{id: id, command_id: command_id} = command

      assert :ok = dispatch(BareRouter, command)

      assert_received {:executed, ^command}

      assert [%{data: %Touched{id: ^id, command_id: ^command_id}}] =
               DefaultApp |> EventStore.stream_forward("toucher-" <> id) |> Enum.to_list()
    end
  end
end
