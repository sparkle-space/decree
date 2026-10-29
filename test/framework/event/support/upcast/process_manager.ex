defmodule Decree.Event.Upcast.ProcessManager do
  defmodule Ok do
    defstruct [:id]
  end

  defmodule Aggregate do
    defstruct []

    def execute(_, _), do: []
    def apply(agg, _), do: agg
  end

  defmodule Router do
    use Decree.Commands.Router

    dispatch(Ok, to: Aggregate, identity: :id)
  end

  defmodule Application do
    use Decree.Application, otp_app: :decree

    router(Router)
  end

  @derive Jason.Encoder
  defstruct []

  alias Decree.Event.Upcast.Events.{EventFour, EventOne, EventThree, EventTwo, Stop}
  alias Decree.UUID

  use Decree.ProcessManagers.ProcessManager,
    application: Application,
    name: "UpcastProcessManager"

  def interested?(%EventOne{process_id: process_id}), do: {:start, process_id}
  def interested?(%EventTwo{process_id: process_id}), do: {:start, process_id}
  def interested?(%EventThree{process_id: process_id}), do: {:start, process_id}
  def interested?(%EventFour{process_id: process_id}), do: {:start, process_id}
  def interested?(%Stop{process_id: process_id}), do: {:stop, process_id}

  def handle(_, %EventOne{} = e), do: send_reply(e)
  def handle(_, %EventTwo{} = e), do: send_reply(e)
  def handle(_, %EventThree{} = e), do: send_reply(e)
  def handle(_, %EventFour{} = e), do: send_reply(e)

  defp send_reply(%{reply_to: reply_to} = e) do
    send(:erlang.list_to_pid(reply_to), e)

    %Ok{id: UUID.uuid4()}
  end
end
