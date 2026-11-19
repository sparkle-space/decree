defmodule Decree.Aggregates.LostAppendAggregate do
  @moduledoc false

  # A door: `Close` is refused unless an `Opened` event was applied, so its answer
  # depends on whether the aggregate's state includes an earlier written event.

  @derive Jason.Encoder
  defstruct [:uuid, opened?: false]

  defmodule Open do
    @derive Jason.Encoder
    defstruct [:uuid, :command_id]
  end

  defmodule Close do
    @derive Jason.Encoder
    defstruct [:uuid, :command_id]
  end

  defmodule Opened do
    @derive Jason.Encoder
    defstruct [:uuid]
  end

  defmodule Closed do
    @derive Jason.Encoder
    defstruct [:uuid]
  end

  alias Decree.Aggregates.LostAppendAggregate

  def execute(%LostAppendAggregate{opened?: true}, %Open{}), do: {:error, :already_open}
  def execute(%LostAppendAggregate{}, %Open{uuid: uuid}), do: %Opened{uuid: uuid}

  def execute(%LostAppendAggregate{opened?: false}, %Close{}), do: {:error, :not_open}
  def execute(%LostAppendAggregate{}, %Close{uuid: uuid}), do: %Closed{uuid: uuid}

  def apply(%LostAppendAggregate{} = state, %Opened{uuid: uuid}),
    do: %LostAppendAggregate{state | uuid: uuid, opened?: true}

  def apply(%LostAppendAggregate{} = state, %Closed{}),
    do: %LostAppendAggregate{state | opened?: false}
end

defmodule Decree.Aggregates.LostAppendRouter do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.Aggregates.LostAppendAggregate
  alias Decree.Aggregates.LostAppendAggregate.{Close, Open}

  middleware Decree.Helpers.CommandAuditMiddleware

  dispatch [Open, Close], to: LostAppendAggregate, identity: :uuid
end

defmodule Decree.Aggregates.LostAppendApp do
  @moduledoc false
  use Decree.Application,
    otp_app: :decree,
    event_store:
      Decree.FrameworkStorage.event_store_config(event_store: Decree.Aggregates.LostAppendStore),
    registry: :local

  router(Decree.Aggregates.LostAppendRouter)

  # Its own event store process name, as `Decree.FrameworkStorage.name_event_store/1`
  # gives the other applications.
  def init(config) do
    instance = Keyword.get(config, :name) || Keyword.fetch!(config, :application)

    {:ok,
     Keyword.update(config, :event_store, [], fn event_store ->
       Keyword.put_new(event_store, :name, Module.concat([instance, "FrameworkEventStore"]))
     end)}
  end
end
