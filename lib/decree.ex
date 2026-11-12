defmodule Decree do
  @moduledoc """
  Use Decree to build your own Elixir applications following the CQRS/ES pattern.

  Provides support for:

  - Defining applications (`Decree.Application`).
  - Command registration and dispatch (`Decree.Commands.Router`).
  - Hosting and delegation to aggregates (`Decree.Aggregates.Aggregate`).
  - Event handling (`Decree.Event.Handler`).

  Events persist in `Decree.Store`, the Postgres event store, which an application reaches
  through `Decree.EventStore`.
  """

  use Application

  alias Decree.Aggregates.Aggregate
  alias Decree.Application.Config

  @doc false
  def start(_type, _args) do
    children = [
      Decree.Store.Application,
      Config
    ]

    opts = [strategy: :one_for_one, name: Decree.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @doc """
  Retrieve aggregate state of an aggregate.

  Retrieving aggregate state is done by calling to the opened aggregate,
  or querying the event store for an optional state snapshot
  and then replaying the aggregate's event stream.
  """
  @spec aggregate_state(
          application :: Decree.Application.t(),
          aggregate_module :: module(),
          aggregate_uuid :: Aggregate.uuid(),
          timeout :: integer
        ) :: Aggregate.state()
  def aggregate_state(application, aggregate_module, aggregate_uuid, timeout \\ 5_000) do
    Aggregate.aggregate_state(
      application,
      aggregate_module,
      aggregate_uuid,
      timeout
    )
  end
end
