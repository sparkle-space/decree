defmodule Decree.Store.FiniteTypeProvider do
  @moduledoc false

  # A custom `Decree.EventStore.TypeProvider` that knows only the type names it has a clause
  # for, as a hand-written provider does: `to_struct/1` raises `FunctionClauseError` for any
  # other name, even one naming a loaded module (sparkle-space/sparklespace#7804). It is
  # configured through the global `:type_provider` app env, so only a test that is not async
  # may set it.

  @behaviour Decree.EventStore.TypeProvider

  alias Decree.Serialization.ModuleNameTypeProvider
  alias Decree.Store.EventFactory

  @known_type "Elixir.Decree.Store.EventFactory.Event"

  def known_type, do: @known_type

  @impl Decree.EventStore.TypeProvider
  def to_string(struct), do: ModuleNameTypeProvider.to_string(struct)

  @impl Decree.EventStore.TypeProvider
  def to_struct(@known_type), do: %EventFactory.Event{}

  # Configures `provider`, this one by default, for the rest of the calling test, restoring
  # the previous one when it exits.
  def configure!(provider \\ __MODULE__) do
    previous = Application.fetch_env(:decree, :type_provider)

    Application.put_env(:decree, :type_provider, provider)

    ExUnit.Callbacks.on_exit(fn ->
      case previous do
        {:ok, provider} -> Application.put_env(:decree, :type_provider, provider)
        :error -> Application.delete_env(:decree, :type_provider)
      end
    end)
  end
end
