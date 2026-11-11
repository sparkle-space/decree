defmodule Decree.DelegatingStore do
  @moduledoc false

  # The framework suites' Postgres store (`Decree.FrameworkEventStore`) under another module
  # name, for a test store that injects a fault into, or records, what reaches one callback:
  # `use` it and override that callback. Every other `Decree.Store` callback delegates as it
  # stands. The store process is the framework store's, started under the name the
  # application's event store config gives it, or this module's own, the name such a config
  # falls back to, so every call reaches it.

  @store Decree.FrameworkEventStore

  defmacro __using__(_opts) do
    store = @store

    callbacks =
      Decree.Store.behaviour_info(:callbacks) --
        [{:start_link, 1} | Decree.Store.behaviour_info(:optional_callbacks)]

    delegations =
      for {fun, arity} <- callbacks do
        args = Macro.generate_arguments(arity, __MODULE__)

        quote do
          @impl Decree.Store
          def unquote(fun)(unquote_splicing(args)),
            do: unquote(store).unquote(fun)(unquote_splicing(args))
        end
      end

    quote do
      @behaviour Decree.Store

      def child_spec(opts),
        do: unquote(store).child_spec(Keyword.put_new(opts, :name, __MODULE__))

      @impl Decree.Store
      def start_link(opts),
        do: unquote(store).start_link(Keyword.put_new(opts, :name, __MODULE__))

      unquote_splicing(delegations)

      defoverridable unquote([{:child_spec, 1}, {:start_link, 1} | callbacks])
    end
  end
end
