defmodule Decree.Store.Subscriptions.GeneratedFsm do
  @moduledoc false

  # `use Fsm` with the expansion marked as generated code.
  #
  # fsm 0.3.1's `__using__/1` injects functions that Elixir 1.20's type checker warns
  # about (a struct update on an untyped variable, and a `change_state/2` clause that
  # no event reaches). The warnings are in the dependency's macro, yet they land on
  # `Decree.Store.Subscriptions.SubscriptionFsm`, which would fail
  # `mix compile --warnings-as-errors`. Marking the expansion `generated: true` keeps
  # the code identical and tells the compiler it is macro output.

  defmacro __using__(opts) do
    {:ok, env} = Macro.Env.define_require(__CALLER__, [], Fsm)
    {:macro, Fsm, expand} = Macro.Env.expand_require(env, [], Fsm, :__using__, 1)

    []
    |> expand.([opts])
    |> Macro.prewalk(fn
      {form, meta, args} when is_list(meta) -> {form, Keyword.put(meta, :generated, true), args}
      other -> other
    end)
  end
end
