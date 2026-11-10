defmodule Decree.Verify.ModuleScopes do
  @moduledoc false

  # Splits a parsed file into one `{module, body}` per `defmodule`, at any depth, in file
  # order, for `Decree.Verify.Eventless`. Syntax only.
  #
  # A nested `defmodule`'s name is qualified against its enclosing `defmodule` chain, as
  # the compiler qualifies it (`defmodule Outer do defmodule Inner` is `Outer.Inner`; the
  # raw head, with no alias expansion). An absolute `defmodule Elixir.X` resets the chain,
  # and so does a literal atom name. A computed name (`defmodule __MODULE__.X`,
  # `defmodule unquote(mod)`) is `nil`, as is everything nested inside one, and so is a
  # relative `defmodule` inside a `quote`, whose real prefix is the caller's module. A
  # `quote` body is otherwise scanned like any other code.
  #
  # `prune_nested_defmodules/1` replaces every nested `defmodule` inside a body with an
  # opaque leaf, so a module's own scope never carries a nested module's definitions; the
  # nested body has its own entry.

  @spec scopes(Macro.t()) :: [{module() | nil, Macro.t()}]
  def scopes(ast) do
    {_ast, {found, _stack}} =
      Macro.traverse(
        ast,
        {[], []},
        fn node, {found, stack} ->
          case node do
            {:defmodule, _meta, [alias_ast, [do: body]]} ->
              stack = push_scope(stack, alias_ast)
              {node, {[{current_module(stack), body} | found], stack}}

            {:quote, _, args} when is_list(args) ->
              {node, {found, [:quoted | stack]}}

            _other ->
              {node, {found, stack}}
          end
        end,
        fn node, {found, stack} ->
          case node do
            {:defmodule, _, [_alias_ast, [do: _body]]} -> {node, {found, tl(stack)}}
            {:quote, _, args} when is_list(args) -> {node, {found, tl(stack)}}
            _other -> {node, {found, stack}}
          end
        end
      )

    Enum.reverse(found)
  end

  @spec prune_nested_defmodules(Macro.t()) :: Macro.t()
  def prune_nested_defmodules(scope_ast) do
    Macro.prewalk(scope_ast, fn
      {:defmodule, _, [_alias, [do: _body]]} -> {:__nested_module_pruned__, [], nil}
      node -> node
    end)
  end

  # A frame is `{:ok, segments}`, `{:literal, module}`, `:computed` or `:quoted`.
  defp push_scope(stack, module) when is_atom(module), do: [{:literal, module} | stack]

  defp push_scope(stack, alias_ast) do
    frame =
      case alias_segments(alias_ast) do
        nil ->
          :computed

        [Elixir | _] = segments ->
          {:ok, segments}

        segments ->
          cond do
            :quoted in stack -> :computed
            true -> qualify(stack, segments)
          end
      end

    [frame | stack]
  end

  defp qualify([{:ok, parent} | _], segments), do: {:ok, parent ++ segments}

  defp qualify([{:literal, parent} | _], segments),
    do: {:literal, Module.concat([parent | segments])}

  defp qualify([:computed | _], _segments), do: :computed
  defp qualify([], segments), do: {:ok, segments}

  defp current_module([{:ok, segments} | _]), do: Module.concat(segments)
  defp current_module([{:literal, module} | _]), do: module
  defp current_module(_stack), do: nil

  defp alias_segments({:__aliases__, _, segments}) when is_list(segments) do
    if Enum.all?(segments, &is_atom/1), do: segments, else: nil
  end

  defp alias_segments(_other), do: nil
end
