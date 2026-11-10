defmodule Decree.Verify.AliasMap do
  @moduledoc false

  # The syntactic `alias` table `Decree.Verify.Eventless` expands a written receiver
  # through.
  #
  # `collect/1,2` returns `%{short_name => full_segments}` for every `alias` in the AST it
  # is handed: `alias A.B`, `alias A.B, as: C` and the brace form `alias A.{B, C.D}`. It is
  # SYNTACTIC: lexical scope is not modelled (an alias written inside one function reads as
  # AST-wide), a later declaration of the same short name replaces an earlier one, and a
  # target that is itself an alias is not chased. A target with a computed segment is
  # skipped, except a leading `__MODULE__` when `collect/2` is given the enclosing module.
  #
  # `expand/2` replaces the FIRST segment of a written name when the table holds it, and
  # returns the segments unchanged otherwise. It performs one step; a caller that wants a
  # chain resolves the result again.

  @type t :: %{optional(atom()) => [atom()]}

  @spec collect(Macro.t(), module() | nil) :: t()
  def collect(ast, module \\ nil) do
    {_ast, aliases} =
      Macro.prewalk(ast, %{}, fn
        {:alias, _, [{:__aliases__, _, segments} | opts]} = node, acc ->
          segments = expand_module(segments, module)
          {node, put(acc, name(segments, opts), segments)}

        {:alias, _, [{{:., _, [{:__aliases__, _, base}, :{}]}, _, children} | _opts]} = node,
        acc ->
          base = expand_module(base, module)

          {node,
           Enum.reduce(children, acc, fn
             {:__aliases__, _, child}, inner -> put(inner, List.last(child), base ++ child)
             _other, inner -> inner
           end)}

        node, acc ->
          {node, acc}
      end)

    aliases
  end

  @spec expand([atom()], t()) :: [atom()]
  def expand([head | rest] = segments, aliases) do
    case Map.fetch(aliases, head) do
      {:ok, target} -> target ++ rest
      :error -> segments
    end
  end

  def expand([], _aliases), do: []

  defp expand_module([{:__MODULE__, _, ctx} | rest] = segments, module)
       when is_atom(ctx) and is_atom(module) and not is_nil(module) do
    case Atom.to_string(module) do
      "Elixir." <> name -> Enum.map(String.split(name, "."), &String.to_atom/1) ++ rest
      _erlang -> segments
    end
  end

  defp expand_module(segments, _module), do: segments

  defp put(acc, name, segments) do
    if is_atom(name) and Enum.all?(segments, &is_atom/1),
      do: Map.put(acc, name, segments),
      else: acc
  end

  defp name(segments, [opts]) when is_list(opts) do
    case List.keyfind(opts, :as, 0) do
      {:as, {:__aliases__, _, [name]}} -> name
      _other -> List.last(segments)
    end
  end

  defp name(segments, _opts), do: List.last(segments)
end
