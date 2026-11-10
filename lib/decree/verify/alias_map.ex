defmodule Decree.Verify.AliasMap do
  @moduledoc false

  # The syntactic `alias` table `Decree.Verify.Eventless` expands a written receiver
  # through.
  #
  # `pairs/1,2` returns `{short_name, full_segments}`, in source order, for every `alias` in
  # the AST it is handed: `alias A.B`, `alias A.B, as: C` and the brace form
  # `alias A.{B, C.D}`. It is SYNTACTIC: lexical scope is not modelled (an alias written
  # inside one function reads as AST-wide), and a target that is itself an alias is not
  # chased. A target with a computed segment is skipped, except a leading `__MODULE__` when
  # `pairs/2` is given the enclosing module. `Map.new/1` of the pairs is the table, a later
  # declaration of the same short name replacing an earlier one.
  #
  # `conflicts/1` returns `%{short_name => [module]}` for every short name the pairs map to
  # two or more different targets; a name declared twice with one target is no conflict,
  # however it is spelled (`Foo.Bar` and `Elixir.Foo.Bar` are one module).
  #
  # `expand/2` replaces the FIRST segment of a written name when the table holds it, and
  # returns the segments unchanged otherwise. It performs one step; a caller that wants a
  # chain resolves the result again.

  @type t :: %{optional(atom()) => [atom()]}

  @spec pairs(Macro.t(), module() | nil) :: [{atom(), [atom()]}]
  def pairs(ast, module \\ nil) do
    {_ast, pairs} =
      Macro.prewalk(ast, [], fn
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

    Enum.reverse(pairs)
  end

  @spec conflicts([{atom(), [atom()]}]) :: %{optional(atom()) => [module()]}
  def conflicts(pairs) do
    for {name, targets} <- Enum.group_by(pairs, &elem(&1, 0), &elem(&1, 1)),
        targets = targets |> Enum.map(&Module.concat/1) |> Enum.uniq(),
        length(targets) > 1,
        into: %{},
        do: {name, Enum.sort(targets)}
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
      do: [{name, segments} | acc],
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
