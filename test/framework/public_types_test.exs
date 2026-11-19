defmodule Decree.PublicTypesTest do
  # ex_doc refuses a public spec or type that names an undefined type, or a type of a module
  # whose docs are hidden. `mix docs --warnings-as-errors` in CI is the general check; these
  # pin the two it found.
  use ExUnit.Case, async: true

  defp type(module, name) do
    {:ok, types} = Code.Typespec.fetch_types(module)

    Enum.find_value(types, fn
      {kind, {^name, ast, []}} when kind in [:type, :opaque] -> ast
      _other -> nil
    end)
  end

  defp remote_modules(ast) do
    {_ast, modules} =
      Macro.prewalk(ast, [], fn
        {:remote_type, _line, [{:atom, _, module}, {:atom, _, _name}, _args]} = node, acc ->
          {node, [module | acc]}

        node, acc ->
          {node, acc}
      end)

    modules
  end

  defp hidden?(module) do
    match?({:docs_v1, _, _, _, :hidden, _, _}, Code.fetch_docs(module))
  end

  test "Decree.Aggregates.ExecutionContext defines t/0, which Decree.Event.Append's specs name" do
    assert type(Decree.Aggregates.ExecutionContext, :t)
  end

  test "Decree.PersonalFields.answers/0 names no type of a hidden module" do
    ast = type(Decree.PersonalFields, :answers)

    assert ast
    refute Enum.any?(remote_modules(ast), &hidden?/1)
  end
end
