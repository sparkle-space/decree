# The layer check: the store never references the framework.
#
#     mix run --no-start scripts/check_layers.exs
#
# The store layer is every module in decree's ebin, except the test support modules
# compiled from test/ in the :test environment, whose name is Decree.Store,
# Decree.Store.*, Mix.Decree.Store or Mix.Tasks.Decree.Store.*. The framework is every
# other Decree module. The check reads each store module's compiled Erlang code (its
# debug info) and its atom table, so remote calls, struct names, and module names inside
# literals all count, and fails on any atom naming a framework module. It also fails
# when a file under the store's directories defines a module outside the store's
# namespace.
#
# A module name built at runtime (`Module.concat(["Decree", "UUID"])`,
# `String.to_atom("Elixir.Decree.X")`) leaves no framework atom in the BEAM, so the check
# also scans the store layer's source files (`lib/decree/store.ex`, `lib/decree/store/**`,
# `lib/mix/**`). It is deliberately conservative and fails on:
#
# - a string or charlist literal starting with `Decree` or `Elixir.Decree` that does not
#   name a `Decree.Store` module;
# - a `Module.concat/1,2`, `Module.safe_concat/1,2`, `String.to_atom/1`,
#   `String.to_existing_atom/1`, `List.to_atom/1`, `List.to_existing_atom/1` or
#   `:erlang.binary_to_atom`/`binary_to_existing_atom`/`list_to_atom`/
#   `list_to_existing_atom` call whose argument visibly names `Decree` outside
#   `Decree.Store`, in any form (string, charlist, atom such as `:Decree`, or alias).

defmodule Decree.CheckLayers do
  @store_prefixes [
    "Elixir.Decree.Store",
    "Elixir.Mix.Decree.Store",
    "Elixir.Mix.Tasks.Decree.Store"
  ]
  @store_paths [
    "lib/decree/store/",
    "lib/decree/store.ex",
    "lib/mix/decree/",
    "lib/mix/tasks/decree.store."
  ]

  @source_globs ["lib/decree/store.ex", "lib/decree/store/**/*.{ex,exs}", "lib/mix/**/*.{ex,exs}"]

  # A literal opening with a framework name: `"Decree"`, `"Decree.UUID"`, `'Elixir.Decree.X'`,
  # but not `"Decree.Store"`, `"Decree.Store.X"` or `"Decree.Store: ..."`.
  @framework_literal ~r/["'](?:Elixir\.)?Decree(?!\.Store\b)(?!\w)/

  # A framework name anywhere in a name-building call's argument, in any form.
  @framework_name ~r/(?<![\w.])(?:Elixir\.)?Decree(?!\.Store\b)(?!\w)/

  @name_builders [
    {[:Module], [:concat, :safe_concat]},
    {[:String], [:to_atom, :to_existing_atom]},
    {[:List], [:to_atom, :to_existing_atom]},
    {:erlang, [:binary_to_atom, :binary_to_existing_atom, :list_to_atom, :list_to_existing_atom]}
  ]

  def run do
    root = File.cwd!()

    modules =
      Mix.Project.compile_path()
      |> Path.join("*.beam")
      |> Path.wildcard()
      |> Enum.map(&read_beam(&1, root))
      # Test support modules compile into the same ebin in :test; they are not the layers.
      # A module a dependency's macro generates (Decree.Store.PostgresTypes) has that
      # dependency's source path, and is checked like the rest.
      |> Enum.reject(&String.starts_with?(&1.source, "test/"))

    store = Enum.filter(modules, &store_module?(&1.name))

    misplaced =
      for %{name: name, source: source} <- modules,
          store_path?(source) and not store_module?(name),
          do: "#{source}: #{inspect_module(name)} is outside the Decree.Store namespace"

    violations =
      for %{name: name, source: source, atoms: atoms} <- store,
          atom <- atoms,
          framework_module?(atom),
          do: "#{source}: #{inspect_module(name)} references #{inspect_module(atom)}"

    sources =
      @source_globs
      |> Enum.flat_map(&Path.wildcard/1)
      |> Enum.uniq()
      |> Enum.sort()

    source_violations = Enum.flat_map(sources, &scan_source/1)

    case misplaced ++ Enum.uniq(violations) ++ source_violations do
      [] ->
        IO.puts(
          "Layer check passed: #{length(store)} store modules reference no framework module; " <>
            "#{length(sources)} store source files build no framework module name."
        )

      errors ->
        Enum.each(errors, &IO.puts(:stderr, &1))
        IO.puts(:stderr, "Layer check failed: #{length(errors)} violation(s).")
        System.halt(1)
    end
  end

  # Source-level scan for module names built at runtime.
  defp scan_source(path) do
    source = File.read!(path)

    literals =
      source
      |> String.split("\n")
      |> Enum.with_index(1)
      |> Enum.filter(fn {line, _number} -> Regex.match?(@framework_literal, line) end)
      |> Enum.map(fn {line, number} ->
        "#{path}:#{number}: literal names a framework module: #{String.trim(line)}"
      end)

    {_ast, calls} =
      source
      |> Code.string_to_quoted!(file: path, columns: true)
      |> Macro.prewalk([], fn node, acc -> {node, name_builder_violations(node, path) ++ acc} end)

    literals ++ Enum.reverse(calls)
  end

  # `x |> String.to_atom()`: the piped value is the argument.
  defp name_builder_violations({:|>, _meta, [lhs, {call, meta, args}]}, path)
       when is_list(args) do
    if name_builder?(call), do: check_args(path, meta, call, [lhs | args]), else: []
  end

  defp name_builder_violations({call, meta, args}, path) when is_list(args) do
    if name_builder?(call), do: check_args(path, meta, call, args), else: []
  end

  defp name_builder_violations(_node, _path), do: []

  defp name_builder?({:., _, [{:__aliases__, _, mod}, fun]}),
    do: Enum.any?(@name_builders, fn {m, funs} -> m == mod and fun in funs end)

  defp name_builder?({:., _, [mod, fun]}) when is_atom(mod),
    do: Enum.any?(@name_builders, fn {m, funs} -> m == mod and fun in funs end)

  defp name_builder?(_call), do: false

  defp check_args(path, meta, call, args) do
    text = Macro.to_string(args)

    if Regex.match?(@framework_name, text) do
      [
        "#{path}:#{meta[:line]}: #{call_name(call)} builds a framework module name: " <>
          text
      ]
    else
      []
    end
  end

  defp call_name({:., _, [{:__aliases__, _, mod}, fun]}), do: "#{Enum.join(mod, ".")}.#{fun}"
  defp call_name({:., _, [mod, fun]}), do: "#{inspect(mod)}.#{fun}"

  defp read_beam(path, root) do
    {:ok, {module, [atoms: atoms, compile_info: info, debug_info: debug_info]}} =
      :beam_lib.chunks(String.to_charlist(path), [:atoms, :compile_info, :debug_info])

    {:debug_info_v1, backend, data} = debug_info
    {:ok, forms} = backend.debug_info(:erlang_v1, module, data, [])

    source = info |> Keyword.get(:source, ~c"") |> to_string() |> Path.relative_to(root)
    atoms = Enum.map(atoms, fn {_index, atom} -> atom end) ++ collect_atoms(forms, [])

    %{name: Atom.to_string(module), source: source, atoms: Enum.uniq(atoms)}
  end

  defp collect_atoms(term, acc) when is_atom(term), do: [term | acc]
  defp collect_atoms(term, acc) when is_list(term), do: Enum.reduce(term, acc, &collect_atoms/2)

  defp collect_atoms(term, acc) when is_tuple(term),
    do: term |> Tuple.to_list() |> collect_atoms(acc)

  defp collect_atoms(%{} = term, acc), do: term |> Map.to_list() |> collect_atoms(acc)
  defp collect_atoms(_term, acc), do: acc

  defp store_module?(name), do: Enum.any?(@store_prefixes, &namespaced?(name, &1))

  defp store_path?(source), do: Enum.any?(@store_paths, &String.starts_with?(source, &1))

  defp framework_module?(atom) do
    name = Atom.to_string(atom)
    namespaced?(name, "Elixir.Decree") and not namespaced?(name, "Elixir.Decree.Store")
  end

  defp namespaced?(name, prefix), do: name == prefix or String.starts_with?(name, prefix <> ".")

  defp inspect_module(name) when is_binary(name), do: String.replace_prefix(name, "Elixir.", "")
  defp inspect_module(atom), do: atom |> Atom.to_string() |> inspect_module()
end

Decree.CheckLayers.run()
