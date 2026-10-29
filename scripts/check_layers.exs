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

defmodule Decree.CheckLayers do
  @store_prefixes ["Elixir.Decree.Store", "Elixir.Mix.Decree.Store", "Elixir.Mix.Tasks.Decree.Store"]
  @store_paths ["lib/decree/store/", "lib/decree/store.ex", "lib/mix/decree/", "lib/mix/tasks/decree.store."]

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

    case misplaced ++ Enum.uniq(violations) do
      [] ->
        IO.puts("Layer check passed: #{length(store)} store modules reference no framework module.")

      errors ->
        Enum.each(errors, &IO.puts(:stderr, &1))
        IO.puts(:stderr, "Layer check failed: #{length(errors)} violation(s).")
        System.halt(1)
    end
  end

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
