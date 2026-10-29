# Renames the copied Commanded and EventStore code to decree's namespaces.
#
#     elixir scripts/rename.exs
#
# Deterministic and idempotent: a second run on renamed code changes nothing.
#
#   Commanded.X / Commanded           -> Decree.X / Decree           (framework layer)
#   EventStore.X / EventStore (head)  -> Decree.Store.X / Decree.Store (store layer)
#   Mix.EventStore                    -> Mix.Decree.Store
#   Mix.Tasks.EventStore.X            -> Mix.Tasks.Decree.Store.X
#   event_store.<task>                -> decree.store.<task>         (Mix task names)
#   :commanded, :eventstore,
#   :commanded_eventstore_adapter,
#   :commanded_ecto_projections       -> :decree                     (OTP app, telemetry prefix;
#                                                                   not the `:eventstore` option key)
#   "Elixir.EventStore.X"             -> "Elixir.Decree.Store.X"     (test fixture type strings)
#   priv/event_store                  -> priv/decree/store
#
# `EventStore` is renamed only at the head of a module name: never when preceded by a
# word character or a dot, so `TestEventStore` and `Decree.EventStore.Adapters.EventStore`
# survive. A file that binds the name `EventStore` with an alias (`alias X.EventStore`,
# `alias X.{.., EventStore}`, `alias X, as: EventStore`) keeps every bare `EventStore`
# from that alias onwards: it resolves to the alias, not to the store.
#
# Persisted and on-the-wire strings are never touched: `.sql` and `.sql.eex` files are
# skipped, and the trigger error-message prefix `EventStore: ` is masked where it is
# Postgres's text (the SQL literal and the messages the store suite matches).

defmodule Decree.Rename do
  @root Path.expand("..", __DIR__)

  # Directory and file moves, applied in order (source relative to the repo root).
  @moves [
    {"lib/commanded.ex", "lib/decree.ex"},
    {"lib/commanded", "lib/decree"},
    {"lib/event_store.ex", "lib/decree/store.ex"},
    {"lib/event_store", "lib/decree/store"},
    {"lib/application.ex", "lib/decree/application.ex"},
    {"lib/projections", "lib/decree/projections"},
    {"lib/register_postgres_types.ex", "lib/decree/store/postgres_types.ex"},
    {"lib/mix/event_store.ex", "lib/mix/decree/store.ex"},
    {"lib/mix/tasks/event_store.create.ex", "lib/mix/tasks/decree.store.create.ex"},
    {"lib/mix/tasks/event_store.drop.ex", "lib/mix/tasks/decree.store.drop.ex"},
    {"lib/mix/tasks/event_store.init.ex", "lib/mix/tasks/decree.store.init.ex"},
    {"lib/mix/tasks/event_store.migrate.ex", "lib/mix/tasks/decree.store.migrate.ex"},
    {"lib/mix/tasks/event_store.migrations.ex", "lib/mix/tasks/decree.store.migrations.ex"},
    {"priv/event_store", "priv/decree/store"}
  ]

  @content_dirs ["lib", "test"]
  @content_exts [".ex", ".exs"]

  # Literals the rules would otherwise rewrite, masked during the rewrite:
  #
  # - the store's name in Postgres: the trigger functions' error-message prefix (in
  #   the SQL) and the messages the store suite matches;
  # - `:eventstore` where it is not the OTP application: the OptionParser key of the
  #   `--eventstore` / `-e` switch of the Mix tasks (and of
  #   `Decree.Store.Tasks.Migrations.exec/2`), and the store suite's instance name
  #   `name: :eventstore`.
  @protected [
    "'EventStore: '",
    "\"EventStore: Cannot ",
    "e: :eventstore",
    "Keyword.put(:eventstore, ",
    "Keyword.get(opts, :eventstore, ",
    "name: :eventstore"
  ]

  # Rewrites applied to every file, in order.
  @rules [
    {~r/\bMix\.Tasks\.EventStore\./, "Mix.Tasks.Decree.Store."},
    {~r/\bMix\.EventStore\b/, "Mix.Decree.Store"},
    # Task names only: `event_store.init(config)` is a call on a variable.
    {~r/\bevent_store\.(migrations|migrate|create|drop|init|setup|reset)\b(?!\()/,
     "decree.store.\\1"},
    {~r/(?<![\w:]):(commanded_eventstore_adapter|commanded_ecto_projections|commanded|eventstore)\b/,
     ":decree"},
    {~r/(?<!\w)Commanded\b/, "Decree"},
    {~r/\bElixir\.EventStore\b/, "Elixir.Decree.Store"},
    {~r{\bpriv/event_store\b}, "priv/decree/store"},
    # The article before upstream's "an EventStore" ("is not an EventStore").
    {~r/\ban (?=Decree\.Store\b)/, "a "}
  ]

  # The head rule for bare `EventStore`, applied only before the file's first alias of it.
  @head_rule {~r/(?<![\w.])EventStore\b/, "Decree.Store"}

  @alias_binders [
    # alias Foo.EventStore   (the alias ends the statement)
    ~r/^[ \t]*alias [\w.]+\.EventStore[ \t]*(#.*)?$/m,
    # alias Foo.{Bar, EventStore}, possibly across lines
    ~r/^[ \t]*alias [\w.]+\.\{[^}]*\bEventStore\b[^}]*\}/m,
    # alias Foo, as: EventStore
    ~r/^[ \t]*alias [\w.]+, as: EventStore\b/m
  ]

  def run do
    File.cd!(@root)

    moved = Enum.flat_map(@moves, &move/1)
    changed = Enum.filter(content_files(), &rewrite_file/1)
    provenance = rewrite_provenance()

    IO.puts("moved: #{length(moved)}, rewritten: #{length(changed)}, provenance: #{provenance}")

    for path <- protected_alias_files(), do: IO.puts("alias-protected: #{path}")
  end

  defp move({from, to}) do
    if File.exists?(from) do
      File.mkdir_p!(Path.dirname(to))
      {_, 0} = System.cmd("git", ["mv", from, to])
      [from]
    else
      []
    end
  end

  defp content_files do
    @content_dirs
    |> Enum.flat_map(&Path.wildcard(Path.join(&1, "**/*")))
    |> Enum.filter(&(Path.extname(&1) in @content_exts and File.regular?(&1)))
    |> Enum.sort()
  end

  defp rewrite_file(path) do
    original = File.read!(path)
    renamed = rename(original)

    if renamed != original do
      File.write!(path, renamed)
      true
    else
      false
    end
  end

  def rename(text) do
    {masked, masks} = mask(text)

    masked
    |> apply_head_rule()
    |> then(fn text ->
      Enum.reduce(@rules, text, fn {re, to}, acc -> Regex.replace(re, acc, to) end)
    end)
    |> unmask(masks)
  end

  # The bare `EventStore` rule stops at the first alias that binds the name.
  defp apply_head_rule(text) do
    {re, to} = @head_rule

    case first_alias_offset(text) do
      nil ->
        Regex.replace(re, text, to)

      offset ->
        <<head::binary-size(^offset), tail::binary>> = text
        Regex.replace(re, head, to) <> tail
    end
  end

  defp first_alias_offset(text) do
    @alias_binders
    |> Enum.flat_map(fn re ->
      case Regex.run(re, text, return: :index) do
        [{offset, _} | _] -> [offset]
        nil -> []
      end
    end)
    |> Enum.min(fn -> nil end)
  end

  defp protected_alias_files do
    Enum.filter(content_files(), fn path ->
      text = File.read!(path)
      first_alias_offset(text) != nil and Regex.match?(~r/(?<![\w.])EventStore\b/, text)
    end)
  end

  defp mask(text) do
    @protected
    |> Enum.with_index()
    |> Enum.reduce({text, []}, fn {literal, index}, {acc, masks} ->
      token = "__DECREE_RENAME_MASK_#{index}__"
      {String.replace(acc, literal, token), [{token, literal} | masks]}
    end)
  end

  defp unmask(text, masks) do
    Enum.reduce(masks, text, fn {token, literal}, acc -> String.replace(acc, token, literal) end)
  end

  # PROVENANCE.tsv: only the destination path (column 1) follows the moves.
  defp rewrite_provenance do
    path = "PROVENANCE.tsv"
    original = File.read!(path)

    renamed =
      original
      |> String.split("\n")
      |> Enum.map(fn line ->
        case String.split(line, "\t") do
          [dest | rest] when rest != [] ->
            if String.starts_with?(dest, "#"),
              do: line,
              else: Enum.join([move_path(dest) | rest], "\t")

          _ ->
            line
        end
      end)
      |> Enum.join("\n")

    if renamed != original do
      File.write!(path, renamed)
      "rewritten"
    else
      "unchanged"
    end
  end

  defp move_path(path) do
    Enum.find_value(@moves, path, fn {from, to} ->
      cond do
        path == from -> to
        String.starts_with?(path, from <> "/") -> to <> String.trim_leading(path, from)
        true -> nil
      end
    end)
  end
end

Decree.Rename.run()
