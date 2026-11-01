defmodule Decree.Store.Query do
  @moduledoc """
  What `c:Decree.Store.read_query/2` and `c:Decree.Store.stream_query/2` search for.

  A query is a non-empty list of items, and it matches an event when any item does.
  An item is a keyword list or a map with `:types` and `:tags`, each a list of strings,
  either of which may be absent. It matches an event whose type is one of its types, if
  it names any, and which carries all of its tags:

      [
        # Every event of either type.
        [types: ["OrderPlaced", "OrderCancelled"]],

        # Every event carrying both tags, whatever its type.
        [tags: ["order:1", "tenant:a"]],

        # Every event of this type carrying this tag.
        [types: ["InvoiceRaised"], tags: ["order:1"]]
      ]

  Tags and types are strings. An item's repeats count once. An item naming neither
  types nor tags cannot be served by an index, so it is refused. A store that declares
  `deletable` also refuses a query naming what it declares, with
  `{:error, :query_names_deletable}`: see "Deletable streams" in `Decree.Store`.

  ## The first tag

  An item's first tag is the one the index searches, and the item's other tags are
  checked against each hit. Put the narrowest tag first: a broad first tag costs read
  time, and never changes the answer.
  """

  alias Decree.Store.EventData

  @type item ::
          [types: [String.t()], tags: [String.t()]]
          | %{optional(:types) => [String.t()], optional(:tags) => [String.t()]}

  @type t :: %__MODULE__{
          probe_items: [non_neg_integer()],
          probe_tags: [String.t() | nil],
          probe_types: [String.t() | nil],
          rest_items: [non_neg_integer()],
          rest_tags: [String.t()]
        }

  # One probe per item and type, or one with no type if the item names none. `probe_*`
  # are parallel arrays and so are `rest_*`, because Postgres arrays are rectangular.
  # A probe's tag is its item's first; `rest_*` hold the item's other tags.
  defstruct [:probe_items, :probe_tags, :probe_types, :rest_items, :rest_tags]

  @doc """
  Compiles a list of items.

  Answers `{:error, :invalid_query}` for anything but a non-empty list of items, and
  `{:error, :tag_too_long}` for a tag over `Decree.Store.EventData.max_tag_bytes/0`. A
  refusal sends nothing to the database, so a caller's transaction stays alive.
  """
  @spec compile(items :: [item()]) ::
          {:ok, t()} | {:error, :invalid_query} | {:error, :tag_too_long}
  def compile([_item | _items] = items) do
    if List.improper?(items) do
      {:error, :invalid_query}
    else
      items |> Enum.map(&parse_item/1) |> compile_parsed()
    end
  end

  def compile(_items), do: {:error, :invalid_query}

  @doc false
  def names_tag?(%__MODULE__{} = query, tag) do
    tag in query.probe_tags or tag in query.rest_tags
  end

  # Whether a query names a declared event type or a tag of a declared kind. The refusal is
  # per name, so a `command:` tag beside either does not save an item. `nil` marks a probe
  # with no type, or no tag.
  @doc false
  def names_deletable?(%__MODULE__{} = query, deletable) do
    types = Keyword.get(deletable, :event_types, [])
    kinds = Keyword.get(deletable, :tag_kinds, [])

    Enum.any?(query.probe_types, &(&1 in types)) or
      Enum.any?(query.probe_tags, &tag_of_kind?(&1, kinds)) or
      Enum.any?(query.rest_tags, &tag_of_kind?(&1, kinds))
  end

  # A list of what a tag's kind can be: not empty, no colon, and a string Postgres can hold.
  @doc false
  def tag_kinds?([]), do: true

  def tag_kinds?([kind | kinds]) when is_binary(kind) and kind != "" do
    strings?([kind]) and not String.contains?(kind, ":") and tag_kinds?(kinds)
  end

  def tag_kinds?(_kinds), do: false

  defp tag_of_kind?(nil, _kinds), do: false
  defp tag_of_kind?(tag, kinds), do: tag_kind(tag) in kinds

  # `<kind>:<value>`, where the kind has no colon and the value may. A tag with no colon is
  # its own kind. The same split as `split_part(tag, ':', 1)` in `delete_stream_tags`.
  defp tag_kind(tag), do: tag |> String.split(":", parts: 2) |> hd()

  defp compile_parsed(parsed) do
    cond do
      :error in parsed -> {:error, :invalid_query}
      Enum.any?(parsed, &tag_too_long?/1) -> {:error, :tag_too_long}
      true -> {:ok, build(parsed)}
    end
  end

  defp parse_item(item) when is_map(item) and not is_struct(item) do
    item |> Map.to_list() |> parse_pairs()
  end

  defp parse_item(item) when is_list(item) do
    if Keyword.keyword?(item), do: parse_pairs(item), else: :error
  end

  defp parse_item(_item), do: :error

  # A misspelt or repeated key would silently widen the item, so the keys must be exact.
  defp parse_pairs(pairs) do
    keys = pairs |> Enum.map(&elem(&1, 0)) |> Enum.sort()
    types = pairs |> List.keyfind(:types, 0, {:types, []}) |> elem(1)
    tags = pairs |> List.keyfind(:tags, 0, {:tags, []}) |> elem(1)

    if keys in [[:tags], [:types], [:tags, :types]] and strings?(types) and strings?(tags) and
         (types != [] or tags != []) do
      {Enum.uniq(types), Enum.uniq(tags)}
    else
      :error
    end
  end

  defp strings?([]), do: true

  # A string Postgres cannot hold could match nothing, and sending it would abort a
  # caller's transaction.
  defp strings?([string | strings]) when is_binary(string) do
    EventData.valid_tag?(string) and strings?(strings)
  end

  defp strings?(_strings), do: false

  defp tag_too_long?({_types, tags}) do
    Enum.any?(tags, fn tag -> byte_size(tag) > EventData.max_tag_bytes() end)
  end

  defp build(parsed) do
    indexed = Enum.with_index(parsed)

    probes =
      for {{types, tags}, item} <- indexed, type <- probe_types(types) do
        {item, List.first(tags), type}
      end

    rest = for {{_types, tags}, item} <- indexed, tag <- Enum.drop(tags, 1), do: {item, tag}

    %__MODULE__{
      probe_items: Enum.map(probes, &elem(&1, 0)),
      probe_tags: Enum.map(probes, &elem(&1, 1)),
      probe_types: Enum.map(probes, &elem(&1, 2)),
      rest_items: Enum.map(rest, &elem(&1, 0)),
      rest_tags: Enum.map(rest, &elem(&1, 1))
    }
  end

  defp probe_types([]), do: [nil]
  defp probe_types(types), do: types
end
