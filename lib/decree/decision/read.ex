defmodule Decree.Decision.Read do
  @moduledoc false

  # A decision's merged read, with no store in it. A read's items sit in one fixed layout:
  # the decision's own at 0..n-1, then the framework's, the command item first, then the
  # index item and any activation item, then read-only narrowing items, which are read and
  # never go in the condition. A second read's indexes are offset by the first read's item
  # count.
  #
  # The struct is one read's layout. `Merged` is what the reads answered, together.

  alias Decree.Event.Append
  alias Decree.EventStore.RecordedEvent
  alias Decree.Store.{Query, ReadResult, TagIndex}

  # `probes` is the condition's probes as of this read: `nil` until they are counted, and for
  # items the store will refuse.
  defstruct [
    :items,
    :own,
    :framework,
    :decision_indexes,
    :framework_indexes,
    :command_index,
    :index_index,
    :activation_index,
    :replay_indexes,
    :probes
  ]

  defmodule Merged do
    @moduledoc false

    # `after`, `read_at` and `command_stamps` are always the first read's: `after` is what
    # the condition protects, and a command match the first read did not have restarts
    # the dispatch instead of being merged. `decision`, `framework`, `command`, `index`,
    # `activation` and `replay` are sets of merged item indexes. `first_commands` is the first
    # read's command matches.
    defstruct [
      :events,
      :after,
      :read_at,
      :command_stamps,
      :matched_items,
      :decision,
      :framework,
      :command,
      :index,
      :activation,
      :replay,
      :offset,
      :first_commands
    ]
  end

  @type parsed :: %{item: keyword() | map(), replay: boolean()}

  @doc """
  Checks a decision's items: each a keyword list or a map naming `:types` and at most
  `:tags` and `:replay`. A `replay: true` item names a `command_kind:` tag and at least one
  other, and that it names a scope tag is `replays_outside?/2`, which needs the dispatch. The
  store checks the rest, so a repeated key is left for it.
  """
  @spec parse(term()) :: {:ok, [parsed()]} | {:error, :invalid_query}
  def parse([_item | _items] = items) do
    if List.improper?(items) do
      {:error, :invalid_query}
    else
      parsed = Enum.map(items, &parse_item/1)

      if :error in parsed, do: {:error, :invalid_query}, else: {:ok, parsed}
    end
  end

  def parse(_items), do: {:error, :invalid_query}

  defp parse_item(item) when is_list(item) do
    if Keyword.keyword?(item) do
      with {:ok, replay} <- replay_flag(Keyword.get_values(item, :replay)),
           :ok <- names_types(List.keyfind(item, :types, 0)),
           :ok <- known_keys(Keyword.keys(item)),
           :ok <- replay_shape(replay, item_tags(item)) do
        %{item: Keyword.delete(item, :replay), replay: replay}
      end
    else
      :error
    end
  end

  defp parse_item(item) when is_map(item) and not is_struct(item) do
    with {:ok, replay} <- replay_flag(item |> Map.take([:replay]) |> Map.values()),
         :ok <- names_types(item |> Map.take([:types]) |> Map.to_list() |> List.first()),
         :ok <- known_keys(Map.keys(item)),
         :ok <- replay_shape(replay, item_tags(item)) do
      %{item: Map.delete(item, :replay), replay: replay}
    end
  end

  defp parse_item(_item), do: :error

  defp replay_flag([]), do: {:ok, false}
  defp replay_flag([flag]) when is_boolean(flag), do: {:ok, flag}
  defp replay_flag(_flags), do: :error

  defp names_types({:types, [_type | _types]}), do: :ok
  defp names_types(_types), do: :error

  defp known_keys(keys) do
    if Enum.all?(keys, &(&1 in [:types, :tags, :replay])), do: :ok, else: :error
  end

  defp replay_shape(false, _tags), do: :ok

  defp replay_shape(true, [_tag | _tags] = tags) do
    if not List.improper?(tags) and Enum.any?(tags, &Append.kind_tag?/1) and
         not Enum.all?(tags, &Append.kind_tag?/1) do
      :ok
    else
      :error
    end
  end

  defp replay_shape(true, _tags), do: :error

  @doc """
  Whether a `replay: true` item names `tag`.
  """
  @spec replays_tag?([parsed()], String.t()) :: boolean()
  def replays_tag?(parsed, tag) do
    Enum.any?(parsed, fn %{item: item, replay: replay} ->
      replay and tag in item_tags(item)
    end)
  end

  @doc """
  Whether a `replay: true` item names no tag of `scope`.
  """
  @spec replays_outside?([parsed()], [String.t()]) :: boolean()
  def replays_outside?(parsed, scope) do
    Enum.any?(parsed, fn %{item: item, replay: replay} ->
      replay and not Enum.any?(item_tags(item), &(&1 in scope))
    end)
  end

  @doc """
  Lays out one read.
  """
  @spec layout([parsed()], [keyword()], [keyword()]) :: %__MODULE__{}
  def layout(parsed, [_command_item | _] = framework_items, read_only_items \\ []) do
    %__MODULE__{} = read = lay_out(parsed, framework_items, read_only_items)

    %__MODULE__{read | command_index: length(read.own)}
  end

  @doc """
  Lays out one read of a `Decree.Decision.Rule`. A rule has no command item, so nothing here
  is one, whatever framework items it holds.
  """
  @spec layout_rule([parsed()], [keyword()]) :: %__MODULE__{}
  def layout_rule(parsed, framework_items), do: lay_out(parsed, framework_items, [])

  defp lay_out(parsed, framework_items, read_only_items) do
    own = Enum.map(parsed, & &1.item)
    n = length(own)

    replay_indexes =
      for {%{replay: true}, index} <- Enum.with_index(parsed), do: index

    index_index = index_item_index(framework_items, n)

    %__MODULE__{
      items: own ++ framework_items ++ read_only_items,
      own: own,
      framework: framework_items,
      decision_indexes: Enum.to_list(0..(n - 1)//1),
      framework_indexes: Enum.to_list(n..(n + length(framework_items) - 1)//1),
      index_index: index_index,
      activation_index: activation_index(framework_items, n, index_index),
      replay_indexes: replay_indexes
    }
  end

  # The index item is the framework item finding the facts, and an activation item is the one
  # after it. A layout without it has no index item, so it reads no fact, and a fold over no
  # fact enables nothing.
  defp index_item_index(framework_items, n) do
    case Enum.find_index(framework_items, &(&1 == index_item())) do
      nil -> nil
      index -> n + index
    end
  end

  defp activation_index(_framework_items, _n, nil), do: nil

  defp activation_index(framework_items, n, index_index) do
    if index_index + 1 < n + length(framework_items), do: index_index + 1
  end

  @doc """
  The framework item that finds the tag index facts.
  """
  @spec index_item() :: keyword()
  def index_item, do: [tags: [TagIndex.fact_tag()]]

  @doc """
  The items of the condition: every read's own, then the framework's once. A second read
  carries the framework's items too, and read-only items are never a condition's.
  """
  @spec condition_items([%__MODULE__{}]) :: [keyword() | map()]
  def condition_items([%__MODULE__{framework: framework} | _reads] = reads) do
    Enum.flat_map(reads, & &1.own) ++ framework
  end

  @doc """
  The types an item names. The command item names none.
  """
  @spec item_types(keyword() | map()) :: [String.t()]
  def item_types(item), do: item_field(item, :types)

  @doc """
  The tags on an item.
  """
  @spec item_tags(keyword() | map()) :: [String.t()]
  def item_tags(item), do: item_field(item, :tags)

  defp item_field(item, field) when is_list(item), do: Keyword.get(item, field, [])
  defp item_field(item, field) when is_map(item), do: Map.get(item, field, [])

  @doc """
  The types `items` name, each once.
  """
  @spec named_types([keyword() | map()]) :: [String.t()]
  def named_types(items), do: items |> Enum.flat_map(&item_types/1) |> Enum.uniq()

  @doc """
  The kinds of the tags on the items naming `type`, each once, so a `--` takes a kind out
  however many items name it.
  """
  @spec kinds_naming([keyword() | map()], String.t()) :: [String.t()]
  def kinds_naming(items, type) do
    items
    |> Enum.filter(&(type in item_types(&1)))
    |> Enum.flat_map(&item_tags/1)
    |> Enum.map(&Query.tag_kind/1)
    |> Enum.uniq()
  end

  @doc """
  The tags of `kind`, as a set. Anything that is not a binary is the store's to refuse.
  """
  @spec tags_of_kind(list(), String.t()) :: MapSet.t(String.t())
  def tags_of_kind(tags, kind) do
    MapSet.new(for tag <- tags, is_binary(tag), Query.tag_kind(tag) == kind, do: tag)
  end

  @doc """
  What `max_items` counts.
  """
  @spec condition_count([%__MODULE__{}]) :: non_neg_integer()
  def condition_count(reads), do: reads |> condition_items() |> length()

  @doc """
  What `max_probes` counts: the probes the store compiles the condition's items to. `nil` for
  items it refuses to compile, because the read answers those.
  """
  @spec count_probes([%__MODULE__{}]) :: non_neg_integer() | nil
  def count_probes(reads) do
    case reads |> condition_items() |> Query.compile() do
      {:ok, %Query{probe_items: probe_items}} -> length(probe_items)
      {:error, _reason} -> nil
    end
  end

  @doc """
  The condition's probes, which the last read holds.
  """
  @spec condition_probes([%__MODULE__{}]) :: non_neg_integer()
  def condition_probes(reads), do: List.last(reads).probes

  @doc """
  The first read's answer, as the merged read so far.
  """
  @spec first(%__MODULE__{}, ReadResult.t()) :: %Merged{}
  def first(%__MODULE__{} = read, %ReadResult{} = result) do
    %Merged{
      events: result.events,
      after: result.after,
      read_at: result.read_at,
      command_stamps: result.command_stamps,
      matched_items: result.matched_items,
      decision: MapSet.new(read.decision_indexes),
      framework: MapSet.new(read.framework_indexes),
      command: read |> command_indexes() |> MapSet.new(),
      index: read.index_index |> List.wrap() |> MapSet.new(),
      activation: read.activation_index |> List.wrap() |> MapSet.new(),
      replay: MapSet.new(read.replay_indexes),
      offset: length(read.items),
      first_commands:
        result.matched_items |> positions_matching(command_indexes(read)) |> MapSet.new()
    }
  end

  @doc """
  Merges a second read in by position. An event both reads hold counts once.
  """
  @spec merge(%Merged{}, %__MODULE__{}, ReadResult.t()) :: %Merged{}
  def merge(%Merged{offset: offset} = merged, %__MODULE__{} = read, %ReadResult{} = result) do
    shift = fn indexes -> MapSet.new(indexes, &(&1 + offset)) end

    second =
      Map.new(result.matched_items, fn {position, indexes} ->
        {position, Enum.map(indexes, &(&1 + offset))}
      end)

    %Merged{
      merged
      | events:
          (merged.events ++ result.events)
          |> Enum.uniq_by(&position/1)
          |> Enum.sort_by(&position/1),
        matched_items:
          Map.merge(merged.matched_items, second, fn _position, indexes, more ->
            Enum.sort(indexes ++ more)
          end),
        decision: MapSet.union(merged.decision, shift.(read.decision_indexes)),
        framework: MapSet.union(merged.framework, shift.(read.framework_indexes)),
        command: MapSet.union(merged.command, shift.(command_indexes(read))),
        index: MapSet.union(merged.index, shift.(List.wrap(read.index_index))),
        activation: MapSet.union(merged.activation, shift.(List.wrap(read.activation_index))),
        replay: MapSet.union(merged.replay, shift.(read.replay_indexes)),
        offset: offset + length(read.items)
    }
  end

  @doc """
  Whether a second read holds a command match the first read did not. Its snapshot is
  later than the first's, so the two no longer describe one state.
  """
  @spec new_command_match?(%Merged{}, %__MODULE__{}, ReadResult.t()) :: boolean()
  def new_command_match?(%Merged{first_commands: known}, %__MODULE__{} = read, result) do
    result.matched_items
    |> positions_matching(command_indexes(read))
    |> Enum.any?(&(not MapSet.member?(known, &1)))
  end

  @doc """
  The events the decision's own items matched, in position order, as `evolve` sees them.
  An event only the framework's items matched is not the decision's business.
  """
  @spec business_events(%Merged{}) :: [RecordedEvent.t()]
  def business_events(%Merged{} = merged) do
    Enum.filter(merged.events, fn event ->
      merged.matched_items
      |> Map.get(position(event), [])
      |> Enum.any?(&MapSet.member?(merged.decision, &1))
    end)
  end

  @doc """
  The matched rows, both reads together, as `max_events` counts them.
  """
  @spec matched_count(%Merged{}) :: non_neg_integer()
  def matched_count(%Merged{events: events}), do: length(events)

  @doc """
  The positions matching a framework item, which is what the one tag fetch reads.
  """
  @spec framework_positions(%Merged{}) :: [non_neg_integer()]
  def framework_positions(%Merged{} = merged) do
    positions_matching(merged.matched_items, merged.framework)
  end

  @doc """
  The positions matching the command item.
  """
  @spec command_positions(%Merged{}) :: [non_neg_integer()]
  def command_positions(%Merged{} = merged) do
    positions_matching(merged.matched_items, merged.command)
  end

  @doc """
  The positions matching the index item, in position order.
  """
  @spec index_fact_positions(%Merged{}) :: [non_neg_integer()]
  def index_fact_positions(%Merged{} = merged) do
    positions_matching(merged.matched_items, merged.index)
  end

  @doc """
  The positions matching the activation item, in position order.
  """
  @spec activation_positions(%Merged{}) :: [non_neg_integer()]
  def activation_positions(%Merged{} = merged) do
    positions_matching(merged.matched_items, merged.activation)
  end

  @doc """
  Whether a position matched a `replay: true` item.
  """
  @spec replay?(%Merged{}, non_neg_integer()) :: boolean()
  def replay?(%Merged{} = merged, position) do
    merged.matched_items
    |> Map.get(position, [])
    |> Enum.any?(&MapSet.member?(merged.replay, &1))
  end

  @doc """
  Whether a stamp is inside the window at `read_at`. Both are the database's clock. The edge
  is outside, a negative age is inside, and a nil stamp is outside.
  """
  @spec within_window?(DateTime.t(), DateTime.t() | nil, pos_integer()) :: boolean()
  def within_window?(_read_at, nil, _window_ms), do: false

  def within_window?(%DateTime{} = read_at, %DateTime{} = stamp, window_ms) do
    DateTime.diff(read_at, stamp, :microsecond) < window_ms * 1_000
  end

  @doc """
  Step 7. What the command matches inside the window mean for this dispatch: `:open` for none,
  and otherwise `:reused`, `:replay` or `:duplicate`. `tags` is the one fetch, where a position
  with no entry carries nothing, so it reads as `:reused`. A scope may be split across the
  matches, as a batch's is.

  A command position always has a stamp entry, since a match only a second read has restarts
  the dispatch, so a miss raises rather than classify on nothing.
  """
  @spec classify(%Merged{}, map(), String.t(), [String.t()], pos_integer()) ::
          :open | :replay | :duplicate | :reused
  def classify(%Merged{} = merged, tags, kind_tag, scope, window_ms) do
    window =
      merged
      |> command_positions()
      |> Enum.filter(fn position ->
        within_window?(merged.read_at, Map.fetch!(merged.command_stamps, position), window_ms)
      end)

    carried = Enum.map(window, &Map.get(tags, &1, []))

    cond do
      window == [] -> :open
      not Enum.all?(carried, &(kind_tag in &1)) -> :reused
      not Enum.all?(scope, fn tag -> Enum.any?(carried, &(tag in &1)) end) -> :reused
      Enum.any?(window, &replay?(merged, &1)) -> :replay
      true -> :duplicate
    end
  end

  defp command_indexes(%__MODULE__{command_index: nil}), do: []
  defp command_indexes(%__MODULE__{command_index: index}), do: [index]

  defp positions_matching(matched_items, indexes) do
    matched_items
    |> Enum.filter(fn {_position, matched} -> Enum.any?(matched, &(&1 in indexes)) end)
    |> Enum.map(fn {position, _matched} -> position end)
    |> Enum.sort()
  end

  defp position(%{event_number: position}), do: position
end
