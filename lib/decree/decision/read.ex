defmodule Decree.Decision.Read do
  @moduledoc false

  # A decision's merged read, with no store in it. A read's items sit in one fixed layout:
  # the decision's own at 0..n-1, then the framework's, the command item first, then
  # read-only narrowing items, which are read and never go in the condition. A second
  # read's indexes are offset by the first read's item count.
  #
  # The struct is one read's layout. `Merged` is what the reads answered, together.

  alias Decree.Event.Append
  alias Decree.EventStore.RecordedEvent
  alias Decree.Store.{Query, ReadResult}

  # `probes` is the condition's probes as of this read: `nil` until they are counted, and for
  # items the store will refuse.
  defstruct [
    :items,
    :own,
    :framework,
    :decision_indexes,
    :framework_indexes,
    :command_index,
    :replay_indexes,
    :probes
  ]

  defmodule Merged do
    @moduledoc false

    # `after`, `read_at` and `command_stamps` are always the first read's: `after` is what
    # the condition protects, and a command match the first read did not have restarts
    # the dispatch instead of being merged. `decision`, `framework`, `command` and `replay`
    # are sets of merged item indexes. `first_commands` is the first read's command matches.
    defstruct [
      :events,
      :after,
      :read_at,
      :command_stamps,
      :matched_items,
      :decision,
      :framework,
      :command,
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

  defp item_tags(item) when is_list(item), do: Keyword.get(item, :tags)
  defp item_tags(item), do: Map.get(item, :tags)

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
    own = Enum.map(parsed, & &1.item)
    n = length(own)

    replay_indexes =
      for {%{replay: true}, index} <- Enum.with_index(parsed), do: index

    %__MODULE__{
      items: own ++ framework_items ++ read_only_items,
      own: own,
      framework: framework_items,
      decision_indexes: Enum.to_list(0..(n - 1)//1),
      framework_indexes: Enum.to_list(n..(n + length(framework_items) - 1)//1),
      command_index: n,
      replay_indexes: replay_indexes
    }
  end

  @doc """
  The items of the condition: every read's own, then the framework's once. A second read
  carries the framework's items too, and read-only items are never a condition's.
  """
  @spec condition_items([%__MODULE__{}]) :: [keyword() | map()]
  def condition_items([%__MODULE__{framework: framework} | _reads] = reads) do
    Enum.flat_map(reads, & &1.own) ++ framework
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
      command: MapSet.new([read.command_index]),
      replay: MapSet.new(read.replay_indexes),
      offset: length(read.items),
      first_commands:
        result.matched_items |> positions_matching([read.command_index]) |> MapSet.new()
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
        command: MapSet.put(merged.command, read.command_index + offset),
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
    |> positions_matching([read.command_index])
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

  defp positions_matching(matched_items, indexes) do
    matched_items
    |> Enum.filter(fn {_position, matched} -> Enum.any?(matched, &(&1 in indexes)) end)
    |> Enum.map(fn {position, _matched} -> position end)
    |> Enum.sort()
  end

  defp position(%{event_number: position}), do: position
end
