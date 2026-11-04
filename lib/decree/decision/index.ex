defmodule Decree.Decision.Index do
  @moduledoc false

  # Step 3 with no store in it: what a dispatch's items require of the tag index, and whether
  # the facts its own read found enable all of it. "Index facts" in `Decree.Decision` is the
  # rule. Nothing here is kept between dispatches: the facts are items of the dispatch's own
  # read, and the condition holds them.

  alias Decree.Decision.Read
  alias Decree.Decision.Read.Merged
  alias Decree.Store.{Query, TagIndex}

  @doc """
  Whether every family and key the reads require is enabled by the facts at or below the
  first read's `after`. `command?` is whether the dispatch has a command item, and `tags` the
  reads' tags of the framework items' matches.
  """
  @spec check(boolean(), [%Read{}], %Merged{}, map(), map()) ::
          :ok | {:error, :tag_index_not_enabled}
  def check(command?, reads, %Merged{} = merged, tags, families) do
    with {:ok, required} <- requirements(command?, Enum.flat_map(reads, & &1.own), families),
         {:ok, states} <- fold(merged, tags),
         true <- Enum.all?(required, &(Map.get(states, &1) == :enabled)) do
      :ok
    else
      _disabled -> {:error, :tag_index_not_enabled}
    end
  end

  @doc """
  The activation step: an event the activation item matched at or below `after`. A dispatch
  with no activation item has nothing to find.
  """
  @spec activated(%Merged{}) :: :ok | {:error, :decision_not_activated}
  def activated(%Merged{} = merged) do
    cond do
      MapSet.size(merged.activation) == 0 -> :ok
      Enum.any?(Read.activation_positions(merged), &(&1 <= merged.after)) -> :ok
      true -> {:error, :decision_not_activated}
    end
  end

  @doc """
  The families and keys the items require, `command` first when there is a command item, or
  `:error` for a digest tag whose fingerprint does not read.
  """
  @spec requirements(boolean(), [keyword() | map()], map()) ::
          {:ok, MapSet.t({String.t(), String.t() | nil})} | :error
  def requirements(command?, items, families) do
    initial = if command?, do: MapSet.new([{"command", nil}]), else: MapSet.new()

    items
    |> Enum.flat_map(&Read.item_tags/1)
    |> Enum.reduce_while({:ok, initial}, fn tag, {:ok, required} ->
      case requirement(tag, families) do
        :none -> {:cont, {:ok, required}}
        {:ok, family_key} -> {:cont, {:ok, MapSet.put(required, family_key)}}
        :error -> {:halt, :error}
      end
    end)
  end

  defp requirement(tag, families) when is_binary(tag) do
    kind = Query.tag_kind(tag)

    cond do
      kind in TagIndex.reserved_kinds() -> :none
      Map.get(families, kind) == :digest -> digest_requirement(kind, tag)
      kind in TagIndex.named_families() or Map.has_key?(families, kind) -> {:ok, {kind, nil}}
      true -> {:ok, {"entity", nil}}
    end
  end

  defp requirement(_tag, _families), do: :error

  # A fingerprint that does not read is no key at all, so it never stands for `-`.
  defp digest_requirement(kind, tag) do
    size = byte_size(kind)

    with <<^kind::binary-size(^size), ":", value::binary>> <- tag,
         {:ok, fingerprint} <- TagIndex.digest_key(value) do
      {:ok, {kind, fingerprint}}
    else
      _unreadable -> :error
    end
  end

  # The last state per family and key, folding the facts in position order. Only the facts at
  # or below `after` are folded: the condition protects that snapshot, and a fact above it, one
  # a second read found, fails the append, whose retry reads it again. A fact with no state
  # tag left could have been a disable, so it answers `:error`.
  defp fold(%Merged{} = merged, tags) do
    merged
    |> Read.index_fact_positions()
    |> Enum.filter(&(&1 <= merged.after))
    |> Enum.reduce_while({:ok, %{}}, fn position, {:ok, states} ->
      case fact_states(Map.get(tags, position, [])) do
        {:ok, fact} -> {:cont, {:ok, Map.merge(states, fact)}}
        :error -> {:halt, :error}
      end
    end)
  end

  # One fact's states. A fact naming one family and key both ways reads as disabled, since the
  # read keeps no order within an event.
  defp fact_states(tags) do
    parsed =
      for tag <- tags, Query.tag_kind(tag) == "tag_index_state", do: TagIndex.parse_state(tag)

    if parsed == [] or :error in parsed do
      :error
    else
      {:ok,
       Enum.reduce(parsed, %{}, fn {:ok, {family, key, state}}, states ->
         Map.update(states, {family, key}, state, &lower(&1, state))
       end)}
    end
  end

  defp lower(:enabled, :enabled), do: :enabled
  defp lower(_state, _other), do: :disabled
end
