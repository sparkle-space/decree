defmodule Decree.Decision.Index do
  @moduledoc """
  What a dispatch's items require of the tag index. "Index facts" in `Decree.Decision` is the
  rule: a dispatch appends only when the facts its own read found enable every family and key
  its items require. Nothing here is kept between dispatches.

  `requirements/3` is for an application's own test of the list it gives
  `Decree.Store.TagIndex.bootstrap/3`: that the list covers every family its rules and
  decisions read. The README's "A minimal application" shows such a test.
  """

  alias Decree.Decision.Read
  alias Decree.Decision.Read.Merged
  alias Decree.Store.{Query, TagIndex}

  @doc false
  # Whether every family and key the reads require is enabled by the facts at or below the
  # first read's `after`. `command?` is whether the dispatch has a command item, and `tags` the
  # reads' tags of the framework items' matches.
  @spec check(boolean(), [%Read{}], %Merged{}, map(), map()) ::
          :ok | {:error, :tag_index_not_enabled}
  def check(command?, reads, %Merged{} = merged, tags, families) do
    # Only the facts at or below `after` are folded, in position order: the condition
    # protects that snapshot, and a fact above it, one a second read found, fails the append,
    # whose retry reads it again. A fact with no state tag left could have been a disable, so
    # the fold answers `:error`.
    with {:ok, required} <-
           required(command?, Enum.flat_map(reads, & &1.own), families),
         {:ok, states} <-
           merged
           |> Read.index_fact_positions()
           |> Enum.filter(&(&1 <= merged.after))
           |> Enum.map(&Map.get(tags, &1, []))
           |> TagIndex.fold(),
         true <- Enum.all?(required, &(Map.get(states, &1) == :enabled)) do
      :ok
    else
      _disabled -> {:error, :tag_index_not_enabled}
    end
  end

  @doc false
  # The activation step: an event the activation item matched at or below `after`. A dispatch
  # with no activation item has nothing to find.
  @spec activated(%Merged{}) :: :ok | {:error, :decision_not_activated}
  def activated(%Merged{} = merged) do
    cond do
      MapSet.size(merged.activation) == 0 -> :ok
      Enum.any?(Read.activation_positions(merged), &(&1 <= merged.after)) -> :ok
      true -> {:error, :decision_not_activated}
    end
  end

  @doc """
  The families and keys a dispatch's items require of the tag index, each once.

  - `command?` is true when the dispatch reads a command item: a `Decree.Decision`, and a
    `Decree.Decision.Rule`, or no rule, on an aggregate that declares a command window
    (`use Decree.Aggregate, command_window: [ms: n]`, see "Command window" in
    `Decree.Aggregate`). It is false for a rule on any other aggregate route, the one dispatch
    with no command item.
  - `items` are the items the rule's or decision's `query/2` answers, and the items of its
    `followup_query/3` when it has one.
  - `families` is the module's `index_families/0`.

  The answer is `{:ok, requirements}`, in the grammar of `t:Decree.Store.TagIndex.requirement/0`:
  `"command"` first when `command?` is true, then each family in the order the items' tags first
  name it. A keyless family is its bare name, and a digest family `{family, fingerprint}`. The
  tag index's own kinds require nothing. The answer is `:error` for a digest tag whose
  fingerprint does not read, or a tag that is not a binary.

      items = [[tags: ["handle:ada"]]]
      Decree.Decision.Index.requirements(false, items, %{})
      #=> {:ok, ["entity"]}

  The answer drops into the list `Decree.Store.TagIndex.bootstrap/3` takes. With `families`
  that list, `required -- families == []` checks a list that writes keyless families bare, and
  `Decree.Store.TagIndex.missing/2` checks any list once it is enabled into states. The README
  shows an application test that does this.
  """
  @spec requirements(boolean(), [Decree.Decision.item()], %{
          optional(String.t()) => :digest | :plain
        }) :: {:ok, [TagIndex.requirement()]} | :error
  def requirements(command?, items, families) do
    with {:ok, required} <- required(command?, items, families) do
      {:ok, Enum.map(required, &requirement/1)}
    end
  end

  defp requirement({family, nil}), do: family
  defp requirement({_family, _key} = family_key), do: family_key

  defp required(command?, items, families) do
    initial = if command?, do: [{"command", nil}], else: []

    items
    |> Enum.flat_map(&Read.item_tags/1)
    |> Enum.reduce_while({:ok, initial}, fn tag, {:ok, required} ->
      case family_key(tag, families) do
        :none -> {:cont, {:ok, required}}
        {:ok, family_key} -> {:cont, {:ok, [family_key | required]}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, required} -> {:ok, required |> Enum.reverse() |> Enum.uniq()}
      :error -> :error
    end
  end

  defp family_key(tag, families) when is_binary(tag) do
    kind = Query.tag_kind(tag)

    cond do
      kind in TagIndex.reserved_kinds() -> :none
      Map.get(families, kind) == :digest -> digest_requirement(kind, tag)
      kind in TagIndex.named_families() or Map.has_key?(families, kind) -> {:ok, {kind, nil}}
      true -> {:ok, {"entity", nil}}
    end
  end

  defp family_key(_tag, _families), do: :error

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
end
