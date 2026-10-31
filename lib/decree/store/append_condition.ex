defmodule Decree.Store.AppendCondition do
  @moduledoc """
  The `:condition` of a conditional `c:Decree.Store.append_to_stream/4`.

  A condition is a keyword list:

    - `items` - a `Decree.Store.Query` list of items.
    - `after` - a position in `$all`, the `after` of a complete
      `c:Decree.Store.read_query/2`. The append fails if any event past it matches
      `items`. Mandatory: an absent `after` must never become a plain append.
    - `target` - optional, any term. It names what the condition protects, for a store
      that declares it in `conditional_appends`, and is compared and nothing else.

  See "Conditional appends" in `Decree.Store`.
  """

  alias Decree.Store.Query

  # The function bounds its probes with `p_after + 1`, which must fit a bigint.
  @max_after 9_223_372_036_854_775_806

  @type t :: %__MODULE__{
          query: Query.t(),
          after: non_neg_integer(),
          target: term()
        }

  defstruct [:query, :after, :target]

  @doc """
  Compiles a condition.

  Answers `{:error, :invalid_condition}` for anything but a keyword list of exactly the
  keys above with a usable `after`, and passes on `{:error, :invalid_query}` and
  `{:error, :tag_too_long}` from `Decree.Store.Query.compile/1`. A refusal sends nothing
  to the database, so a caller's transaction stays alive.
  """
  @spec compile(condition :: term()) ::
          {:ok, t()}
          | {:error, :invalid_condition}
          | {:error, :invalid_query}
          | {:error, :tag_too_long}
  def compile(condition) do
    with {:ok, items, position, target} <- parse(condition),
         {:ok, query} <- Query.compile(items) do
      {:ok, %__MODULE__{query: query, after: position, target: target}}
    end
  end

  defp parse(condition) do
    if Keyword.keyword?(condition) and exact_keys?(condition) do
      parse_after(condition[:items], condition[:after], condition[:target])
    else
      {:error, :invalid_condition}
    end
  end

  # A misspelt or repeated key would silently change what is protected, so the keys must
  # be exact.
  defp exact_keys?(condition) do
    keys = condition |> Keyword.keys() |> Enum.sort()

    keys in [[:after, :items], [:after, :items, :target]]
  end

  defp parse_after(items, position, target)
       when is_integer(position) and position >= 0 and position <= @max_after do
    {:ok, items, position, target}
  end

  defp parse_after(_items, _position, _target), do: {:error, :invalid_condition}
end
