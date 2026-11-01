defmodule Decree.Event.RequiredTags do
  @moduledoc """
  The tag kinds an event may not be appended without.

  A condition finds an event by its tags, so an event that should carry a tag and does not
  is an event the condition misses, and nothing fails. An event opts in to having that
  refused, in its own module, by exporting three functions:

      defmodule AddressChanged do
        @derive Jason.Encoder
        defstruct [:user_id, :address]

        def __required_tag_kinds__(%AddressChanged{address: nil}, _tag_context), do: []
        def __required_tag_kinds__(%AddressChanged{}, _tag_context), do: ["address"]

        def __conditional_tag_kinds__, do: ["address"]

        def __tag_kinds__, do: ["address"]
      end

    - `__required_tag_kinds__(event, tag_context)` answers the kinds this event must carry.
      It is called where `Decree.Event.Tags.tags/2` is, with the same plaintext and the same
      `Decree.Event.TagContext`, so an erased subject's dropped value reads as absent, and
      fails as `tags/2` fails, redactions included. Answering `{:error, reason}` fails the
      append.
    - `__conditional_tag_kinds__()` lists the kinds an event of this module can lack. A
      condition naming one requires nothing of itself: the tag is required when
      `__required_tag_kinds__/2` answers its kind, and only then. So a tag is present if and
      only if what it comes from is.
    - `__tag_kinds__()` lists every kind the module's `Decree.Event.Tags` implementation can
      produce. Nothing reads it at runtime. `Decree.Decision.AppenderMap` does.

  An event is opted in by exporting `__required_tag_kinds__/2`, and its list is `always/0`
  followed by its answer. So a command with no usable `command_id` cannot append one, on any
  route. The stream's entity tag is not among them: a stream whose id is not a UUID gets
  none. An event without the export has an empty list, and the store checks nothing for it.

  The store refuses, before any statement: `{:error, :missing_required_tag}` for an event
  lacking a kind on its list, and `{:error, :invalid_required_tag_kinds}` for a list that is
  not kinds. A kind is carried by a tag equal to it, or by `<kind>:<value>` with a value.

  ## On a decision

  A decision's own append requires more, of every event it appends, opted in or not: the
  kinds of the tags on the condition's items naming the event's type, less the module's
  `__conditional_tag_kinds__/0`. That takes kinds from what the items ask and from nothing
  else, so a conditional kind `__required_tag_kinds__/2` answers stays required. See step 8 of
  `Decree.Decision`.

  That holds on a decision's own append and nowhere else. A plain aggregate appending a type
  a decision queries has no condition to take kinds from, so it gets only what the event
  itself answers. `Decree.Decision.AppenderMap` is what flags that aggregate.
  """

  alias Decree.Event.TagContext

  @always ["command", "command_kind"]
  @framework ["command", "command_kind", "correlation"]

  @doc """
  The kinds every opted-in event requires.
  """
  @spec always() :: [String.t(), ...]
  def always, do: @always

  @doc """
  The kinds this event requires: `always/0` and its answer, or none for an event without the
  export. What the answer holds is the store's to check.
  """
  @spec required(struct(), TagContext.t()) :: {:ok, list()} | {:error, term()}
  def required(%module{} = event, %TagContext{} = context) do
    if exports?(module, :__required_tag_kinds__, 2) do
      case module.__required_tag_kinds__(event, context) do
        kinds when is_list(kinds) -> {:ok, @always ++ kinds}
        {:error, _reason} = error -> error
      end
    else
      {:ok, []}
    end
  end

  @doc """
  The kinds an event of this module can lack. None for a module without the export.
  """
  @spec conditional_kinds(module()) :: [String.t()]
  def conditional_kinds(module) do
    if exports?(module, :__conditional_tag_kinds__, 0) do
      module.__conditional_tag_kinds__()
    else
      []
    end
  end

  @doc """
  The kinds an event of this module can carry: its `__tag_kinds__/0` and the framework's. The
  framework's alone for a module without the export.
  """
  @spec producible_kinds(module()) :: [String.t()]
  def producible_kinds(module) do
    if exports?(module, :__tag_kinds__, 0) do
      @framework ++ module.__tag_kinds__()
    else
      @framework
    end
  end

  # A struct literal does not load its module, so `function_exported?/3` alone would read an
  # opted-in event as one that has not.
  defp exports?(module, function, arity) do
    Code.ensure_loaded?(module) and function_exported?(module, function, arity)
  end
end
