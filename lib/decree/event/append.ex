defmodule Decree.Event.Append do
  @moduledoc """
  The one function that runs just before the store append.
  """

  alias Decree.Aggregates.ExecutionContext
  alias Decree.Event.{TagContext, Tags}
  alias Decree.EventStore.EventData
  alias Decree.UUID

  @doc """
  Answers a copy of `event_data` ready for the store.

  Every framework append path calls this one function. Today it does one thing: it sets
  each event's `tags`, from `Decree.Event.Tags.tags/2` followed by the framework's. Key
  lookup, drop and encrypt are later steps of this function, around the tags, which is why
  it is not named after any one of them.

  It is pure. It works on copies and `ExecutionContext.metadata` is untouched, so a
  `:wrong_expected_version` retry derives afresh from plaintext. It runs once per attempt,
  and it is deterministic: the same event and context give the same tags.

  The first `{:error, reason}` from an implementation stops and is returned as it stands.
  A raise from one is not rescued here: the aggregate answers it, with its state unchanged.

  ## Framework tags

    - `command:<command_id>`, from the command's `command_id` field when that is a
      non-empty binary. There is no fallback, default or derived id: a command with none
      gets no `command:` tag.

    - `command_kind:<module>`, the command's module in `inspect/1` form. Always present.

    - `correlation:<key>`, from the command's `correlation_id` field when that is a
      non-empty binary. `ExecutionContext.correlation_id` is a different value, by default
      a fresh UUID per dispatch, and is never read.

    - `<kind>:<id>`, the stream's entity tag, when `ExecutionContext.aggregate_identity`,
      the identity the stream was routed by, is a lowercase canonical UUID
      (`Decree.UUID.canonical?/1`). It is never read from the command again, and never the
      prefixed stream name. See `Decree.Commands.Router.dispatch/2` for its kind.

  The store removes a repeat, so a domain tag equal to a framework tag is stored once.
  """
  @spec prepare([EventData.t()], ExecutionContext.t()) ::
          {:ok, [EventData.t()]} | {:error, term()}
  def prepare(event_data, %ExecutionContext{} = context) when is_list(event_data) do
    tag_context = %TagContext{metadata: context.metadata, command: context.command}
    framework_tags = framework_tags(context)

    event_data
    |> Enum.reduce_while([], fn %EventData{data: event} = data, prepared ->
      case Tags.tags(event, tag_context) do
        {:ok, tags} when is_list(tags) ->
          {:cont, [%EventData{data | tags: tags ++ framework_tags} | prepared]}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      prepared -> {:ok, Enum.reverse(prepared)}
    end
  end

  defp framework_tags(%ExecutionContext{command: command} = context) do
    Enum.reject(
      [
        key_tag("command", Map.get(command, :command_id)),
        "command_kind:" <> inspect(command.__struct__),
        key_tag("correlation", Map.get(command, :correlation_id)),
        stream_tag(context)
      ],
      &is_nil/1
    )
  end

  defp key_tag(kind, key) when is_binary(key) and key != "", do: kind <> ":" <> key
  defp key_tag(_kind, _key), do: nil

  defp stream_tag(%ExecutionContext{aggregate_identity: nil}), do: nil

  defp stream_tag(%ExecutionContext{aggregate_identity: identity} = context) do
    entity_tag(kind(context), identity)
  end

  defp kind(%ExecutionContext{stream_tag: kind}) when not is_nil(kind), do: kind
  defp kind(%ExecutionContext{identity: nil}), do: nil
  defp kind(%ExecutionContext{identity: field}) when is_atom(field), do: stem(field)
  # A function has no field name to take a kind from.
  defp kind(%ExecutionContext{}), do: nil

  defp stem(field), do: field |> Atom.to_string() |> String.replace_suffix("_id", "")

  defp entity_tag(kind, _identity) when kind in [nil, ""], do: nil

  defp entity_tag(kind, identity) do
    if UUID.canonical?(identity), do: "#{kind}:#{identity}"
  end
end
