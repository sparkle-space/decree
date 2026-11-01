defmodule Decree.Event.Append do
  @moduledoc """
  The one function that runs just before the store append.
  """

  alias Decree.Aggregates.ExecutionContext
  alias Decree.Event.{Mapper, RequiredTags, TagContext, Tags}
  alias Decree.EventStore.EventData
  alias Decree.PersonalFields
  alias Decree.PersonalFields.RedactedError
  alias Decree.UUID

  @kind_prefix "command_kind:"

  @doc """
  Answers a copy of `event_data` ready for the store.

  Every framework append path reaches this one function, through `prepare_events/4`, with the
  application's name and the stream's. Each event takes these steps, on copies:

    1. **Lookup.** One `Decree.PersonalFields.KeySource` lookup per subject, for each declared
       value that is not nil.

    2. **Resolve.** An unknown key fails the whole append. An erased one removes the value: a
       field becomes nil and a metadata key goes. A missing subject, a value that is already an
       envelope and metadata the application fences also stop here.

    3. **Tags.** `Decree.Event.Tags.tags/2`, followed by the framework's. An implementation
       sees the plaintext that will be stored, so an erased subject gets no tag derived from
       its value, and no tag holds ciphertext. Then the kinds the event requires, from
       `Decree.Event.RequiredTags.required/2`, on the same copy and the same tag context, so an
       erased value reads as absent. They go in `required_tag_kinds`, for the store to refuse.
       An event that requires kinds, on a command with no `command:` tag of its own, stops here
       with `{:error, :missing_required_tag}`, whatever tags the implementation answered.

    4. **Encrypt.** Declared fields and metadata entries become envelopes.

  An event that declares nothing takes step 3 alone, and comes out as it went in, with tags.

  It writes nothing, and `ExecutionContext.metadata` is untouched, so a `:wrong_expected_version`
  retry derives afresh from plaintext. It runs once per attempt. A lookup aside, it is
  deterministic: the same event and context give the same tags.

  The first `{:error, reason}` stops and is returned. A raise from an implementation is not
  rescued here: the aggregate or the decision answers it, having written nothing. On an event
  with a declaration anything leaving an implementation can carry the value it failed on, so
  its error reason, exit and throw are reduced by `Decree.PersonalFields.RedactedError.reduce/1`,
  and a raise becomes a `Decree.PersonalFields.RedactedError`. An event's
  `__required_tag_kinds__/2` is treated the same way, because it sees the same plaintext.

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
  @spec prepare(
          Decree.Application.t(),
          String.t(),
          [EventData.t()],
          ExecutionContext.t()
        ) :: {:ok, [EventData.t()]} | {:error, term()}
  def prepare(application, stream_uuid, event_data, %ExecutionContext{} = context)
      when is_binary(stream_uuid) and is_list(event_data) do
    framework_tags = framework_tags(context)
    fence = PersonalFields.metadata_fence(application, event_data)

    event_data
    |> Enum.reduce_while([], fn %EventData{} = data, prepared ->
      with {:ok, resolved, plan} <- PersonalFields.resolve_append(application, data, fence),
           {:ok, tags, required_tag_kinds} <- event_tags(resolved, plan, context),
           {:ok, %EventData{} = protected} <-
             PersonalFields.encrypt_append(resolved, plan, stream_uuid) do
        ready = %EventData{
          protected
          | tags: tags ++ framework_tags,
            required_tag_kinds: required_tag_kinds
        }

        {:cont, [ready | prepared]}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      prepared -> {:ok, Enum.reverse(prepared)}
    end
  end

  @doc """
  Maps `events` to event data for the dispatch `context` describes, and answers them through
  `prepare/4`. Every path that appends a command's events calls this one function, per stream.
  No events prepare to `{:ok, []}`.
  """
  @spec prepare_events(Decree.Application.t(), String.t(), [struct()], ExecutionContext.t()) ::
          {:ok, [EventData.t()]} | {:error, term()}
  def prepare_events(_application, _stream_uuid, [], %ExecutionContext{}), do: {:ok, []}

  def prepare_events(application, stream_uuid, events, %ExecutionContext{} = context) do
    %ExecutionContext{
      causation_id: causation_id,
      correlation_id: correlation_id,
      metadata: metadata
    } = context

    event_data =
      Mapper.map_to_event_data(events,
        causation_id: causation_id,
        correlation_id: correlation_id,
        metadata: metadata
      )

    prepare(application, stream_uuid, event_data, context)
  end

  # The tag every event a command appends carries, for a read that has to name it: being
  # the code that writes the tag, it cannot differ from what the store holds.
  @doc false
  @spec command_tag(struct()) :: String.t() | nil
  def command_tag(command), do: key_tag("command", Map.get(command, :command_id))

  # The same for the command's kind, always present.
  @doc false
  @spec kind_tag(struct()) :: String.t()
  def kind_tag(command), do: @kind_prefix <> inspect(command.__struct__)

  # Whether a tag is a kind tag, for a read that has to tell one from any other tag.
  @doc false
  @spec kind_tag?(term()) :: boolean()
  def kind_tag?(tag), do: is_binary(tag) and String.starts_with?(tag, @kind_prefix)

  # The same for the stream's entity tag: nil when the context names no kind or the identity
  # is not canonical, as it is on the append.
  @doc false
  @spec stream_tag(ExecutionContext.t()) :: String.t() | nil
  def stream_tag(%ExecutionContext{aggregate_identity: nil}), do: nil

  def stream_tag(%ExecutionContext{aggregate_identity: identity} = context) do
    entity_tag(kind(context), identity)
  end

  # `plan` is nil for an event with no declaration.
  defp event_tags(%EventData{} = data, nil, %ExecutionContext{} = context) do
    tags(data, context)
  end

  # The event holds its plaintext here, and anything leaving this function can hold it too.
  defp event_tags(%EventData{} = data, _plan, %ExecutionContext{} = context) do
    case tags(data, context) do
      {:error, reason} -> {:error, RedactedError.reduce(reason)}
      {:ok, _tags, _required_tag_kinds} = tagged -> tagged
    end
  rescue
    error ->
      reraise RedactedError.redact(error), RedactedError.scrub_stacktrace(__STACKTRACE__)
  catch
    :exit, reason -> exit(RedactedError.reduce(reason))
    :throw, value -> throw(RedactedError.reduce(value))
  end

  # The tag context's metadata is this event's copy, after the drop and before the encrypt.
  defp tags(%EventData{data: event, metadata: metadata}, %ExecutionContext{command: command}) do
    tag_context = %TagContext{metadata: metadata, command: command}

    case Tags.tags(event, tag_context) do
      {:ok, tags} when is_list(tags) ->
        with {:ok, required_tag_kinds} <- RequiredTags.required(event, tag_context),
             :ok <- has_command_tag(required_tag_kinds, command) do
          {:ok, tags, required_tag_kinds}
        end

      {:error, _reason} = error ->
        error
    end
  end

  # The store takes any tag of a kind, so an implementation answering a `command` tag would
  # satisfy it. The kind is the framework's: only the tag `command_tag/1` writes counts, and
  # the check reads that and never `tags`. An event with no list has not opted in.
  defp has_command_tag([], _command), do: :ok

  defp has_command_tag(_required_tag_kinds, command) do
    if command_tag(command), do: :ok, else: {:error, :missing_required_tag}
  end

  defp framework_tags(%ExecutionContext{command: command} = context) do
    Enum.reject(
      [
        command_tag(command),
        kind_tag(command),
        key_tag("correlation", Map.get(command, :correlation_id)),
        stream_tag(context)
      ],
      &is_nil/1
    )
  end

  defp key_tag(kind, key) when is_binary(key) and key != "", do: kind <> ":" <> key
  defp key_tag(_kind, _key), do: nil

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
