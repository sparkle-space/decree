defmodule Decree.Decision.Dispatch do
  @moduledoc false

  # One dispatch of a decision, in the dispatcher's task. The steps are those of
  # `Decree.Decision`'s moduledoc, and each is named below by its number there.

  alias Decree.Aggregates.ExecutionContext
  alias Decree.Commands.Router
  alias Decree.Decision.Read
  alias Decree.Decision.Read.Merged
  alias Decree.Event.{Append, TagContext}
  alias Decree.EventStore
  alias Decree.PersonalFields
  alias Decree.PersonalFields.RedactedError
  alias Decree.Store.{AppendResult, ReadResult}
  alias Decree.Telemetry
  alias Decree.UUID

  require Logger

  @execute [:decree, :decision, :execute]
  @read [:decree, :decision, :read]
  @retry [:decree, :decision, :retry]
  @refused [:decree, :decision, :refused]

  defstruct [
    :application,
    :decision,
    :stream_uuid,
    :identity_prefix,
    :context,
    :tag_context,
    :command_tag,
    :kind_tag,
    :stream_tag,
    :scope,
    :bounds,
    :fold,
    attempt: 1
  ]

  @doc """
  Runs the dispatch. Answers `{:decided, outcome, reply}`, `reply` being a
  `ExecutionContext.format_reply/3` tuple, or `{:error, reason}`, or
  `{:error, error, stacktrace}` for a raise. An exit or a throw does not leave: this runs in a
  task with the context in its arguments, and a crash report prints those.
  """
  def execute(application, decision, stream_uuid, identity_prefix, %ExecutionContext{} = context) do
    metadata = %{
      application: application,
      decision: decision,
      stream_uuid: stream_uuid,
      execution_context: context
    }

    start_time = Telemetry.start(@execute, metadata)

    dispatch = %__MODULE__{
      application: application,
      decision: decision,
      stream_uuid: stream_uuid,
      identity_prefix: identity_prefix,
      context: context
    }

    {result, attempts} = run(dispatch)

    telemetry_stop(result, start_time, Map.put(metadata, :attempts, attempts))
    answer(result, context)
  end

  @doc """
  What a `decide/2` answer means, with no store and no route in it.
  """
  def shape(:duplicate), do: {:error, :duplicate_command}
  def shape({:error, _reason} = error), do: error
  def shape(none) when none in [[], :ok, nil, {:streams, []}], do: no_events()
  def shape({:streams, [_ | _] = members}), do: members(members)

  def shape(events) when is_struct(events) or is_list(events) do
    with {:ok, events} <- events(events), do: {:ok, {:route, events}}
  end

  def shape(_invalid), do: {:error, :invalid_decision_return}

  @doc """
  The events of a `shape/1`, in the order they append.
  """
  def events_of({:route, events}), do: events

  def events_of({:streams, members}) do
    Enum.flat_map(members, fn {_kind, _identity, events} -> events end)
  end

  defp no_events, do: {:error, :decision_returned_no_events}

  defp events(events) do
    case List.wrap(events) do
      [] ->
        no_events()

      [_event | _] = events ->
        if not List.improper?(events) and Enum.all?(events, &is_struct/1) do
          {:ok, events}
        else
          {:error, :invalid_decision_return}
        end
    end
  end

  defp members(members) do
    members
    |> Enum.reduce_while([], fn member, members ->
      case member(member) do
        {:ok, member} -> {:cont, [member | members]}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      members -> {:ok, {:streams, Enum.reverse(members)}}
    end
  end

  defp member({kind, identity, events}) do
    cond do
      not Router.stream_tag_kind?(kind) ->
        {:error, :invalid_decision_stream}

      not UUID.canonical?(identity) ->
        {:error, :invalid_decision_identity}

      true ->
        with {:ok, events} <- events(events), do: {:ok, {kind, identity, events}}
    end
  end

  defp member(_member), do: {:error, :invalid_decision_stream}

  # Everything a raise can come from runs under this, as in an aggregate's `execute_command`.
  defp run(%__MODULE__{decision: decision} = dispatch) do
    with {:ok, dispatch} <- ready(dispatch) do
      attempt(dispatch)
    else
      {:error, _reason} = error -> {error, 0}
    end
  rescue
    error ->
      {error, stacktrace} = PersonalFields.reported_exception(decision, error, __STACKTRACE__)
      Logger.error(Exception.format(:error, error, stacktrace))

      {{:error, error, stacktrace}, nil}
  catch
    kind, value ->
      reduced = RedactedError.reduce(value)
      Logger.error(fn -> "#{inspect(decision)} left by #{kind}: #{inspect(reduced)}" end)

      {{:left, kind, reduced, RedactedError.scrub_stacktrace(__STACKTRACE__)}, nil}
  end

  # Step 0.
  defp ready(%__MODULE__{context: context} = dispatch) do
    %ExecutionContext{command: command, aggregate_identity: identity} = context

    case {Append.command_tag(command), UUID.canonical?(identity), Append.stream_tag(context)} do
      {nil, _canonical?, _stream_tag} ->
        {:error, :missing_command_id}

      {_command_tag, false, _stream_tag} ->
        {:error, :invalid_decision_identity}

      {_command_tag, true, nil} ->
        {:error, :invalid_decision_identity}

      {command_tag, true, stream_tag} ->
        with {:ok, scope_tags} <- scope_tags(dispatch) do
          {:ok,
           %__MODULE__{
             dispatch
             | tag_context: %TagContext{metadata: context.metadata, command: command},
               command_tag: command_tag,
               kind_tag: Append.kind_tag(command),
               stream_tag: stream_tag,
               scope: Enum.uniq([stream_tag | scope_tags]),
               bounds: dispatch.decision.__decision__(),
               fold: PersonalFields.fold_events(dispatch.decision)
           }}
        end
    end
  end

  defp scope_tags(%__MODULE__{decision: decision, context: context}) do
    scope_tags = decision.scope_tags(context.command)

    if is_list(scope_tags) and not List.improper?(scope_tags) and
         Enum.all?(scope_tags, &(is_binary(&1) and &1 != "")) do
      {:ok, scope_tags}
    else
      {:error, :invalid_scope_tags}
    end
  end

  defp attempt(%__MODULE__{attempt: attempt} = dispatch) do
    case cycle(dispatch) do
      {:retry, reason} -> retry(dispatch, reason)
      result -> {result, attempt}
    end
  end

  # From step 1, on what the store holds now.
  defp retry(%__MODULE__{context: context, attempt: attempt} = dispatch, reason) do
    case ExecutionContext.retry(context) do
      {:ok, context} ->
        :telemetry.execute(@retry, %{attempt: attempt}, %{
          application: dispatch.application,
          decision: dispatch.decision,
          reason: reason
        })

        attempt(%__MODULE__{dispatch | context: context, attempt: attempt + 1})

      {:error, :too_many_attempts} = error ->
        {error, attempt}
    end
  end

  # Steps 1 to 8, once. Key lookups are remembered for this attempt and no longer, so a retry
  # sees a key erased since.
  defp cycle(%__MODULE__{} = dispatch) do
    started = System.monotonic_time()

    with {:ok, read} <- first_read(dispatch),
         {:ok, result} <- read_query(dispatch, read),
         merged = Read.first(read, result),
         :ok <- within_events(dispatch, merged, [read], started),
         {:ok, merged, reads, answers} <- second_read(dispatch, merged, read, started),
         {:ok, tags} <- fetch_tags(dispatch, merged),
         :ok <- check_tag_index(tags),
         read_time = System.monotonic_time() - started,
         {:ok, state} <- fold(dispatch, merged, reads, read_time, answers),
         :ok <- admit(dispatch, state),
         {:ok, mode} <- classify(dispatch, merged, tags) do
      decide(mode, dispatch, state, merged, reads)
    end
  end

  # Step 1, up to the read.
  defp first_read(%__MODULE__{decision: decision, context: context} = dispatch) do
    case decision.query(context.command, dispatch.tag_context) do
      {:error, _reason} = error ->
        error

      items ->
        with {:ok, parsed} <- Read.parse(own_items(dispatch, items)),
             :ok <- no_replay_of_fact(dispatch, parsed) do
          bounded(dispatch, [], Read.layout(parsed, framework_items(dispatch)))
        end
    end
  end

  # A create's fact item is the decision's own, in the first read only. Anything but a list
  # makes an improper one here, which `Read.parse/1` refuses as it refuses anything else.
  defp own_items(%__MODULE__{bounds: %{create: false}}, items), do: items

  defp own_items(%__MODULE__{bounds: %{create: types}, stream_tag: stream_tag}, items) do
    [[types: types, tags: [stream_tag]] | items]
  end

  defp no_replay_of_fact(%__MODULE__{bounds: %{create: false}}, _parsed), do: :ok

  defp no_replay_of_fact(%__MODULE__{stream_tag: stream_tag}, parsed) do
    if Read.replays_tag?(parsed, stream_tag), do: {:error, :invalid_query}, else: :ok
  end

  # Step 2.
  defp second_read(%__MODULE__{bounds: %{followup_query?: false}}, merged, read, _started) do
    {:ok, merged, [read], %{}}
  end

  defp second_read(%__MODULE__{} = dispatch, %Merged{} = merged, first, started) do
    %__MODULE__{decision: decision, context: context, tag_context: tag_context} = dispatch

    with {:ok, business_events, answers} <- readable(dispatch, merged, %{}) do
      case decision.followup_query(context.command, tag_context, business_events) do
        [] ->
          {:ok, merged, [first], answers}

        {:error, _reason} = error ->
          callback_answer(dispatch, error)

        items ->
          with {:ok, parsed} <- Read.parse(items),
               :ok <- no_replay_of_fact(dispatch, parsed),
               read = Read.layout(parsed, framework_items(dispatch)),
               {:ok, read} <- bounded(dispatch, [first], read) do
            reads = [first, read]

            with {:ok, result} <- read_query(dispatch, read),
                 :ok <- no_new_command_match(merged, read, result) do
              merged = Read.merge(merged, read, result)

              with :ok <- within_events(dispatch, merged, reads, started) do
                {:ok, merged, reads, answers}
              end
            end
          end
      end
    end
  end

  defp no_new_command_match(%Merged{} = merged, %Read{} = read, %ReadResult{} = result) do
    if Read.new_command_match?(merged, read, result) do
      {:retry, :new_command_match}
    else
      :ok
    end
  end

  defp framework_items(%__MODULE__{command_tag: command_tag}), do: [[tags: [command_tag]]]

  defp read_query(%__MODULE__{} = dispatch, %Read{items: items}) do
    %__MODULE__{application: application, bounds: bounds, command_tag: command_tag} = dispatch

    EventStore.read_query(application, items,
      count: bounds.max_events + 1,
      command_tag: command_tag
    )
  end

  # The one tag fetch, for every position a framework item matched. No positions send no
  # statement.
  defp fetch_tags(%__MODULE__{application: application}, %Merged{} = merged) do
    EventStore.read_tags(application, Read.framework_positions(merged))
  end

  # Step 3: the place a tag index check reads the fetched tags.
  defp check_tag_index(_tags), do: :ok

  # Steps 4 and 5.
  defp fold(%__MODULE__{} = dispatch, %Merged{} = merged, reads, read_time, answers) do
    %__MODULE__{decision: decision} = dispatch
    started = System.monotonic_time()

    with {:ok, business_events, _answers} <- readable(dispatch, merged, answers) do
      state = Enum.reduce(business_events, decision.initial_state(), &decision.evolve(&2, &1))

      telemetry_read(dispatch, merged, reads, read_time, System.monotonic_time() - started)

      {:ok, state}
    end
  end

  # The events the decision's own items matched, as a callback may see them. `fold` is nil for
  # a decision that lists none.
  defp readable(%__MODULE__{fold: nil}, %Merged{} = merged, answers) do
    {:ok, merged |> Read.business_events() |> Enum.map(& &1.data), answers}
  end

  defp readable(%__MODULE__{application: application, fold: fold}, %Merged{} = merged, answers) do
    merged
    |> Read.business_events()
    |> Enum.reduce_while({[], answers}, fn event, {readable, answers} ->
      case PersonalFields.readable_event(application, event, fold, answers) do
        {:ok, data, answers} -> {:cont, {[data | readable], answers}}
        {:error, :key_unknown} = error -> {:halt, error}
      end
    end)
    |> case do
      {:error, :key_unknown} = error -> error
      {readable, answers} -> {:ok, Enum.reverse(readable), answers}
    end
  end

  # Step 6.
  defp admit(%__MODULE__{decision: decision, context: context} = dispatch, state) do
    case decision.admit(state, context.command) do
      :ok -> :ok
      {:error, _reason} = error -> callback_answer(dispatch, error)
    end
  end

  # An answer from a callback that saw decrypted events or state: its reason is returned to the
  # caller and goes to `after_failure` and telemetry, so an adopted decision's is reduced, as an
  # exit's is. In the steps, not in `shape/1`: `Decree.Decision.Assertions` shares that, with no
  # logs or telemetry to keep a value from.
  defp callback_answer(%__MODULE__{fold: nil}, answer), do: answer

  defp callback_answer(%__MODULE__{}, {:error, reason}) do
    {:error, RedactedError.reduce(reason)}
  end

  defp callback_answer(%__MODULE__{}, answer), do: answer

  # Step 7: the mode is what step 8 takes. The window is read per attempt, and only for a match.
  defp classify(%__MODULE__{} = dispatch, %Merged{} = merged, tags) do
    case Read.command_positions(merged) do
      [] ->
        {:ok, :open}

      [_position | _] ->
        with {:ok, window_ms} <- command_window(dispatch) do
          %__MODULE__{kind_tag: kind_tag, scope: scope} = dispatch

          case Read.classify(merged, tags, kind_tag, scope, window_ms) do
            :reused -> {:error, :reused_command_id}
            mode -> {:ok, mode}
          end
        end
    end
  end

  defp command_window(%__MODULE__{decision: decision}) do
    case decision.command_window_ms() do
      window_ms when is_integer(window_ms) and window_ms > 0 -> {:ok, window_ms}
      _invalid -> {:error, :invalid_command_window}
    end
  end

  # Step 8.
  defp decide(:duplicate, %__MODULE__{}, _state, _merged, _reads), do: shape(:duplicate)

  # A replay exit may answer only a refusal: an event at or before `after` escapes the
  # condition, so anything appended here could commit one `command_id` twice.
  defp decide(:replay, %__MODULE__{decision: decision} = dispatch, state, _merged, _reads) do
    answer = callback_answer(dispatch, decision.decide(state, dispatch.context.command))

    with {:ok, _shape} <- shape(answer), do: {:error, :reused_command_id}
  end

  defp decide(:open, %__MODULE__{decision: decision} = dispatch, state, merged, reads) do
    with {:ok, shape} <-
           shape(callback_answer(dispatch, decision.decide(state, dispatch.context.command))),
         events = events_of(shape),
         :ok <- within_batch(dispatch, events),
         # Before anything commits: a raise here has to leave the store as it was.
         state = Enum.reduce(events, state, &decision.evolve(&2, &1)),
         {:ok, appends} <- prepare_members(dispatch, shape),
         :ok <- within_scope(dispatch, appends) do
      append(dispatch, appends, merged, reads, events, state)
    end
  end

  # What step 7 asks of a resend, asked of the batch that lands: each scope tag on some event.
  defp within_scope(%__MODULE__{scope: scope}, appends) do
    carried =
      for {_stream_uuid, _version, prepared} <- appends,
          %EventStore.EventData{tags: tags} <- prepared,
          tag <- tags,
          into: MapSet.new(),
          do: tag

    if Enum.all?(scope, &MapSet.member?(carried, &1)) do
      :ok
    else
      {:error, :batch_lacks_scope_tag}
    end
  end

  # The one prepare step, per member, each with its own stream. Its error is answered here, ahead
  # of `append/6`'s arms: those read the store's answer, and this is not one. So it never retries.
  defp prepare_members(%__MODULE__{application: application} = dispatch, shape) do
    dispatch
    |> members_of(shape)
    |> Enum.reduce_while([], fn {stream_uuid, context, events}, appends ->
      case Append.prepare_events(application, stream_uuid, events, context) do
        {:ok, prepared} -> {:cont, [{stream_uuid, :any_version, prepared} | appends]}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      appends -> {:ok, Enum.reverse(appends)}
    end
  end

  defp members_of(%__MODULE__{stream_uuid: stream_uuid, context: context}, {:route, events}) do
    [{stream_uuid, context, events}]
  end

  defp members_of(%__MODULE__{identity_prefix: prefix} = dispatch, {:streams, members}) do
    %__MODULE__{context: %ExecutionContext{} = context} = dispatch

    for {kind, identity, events} <- members do
      member_context = %ExecutionContext{context | aggregate_identity: identity, stream_tag: kind}

      {prefix <> identity, member_context, events}
    end
  end

  defp append(%__MODULE__{} = dispatch, appends, %Merged{} = merged, reads, events, state) do
    condition = [items: Read.condition_items(reads), after: merged.after]
    opts = [condition: condition, returning: :append_result]

    case EventStore.append_to_streams(dispatch.application, appends, opts) do
      {:ok, %AppendResult{} = result} ->
        decided(dispatch, appends, result, events, state)

      {:error, :append_condition_failed} ->
        {:retry, :append_condition_failed}

      {:error, _reason} = error ->
        error
    end
  end

  defp decided(%__MODULE__{}, [{stream_uuid, _, _} | _], result, events, state) do
    %AppendResult{position: position, stream_versions: stream_versions} = result

    {:ok,
     %{
       aggregate_uuid: stream_uuid,
       aggregate_state: state,
       aggregate_version: Map.fetch!(stream_versions, stream_uuid),
       position: position,
       stream_versions: stream_versions
     }, events}
  end

  defp answer({:ok, outcome, events}, %ExecutionContext{} = context) do
    {:decided, outcome, ExecutionContext.format_reply({:ok, events}, context, outcome)}
  end

  defp answer({:error, _reason} = error, _context), do: error
  defp answer({:error, _error, _stacktrace} = error, _context), do: error

  # What the dispatcher answers for a task that died.
  defp answer({:left, _kind, _value, _stacktrace}, _context) do
    {:error, :decision_execution_failed}
  end

  # Steps 1 and 2: what the condition holds so far, with this read not yet sent. One count per
  # step, which the read keeps for `telemetry_read/5`.
  defp bounded(%__MODULE__{} = dispatch, earlier_reads, %Read{} = read) do
    reads = earlier_reads ++ [read]

    with :ok <- within_items(dispatch, reads),
         read = %Read{read | probes: Read.count_probes(reads)},
         :ok <- within_probes(dispatch, read) do
      {:ok, read}
    end
  end

  defp within_items(%__MODULE__{bounds: %{max_items: limit}} = dispatch, reads) do
    within(dispatch, :items, Read.condition_count(reads), limit)
  end

  # The read answers what would not compile, as it would with no bound here.
  defp within_probes(%__MODULE__{}, %Read{probes: nil}), do: :ok

  defp within_probes(%__MODULE__{bounds: %{max_probes: limit}} = dispatch, %Read{probes: probes}) do
    within(dispatch, :probes, probes, limit)
  end

  defp within_events(%__MODULE__{bounds: %{max_events: limit}} = dispatch, merged, reads, started) do
    matched = Read.matched_count(merged)

    if matched > limit do
      telemetry_read(dispatch, merged, reads, System.monotonic_time() - started, 0)
    end

    within(dispatch, :events, matched, limit)
  end

  defp within_batch(%__MODULE__{bounds: %{max_batch_events: limit}} = dispatch, events) do
    within(dispatch, :batch_events, length(events), limit)
  end

  defp within(_dispatch, _bound, value, limit) when value <= limit, do: :ok

  defp within(%__MODULE__{application: application, decision: decision}, bound, value, limit) do
    Logger.error(fn ->
      "#{inspect(decision)} refused on the #{bound} bound: #{value} over #{limit}"
    end)

    :telemetry.execute(@refused, %{value: value, limit: limit}, %{
      application: application,
      decision: decision,
      bound: bound
    })

    {:error, refusal(bound)}
  end

  defp refusal(:batch_events), do: :decision_batch_too_large
  defp refusal(_bound), do: :decision_query_too_large

  defp telemetry_read(%__MODULE__{} = dispatch, %Merged{} = merged, reads, read_time, fold_time) do
    measurements = %{
      items: Read.condition_count(reads),
      probes: Read.condition_probes(reads),
      matched_events: Read.matched_count(merged),
      read_time: read_time,
      fold_time: fold_time
    }

    :telemetry.execute(@read, measurements, %{
      application: dispatch.application,
      decision: dispatch.decision,
      attempt: dispatch.attempt
    })
  end

  defp telemetry_stop({:ok, _outcome, events}, start_time, metadata) do
    Telemetry.stop(@execute, start_time, Map.put(metadata, :events, events))
  end

  defp telemetry_stop({:error, error}, start_time, metadata) do
    Telemetry.stop(@execute, start_time, Map.put(metadata, :error, error))
  end

  defp telemetry_stop({:error, error, stacktrace}, start_time, metadata) do
    metadata = Map.delete(metadata, :attempts)

    Telemetry.exception(@execute, start_time, :error, error, stacktrace, metadata)
  end

  defp telemetry_stop({:left, kind, value, stacktrace}, start_time, metadata) do
    metadata = Map.delete(metadata, :attempts)

    Telemetry.exception(@execute, start_time, kind, value, stacktrace, metadata)
  end
end
