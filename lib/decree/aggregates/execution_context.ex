defmodule Decree.Aggregates.ExecutionContext do
  @moduledoc """
  Defines the arguments used to execute a command for an aggregate.

  The available options are:

    - `command` - the command to execute, typically a struct
      (e.g. `%OpenBankAccount{...}`).

    - `retry_attempts` - the number of retries permitted if an
      `{:error, :wrong_expected_version}` is encountered when appending events.

    - `causation_id` - the UUID assigned to the dispatched command.

    - `correlation_id` - a UUID used to correlate related commands/events.

    - `metadata` - a map of key/value pairs containing the metadata to be
      associated with all events created by the command.

    - `handler` - the module that handles the command. It may be either the
      aggregate module itself or a separate command handler module.

    - `function` - the name of the function, as an atom, that handles the command.
      The default value is `:execute`, used to support command dispatch directly
      to the aggregate module. For command handlers the `:handle` function is
      used.

    - `before_execute` - the name of the function, as an atom, that prepares the
      command before execution, called just before `function`. The default value
      is `nil`, disabling it. It should return `:ok` on success or `{:error, any()}`
      to cancel the dispatch.

    - `lifespan` - a module implementing the `Decree.Aggregates.AggregateLifespan`
      behaviour to control the aggregate instance process lifespan. The default
      value, `Decree.Aggregates.DefaultLifespan`, keeps the process running
      indefinitely.

    - `identity` - how the route finds the aggregate's identity: the name of a command
      field, or a one argument function returning it. Only
      `Decree.Event.Append.prepare/4` reads it, and only for the kind of the stream's
      entity tag. It is not called for that: `aggregate_identity` is what it resolved to.

    - `aggregate_identity` - the identity the stream was routed by, as the string
      `Decree.Middleware.ExtractAggregateIdentity` resolved once for the dispatch, before
      any `identify` prefix. It is the id of the stream's entity tag. It is `nil` for a
      context not built by a dispatch, which gives no stream tag.

    - `stream_tag` - the kind the route named for the stream's entity tag, an atom, or
      `nil`. See `Decree.Commands.Router.dispatch/2`.

    - `rule` - the route's `Decree.Decision.Rule`, or `nil`. What its read answers never
      reaches this struct: `command` stays the plain command.

    - `rule_retries` - the number of retries permitted if a rule's condition fails, set from
      `retry_attempts` and spent apart from it, so neither loop spends the other's budget.

    - `attempt` - the number of this attempt at the command in this dispatch, which a rule's
      telemetry reports.

  """

  alias Decree.Aggregates.DefaultLifespan
  alias Decree.Aggregates.ExecutionContext
  alias Decree.Commands.ExecutionResult

  defstruct [
    :command,
    :causation_id,
    :correlation_id,
    :handler,
    :function,
    :identity,
    :aggregate_identity,
    :stream_tag,
    :rule,
    :rule_retries,
    before_execute: nil,
    attempt: 1,
    retry_attempts: 0,
    returning: false,
    lifespan: Application.compile_env(:decree, :aggregate_lifespan, DefaultLifespan),
    metadata: %{}
  ]

  def retry(%ExecutionContext{retry_attempts: nil}),
    do: {:error, :too_many_attempts}

  def retry(%ExecutionContext{retry_attempts: retry_attempts}) when retry_attempts <= 0,
    do: {:error, :too_many_attempts}

  def retry(%ExecutionContext{} = context) do
    %ExecutionContext{retry_attempts: retry_attempts} = context

    context = %ExecutionContext{
      context
      | retry_attempts: retry_attempts - 1,
        attempt: context.attempt + 1
    }

    {:ok, context}
  end

  @doc false
  def retry_rule(%ExecutionContext{rule_retries: nil}),
    do: {:error, :too_many_attempts}

  def retry_rule(%ExecutionContext{rule_retries: rule_retries}) when rule_retries <= 0,
    do: {:error, :too_many_attempts}

  def retry_rule(%ExecutionContext{} = context) do
    %ExecutionContext{rule_retries: rule_retries, attempt: attempt} = context

    {:ok, %ExecutionContext{context | rule_retries: rule_retries - 1, attempt: attempt + 1}}
  end

  # `outcome` is an `%Aggregate{}`, or a decision's map of the same three keys. A decision
  # has no process, so nothing else of an aggregate's applies, and it adds what its append
  # wrote: `:position` and `:stream_versions`, which an aggregate has not got.
  def format_reply({:ok, events}, %ExecutionContext{} = context, outcome) do
    %{
      aggregate_uuid: aggregate_uuid,
      aggregate_state: aggregate_state,
      aggregate_version: aggregate_version
    } = outcome

    %ExecutionContext{metadata: metadata, returning: returning} = context

    case returning do
      :aggregate_state ->
        {:ok, aggregate_version, events, aggregate_state, aggregate_state}

      :aggregate_version ->
        {:ok, aggregate_version, events, aggregate_state, aggregate_version}

      :events ->
        {:ok, aggregate_version, events, aggregate_state, events}

      :execution_result ->
        result = %ExecutionResult{
          aggregate_uuid: aggregate_uuid,
          aggregate_state: aggregate_state,
          aggregate_version: aggregate_version,
          events: events,
          metadata: metadata,
          position: Map.get(outcome, :position),
          stream_versions: Map.get(outcome, :stream_versions)
        }

        {:ok, aggregate_version, events, aggregate_state, result}

      false ->
        {:ok, aggregate_version, events, aggregate_state}
    end
  end

  def format_reply({:error, _error} = reply, _context, _aggregate) do
    reply
  end

  def format_reply({:error, error, _stacktrace}, _context, _aggregate) do
    {:error, error}
  end
end
