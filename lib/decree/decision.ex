defmodule Decree.Decision do
  @moduledoc """
  A rule decided over events, with no aggregate process and no stream of its own to replay.

  A decision names what it needs to read, folds what the read answers into a state, and
  decides. Its events are appended under a condition: the append fails if any event it
  read about has appeared since the read, and the decision runs again on what is there
  now. It keeps nothing between commands, and it reacts to no event: it is called by a
  dispatch and answers that dispatch.

      defmodule ClaimEmail do
        use Decree.Decision

        defmodule State do
          defstruct claimed?: false
        end

        def query(%ClaimEmailCommand{digest: digest}, _tag_context) do
          [[types: ["Elixir.EmailClaimed"], tags: ["email:" <> digest]]]
        end

        def initial_state, do: %State{}

        def evolve(%State{} = state, %EmailClaimed{}), do: %State{state | claimed?: true}

        def decide(%State{claimed?: true}, _command), do: {:error, :email_taken}

        def decide(%State{}, %ClaimEmailCommand{} = command) do
          %EmailClaimed{claim_id: command.claim_id, digest: command.digest}
        end
      end

  A router routes a command to it with `Decree.Commands.Router.decide/2`. The route's
  stream prefix must be declared in the event store's `conditional_appends`, which is also
  what keeps every other writer out of those streams.

  ## Callbacks

    - `c:query/2` names what to read: a list of items, or `{:error, reason}`.
    - `c:followup_query/3`, optional, names a second read that depends on what the first
      found: a list of items, `[]` to skip it, or `{:error, reason}`.
    - `c:initial_state/0` is the state a fold starts from.
    - `c:evolve/2` folds one event into the state.
    - `c:admit/2`, optional, answers `:ok` or `{:error, reason}`.
    - `c:decide/2` answers what to append.

  The state comes first, as in an aggregate's `execute/2` and `apply/2`. These callbacks
  receive the command, the tag context, which holds the dispatch metadata, and what the
  reads answered, and nothing else: no time, no application, no connection and no read
  model. They may run several times for one dispatch, so they must have no side effects.

  ## Items

  An item is a keyword list or a map with `:types`, a non-empty list of event types,
  optional `:tags` and optional `replay: true`. `:types` and `:tags` are those of
  `Decree.Store.Query`, which has the rest of what they mean. An item naming no types
  answers `{:error, :invalid_query}`, and so does `c:query/2` answering `[]`: a decision
  that reads nothing has nothing to decide on. `replay: true` marks an item as finding this
  command's own earlier work.

  **Lead with the narrowest tag.** An item's first tag is the one the index searches, and
  what a condition costs under the `$all` lock is that tag's hits after `after`. So a broad
  first tag makes every append in the store wait. Put the tag that narrows most first.

  A query that does not shrink over time counts against the same bound as any other:
  replay sets, ended episodes and anything else nothing evicts. Say so in a comment at the
  item, because such a query reaches `max_events` by being used, not by being wrong.

  ## What a dispatch does

  Middleware runs as it does on an aggregate route, then, in a task as an aggregate command
  runs but with no aggregate process and no registry entry:

    0. Refuse a command with no `command_id`, `{:error, :missing_command_id}`, and a
       route identity that is not a canonical UUID, `{:error, :invalid_decision_identity}`.
       Neither sends a statement.
    1. Call `c:query/2`. Its error is the answer. Refuse an item naming no types. Refuse
       more condition items than `max_items`. Read the items and the command item.
    2. Call `c:followup_query/3` if it is defined, with the events the first read's own
       items matched. Check the items again across both reads, and read them and the
       command item. A command match the first read did not have starts over from step 1,
       using a retry. Merge by position. The first read's `after` stands for both. More
       matched events than `max_events`, both reads together, refuses.
    3. Fetch the tags of the events matching the command item, in one read by position.
    4. Keep the events the decision's own items matched. An event only the command item
       matched is not shown to `c:evolve/2`.
    5. Fold them with `c:evolve/2`.
    6. Call `c:admit/2`. Its error is the answer.
    7. Classify the command matches. Any match refuses with
       `{:error, :unclassified_command_match}`: this command's events are already in the
       store, and nothing here can tell which outcome the caller is owed.
    8. Call `c:decide/2`, apply the new events to the state, prepare them, and append them
       at `:any_version` under the condition.

  Then `after_dispatch` or `after_failure` middleware runs as on an aggregate route.

  An item-index layout ties the reads together. A read's items are the decision's own, then
  the command item, then any read-only item, which is read and never in the condition. A
  second read's indexes are offset by the first read's item count, so which items matched
  an event is one merged answer. A read returns no tags, so what an event carries comes
  from the one fetch in step 3, and a position with no row left is one that does not carry
  the tag.

  The condition's items are the decision's own, from both reads, and the command item
  once. Its `after` is the first read's. So an event committed after the first read, the
  gap between the two reads included, fails the append if it matches.

  ## What decide answers

    - an event or a list of events, appended to the route's stream.
    - `{:streams, [{kind, identity, event_or_events}]}`, appended to several streams
      atomically. Each member lands on `<route prefix><identity>`, `kind` is that member's
      entity tag kind, an atom of the grammar of `stream_tag:` on the route, and `identity` a
      canonical UUID. Each member's events carry its own stream tag and not another's.
    - `:duplicate`, which answers `{:error, :duplicate_command}` and appends nothing.
    - `{:error, reason}`, which is the answer as it stands and never retries.

  ## Answers

  A raise from any callback, or from a `Decree.Event.Tags` implementation, is rescued, and
  the dispatch answers `{:error, error}`, as a raise from `execute/2` does. An error from a
  `Decree.Event.Tags` implementation is the answer as it stands.

  Only the store's `{:error, :append_condition_failed}` retries. It runs the dispatch again
  from step 1 while `retry_attempts` lasts, and spent answers
  `{:error, :too_many_attempts}`, never a duplicate. A dispatch that overruns its timeout
  answers `{:error, :decision_execution_timeout}`, whether or not its append landed, and
  one whose task died answers `{:error, :decision_execution_failed}`.

  Four refusals are named arms for an event handler, because a handler that dispatches a
  decision must stop or park on them rather than retry:

    - `:decision_query_too_large` - more condition items than `max_items`, or more matched
      events than `max_events`.
    - `:decision_batch_too_large` - more new events than `max_batch_events`.
    - `:too_many_attempts` - retries are spent.
    - `:unclassified_command_match` - this command already left events in the store.

  `:reused_command_id` and `:tag_index_not_enabled` join them with the steps that answer
  them: step 7's classification and step 3's tag index check.

  The other refusals are a bug in a command or a decision: `:missing_command_id`,
  `:invalid_decision_identity`, `:invalid_query`, `:decision_returned_no_events` for `[]`,
  `:ok` or `nil` from `c:decide/2` and `[]` for a member, `:invalid_decision_return` for
  anything else it may not answer, and `:invalid_decision_stream` for a malformed member.

  ## Bounds

      use Decree.Decision,
        max_items: 16,
        max_events: 1_000,
        max_batch_events: 100,
        bound_reason: "..."

  Those are the defaults. Lowering one is free. Raising one past its default needs a
  non-empty `bound_reason:`, and the build fails without it, because the default is what
  was measured.

    - `max_items` counts condition items, the framework's included: 15 of a decision's own
      plus the command item is 16. A condition's items run under the `$all` lock.
    - `max_events` counts matched rows, both reads together, an event in both counted
      once, command matches included, because a read returning `count` rows cannot be told
      from one that was cut.
    - `max_batch_events` counts new events, across every member. `$all` is held for each
      one, about 0.1 ms.

  A refusal on a bound is an alarm: it logs an error and emits
  `[:decree, :decision, :refused]`.

  ## `admit/2`

  `c:admit/2` is for checks shaped like authorization: is this actor allowed to ask? It
  runs over the fold on every dispatch, a genuine resend included, so a test of later state
  would refuse a resend that should be answered as a duplicate. A check of the state the
  fold reached belongs in `c:decide/2`.

  ## Telemetry

    - `[:decree, :decision, :execute, :start | :stop | :exception]`, a span, with metadata
      `application`, `decision`, `stream_uuid` and `execution_context`, and on stop `attempts`
      and `events` or `error`.
    - `[:decree, :decision, :read]`, once per completed read cycle, one refused on
      `max_events` included. Measurements `items`, `matched_events`, `read_time` and
      `fold_time`, the last two in native units, `fold_time` 0 when refused. Metadata
      `application`, `decision` and `attempt`.
    - `[:decree, :decision, :retry]`, measurement `attempt`, the one that failed. Metadata
      `application`, `decision` and `reason`, `:append_condition_failed` or
      `:new_command_match`.
    - `[:decree, :decision, :refused]`, measurements `value` and `limit`. Metadata
      `application`, `decision` and `bound`, one of `:items`, `:events` and `:batch_events`.

  ## Testing

  `Decree.Decision.Assertions` runs a decision with no store.
  """

  alias Decree.Event.TagContext

  @type state :: term()
  @type command :: struct()
  @type event :: struct()
  @type item :: keyword() | map()

  @typedoc """
  What `c:decide/2` may answer. See "What decide answers".
  """
  @type decided ::
          event()
          | [event()]
          | {:streams, [{kind :: atom(), identity :: String.t(), event() | [event()]}]}
          | :duplicate
          | {:error, term()}

  @doc """
  The items to read for a command.
  """
  @callback query(command(), TagContext.t()) :: [item()] | {:error, term()}

  @doc """
  The items of a second read, given the events the first read's own items matched. `[]`
  skips it.
  """
  @callback followup_query(command(), TagContext.t(), [event()]) :: [item()] | {:error, term()}

  @doc """
  The state a fold starts from.
  """
  @callback initial_state() :: state()

  @doc """
  Folds one event into the state.
  """
  @callback evolve(state(), event()) :: state()

  @doc """
  Whether the command may be asked at all. See "`admit/2`". Defaults to `:ok`.
  """
  @callback admit(state(), command()) :: :ok | {:error, term()}

  @doc """
  What to append. See "What decide answers".
  """
  @callback decide(state(), command()) :: decided()

  @optional_callbacks followup_query: 3, admit: 2

  @bounds [max_items: 16, max_events: 1_000, max_batch_events: 100]
  @options [:bound_reason | Keyword.keys(@bounds)]

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Decree.Decision
      @before_compile Decree.Decision

      @decree_decision_bounds Decree.Decision.__bounds__!(__MODULE__, opts)

      @doc false
      def admit(_state, _command), do: :ok

      defoverridable admit: 2
    end
  end

  @doc false
  defmacro __before_compile__(%Macro.Env{module: module}) do
    behaviours = Module.get_attribute(module, :behaviour, [])

    if Module.defines?(module, {:handle, 2}) or Decree.Event.Handler in behaviours do
      raise ArgumentError,
            "#{inspect(module)} uses Decree.Decision and handles events: a decision reacts " <>
              "to no event, so it may not define `handle/2` or declare Decree.Event.Handler"
    end

    followup_query? = Module.defines?(module, {:followup_query, 3}, :def)

    quote do
      @doc false
      def __decision__ do
        Map.put(@decree_decision_bounds, :followup_query?, unquote(followup_query?))
      end
    end
  end

  @doc false
  def __bounds__!(module, opts) do
    with [unknown | _] <- Keyword.keys(opts) -- @options do
      raise ArgumentError, "#{inspect(module)} has an unknown option: #{inspect(unknown)}"
    end

    bounds =
      Map.new(@bounds, fn {bound, default} -> {bound, bound!(module, opts, bound, default)} end)

    with [bound | _] <- raised(bounds),
         false <- reasoned?(Keyword.get(opts, :bound_reason)) do
      raise ArgumentError,
            "#{inspect(module)} raises `#{bound}` past its default, which needs a non-empty " <>
              "`bound_reason:`"
    end

    bounds
  end

  defp bound!(module, opts, bound, default) do
    case Keyword.get(opts, bound, default) do
      value when is_integer(value) and value > 0 ->
        value

      invalid ->
        raise ArgumentError,
              "#{inspect(module)} has an invalid `#{bound}`: #{inspect(invalid)}, expected a " <>
                "positive integer"
    end
  end

  defp raised(bounds), do: for({bound, default} <- @bounds, bounds[bound] > default, do: bound)

  defp reasoned?(reason), do: is_binary(reason) and String.trim(reason) != ""
end
