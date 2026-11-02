defmodule Decree.Decision.Rule do
  @moduledoc """
  What an aggregate's append has to check beyond its own stream.

  An aggregate decides over its stream, and a rule such as one holder per address spans many.
  A rule names what to read. The aggregate reads it for one command, `execute/2` sees what the
  read answered, and the append goes in on the aggregate's own stream under a condition: it
  fails if anything the rule read about has appeared since. The aggregate keeps its process,
  its stream, its expected version and its state.

      defmodule EmailRule do
        use Decree.Decision.Rule

        def query(%RegisterUser{digest: nil}, _tag_context), do: []

        def query(%RegisterUser{digest: digest}, _tag_context) do
          [[types: ["Elixir.UserRegistered"], tags: ["email:" <> digest]]]
        end

        def followup_query(%RegisterUser{}, _tag_context, registrations) do
          for %UserRegistered{user_id: user_id} <- registrations do
            [types: ["Elixir.UserErased"], tags: ["user:" <> user_id]]
          end
        end
      end

  A route names the rule, and the command names a place for what the read answers:

      defmodule RegisterUser do
        defstruct [:command_id, :user_id, :digest, :rule_result]
      end

      dispatch RegisterUser, to: User, identity: :user_id, rule: EmailRule

      def execute(%User{}, %RegisterUser{rule_result: %Result{events: events}} = command) do
        case holders(events) do
          [] -> %UserRegistered{user_id: command.user_id, digest: command.digest}
          [_holder | _] -> {:error, :email_taken}
        end
      end

  The route is per command, so the aggregate's other commands stay plain. See "Two
  declarations" for the store's half.

  ## Callbacks

    - `c:query/2` names what to read: a list of items, `[]` for nothing to check, or
      `{:error, reason}`.
    - `c:followup_query/3`, optional, names a second read that depends on what the first
      found: a list of items, `[]` to skip it, or `{:error, reason}`.
    - `c:index_families/0`, optional, says which tag kinds hold digests. See "Required tags".

  Items are those of `Decree.Decision`, with one difference: `replay: true` answers
  `{:error, :invalid_query}`, because a rule has no scope to hold one to. Both callbacks may run
  several times for one dispatch, so they must have no side effects. They run in the aggregate's
  process and hold up its mailbox, as a rebuild does.

  The build fails on anything a rule does not have: `evolve/2`, `decide/2`, `admit/2`,
  `scope_tags/1`, `initial_state/0`, `command_window_ms/0`, `handle/2` and `create:`.

  ## What a dispatch does

  In the aggregate's process, after `before_execute`, on each attempt:

    1. Call `c:query/2`. Its error is the answer. `[]` skips to step 4 with no statement.
       Refuse an item naming no types, a `replay: true` item, and more items or probes than
       the bounds allow. Read the items.
    2. Call `c:followup_query/3` if it is defined, with the events the first read matched, those
       the rule lists decrypted. Check both bounds again across both reads, read the items,
       and merge by position. The first read's `after` stands for both. More matched events
       than `max_events`, both reads together, refuses.
    3. Keep the events the rule's own items matched, and decrypt the ones it lists.
    4. Call `execute/2` with those events on a local copy of the command. Everything
       after that is the aggregate's as on any other route: `apply/2`, preparing, tags.
    5. Refuse more new events than `max_batch_events`, compare the digests and require the
       tags, as "Required tags" says, and append at the expected version under the condition.
       Its items are the rule's own from both reads, and its `after` is the first read's: the
       store's head at that read, and never 0 on an empty store.

  So an event committed after the first read, the gap between the two reads included, fails
  the append if it matches. An `after` taken from the second read would miss what landed in
  that gap.

  Fold, `admit/2`, classification, the command window and scope checks do not run: those are a
  decision's. A rule's reads have no command item, and an aggregate keeps its own "already
  exists" guard, so a `command_id` reused across two streams behaves as it does on any
  aggregate route. A rule reads through a decision's path, with its compiled bounds, so a query
  naming something deletable answers `{:error, :query_names_deletable}`.

  ## What rides

  `execute/2` stays pure and of arity 2. The command is its only added input, because what a
  read answers is never aggregate state.

  The command's `:rule_result` field holds a `Decree.Decision.Rule.Result`, its `events` being
  the data of the events the rule's own items matched, both reads', in position order. A caller's
  value there is overwritten. A route with no rule leaves the field as sent.

  It is set on a copy, for one call. `Decree.Aggregates.ExecutionContext` keeps the plain
  command, so what follows the call never holds a read: no reply, no event, no metadata, no
  state, no snapshot. `Inspect` shows nothing of a result, and writing one into an event raises.

  **`[]`** from `c:query/2` means this command has nothing to check, as a registration with no
  address has not. `execute/2` gets an empty result and the append is a plain one. A decision
  refuses `[]`, because a decision that reads nothing has nothing to decide on, and a rule
  decides nothing.

  ## Required tags

  A condition finds an event by its tags, so a rule's conditional append requires of every event
  what a decision's does, through the same code, opted in or not: `always/0` of
  `Decree.Event.RequiredTags`, the kinds of the tags on the condition's items naming the event's
  type, less the module's `__conditional_tag_kinds__/0`, and the event's own answer. All the
  condition's items count, since a rule has no fact item. So a kind a query names is required
  here though the same event on a plain route is not refused, and a command with no usable
  `command_id` cannot append on a rule route.

  `c:query/2` answering `[]` makes a plain append, with no condition to take kinds from, so an
  event there gets its own answer only.

  `c:index_families/0` works as "Digest tags" in `Decree.Decision` says, on the condition's items
  from both reads, and fails the build on the same bad answers. The refusals are
  `{:error, :missing_required_tag}` and `{:error, :digest_tag_mismatch}`, an atom with no digest
  in it. Each answers with nothing appended, before any statement, and never retries.

  ## Retries

  A failed condition runs the command again from `before_execute`, on the state the aggregate
  has, and spends one of the attempts `retry_attempts` gives it. So does a lost expected
  version, which rebuilds the state first, from a budget of its own: neither spends the other's,
  and one dispatch runs `execute/2` at most `2 * retry_attempts + 1` times. Each attempt reads
  afresh, with a fresh `after`. A condition spent answers `{:error, :too_many_attempts}`,
  with the state unchanged, and never a success or a duplicate. `retry_attempts: 0` tries
  once. An aggregate stopped under a dispatch restarts the dispatch, bounded by `retry_attempts`
  as it is on any route, and the condition's budget starts again.

  ## Bounds

      use Decree.Decision.Rule, max_items: 8

  `max_items`, `max_probes`, `max_events`, `max_batch_events` and `bound_reason:` are those of
  "Bounds" in `Decree.Decision`, with the same defaults, the same rule for raising one, and the
  same answers: `:decision_query_too_large` and `:decision_batch_too_large`. A condition's items
  run under the `$all` lock on an aggregate route as on a decision's, so lead each item with
  its narrowest tag.

  ## Leaks

  A rule route is redacted whether or not the aggregate uses `Decree.PersonalFields.Aggregate`,
  because a read can bring in another subject's plaintext. A raise from a callback, from
  `execute/2`, `apply/2` or `before_execute`, from preparing, or from an upcaster on an event
  the rule reads, reaches logs, telemetry, `after_failure` and the answer as a
  `Decree.PersonalFields.RedactedError` with a scrubbed stacktrace. An exit leaves reduced by
  `Decree.PersonalFields.RedactedError.reduce/1`. A throw is reduced the same way and leaves as
  an exit, `{:nocatch, reduced}`, because `gen_server` reads a term thrown from a callback as its
  return, and a reduced `{:noreply, _}` is a valid one. Either takes the aggregate with it, and
  the caller gets `{:error, :aggregate_execution_failed}`. An `{:error, reason}` from
  `c:followup_query/3` is reduced the same way. `c:query/2` has read nothing, so its reason
  stands.

  The upcast runs after the read's `[:decree, :event_store, :read_query]` span has stopped, so
  an upcaster's failure reaches the boundary above and never that span's exception event.

  **An `{:error, reason}` from `execute/2` is not reduced.** It goes back to the caller, to
  `after_failure` and to telemetry as it stands, as on any aggregate route. So `execute/2` answers
  an atom or a tuple of its own, never a value it read.

  An item's tag goes to the store and to telemetry as it is, so `c:followup_query/3` names an id
  or a digest, never a value it decrypted.

  ## Personal fields

  A rule lists the events it reads as a decision does:

      defmodule EmailRule do
        use Decree.Decision.Rule
        use Decree.PersonalFields.Aggregate

        @personal_fold [UserRegistered]
      end

  A listed event reaches `c:followup_query/3` and `execute/2` with its body decrypted, and an
  erased subject's field nil. An unlisted one reaches them as stored. One no key can read
  answers `{:error, :key_unknown}`, with nothing appended, no retry, and the aggregate's state
  still good. `@personal_state` and `@personal_state_subject` mean nothing on a rule.

  ## Two declarations

  A route's `rule:` says which commands a rule covers. The store's `conditional_appends`
  says which conditions it will run, because a condition takes time under `$all`:

      conditional_appends: [targets: [User]]

  The condition's `target` is the route's aggregate module. The two must agree. An application
  with a rule route checks them as it starts, right after its store has started, against the
  config the running store parsed:

    - A rule route's aggregate with no store target raises an `ArgumentError`, naming both, and
      the application does not start: `start_link/1` answers
      `{:error, {:shutdown, {:failed_to_start_child, _id, _reason}}}`.
    - A store target no route declares logs a warning. It is not refused: a store can be
      shared, and a direct store caller may use a target.
    - A router outside the application is not checked at start, so a dispatch through it
      answers the store's `{:error, :undeclared_conditional_target}`.

  An application with no rule route checks nothing and asks its store nothing, so a target
  its store declares draws no warning there.

  ## What declaring leaves alone

  A target is compared on a conditional append and nowhere else, and everything below goes by
  a stream's prefix. So a declared aggregate's stream takes, exactly as before, plain appends
  from its other commands, a link into it, a hard delete of it and a snapshot of it. Hard deleting
  such a stream removes what a rule reads there, which is a release: a rule must not hang
  anything on those events staying.

  The aggregate's own prefix should not be one of `conditional_appends`' `stream_prefixes`, which
  would refuse its plain commands.

  ## The appender map

  `Decree.Decision.AppenderMap` audits a rule as it does a decision. A rule has no
  `__decision__/0`, so it is no create. List it as `{EmailRule, items}` with the row
  `{RegisterUser, User}`, and `conditional: [RegisterUser]` says the aggregate appends under it.
  Or list it under the aggregate, `{User, items}`: the row is then the rule's own, and another
  rule naming the type is covered only if every rule listed under `User` naming it has its kinds.

  ## Telemetry

  An aggregate's span runs as on any route. There is no `[:decree, :decision, :execute]` span,
  and `[:decree, :decision, :read]`, `[:decree, :decision, :retry]` and
  `[:decree, :decision, :refused]` are those of `Decree.Decision`, with `decision` being the rule:

    - `:read` fires once per attempt that read anything, and `fold_time` is what step 3 took.
    - `:retry` fires only for `:append_condition_failed`. A lost expected version has no event of
      its own, on this route as on a plain one.
    - `attempt` counts every run of the command in this dispatch, for either reason.

  A route with no rule fires none of them. The reads' `[:decree, :event_store, :read_query]`
  span covers the store's answer and not the upcast: see "Leaks".
  """

  alias Decree.Event.TagContext

  defmodule Result do
    @moduledoc """
    What a rule's reads answered, for one call of `execute/2`.

      - `events` - the data of the events the rule's own items matched, in position order.

    `Inspect` shows nothing of them, and JSON refuses it, so it can reach neither a log nor an
    event by accident.
    """

    @derive {Inspect, only: []}
    defstruct events: []

    @type t :: %__MODULE__{events: [struct()]}

    if Code.ensure_loaded?(Jason.Encoder) do
      # The default answer for a struct is a `Protocol.UndefinedError` holding the value, and an
      # append's telemetry span would publish that.
      defimpl Jason.Encoder do
        def encode(_result, _opts) do
          raise ArgumentError, "a rule's result is not an event's to hold"
        end
      end
    end
  end

  @typedoc "An item of `Decree.Decision`, which has what one may hold."
  @type item :: Decree.Decision.item()

  @doc """
  The items to read for a command, `[]` for nothing to check.
  """
  @callback query(command :: struct(), TagContext.t()) :: [item()] | {:error, term()}

  @doc """
  The items of a second read, given the events the first read's own items matched. `[]`
  skips it.
  """
  @callback followup_query(command :: struct(), TagContext.t(), [struct()]) ::
              [item()] | {:error, term()}

  @doc """
  Which tag kinds hold digests. See "Digest tags" in `Decree.Decision`. Defaults to none.
  """
  @callback index_families() :: %{String.t() => :digest | :plain}

  @optional_callbacks followup_query: 3, index_families: 0

  @refused [
    evolve: 2,
    decide: 2,
    admit: 2,
    scope_tags: 1,
    initial_state: 0,
    command_window_ms: 0,
    handle: 2
  ]

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Decree.Decision.Rule
      @before_compile Decree.Decision.Rule
      @after_compile Decree.Decision

      @decree_rule_bounds Decree.Decision.Rule.__bounds__!(__MODULE__, opts)
    end
  end

  @doc false
  defmacro __before_compile__(%Macro.Env{module: module}) do
    with [{name, arity} | _] <- Enum.filter(@refused, &Module.defines?(module, &1)) do
      raise ArgumentError,
            "#{inspect(module)} uses Decree.Decision.Rule and defines `#{name}/#{arity}`: a " <>
              "rule reads, and the aggregate decides"
    end

    if Decree.Event.Handler in Module.get_attribute(module, :behaviour, []) do
      raise ArgumentError,
            "#{inspect(module)} uses Decree.Decision.Rule and declares " <>
              "Decree.Event.Handler: a rule reacts to no event"
    end

    followup_query? = Module.defines?(module, {:followup_query, 3}, :def)

    quote do
      unquote(Decree.Decision.__index_families__(module))

      @doc false
      def __rule__ do
        Map.merge(@decree_rule_bounds, %{
          create: false,
          followup_query?: unquote(followup_query?),
          index_families: index_families()
        })
      end
    end
  end

  @doc false
  def __bounds__!(module, opts) do
    if Keyword.has_key?(opts, :create) do
      raise ArgumentError,
            "#{inspect(module)} uses Decree.Decision.Rule with `create:`: the aggregate's own " <>
              "guard keeps a rule route's fact"
    end

    Decree.Decision.__bounds__!(module, opts)
  end
end
