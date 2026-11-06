defmodule Decree.Decision do
  @moduledoc """
  A rule decided over events, with no aggregate process and no stream of its own to replay.

  A decision names what it needs to read, folds what the read answers into a state, and
  decides. Its events are appended under a condition: the append fails if any event it
  read about has appeared since the read, and the decision runs again on what is there
  now. It keeps nothing between commands, and it reacts to no event: it is called by a
  dispatch and answers that dispatch.

      defmodule ClaimEmail do
        use Decree.Decision, create: false

        defmodule State do
          defstruct claimed?: false
        end

        def query(%ClaimEmailCommand{digest: digest}, _tag_context) do
          [[types: ["Elixir.EmailClaimed"], tags: ["email:" <> digest]]]
        end

        def command_window_ms, do: :timer.minutes(5)

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

  An aggregate that keeps its process and its stream, and has one command to check beyond
  that stream, wants a `Decree.Decision.Rule`, which reads and appends through the same path.

  ## Callbacks

    - `c:query/2` names what to read: a list of items, or `{:error, reason}`.
    - `c:followup_query/3`, optional, names a second read that depends on what the first
      found: a list of items, `[]` to skip it, or `{:error, reason}`.
    - `c:initial_state/0` is the state a fold starts from.
    - `c:evolve/2` folds one event into the state.
    - `c:admit/2`, optional, answers `:ok` or `{:error, reason}`.
    - `c:decide/2` answers what to append.
    - `c:command_window_ms/0` says how long a command match counts as this command's. See
      "The command window".
    - `c:scope_tags/1`, optional, names the tags a command's events must carry besides the
      route's stream tag. See "The command window".
    - `c:index_families/0`, optional, says which tag kinds hold digests. See "Digest tags".

  The state comes first, as in an aggregate's `execute/2` and `apply/2`. These callbacks
  receive the command, the tag context, which holds the dispatch metadata, and what the
  reads answered, and nothing else: no time, no application, no connection and no read
  model. They may run several times for one dispatch, so they must have no side effects.

  ## Items

  An item is a keyword list or a map with `:types`, a non-empty list of event types,
  optional `:tags` and optional `replay: true`. `:types` and `:tags` are those of
  `Decree.Store.Query`, which has the rest of what they mean. An item naming no types
  answers `{:error, :invalid_query}`, and so does `c:query/2` answering `[]`: a decision
  that reads nothing has nothing to decide on, unless it is a create. `replay: true` marks an
  item as finding this command's own earlier work, and sends a resend to `c:decide/2` to say
  what its first answer was: see "Replay safety". Its `:tags` must hold a `command_kind:` tag
  and a tag of the command's scope, the route's stream tag or one of `c:scope_tags/1`, or it
  answers `{:error, :invalid_query}`.

  **Lead with the narrowest tag.** An item's first tag is the one the index searches, and
  what a condition costs under the `$all` lock is that tag's hits after `after`. So a broad
  first tag makes every append in the store wait. Put the tag that narrows most first.

  A query that does not shrink over time counts against the same bound as any other:
  replay sets, ended episodes and anything else nothing evicts. Say so in a comment at the
  item, because such a query reaches `max_events` by being used, not by being wrong.

  ## What a dispatch does

  Middleware runs as it does on an aggregate route, then, in a task as an aggregate command
  runs but with no aggregate process and no registry entry:

    0. Refuse a command with no `command_id`, `{:error, :missing_command_id}`, a route
       identity that is not a canonical UUID or has no stream tag,
       `{:error, :invalid_decision_identity}`, and a `c:scope_tags/1` answer that is not a
       list of non-empty strings, `{:error, :invalid_scope_tags}`. None sends a statement.
    1. Call `c:query/2`. Its error is the answer. On a create, put the fact item first.
       Refuse an item naming no types or a malformed `replay: true` item. Refuse more
       condition items than `max_items` and more probes than `max_probes`. Read the items
       with the command item, the index item and any activation item, and in the same
       statement the tags of the events a framework item matched.
    2. Call `c:followup_query/3` if it is defined, with the events the first read's own
       items matched, those the decision lists decrypted. Check both bounds again across both
       reads, and read the items with the framework's. A command match the first read did
       not have starts over from step 1, using a retry. Merge by position. The first read's
       `after` stands for both. More matched events than `max_events`, both reads together,
       refuses.
    3. Check the index: unless the facts at or below `after` enable every family the reads'
       own items require, `{:error, :tag_index_not_enabled}` (see "Index facts"). Then, on a
       decision with `activation:`, unless an event the activation item matched is at or
       below `after`, `{:error, :decision_not_activated}` (see "Activation").
    4. Keep the events the decision's own items matched. An event only a framework item
       matched is not shown to `c:evolve/2`.
    5. Decrypt the ones the decision lists, and fold them all with `c:evolve/2`.
    6. Call `c:admit/2`. Its error is the answer.
    7. Classify the command matches inside the window, with the database's stamps, in this
       order. None: go on to step 8. An event of another kind, or a scope tag no event
       carries: `{:error, :reused_command_id}`. A `replay: true` item selected one of them:
       a replay, for step 8. Otherwise a duplicate, `{:error, :duplicate_command}`.
    8. Call `c:decide/2`. On a replay its error or `:duplicate` is the answer, and events
       answer `{:error, :reused_command_id}`, with nothing appended. Otherwise apply the new
       events to the state and prepare them, per member, which takes their tags from the
       plaintext and encrypts what they declare. Its error is the answer. Refuse a batch
       lacking a scope tag, `{:error, :batch_lacks_scope_tag}`, and on a create a batch with
       no event of a declared type on the stream tag, `{:error, :batch_lacks_fact}`. Compare
       the digest kinds with the condition's items (see "Digest tags"), and refuse on a
       mismatch. Require of each event, whether or not it opted in,
       `Decree.Event.RequiredTags.always/0` and the kinds of the tags on the condition's
       items naming its type, a create's fact item left out, less those its module lists in
       `__conditional_tag_kinds__/0`, and of an event that opted in its own answer as well.
       Append them at `:any_version` under the condition, and the store refuses an event
       lacking a kind on its list. The condition holds the index item, so a fact committed
       since the first read fails it, and the retry checks the index again.

  Then `after_dispatch` or `after_failure` middleware runs as on an aggregate route.

  An item-index layout ties the reads together. A read's items are the decision's own, then
  the framework's: the command item, the index item and any activation item, in that order.
  Then any read-only item, which is read and never in the condition. A second read's indexes
  are offset by the first read's item count, so which items matched an event is one merged
  answer. Each read returns the tags of the events its framework items matched, and nothing
  else's, so what such an event carries comes from the reads, and a position with no row left
  is one that does not carry the tag. No other statement reads a tag.

  The condition's items are the decision's own, from both reads, and the framework's once.
  Its `after` is the first read's. So an event committed after the first read, the gap
  between the two reads included, fails the append if it matches, and any index fact does,
  whatever family it names.

  ## What decide answers

    - an event or a list of events, appended to the route's stream.
    - `{:streams, [{kind, identity, event_or_events}]}`, appended to several streams
      atomically. Each member lands on `<route prefix><identity>`, `kind` is that member's
      entity tag kind, an atom of the grammar of `stream_tag:` on the route, and `identity` a
      canonical UUID. Each member's events carry its own stream tag and not another's. Every
      scope tag has to land on some event of the batch, so the route's stream tag needs a
      member for the route's entity, with the route's kind, or an event whose own tags carry
      it.
    - `:duplicate`, which answers `{:error, :duplicate_command}` and appends nothing.
    - `{:error, reason}`, which is the answer as it stands and never retries.

  After a replay exit it may answer only `:duplicate` or `{:error, reason}`. Events answer
  `{:error, :reused_command_id}`: an event at or before `after` escapes the condition, so
  appending there could commit one `command_id` twice. `[]` and a return it may not give keep
  their own atoms.

  ## Creates

  `create:` is required on every decision. A decision whose command makes a fact, with the
  route identity as the fact's id, names the fact's event types:

      use Decree.Decision, create: [types: ["Elixir.EmailClaimed"]]

  The first read's items then start with `[types: types, tags: [stream_tag]]`, the stream tag
  being the route's. That is the decision's own item: it folds, it counts against `max_items`,
  and the condition holds it, so a racing create with another `command_id` fails its append.
  `c:query/2` may answer `[]`. Past the window a resend is a new command, and this item is what
  still finds the fact, so `c:decide/2` answers its own "already exists".

  What that item reads has to land. A create's batch needs at least one event whose type is
  declared and whose own tags hold the stream tag, or the dispatch answers
  `{:error, :batch_lacks_fact}` with nothing appended. One event has to do both: a declared
  type on another member and the stream tag on another event add up to nothing the item finds.
  A create that records a refusal as an event lists that type in `create:` too, so a later
  create folds it, and is otherwise refused the same way: the rule fails closed.

  Step 8 asks no event for the fact item's kinds, because the decision never wrote that item
  and the route's member carries the stream tag anyway. So a declared type on another member
  need not carry the route's kind. The digest comparison does read the item.

  Anything else says `create: false`. The item cannot be `replay: true`, which fails the build,
  and a `replay: true` item of `c:query/2` or `c:followup_query/3` naming the stream tag on a
  create answers `{:error, :invalid_query}` before that read is sent. So a create's replay item
  takes its scope tag from `c:scope_tags/1`.

  ## Replay safety

  A resend inside the window is answered from the window. Past it, or for a process that
  replays hours later, the command is new, so what makes it safe is a fact it can find. An
  aggregate keeps its four cases. A decision-owned fact has a fifth: a scoped lifetime tag
  query, with a `replay: true` item per command kind sharing the gate, no lower bound and no
  window:

      [types: ["Elixir.BoardGated"], tags: [key_tag, target_tag, "command_kind:CloseBoard"],
       replay: true]

  The item needs its `command_kind:` tag and a scope tag. `target_tag` is that one: past the
  window nothing else keeps another entity's gate out of the fold, so a target other than the
  route goes in `c:scope_tags/1`.

  It folds what the first run recorded, so `c:decide/2` can reproduce a refusal it recorded as
  an event, or answer `:duplicate`. A batch of a gate event and another answers from the gate.
  The same `command_id` on another kind or entity is still reused: the kind and the scope are
  checked before the item is.

  ## The command window

  `c:command_window_ms/0` is how long a command match counts as this command's, a positive
  integer of milliseconds. Decree has no default for it. It is read at step 7 of each attempt
  and only for a match, and anything else answers `{:error, :invalid_command_window}`.

  A match is inside when the read's `read_at` less its stamp is under the window. Both are the
  database's clock, never the host's, and the decision is the framework's: `c:evolve/2`,
  `c:admit/2` and `c:decide/2` see no time. What a decision sees is the outcome. Inside, a
  resend never reaches `c:decide/2` unless a replay item selected it. Outside, the match is
  not classified at all, and shows up only if the decision's own items matched it.

    - The edge is outside, a negative age is inside, and a nil stamp is outside.
    - A clock step or a failover between the stamp and the check shifts the window. This is a
      named residual: the window is a bound on a lost reply, not a proof of order.
    - After the window a reused id is a new command, and it commits.
    - A condition failure crossing the edge decides from the new read, whose window is the
      only one that counts.
    - An event of a deletable type, or a scope tag of a deletable kind, reads as reused and
      fails closed, because the read's tags leave them out.
    - A hard delete takes a stream's `command:` rows, so a resend after it is a new command.
      A hard delete of a stream linking a decision's events takes those events too, with their
      `command:` rows, wherever they were appended, so a resend after that is a new command
      as well.

  The scope is the route's stream tag and `c:scope_tags/1`, deduplicated. Each tag must be on
  some event of the matches inside the window, so a batch whose reply was lost is a duplicate
  though no event of it carries every tag, and a batch that lands must carry them the same way.
  A route entity that was a member of an earlier batch under the same id reads as that batch's
  duplicate, since its tag is on an event in the window, and nothing commits.

  ## Digest tags

  A condition on a digest holds only if the event it lets through carries that same digest:
  one derived with another key, or from another value, would land where the next read does
  not look. So a decision says which kinds hold digests:

      def index_families, do: %{"email" => :digest}

  For each kind marked `:digest`, the set of that kind's tags across every event of the
  batch must equal the set of that kind's tags on the condition's items, from both reads.
  Each kind is compared on its own. An empty batch side passes, because that is what an erased
  subject's dropped value leaves. That side is this comparison's alone: a scope tag has to land
  whatever it says, so a digest kind in `c:scope_tags/1` whose value dropped answers
  `{:error, :batch_lacks_scope_tag}`. A non-empty side with no such item is unequal. Otherwise
  the answer is `{:error, :digest_tag_mismatch}`, an atom and never the digest, with nothing
  appended and no retry. A kind marked `:plain` is not compared, and a decision with no
  `c:index_families/0` compares nothing. An answer that is not a map of tag kinds to `:digest`
  or `:plain` fails the build, because it would compare nothing and say nothing. So does one
  listing `command`, `command_kind`, `correlation`, `tag_index` or `tag_index_state`: a
  framework kind is never a digest, and the tag index's own kinds are in no family.

  A kind listed here, either way, is its own family to the tag index, and a `:digest` kind's
  family is keyed by the fingerprint its tags carry: see "Index facts".

  ## Index facts

  A condition finds an event by its tags, so it holds only while the tag rows are complete.
  Whether they are is a fact on the ledger, in events the application appends in the grammar
  of `Decree.Store.TagIndex`. Decree ships no such event. The dispatch reads the facts with its
  own items: the index item, `[tags: ["tag_index:fact"]]`, is a framework item of every read,
  in the condition, and never folded.

  What the reads require comes from their own items, both reads', a create's fact item and
  replay items included:

    - `command`, because a decision has a command item, though its query names no `command:`
      tag.
    - For each tag on an item, its kind's family: the kind itself when it is one of
      `Decree.Store.TagIndex.named_families/0` or a key of `c:index_families/0`, and `entity`
      otherwise. A `tag_index` or `tag_index_state` tag requires nothing, and nor do an item's
      types: the types index is not a tag family.
    - A kind listed `:digest` holds `<fingerprint>:<digest>`, and requires its family under
      that fingerprint. A value whose fingerprint does not read refuses: it never stands for
      the family with no key.

  The fold takes the facts the index item matched at or below the first read's `after`, in
  position order, with their tags from the reads, and the last state per family and key
  wins. A family and key with no fact is disabled. A fact with no state tag in the read, its
  rows gone or its type deletable, could have been a disable, so it refuses, and so does a
  fact naming one family and key both ways. A fact above `after`, which a second read can
  find, is not folded: the condition fails on it, and the retry reads it again. Any fact
  committed after `after` fails the condition, so a dispatch never appends past a disable it
  did not read, at the cost of a retry for a fact about another family.

  With every requirement enabled the dispatch goes on, and otherwise it answers
  `{:error, :tag_index_not_enabled}`, an atom naming no family and no digest. That is before
  the fold, `c:admit/2`, classification and `c:decide/2`, so a resend inside the window and a
  replay refuse as a new command does. Nothing is kept between dispatches: the facts are the
  dispatch's own read, never a cache or a read model.

  A `Decree.Decision.Rule` requires what its items do, with no `command`, and a rule whose
  `query/2` answers `[]` reads nothing and requires nothing.

  The fold sees a fact only through its `tag_index:fact` row, so the store keeps a fact whole:
  an append refuses a state tag without that row or the reverse, `delete_stream_tags` refuses
  both kinds, and a hard delete refuses a stream holding or linking a fact. A decree before
  this one does none of that. A node still running one can delete a fact's `tag_index:fact`
  row alone, or the whole fact, and an earlier enable then stands. So the first fact is a
  one-way cutover: append it only once no node runs an older decree and no delete started on
  one is still running, and while any fact exists never start a node on an older decree, by
  rollback, scaling or a stale image.

  ## Activation

      use Decree.Decision,
        create: false,
        activation: [types: ["Elixir.ClaimsSwitchedOn"], tags: ["feature:claims"]]

  `activation:` names the event that switches a decision on, and is optional. It is a
  framework item after the index item: read, in the condition, never folded and requiring
  nothing of the index. At step 3, after the index check, a dispatch with no event it matched
  at or below `after` answers `{:error, :decision_not_activated}`. One event is enough, and
  decree models no deactivation. One committed between the read and the append fails the
  condition, and the retry finds it. As any condition item does, it asks its tags' kinds of
  an event of a type it names, and the digest comparison reads its tags.

  It is `[types: types, tags: tags]`, in that order, each a non-empty list of non-empty
  strings and each tag one the store can hold, or the build fails. So does `replay:` in it,
  and so does `activation:` on a `Decree.Decision.Rule`.

  ## Personal fields

  New events are prepared as an aggregate's are, by `Decree.Event.Append.prepare_events/4`,
  once per member with that member's stream, so each envelope is bound to the stream it lands
  on. `{:error, :key_unknown}`, `{:error, :personal_subject_missing}` and
  `{:error, :undeclared_personal_metadata}` are the answer as they stand, with nothing
  appended. A retry prepares again from the plaintext, with fresh lookups.

  What a read answers is ciphertext unless the decision lists it, with the mechanism an
  aggregate uses:

      defmodule ClaimEmail do
        use Decree.Decision, create: false
        use Decree.PersonalFields.Aggregate

        @personal_fold [EmailClaimed]
      end

  A listed event reaches `c:evolve/2` and `c:followup_query/3` with its body decrypted, and
  an erased subject's field nil. Neither callback sees metadata, so that is not decrypted. An
  unlisted event reaches them as stored, envelopes included, so a decision that folds a value
  it never reads is not locked by one subject's lost key. A subject is looked up once in an
  attempt, and a retry looks it up again, so it sees a key erased between attempts.
  `@personal_state` and `@personal_state_subject` mean nothing on a decision, which has no
  snapshot.

  An item's tag goes to the store and to telemetry as it is, so `c:followup_query/3` names an
  id or a digest, never a value it decrypted.

  A listed event no key can read answers `{:error, :key_unknown}`, with nothing appended and
  no retry.

  A decision that uses `Decree.PersonalFields.Aggregate` can hold decrypted values whatever it
  lists. So a raise from any of its callbacks, or from preparing, reaches logs, telemetry,
  `after_failure` and the answer as a `Decree.PersonalFields.RedactedError` with a scrubbed
  stacktrace. A decision that doesn't use it keeps the exception.

  An `{:error, reason}` from `c:followup_query/3`, `c:admit/2` or `c:decide/2` goes the same
  places, so on such a decision `Decree.PersonalFields.RedactedError.reduce/1` reduces its
  reason first: a reason a handler matches on is an atom, or a tuple whose head is one, and
  the rest of a tuple is `:redacted`. `c:query/2` sees nothing decrypted, so its reason
  stands, as does every reason on a decision that doesn't use it. `Decree.Decision.Assertions`
  runs no dispatch and answers the callback's reason whole, where a handler gets the reduced
  one.

  ## Answers

  A raise from any callback, from a `Decree.Event.Tags` implementation, or from reading an
  event the reads matched, in the store's deserializing, the adapter's mapping or an upcaster,
  is rescued, and the dispatch answers `{:error, error}`, as a raise from `execute/2` does. An
  error from a `Decree.Event.Tags` implementation is the answer as it stands. A matched event
  of a type this release does not know (one appended by a newer release) answers
  `{:error, :unknown_event_type}`, with nothing appended and no retry: only an instance that
  knows the type can decide on it.

  An exit or a throw from any of it, on any decision, answers
  `{:error, :decision_execution_failed}`. It logs nothing but what
  `Decree.PersonalFields.RedactedError.reduce/1` leaves of its value, and it stays in the
  dispatch's task, because a crash report would print that task's arguments, which hold the
  command and its metadata.

  Two things retry: the store's `{:error, :append_condition_failed}`, and a command match
  only the second read has (step 2). Each runs the dispatch again from step 1 while
  `retry_attempts` lasts, and spent answers `{:error, :too_many_attempts}`, never a
  duplicate. A dispatch that overruns its timeout
  answers `{:error, :decision_execution_timeout}`, whether or not its append landed, and
  one whose task died answers `{:error, :decision_execution_failed}`.

  Eight refusals are named arms for an event handler, because a handler that dispatches a
  decision must stop or park on them rather than retry:

    - `:decision_query_too_large` - more condition items than `max_items`, more probes than
      `max_probes`, or more matched events than `max_events`.
    - `:decision_batch_too_large` - more new events than `max_batch_events`.
    - `:too_many_attempts` - retries are spent.
    - `:reused_command_id` - this `command_id` already left events inside the window, for
      another kind or entity, or a genuine resend's fold has moved past its gate and
      `c:decide/2` answers events on the replay exit. A handler's in-place retry resends the
      same id, so it stops or parks here instead of spinning.
    - `:key_unknown` - a personal value it reads or writes has no key to hand. Only the key
      coming back changes that.
    - `:unknown_event_type` - a matched event is of a type this release does not know. Only
      an instance that knows it changes that.
    - `:tag_index_not_enabled` - a family its items require has no enabled index, or a fact
      cannot be read. Only a fact changes that.
    - `:decision_not_activated` - its activation event is not there. Only that event changes
      that.

  The other refusals are a bug in a command or a decision: `:missing_command_id`,
  `:invalid_decision_identity`, `:invalid_query`, `:decision_returned_no_events` for `[]`,
  `:ok` or `nil` from `c:decide/2` and `[]` for a member, `:invalid_decision_return` for
  anything else it may not answer, `:invalid_decision_stream` for a malformed member,
  `:batch_lacks_scope_tag`, `:batch_lacks_fact`, `:invalid_scope_tags`,
  `:invalid_command_window`, `:digest_tag_mismatch` for an event whose digest is not the one
  the decision read, `:missing_required_tag` for an event lacking a kind it requires, and
  `:invalid_required_tag_kinds` for an event answering what is not a list, which preparing
  answers, or a list that is not kinds, which the store answers.

  ## Bounds

      use Decree.Decision,
        create: false,
        max_items: 16,
        max_probes: 48,
        max_events: 1_000,
        max_batch_events: 100,
        bound_reason: "..."

  Those bounds are the defaults. Lowering one is free. Raising one past its default needs a
  non-empty `bound_reason:`, and the build fails without it, because the default is what
  was measured.

    - `max_items` counts condition items, the framework's included: 14 of a decision's own
      plus the command item and the index item is 16, and 13 with an activation item. A
      condition's items run under the `$all` lock.
    - `max_probes` counts the probes `Decree.Store.Query` compiles the condition's items to,
      the framework's included, across both reads. An item is one probe per type, so one
      item of 30 types is one condition item and 30 probes. 48 is the size measured: about
      7% more hold than 16 single-type items at 16 writers, 1,139 µs against 1,067 µs. An
      item's further tags are checked per first-tag hit after `after`, so they are not
      counted.
    - `max_events` counts matched rows, both reads together, an event in both counted
      once, command matches and the facts the index item matches included, because a read
      returning `count` rows cannot be told from one that was cut.
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
      and `events` or `error`. On exception `kind` is `:error`, `:exit` or `:throw`, and
      `reason` and `stacktrace` are what "Answers" and "Personal fields" leave of them. As on
      an aggregate's span, `execution_context` holds the command and its metadata in the clear,
      `events` are the new events before they are encrypted, and `error` is the answer. The
      state is on none of these.
    - `[:decree, :event_store, :read_query]`, a span per read, covering the store's answer,
      its deserializing and mapping included, and not the upcast, which runs after it stops.
      Its exception event never fires: a failure inside it stops it with `error` in the stop
      metadata, the kind alone (`:error`, `:exit` or `:throw`), and is raised again after. A
      read that answered stops with no `error`. Either failure reaches the span above as
      "Answers" says.
    - `[:decree, :decision, :read]`, once per completed read cycle, one refused on
      `max_events` included, and none for one refused at step 3. Measurements `items`, `probes`, `matched_events`, `read_time`
      and `fold_time`, the last two in native units, `fold_time` 0 when refused. Metadata
      `application`, `decision` and `attempt`.
    - `[:decree, :decision, :retry]`, measurement `attempt`, the one that failed. Metadata
      `application`, `decision` and `reason`, `:append_condition_failed` or
      `:new_command_match`.
    - `[:decree, :decision, :refused]`, measurements `value` and `limit`. Metadata
      `application`, `decision` and `bound`, one of `:items`, `:probes`, `:events` and
      `:batch_events`.

  ## Testing

  `Decree.Decision.Assertions` runs a decision with no store.
  """

  alias Decree.Event.TagContext
  alias Decree.Store.{EventData, Query, TagIndex}

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

  @doc """
  How long, in milliseconds, a command match counts as this command's. See "The command window".
  """
  @callback command_window_ms() :: pos_integer()

  @doc """
  The tags a command's events must carry besides the route's stream tag. See "The command
  window". Defaults to `[]`.
  """
  @callback scope_tags(command()) :: [String.t()]

  @doc """
  What each tag kind this decision indexes holds, for example `%{"email" => :digest}`. See
  "Digest tags". Defaults to `%{}`. A static map: the build calls this as the module closes,
  so it can lean on nothing compiled later.
  """
  @callback index_families() :: %{String.t() => :digest | :plain}

  @optional_callbacks followup_query: 3, admit: 2, scope_tags: 1, index_families: 0

  @bounds [max_items: 16, max_probes: 48, max_events: 1_000, max_batch_events: 100]
  @options [:bound_reason, :create, :activation | Keyword.keys(@bounds)]

  # The kinds a step of its own reads, whatever a decision lists: a framework kind is never a
  # digest, and the tag index's own kinds are in no family.
  @unlisted_kinds ["command", "command_kind", "correlation" | TagIndex.reserved_kinds()]

  @doc false
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @behaviour Decree.Decision
      @before_compile Decree.Decision
      @after_compile Decree.Decision

      @decree_decision_bounds Decree.Decision.__bounds__!(__MODULE__, opts)
      @decree_decision_create Decree.Decision.__create__!(__MODULE__, opts)
      @decree_decision_activation Decree.Decision.__activation__!(__MODULE__, opts)

      @doc false
      def admit(_state, _command), do: :ok

      @doc false
      def scope_tags(_command), do: []

      defoverridable admit: 2, scope_tags: 1
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

    if not Module.defines?(module, {:command_window_ms, 0}, :def) do
      raise ArgumentError,
            "#{inspect(module)} uses Decree.Decision and does not define " <>
              "`command_window_ms/0`: decree has no default for it"
    end

    followup_query? = Module.defines?(module, {:followup_query, 3}, :def)

    quote do
      unquote(__index_families__(module))

      @doc false
      def __decision__ do
        Map.merge(@decree_decision_bounds, %{
          create: @decree_decision_create,
          activation: @decree_decision_activation,
          followup_query?: unquote(followup_query?),
          index_families: index_families()
        })
      end
    end
  end

  # The fallback, for a module's `@before_compile`, a `Decree.Decision.Rule`'s too. Here and not
  # in `__using__/1`: a default from the module body is final before any `@before_compile` runs,
  # so a later hook's `def` would be a second clause that never matches. One made overridable
  # here gives way to it.
  @doc false
  def __index_families__(module) do
    if not Module.defines?(module, {:index_families, 0}, :def) do
      quote do
        @doc false
        def index_families, do: %{}

        defoverridable index_families: 0
      end
    end
  end

  # A `Decree.Decision.Rule` runs this check too, by naming this module in its `@after_compile`.
  # A key no tag can have, or a family no comparison reads, compares nothing and says so
  # nowhere: a digest check that passes because it never ran. A raise from the callback
  # leaves through here and fails the build too. Called with no export test, because under the
  # compiler the module is not loaded here, so `function_exported?/3` would answer no and let
  # every bad declaration through.
  @doc false
  def __after_compile__(%Macro.Env{module: module}, _bytecode) do
    families = module.index_families()

    if not index_families?(families) do
      raise ArgumentError,
            "#{inspect(module)} has an invalid `index_families/0`: #{inspect(families)}, " <>
              "expected a map of tag kinds to `:digest` or `:plain`"
    end

    with [kind | _] <- Enum.filter(Map.keys(families), &(&1 in @unlisted_kinds)) do
      raise ArgumentError,
            "#{inspect(module)} lists `#{kind}` in `index_families/0`: a framework kind is " <>
              "never a digest, and a tag index kind is in no family"
    end
  end

  defp index_families?(families) when is_map(families) do
    Query.tag_kinds?(Map.keys(families)) and
      Enum.all?(Map.values(families), &(&1 in [:digest, :plain]))
  end

  defp index_families?(_families), do: false

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

  @doc false
  def __create__!(module, opts) do
    case Keyword.fetch(opts, :create) do
      {:ok, false} ->
        false

      {:ok, create} ->
        create_types!(module, create)

      :error ->
        raise ArgumentError,
              "#{inspect(module)} has no `create:` option, expected `create: [types: [...]]` " <>
                "for a command that makes a fact, or `create: false`"
    end
  end

  defp create_types!(module, create) do
    cond do
      Keyword.keyword?(create) and Keyword.has_key?(create, :replay) ->
        raise ArgumentError,
              "#{inspect(module)} has `replay:` in `create:`: the fact item is what a resend " <>
                "past the window still finds, so it cannot be a replay item"

      match?([types: _types], create) and types?(create[:types]) ->
        create[:types]

      true ->
        raise ArgumentError,
              "#{inspect(module)} has an invalid `create`: #{inspect(create)}, expected " <>
                "`false` or `[types: types]`, a non-empty list of non-empty strings"
    end
  end

  @doc false
  def __activation__!(module, opts) do
    case Keyword.fetch(opts, :activation) do
      :error ->
        nil

      {:ok, activation} ->
        activation_item!(module, activation)
    end
  end

  defp activation_item!(module, activation) do
    cond do
      Keyword.keyword?(activation) and Keyword.has_key?(activation, :replay) ->
        raise ArgumentError,
              "#{inspect(module)} has `replay:` in `activation:`: an activation is found, " <>
                "never replayed"

      match?([types: _types, tags: _tags], activation) and types?(activation[:types]) and
          tags?(activation[:tags]) ->
        activation

      true ->
        raise ArgumentError,
              "#{inspect(module)} has an invalid `activation`: #{inspect(activation)}, " <>
                "expected `[types: types, tags: tags]`, each a non-empty list of non-empty " <>
                "strings, the tags no longer than a tag may be"
    end
  end

  defp tags?([_tag | _tags] = tags) do
    not List.improper?(tags) and
      Enum.all?(tags, fn tag ->
        EventData.valid_tag?(tag) and tag != "" and byte_size(tag) <= EventData.max_tag_bytes()
      end)
  end

  defp tags?(_tags), do: false

  defp types?([_type | _types] = types) do
    not List.improper?(types) and Enum.all?(types, &(is_binary(&1) and &1 != ""))
  end

  defp types?(_types), do: false

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
