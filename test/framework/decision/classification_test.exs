defmodule Decree.Decision.ClassificationTest do
  use Decree.DecisionStoreCase

  alias Decree.DecisionFixtures.{
    DeletableApp,
    DeletableStreamsApp,
    ParityAggregateCommand,
    Secret
  }

  alias Decree.Event.Append
  alias Decree.Store.{Config, EventData, LockSession}

  defmodule Kindless do
    defstruct [:command_id, :_id, script: %{}]
  end

  defmodule KindlessRouter do
    use Decree.Commands.Router

    decide(Kindless,
      to: Decree.DecisionFixtures.ScriptedDecision,
      identity: :_id,
      identity_prefix: "scripted-"
    )
  end

  defmodule RefusingCreate do
    defstruct [:command_id, :claim_id, :scope, script: %{}]
  end

  # A create that records a refusal as an event, so `create:` lists that type beside the fact's.
  defmodule RefusingCreateDecision do
    use Decree.DecisionFixtures.Scripting,
      create: [
        types: [
          "Elixir.Decree.DecisionFixtures.Claimed",
          "Elixir.Decree.DecisionFixtures.Released"
        ]
      ]
  end

  defmodule RefusingCreateRouter do
    use Decree.Commands.Router

    decide(RefusingCreate,
      to: RefusingCreateDecision,
      identity: :claim_id,
      identity_prefix: "create-"
    )
  end

  @retry [:decree, :decision, :retry]
  @read_query [:decree, :event_store, :read_query, :start]
  @append [:decree, :event_store, :append_to_streams, :start]
  @an_hour 3_600

  defp stream(command), do: "scripted-" <> command.claim_id

  defp data(recorded), do: Enum.map(recorded, & &1.data)

  defp dispatch_async(command, opts \\ []) do
    Task.async(fn -> App.dispatch(command, [timeout: 60_000] ++ opts) end)
  end

  # A `replay: true` item of the shape the design gives: a key, the target and the kind.
  defp gate(command) do
    [
      types: [Atom.to_string(Claimed)],
      tags: ["scope:" <> command.scope, "claim:" <> command.claim_id, Append.kind_tag(command)],
      replay: true
    ]
  end

  # Decides `first` when the fold is empty, and `resent` when a resend finds what it landed.
  defp first_then(first, resent) do
    fn
      [], command -> first.(command)
      [_landed | _], command -> resent.(command)
    end
  end

  defp claimed(command, note \\ nil) do
    %Claimed{claim_id: command.claim_id, scope: command.scope, note: note}
  end

  defp holding_decide(test, label) do
    fn _state, command ->
      hold(test, {:deciding, label})

      claimed(command)
    end
  end

  describe "a lost response" do
    test "commit, resend, `:duplicate_command`, one event" do
      command = scripted()

      assert :ok = App.dispatch(command)
      assert {:error, :duplicate_command} = App.dispatch(command)

      assert [_one] = stored(stream(command))
    end
  end

  describe "CC-8, an id reused after the window" do
    setup do
      command_id = UUID.uuid4()
      old = scripted(command_id: command_id)

      assert :ok = App.dispatch(old)

      age_stamps(command_id, @an_hour)

      [command_id: command_id, old: old]
    end

    test "commits for another entity, and the old match does not reach `evolve`", context do
      attach_telemetry([[:decree, :decision, :read]])

      test = self()

      decide = fn state, command ->
        send(test, {:state, state})

        claimed(command)
      end

      fresh = scripted(command_id: context.command_id, decide: decide)

      assert :ok = App.dispatch(fresh)

      # The read held the old match, so the filter is what kept it from the fold. The facts are
      # read by position, so the old match is the one matched event.
      matched = 1

      assert_received {:telemetry, [:decree, :decision, :read], %{matched_events: ^matched},
                       _metadata}

      assert_received {:state, []}
      assert [_one] = stored(stream(fresh))
      assert [_one] = stored(stream(context.old))
    end

    test "a resend of the new one is a duplicate", context do
      fresh = scripted(command_id: context.command_id)

      assert :ok = App.dispatch(fresh)
      assert {:error, :duplicate_command} = App.dispatch(fresh)
      assert [_one] = stored(stream(fresh))
    end

    test "then a resend of the old request, in the new one's window, is reused", context do
      fresh = scripted(command_id: context.command_id)

      assert :ok = App.dispatch(fresh)
      assert {:error, :reused_command_id} = App.dispatch(context.old)

      assert [_one] = stored(stream(context.old))
    end
  end

  describe "a replay item" do
    test "CC-10, a recorded refusal with its reply lost: `decide` reproduces it" do
      decide =
        first_then(&claimed(&1, "refused"), fn _command -> {:error, :recorded_refusal} end)

      command = scripted(query: fn command, _context -> [gate(command)] end, decide: decide)

      assert :ok = App.dispatch(command)
      assert {:error, :recorded_refusal} = App.dispatch(command)

      assert [%Claimed{note: "refused"}] = data(stored(stream(command)))
    end

    test "CC-15, a batch of one gate event and one other, same id: the gate answers" do
      first = fn command ->
        [claimed(command, "gate"), %Released{scope: "elsewhere"}]
      end

      decide = first_then(first, fn _command -> {:error, :from_the_gate} end)

      command = scripted(query: fn command, _context -> [gate(command)] end, decide: decide)

      assert :ok = App.dispatch(command)
      assert {:error, :from_the_gate} = App.dispatch(command)

      assert [%Claimed{note: "gate"}, %Released{}] = data(stored(stream(command)))
    end

    test "the kind and the scope come before it" do
      scope = scope()
      first = scripted(scope: scope)

      # Selects the first request's event, of this kind, from another entity.
      shared = [
        types: [Atom.to_string(Claimed)],
        tags: ["scope:" <> scope, Append.kind_tag(first)],
        replay: true
      ]

      reuse =
        scripted(
          command_id: first.command_id,
          scope: scope,
          scope_tags: fn _command -> ["scope:" <> scope] end,
          query: fn _command, _context -> [shared] end,
          decide: fn _state, _command -> {:error, :reached_decide} end
        )

      assert :ok = App.dispatch(first)
      assert {:error, :reused_command_id} = App.dispatch(reuse)
    end
  end

  describe "the replay exit" do
    setup do
      attach_telemetry([@append])
      :ok
    end

    defp replaying(resent) do
      scripted(
        query: fn command, _context -> [gate(command)] end,
        decide: first_then(&claimed/1, resent)
      )
    end

    defp assert_replay_answers(command, answer) do
      assert :ok = App.dispatch(command)
      assert_received {:telemetry, @append, _, _}

      assert App.dispatch(command) == answer

      refute_received {:telemetry, @append, _, _}
      assert [_only_the_first] = stored(stream(command))
    end

    test "an event answers `:reused_command_id`, with nothing stored" do
      assert_replay_answers(replaying(&claimed/1), {:error, :reused_command_id})
    end

    test "a list answers `:reused_command_id`, with nothing stored" do
      assert_replay_answers(
        replaying(&[claimed(&1), claimed(&1)]),
        {:error, :reused_command_id}
      )
    end

    test "`{:streams, ...}` answers `:reused_command_id`, with nothing stored" do
      resent = fn command ->
        {:streams, [{:claim, command.claim_id, claimed(command)}]}
      end

      assert_replay_answers(replaying(resent), {:error, :reused_command_id})
    end

    test "`:duplicate` answers `:duplicate_command`" do
      assert_replay_answers(replaying(fn _c -> :duplicate end), {:error, :duplicate_command})
    end

    test "`{:error, reason}` stands" do
      assert_replay_answers(replaying(fn _c -> {:error, :no} end), {:error, :no})
    end

    test "`[]` keeps its own atom" do
      assert_replay_answers(
        replaying(fn _c -> [] end),
        {:error, :decision_returned_no_events}
      )
    end

    test "an invalid return keeps its own atom" do
      assert_replay_answers(replaying(fn _c -> :nothing end), {:error, :invalid_decision_return})
    end
  end

  describe "a retry" do
    setup do
      attach_telemetry([@retry])
      :ok
    end

    test "spent answers `:too_many_attempts`, never a duplicate" do
      test = self()
      command = scripted(decide: holding_decide(test, :held))

      task = dispatch_async(command, retry_attempts: 0)

      assert_receive {{:deciding, :held}, pid}

      assert :ok = App.dispatch(%{command | script: %{}})
      send(pid, :go)

      assert {:error, :too_many_attempts} = Task.await(task, :infinity)
      assert [_one] = stored(stream(command))
    end

    test "landing on a first copy, same id and same entity, answers a duplicate" do
      test = self()
      command = scripted(decide: holding_decide(test, :held))

      task = dispatch_async(command)

      assert_receive {{:deciding, :held}, pid}

      assert :ok = App.dispatch(%{command | script: %{}})
      send(pid, :go)

      assert {:error, :duplicate_command} = Task.await(task, :infinity)

      assert_received {:telemetry, @retry, %{attempt: 1}, %{reason: :append_condition_failed}}
      assert [_one] = stored(stream(command))
    end
  end

  describe "a reused id" do
    test "two requests, one id, different entities, sequential" do
      first = scripted()
      second = scripted(command_id: first.command_id)

      assert :ok = App.dispatch(first)
      assert {:error, :reused_command_id} = App.dispatch(second)

      assert stored(stream(second)) == []
    end

    test "different kind, same id and same entity" do
      first = scripted()
      second = scripted([command_id: first.command_id, claim_id: first.claim_id], Tight)

      assert :ok = App.dispatch(first)
      assert {:error, :reused_command_id} = App.dispatch(second)

      assert stored("tight-" <> second.claim_id) == []
    end

    test "an aggregate's event first, with the full framework tags of another kind" do
      aggregate = %ParityAggregateCommand{
        command_id: UUID.uuid4(),
        id: UUID.uuid4(),
        scope: "s"
      }

      second = scripted(command_id: aggregate.command_id)

      assert :ok = App.dispatch(aggregate)
      assert {:error, :reused_command_id} = App.dispatch(second)

      assert stored(stream(second)) == []
    end
  end

  describe "a reused id racing, by lock state" do
    setup do
      attach_telemetry([@retry])

      postgrex_config = Config.default_postgrex_opts(Decree.FrameworkStorage.config())

      [observer: LockSession.connect(postgrex_config), holder: LockSession.start(postgrex_config)]
    end

    # An uncommitted append holds `$all`.
    defp hold_all(%LockSession{} = holder) do
      store = Module.concat([App, "FrameworkEventStore"])

      event = %EventData{
        event_type: Atom.to_string(Released),
        data: %Released{scope: "holder"},
        metadata: %{}
      }

      LockSession.run(holder, fn tx ->
        :ok =
          Decree.FrameworkEventStore.append_to_stream(
            "other-" <> UUID.uuid4(),
            :any_version,
            [event],
            name: store,
            conn: tx
          )
      end)
    end

    defp tagged_rows(observer, tag) do
      %Postgrex.Result{rows: [[count]]} =
        Postgrex.query!(observer, "SELECT count(*) FROM event_tags WHERE tag = $1", [tag])

      count
    end

    test "exactly one request commits, and the other's retry reads it as reused", context do
      %{holder: holder, observer: observer} = context

      first = scripted()
      second = scripted(command_id: first.command_id)

      hold_all(holder)

      one = dispatch_async(first)
      other = dispatch_async(second)

      # Both are at the append while `$all` is held, so neither read can have seen the other.
      LockSession.wait_until_waiting(observer, holder.backend, 2)

      assert tagged_rows(observer, "command:" <> first.command_id) == 0

      assert {:ok, :ok} = LockSession.commit(holder)

      answers = [Task.await(one, :infinity), Task.await(other, :infinity)]

      assert Enum.sort(answers) == Enum.sort([:ok, {:error, :reused_command_id}])

      assert_received {:telemetry, @retry, %{attempt: 1}, %{reason: :append_condition_failed}}
      refute_received {:telemetry, @retry, _, _}

      assert [_one] = stored(stream(first)) ++ stored(stream(second))
    end
  end

  describe "a reused id across the window's edge" do
    setup do
      attach_telemetry([@retry])
      :ok
    end

    test "both read `:open`, one commits, and the other's re-read is reused" do
      test = self()
      command_id = UUID.uuid4()

      assert :ok = App.dispatch(scripted(command_id: command_id))

      age_stamps(command_id, @an_hour)

      one = scripted(command_id: command_id, decide: holding_decide(test, :one))
      other = scripted(command_id: command_id, decide: holding_decide(test, :other))

      one_task = dispatch_async(one)
      other_task = dispatch_async(other)

      # `decide` is reached only from `:open`, so each read classified the old match outside.
      assert_receive {{:deciding, :one}, one_pid}
      assert_receive {{:deciding, :other}, other_pid}

      send(one_pid, :go)
      assert :ok = Task.await(one_task, :infinity)

      send(other_pid, :go)
      assert {:error, :reused_command_id} = Task.await(other_task, :infinity)

      assert_received {:telemetry, @retry, %{attempt: 1}, %{reason: :append_condition_failed}}

      assert [_one] = stored(stream(one))
      assert stored(stream(other)) == []
    end

    test "a condition failure crossing the edge decides from the new read, as a new command" do
      test = self()
      calls = :counters.new(1, [])

      # Exactly the ageing below. The first `read_at` precedes the winner's real stamp, so from
      # it the age is under the window, and only the retry's fresh one reads as outside.
      with_window(@an_hour * 1_000)

      # The count crosses into the dispatch's task, so the retry's `query` is what holds.
      query = fn command, _context ->
        :counters.add(calls, 1, 1)

        if :counters.get(calls, 1) == 2, do: hold(test, :retrying)

        [claimed_item(command.scope)]
      end

      decide = fn _state, command ->
        if :counters.get(calls, 1) == 1, do: hold(test, :deciding)

        claimed(command)
      end

      held = scripted(query: query, decide: decide)
      winner = scripted(command_id: held.command_id)

      task = dispatch_async(held)

      assert_receive {:deciding, pid}, 5_000

      # Another entity, so only the unscoped command item fails `held`'s condition.
      assert :ok = App.dispatch(winner)
      send(pid, :go)

      assert_receive {:retrying, ^pid}, 5_000

      age_stamps(winner.command_id, @an_hour)
      send(pid, :go)

      assert :ok = Task.await(task, :infinity)

      assert_received {:telemetry, @retry, %{attempt: 1}, %{reason: :append_condition_failed}}
      refute_received {:telemetry, @retry, _, _}

      assert [_one] = stored(stream(held))
      assert [_one] = stored(stream(winner))
    end
  end

  describe "admission" do
    test "comes first: a caller failing `admit/2` gets its refusal, not a duplicate" do
      command = scripted()

      assert :ok = App.dispatch(command)

      refused = %{command | script: %{admit: fn _state, _command -> {:error, :not_allowed} end}}

      assert {:error, :not_allowed} = App.dispatch(refused)
    end

    test "comes first: a caller failing `admit/2` gets its refusal, not a reused id" do
      first = scripted()

      second =
        scripted(
          command_id: first.command_id,
          admit: fn _state, _command -> {:error, :not_allowed} end
        )

      assert :ok = App.dispatch(first)
      assert {:error, :not_allowed} = App.dispatch(second)
    end
  end

  describe "F12, a create" do
    setup do
      attach_telemetry([@read_query, @retry])
      :ok
    end

    defp create(opts \\ []) do
      decide = first_then(&claimed/1, fn _command -> {:error, :already_exists} end)

      scripted([query: fn _command, _context -> [] end, decide: decide] ++ opts, Create)
    end

    defp fact_stream(command), do: "create-" <> command.claim_id

    defp fact_item(command) do
      [types: [Atom.to_string(Claimed)], tags: ["claim:" <> command.claim_id]]
    end

    test "resent in the window is a duplicate, and after it its own already-exists" do
      command = create()

      assert :ok = App.dispatch(command)
      assert {:error, :duplicate_command} = App.dispatch(command)

      age_stamps(command.command_id, @an_hour)

      assert {:error, :already_exists} = App.dispatch(command)

      assert [_one] = stored(fact_stream(command))
    end

    test "a second create of the fact with its own id answers already-exists" do
      command = create()

      assert :ok = App.dispatch(command)

      assert {:error, :already_exists} =
               App.dispatch(%{command | command_id: UUID.uuid4()})
    end

    test "the fact item leads the first read, and `query` may answer `[]`" do
      command = create()

      assert :ok = App.dispatch(command)

      assert_received {:telemetry, @read_query, _, %{items: items}}

      assert items == [
               fact_item(command),
               [tags: ["command:" <> command.command_id]],
               [tags: ["tag_index:fact"]]
             ]
    end

    test "the fact item leads `query`'s items, in the first read only" do
      item = claimed_item("own")
      followup = claimed_item("followup")

      command =
        create(
          query: fn _command, _context -> [item] end,
          followup: fn _command, _context, _events -> [followup] end
        )

      assert :ok = App.dispatch(command)

      command_item = [tags: ["command:" <> command.command_id]]
      index_item = [tags: ["tag_index:fact"]]

      assert_received {:telemetry, @read_query, _, %{items: first}}
      assert first == [fact_item(command), item, command_item, index_item]

      assert_received {:telemetry, @read_query, _, %{items: second}}
      assert second == [followup, command_item, index_item]
    end

    test "the condition holds it: a racing create with another id fails its append" do
      test = self()

      decide = fn
        [], command ->
          hold(test, :deciding)

          claimed(command)

        [_fact | _], _command ->
          {:error, :already_exists}
      end

      command = create(decide: decide)

      task = dispatch_async(command)

      assert_receive {:deciding, pid}

      assert :ok = App.dispatch(create(claim_id: command.claim_id))
      send(pid, :go)

      assert {:error, :already_exists} = Task.await(task, :infinity)

      assert_received {:telemetry, @retry, %{attempt: 1}, %{reason: :append_condition_failed}}
      assert [_one] = stored(fact_stream(command))
    end

    test "it counts against `max_items`" do
      own = Enum.map(1..13, &claimed_item("item-#{&1}"))
      over = Enum.map(1..14, &claimed_item("item-#{&1}"))

      assert :ok = App.dispatch(create(query: fn _c, _t -> own end))

      assert {:error, :decision_query_too_large} =
               App.dispatch(create(query: fn _c, _t -> over end))
    end

    test "a `replay: true` item on the fact tag is `:invalid_query`, with no statement" do
      query = fn command, _context ->
        [
          [
            types: [Atom.to_string(Claimed)],
            tags: ["claim:" <> command.claim_id, Append.kind_tag(command)],
            replay: true
          ]
        ]
      end

      assert {:error, :invalid_query} = App.dispatch(create(query: query))

      refute_received {:telemetry, @read_query, _, _}
    end

    test "a follow-up's `replay: true` item on the fact tag sends no second read" do
      followup = fn command, _context, _events ->
        [
          [
            types: [Atom.to_string(Claimed)],
            tags: ["claim:" <> command.claim_id, Append.kind_tag(command)],
            replay: true
          ]
        ]
      end

      assert {:error, :invalid_query} = App.dispatch(create(followup: followup))

      assert_received {:telemetry, @read_query, _, _}
      refute_received {:telemetry, @read_query, _, _}
    end

    test "a `replay: true` item on the fact tag is refused with a scope tag beside it" do
      query = fn command, _context ->
        [
          [
            types: [Atom.to_string(Claimed)],
            tags: [
              "claim:" <> command.claim_id,
              "scope:" <> command.scope,
              Append.kind_tag(command)
            ],
            replay: true
          ]
        ]
      end

      command = create(query: query, scope_tags: &["scope:" <> &1.scope])

      assert {:error, :invalid_query} = App.dispatch(command)

      refute_received {:telemetry, @read_query, _, _}
    end

    test "a `replay: true` item's scope tag has to come from `scope_tags/1`" do
      query = fn command, _context ->
        [
          [
            types: [Atom.to_string(Claimed)],
            tags: ["scope:" <> command.scope, Append.kind_tag(command)],
            replay: true
          ]
        ]
      end

      assert {:error, :invalid_query} = App.dispatch(create(query: query))

      refute_received {:telemetry, @read_query, _, _}

      assert :ok = App.dispatch(create(query: query, scope_tags: &["scope:" <> &1.scope]))
    end

    defp released(command), do: %Released{claim_id: command.claim_id, scope: command.scope}

    test "an undeclared type is refused for every `command_id`, with nothing stored" do
      attach_telemetry([@append])

      command = create(decide: fn _state, command -> released(command) end)

      assert {:error, :batch_lacks_fact} = App.dispatch(command)

      assert {:error, :batch_lacks_fact} =
               App.dispatch(%{command | command_id: UUID.uuid4()})

      refute_received {:telemetry, @append, _, _}
      assert stored(fact_stream(command)) == []
    end

    test "the declared type on another member, the route tag on another type, is refused" do
      attach_telemetry([@append])

      other = UUID.uuid4()

      decide = fn _state, command ->
        {:streams,
         [
           {:claim, command.claim_id, released(command)},
           {:claim, other, claimed(command)}
         ]}
      end

      command = create(decide: decide)

      assert {:error, :batch_lacks_fact} = App.dispatch(command)

      refute_received {:telemetry, @append, _, _}
      assert stored(fact_stream(command)) == []
      assert stored("create-" <> other) == []
    end

    test "the declared type on the route commits, beside any other event" do
      route = create(decide: fn _state, command -> claimed(command) end)
      batch = create(decide: fn _state, command -> [released(command), claimed(command)] end)
      other = UUID.uuid4()

      members =
        create(
          decide: fn _state, command ->
            {:streams,
             [
               {:claim, other, released(command)},
               {:claim, command.claim_id, claimed(command)}
             ]}
          end
        )

      assert :ok = App.dispatch(route)
      assert :ok = App.dispatch(batch)
      assert :ok = App.dispatch(members)

      assert [%Claimed{}] = data(stored(fact_stream(route)))
      assert [%Released{}, %Claimed{}] = data(stored(fact_stream(batch)))
      assert [%Claimed{}] = data(stored(fact_stream(members)))
      assert [%Released{}] = data(stored("create-" <> other))
    end

    test "a refusal recorded as an event commits when declared, and a later create folds it" do
      decide = first_then(&released/1, fn _command -> {:error, :refused_before} end)

      command = scripted([query: fn _c, _t -> [] end, decide: decide], RefusingCreate)

      assert :ok = RefusingCreateRouter.dispatch(command, application: App)

      assert {:error, :refused_before} =
               RefusingCreateRouter.dispatch(%{command | command_id: UUID.uuid4()},
                 application: App
               )

      assert [%Released{}] = data(stored(fact_stream(command)))
    end

    test "`:batch_lacks_fact` does not retry" do
      command = create(decide: fn _state, command -> released(command) end)

      assert {:error, :batch_lacks_fact} = App.dispatch(command)

      assert_received {:telemetry, @read_query, _, _}
      refute_received {:telemetry, @read_query, _, _}
      refute_received {:telemetry, @retry, _, _}
    end

    test "a `query` that is no list stays `:invalid_query`" do
      for answer <- [:nothing, nil, %{}] do
        assert {:error, :invalid_query} =
                 App.dispatch(create(query: fn _c, _t -> answer end))
      end

      refute_received {:telemetry, @read_query, _, _}
    end

    test "`query` answering `[]` still refuses a decision that is no create" do
      command = scripted(query: fn _c, _t -> [] end)

      assert {:error, :invalid_query} = App.dispatch(command)
    end
  end

  describe "Q-7, the shape of a replay item" do
    setup do
      attach_telemetry([@read_query])
      :ok
    end

    test "with no `command_kind:` tag is `:invalid_query`" do
      item = [types: ["T"], tags: ["scope:x", "claim:y"], replay: true]

      assert {:error, :invalid_query} = App.dispatch(scripted(query: fn _c, _t -> [item] end))

      refute_received {:telemetry, @read_query, _, _}
    end

    test "with only that tag is `:invalid_query`" do
      item = [types: ["T"], tags: ["command_kind:Elixir.A"], replay: true]

      assert {:error, :invalid_query} = App.dispatch(scripted(query: fn _c, _t -> [item] end))

      refute_received {:telemetry, @read_query, _, _}
    end

    test "holds for a follow-up's items too" do
      item = [types: ["T"], tags: ["scope:x"], replay: true]

      assert {:error, :invalid_query} =
               App.dispatch(scripted(followup: fn _c, _t, _e -> [item] end))
    end

    defp replay_on(command, tag) do
      [types: [Atom.to_string(Claimed)], tags: [tag, Append.kind_tag(command)], replay: true]
    end

    test "naming the kind and no scope tag is `:invalid_query`, with no statement" do
      query = fn command, _context -> [replay_on(command, "correlation:shared")] end

      assert {:error, :invalid_query} = App.dispatch(scripted(query: query))

      refute_received {:telemetry, @read_query, _, _}
    end

    test "naming the kind and no scope tag in a follow-up sends no second read" do
      followup = fn command, _context, _events -> [replay_on(command, "correlation:shared")] end

      assert {:error, :invalid_query} = App.dispatch(scripted(followup: followup))

      assert_received {:telemetry, @read_query, _, _}
      refute_received {:telemetry, @read_query, _, _}
    end

    test "naming a tag of `scope_tags/1` passes, in either read" do
      query = fn command, _context -> [replay_on(command, "scope:" <> command.scope)] end
      followup = fn command, _context, _events -> query.(command, nil) end
      scope_tags = &["scope:" <> &1.scope]

      assert :ok = App.dispatch(scripted(query: query, scope_tags: scope_tags))
      assert :ok = App.dispatch(scripted(followup: followup, scope_tags: scope_tags))
    end

    test "another entity's gate, out of the window, cannot be read by what they share" do
      first = scripted(correlation_id: "shared")

      assert :ok = App.dispatch(first)
      assert_received {:telemetry, @read_query, _, _}

      age_stamps(first.command_id, @an_hour)

      second =
        scripted(
          command_id: first.command_id,
          correlation_id: "shared",
          query: fn command, _context -> [replay_on(command, "correlation:shared")] end,
          decide: fn _state, _command -> {:error, :folded_the_gate} end
        )

      assert {:error, :invalid_query} = App.dispatch(second)

      refute_received {:telemetry, @read_query, _, _}
      assert stored(stream(second)) == []
    end
  end

  describe "UF-1, a decision's item takes no key:" do
    setup do
      attach_telemetry([@read_query])
      :ok
    end

    test "UF-1: `query/2` answering an item with `key:` is `:invalid_query`, with no statement" do
      query = fn command, _context -> [claimed_item(command.scope) ++ [key: :scope]] end

      assert {:error, :invalid_query} = App.dispatch(scripted(query: query))

      refute_received {:telemetry, @read_query, _, _}

      # Valid but for the key: the same item with none reads and appends.
      plain = fn command, _context -> [claimed_item(command.scope)] end

      assert :ok = App.dispatch(scripted(query: plain))
    end

    test "UF-1: `followup_query/3` answering an item with `key:` sends no second read" do
      followup = fn command, _context, _events ->
        [command.scope |> claimed_item() |> Map.new() |> Map.put(:key, :scope)]
      end

      assert {:error, :invalid_query} = App.dispatch(scripted(followup: followup))

      assert_received {:telemetry, @read_query, _, _}
      refute_received {:telemetry, @read_query, _, _}

      plain = fn command, _context, _events -> [Map.new(claimed_item(command.scope))] end

      assert :ok = App.dispatch(scripted(followup: plain))
    end
  end

  describe "a hard delete on a store where it can reach a decision's stream" do
    setup do
      start_supervised!({DeletableStreamsApp, []})

      :ok
    end

    # `Claimed` is declared, so no item may name it.
    defp undeclared(command) do
      [[types: [Atom.to_string(Released)], tags: ["scope:" <> command.scope]]]
    end

    # This ends deduplication early, by ST-6: the delete takes the stream's `command:` rows, so
    # nothing is left to match, and the resend is a new command with a new event.
    test "a resend after it commits a new event" do
      command = scripted(query: fn command, _context -> undeclared(command) end)
      store = Module.concat([DeletableStreamsApp, "FrameworkEventStore"])

      assert :ok = DeletableStreamsApp.dispatch(command)
      assert [_landed] = stored(stream(command), DeletableStreamsApp)

      assert :ok =
               Decree.FrameworkEventStore.delete_stream(stream(command), :any_version, :hard,
                 name: store
               )

      assert stored(stream(command), DeletableStreamsApp) == []

      assert :ok = DeletableStreamsApp.dispatch(command)
      assert [_new] = stored(stream(command), DeletableStreamsApp)
    end

    test "a command match on a deletable-type event reads as reused, failing closed" do
      command = scripted(query: fn command, _context -> undeclared(command) end)

      assert :ok = DeletableStreamsApp.dispatch(command)

      assert {:error, :reused_command_id} = DeletableStreamsApp.dispatch(command)
      assert [_one] = stored(stream(command), DeletableStreamsApp)
    end
  end

  describe "a scope tag of a deletable kind" do
    setup do
      start_supervised!({DeletableApp, []})

      :ok
    end

    test "reads as reused on a resend, failing closed" do
      command =
        scripted(
          scope_tags: fn _command -> ["secret:1"] end,
          decide: fn _state, _command -> %Secret{secret: "1"} end
        )

      assert :ok = DeletableApp.dispatch(command)

      assert {:error, :reused_command_id} = DeletableApp.dispatch(command)
      assert [_one] = stored(stream(command), DeletableApp)
    end
  end

  describe "`command_window_ms/0`" do
    setup do
      attach_telemetry([@append])
      :ok
    end

    for window <- [0, -1, 1.5, "1", nil] do
      test "#{inspect(window)} is `:invalid_command_window`, with nothing appended" do
        command = scripted()

        assert :ok = App.dispatch(command)
        assert_received {:telemetry, @append, _, _}

        with_window(unquote(window))

        assert {:error, :invalid_command_window} = App.dispatch(command)

        refute_received {:telemetry, @append, _, _}
        assert [_one] = stored(stream(command))
      end
    end

    test "is not read without a match" do
      with_window(0)

      assert :ok = App.dispatch(scripted())
    end

    test "a window that has shrunk past a stamp makes the match new" do
      test = self()

      decide = fn state, command ->
        send(test, {:state, state})

        claimed(command)
      end

      command = scripted(decide: decide)

      assert :ok = App.dispatch(command)

      age_stamps(command.command_id, 10)
      with_window(9_000)

      assert :ok = App.dispatch(command)
      assert [_first, _second] = stored(stream(command))

      with_window(30_000)

      assert {:error, :duplicate_command} = App.dispatch(command)
    end
  end

  describe "`scope_tags/1`" do
    setup do
      attach_telemetry([@read_query, @append])
      :ok
    end

    test "anything but a list of non-empty strings is `:invalid_scope_tags`, with no statement" do
      for answer <- [:nope, nil, [1], [""], ["a:b", nil], ["a:b" | :c], {:error, :no}] do
        command = scripted(scope_tags: fn _command -> answer end)

        assert {:error, :invalid_scope_tags} = App.dispatch(command), inspect(answer)
      end

      refute_received {:telemetry, @read_query, _, _}
      refute_received {:telemetry, @append, _, _}
    end

    test "a raise is rescued" do
      command = scripted(scope_tags: fn _command -> raise "scope raised" end)

      assert {:error, %RuntimeError{message: "scope raised"}} = App.dispatch(command)
    end

    test "a batch lacking one is refused, and nothing is stored or retried" do
      attach_telemetry([@retry])

      command = scripted(scope_tags: fn _command -> ["order:1"] end)

      assert {:error, :batch_lacks_scope_tag} = App.dispatch(command)

      refute_received {:telemetry, @append, _, _}
      refute_received {:telemetry, @retry, _, _}
      assert stored(stream(command)) == []
    end

    test "a tag the event's own `Tags` gives counts, and the stream tag may repeat" do
      command =
        scripted(
          scope_tags: fn command -> ["claim:" <> command.claim_id, "scope:" <> command.scope] end
        )

      assert :ok = App.dispatch(command)
      assert {:error, :duplicate_command} = App.dispatch(command)
    end

    test "CC-11, a multi-stream batch's reply lost: a duplicate, though no event has every tag" do
      order = UUID.uuid4()

      decide = fn _state, command ->
        {:streams, [{:claim, command.claim_id, claimed(command)}, {:order, order, %Claimed{}}]}
      end

      command = scripted(scope_tags: fn _command -> ["order:" <> order] end, decide: decide)

      assert {:ok, %{position: position}} = App.dispatch(command, returning: :execution_result)

      assert {:ok, tags} = EventStore.read_tags(App, [position - 1, position])

      refute Enum.any?(
               tags,
               fn {_position, tags} ->
                 ("claim:" <> command.claim_id) in tags and ("order:" <> order) in tags
               end
             )

      assert {:error, :duplicate_command} = App.dispatch(command)
      assert [_one] = stored("scripted-" <> order)
    end
  end

  describe "step 0" do
    test "a route with no stream tag is `:invalid_decision_identity`" do
      command = %Kindless{command_id: UUID.uuid4(), _id: UUID.uuid4()}

      assert {:error, :invalid_decision_identity} =
               KindlessRouter.dispatch(command, application: App)
    end
  end
end
