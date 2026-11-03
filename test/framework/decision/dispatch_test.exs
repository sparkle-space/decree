defmodule Decree.Decision.DispatchTest do
  use Decree.DecisionStoreCase

  alias Decree.Aggregates.Aggregate
  alias Decree.Commands.ExecutionResult

  alias Decree.DecisionFixtures.{
    Burning,
    DeletableApp,
    HardDeletesApp,
    ParityAggregateCommand,
    RecordingDecision,
    StrayRouter,
    Stored,
    TagsAnswering,
    TagsRaising
  }

  alias Decree.Event.Append
  alias Decree.Registration

  @read_query [:decree, :event_store, :read_query, :start]
  @read_tags [:decree, :event_store, :read_tags, :start]
  @append [:decree, :event_store, :append_to_streams, :start]

  defmodule ShortRouter do
    use Decree.Commands.Router

    decide(Scripted,
      to: Decree.DecisionFixtures.ScriptedDecision,
      identity: :claim_id,
      identity_prefix: "scripted-",
      timeout: 50
    )
  end

  defp claim(opts \\ []) do
    struct!(
      Claim,
      [command_id: UUID.uuid4(), claim_id: UUID.uuid4(), scope: scope()] |> Keyword.merge(opts)
    )
  end

  defp data(recorded), do: Enum.map(recorded, & &1.data)

  describe "a dispatch" do
    test "appends what `decide` answers on the route's stream" do
      command = claim()

      assert :ok = App.dispatch(command)

      assert [%Claimed{claim_id: id, scope: scope}] = data(stored("claim-" <> command.claim_id))
      assert id == command.claim_id
      assert scope == command.scope
    end

    test "decides over what its query reads, on any stream" do
      scope = scope()
      seed(%Claimed{scope: scope}, ["scope:" <> scope])

      assert {:error, :already_claimed} = App.dispatch(claim(scope: scope))
    end

    test "sees an earlier decision on the same query" do
      scope = scope()

      assert :ok = App.dispatch(claim(scope: scope))
      assert {:error, :already_claimed} = App.dispatch(claim(scope: scope))
    end

    test "writes causation, correlation and metadata as an aggregate's append does" do
      command = claim()
      causation_id = UUID.uuid4()
      correlation_id = UUID.uuid4()

      assert :ok =
               App.dispatch(command,
                 causation_id: causation_id,
                 correlation_id: correlation_id,
                 metadata: %{"actor" => "a"}
               )

      assert [event] = stored("claim-" <> command.claim_id)
      assert event.causation_id == causation_id
      assert event.correlation_id == correlation_id
      assert event.metadata == %{"actor" => "a"}
    end

    test "the causation id defaults to the command uuid" do
      command = claim()
      command_uuid = UUID.uuid4()

      assert :ok = App.dispatch(command, command_uuid: command_uuid)

      assert [%{causation_id: ^command_uuid}] = stored("claim-" <> command.claim_id)
    end

    test "gives `query` the command and the dispatch metadata" do
      test = self()

      query = fn command, tag_context ->
        send(test, {:tag_context, command, tag_context})

        [claimed_item("q")]
      end

      command = scripted(query: query)

      assert :ok = App.dispatch(command, metadata: %{"actor" => "a"})

      assert_received {:tag_context, ^command,
                       %Decree.Event.TagContext{command: ^command, metadata: %{"actor" => "a"}}}
    end

    test "gives its events the framework's tags and the event's own" do
      command = claim()

      assert {:ok, %ExecutionResult{position: position}} =
               App.dispatch(command, returning: :execution_result)

      assert {:ok, %{^position => tags}} = EventStore.read_tags(App, [position])

      assert tags ==
               Enum.sort([
                 "claim:" <> command.claim_id,
                 "command:" <> command.command_id,
                 "command_kind:Decree.DecisionFixtures.Claim",
                 "scope:" <> command.scope
               ])
    end

    test "a route's `stream_tag` and a function identity name the entity tag" do
      command = scripted([], ByFunction)

      assert {:ok, %ExecutionResult{position: position, aggregate_uuid: stream_uuid}} =
               App.dispatch(command, returning: :execution_result)

      assert stream_uuid == "function-" <> command.claim_id
      assert {:ok, %{^position => tags}} = EventStore.read_tags(App, [position])
      assert ("claim:" <> command.claim_id) in tags
    end

    test "answers an unknown command as a router does" do
      assert {:error, :unregistered_command} = App.dispatch(%{__struct__: UnknownCommand})
    end
  end

  describe "the callback order" do
    setup do
      Process.register(self(), :decision_recorder)
      :ok
    end

    defp recorded_decision_command(scope, script \\ %{}) do
      %Scripted{
        command_id: UUID.uuid4(),
        claim_id: UUID.uuid4(),
        scope: scope,
        script: script
      }
    end

    # Runs a dispatch of `RecordingDecision` straight through the router's machinery: its
    # module is the one route `Scripted` does not have, so it gets a router of its own here.
    defmodule RecordingRouter do
      use Decree.Commands.Router

      decide(Scripted,
        to: RecordingDecision,
        identity: :claim_id,
        identity_prefix: "scripted-"
      )
    end

    defp callbacks(acc \\ []) do
      receive do
        {:callback, call} -> callbacks([call | acc])
      after
        0 -> Enum.reverse(acc)
      end
    end

    test "is query, followup_query, evolve..., admit, decide" do
      scope = scope()
      claimed = %Claimed{scope: scope, note: "first"}
      released = %Released{scope: scope}

      seed(claimed, ["scope:" <> scope])
      seed(released, ["scope:" <> scope])

      command = recorded_decision_command(scope)

      assert :ok = RecordingRouter.dispatch(command, application: App)

      assert [
               {:query, ^scope},
               {:followup_query, [%Claimed{note: "first"}]},
               {:evolve, %Claimed{note: "first"}},
               {:evolve, %Released{}},
               {:admit, [%Claimed{}, %Released{}]},
               {:decide, [%Claimed{}, %Released{}]},
               {:evolve, %Claimed{note: "decided"}}
             ] = callbacks()
    end

    test "`admit`'s refusal comes before `decide`" do
      command = recorded_decision_command(scope(), %{admit: {:error, :not_allowed}})

      assert {:error, :not_allowed} = RecordingRouter.dispatch(command, application: App)

      assert [{:query, _scope}, {:followup_query, []}, {:admit, []}] = callbacks()
    end

    test "`followup_query` gets business events only, with the upcast ones" do
      scope = scope()
      command = recorded_decision_command(scope)

      seed(%Released{scope: "elsewhere"}, ["command:" <> command.command_id])
      seed(%Stored{scope: scope}, [])
      seed(%Claimed{scope: scope, note: "mine"}, ["scope:" <> scope])

      assert {:error, :reused_command_id} =
               RecordingRouter.dispatch(command, application: App)

      assert [{:query, _}, {:followup_query, [%Claimed{note: "mine"}]} | rest] = callbacks()
      assert Enum.any?(rest, &match?({:evolve, %Claimed{note: "mine"}}, &1))
      refute Enum.any?(rest, &match?({:evolve, %Released{}}, &1))
    end
  end

  describe "no aggregate process" do
    test "registers nothing and starts no child" do
      supervisor = Module.concat([App, Decree.Aggregates.Supervisor])
      children = DynamicSupervisor.count_children(supervisor)
      command = scripted()
      stream_uuid = "scripted-" <> command.claim_id

      assert :ok = App.dispatch(command)

      assert Registration.whereis_name(
               App,
               Aggregate.name(App, Decree.DecisionFixtures.ScriptedDecision, stream_uuid)
             ) == :undefined

      assert DynamicSupervisor.count_children(supervisor) == children
    end

    test "while an aggregate route on the same application starts one" do
      supervisor = Module.concat([App, Decree.Aggregates.Supervisor])
      before = DynamicSupervisor.count_children(supervisor).active

      command = %ParityAggregateCommand{id: UUID.uuid4(), scope: "s"}

      assert :ok = App.dispatch(command)
      assert DynamicSupervisor.count_children(supervisor).active == before + 1
    end
  end

  describe "F3, refusals before any statement" do
    setup do
      attach_telemetry([@read_query, @read_tags, @append])
      :ok
    end

    for {name, command_id} <- [{"nil", nil}, {"an empty string", ""}, {"an integer", 123}] do
      test "a `command_id` that is #{name} is `:missing_command_id`" do
        assert {:error, :missing_command_id} =
                 App.dispatch(claim(command_id: unquote(command_id)))

        refute_received {:telemetry, _, _, _}
      end
    end

    for {name, identity} <- [
          {"not a UUID", "not-a-uuid"},
          {"uppercase", "6F9619FF-8B86-D011-B42D-00C04FC964FF"},
          {"an integer", 7}
        ] do
      test "an identity that is #{name} is `:invalid_decision_identity`" do
        assert {:error, :invalid_decision_identity} =
                 App.dispatch(claim(claim_id: unquote(identity)))

        refute_received {:telemetry, _, _, _}
      end
    end

    test "`:missing_command_id` comes before the identity" do
      assert {:error, :missing_command_id} =
               App.dispatch(claim(command_id: nil, claim_id: "not-a-uuid"))
    end

    test "the read's `command:` tag is what the append wrote" do
      command = claim()

      assert {:ok, %ExecutionResult{position: position}} =
               App.dispatch(command, returning: :execution_result)

      assert_received {:telemetry, @read_query, _measurements, %{opts: opts, items: items}}

      command_tag = Keyword.fetch!(opts, :command_tag)

      assert command_tag == Append.command_tag(command)
      assert [tags: [command_tag]] in items
      assert {:ok, %{^position => tags}} = EventStore.read_tags(App, [position])
      assert command_tag in tags
    end
  end

  describe "F9, an implementation's answer and a raise" do
    setup do
      attach_telemetry([[:decree, :decision, :retry], @append])
      :ok
    end

    test "a `Tags` implementation answering `:append_condition_failed` is answered once" do
      test = self()

      decide = fn _state, _command ->
        send(test, :decided)

        %TagsAnswering{reason: :append_condition_failed}
      end

      command = scripted(decide: decide)

      assert {:error, :append_condition_failed} = App.dispatch(command)

      assert_received :decided
      refute_received :decided
      refute_received {:telemetry, [:decree, :decision, :retry], _, _}
      refute_received {:telemetry, @append, _, _}
      assert stored("scripted-" <> command.claim_id) == []
    end

    test "any other answer from `Tags` stands as it is" do
      command = scripted(decide: fn _state, _command -> %TagsAnswering{reason: :tagless} end)

      assert {:error, :tagless} = App.dispatch(command)
    end

    test "a `decide` answering `:append_condition_failed` is its own answer and is not retried" do
      command = scripted(decide: fn _state, _c -> {:error, :append_condition_failed} end)

      assert {:error, :append_condition_failed} = App.dispatch(command)
      refute_received {:telemetry, [:decree, :decision, :retry], _, _}
    end

    test "a `Tags` implementation that raises is rescued" do
      command = scripted(decide: fn _state, c -> %TagsRaising{claim_id: c.claim_id} end)

      assert {:error, %RuntimeError{message: "tags raised"}} = App.dispatch(command)
      refute_received {:telemetry, @append, _, _}
    end

    test "a raise in `query` is rescued" do
      command = scripted(query: fn _command, _context -> raise "query raised" end)

      assert {:error, %RuntimeError{message: "query raised"}} = App.dispatch(command)
    end

    test "a raise in `followup_query` is rescued" do
      command = scripted(followup: fn _c, _t, _e -> raise "followup raised" end)

      assert {:error, %RuntimeError{message: "followup raised"}} = App.dispatch(command)
    end

    test "a raise in `evolve` over what was read is rescued" do
      scope = scope()
      seed(%Burning{scope: scope}, ["scope:" <> scope])

      item = [types: [Atom.to_string(Burning)], tags: ["scope:" <> scope]]
      command = scripted(query: fn _command, _context -> [item] end)

      assert {:error, %RuntimeError{message: "evolve raised"}} = App.dispatch(command)
    end

    test "a raise in `evolve` over a new event comes before the commit" do
      command = scripted(decide: fn _state, c -> %Burning{scope: c.scope} end)

      assert {:error, %RuntimeError{message: "evolve raised"}} = App.dispatch(command)
      refute_received {:telemetry, @append, _, _}
      assert stored("scripted-" <> command.claim_id) == []
    end

    test "a raise in `admit` is rescued" do
      command = scripted(admit: fn _state, _command -> raise "admit raised" end)

      assert {:error, %RuntimeError{message: "admit raised"}} = App.dispatch(command)
    end

    test "an answer `admit` may not give is rescued as a raise" do
      command = scripted(admit: fn _state, _command -> :maybe end)

      assert {:error, %CaseClauseError{}} = App.dispatch(command)
    end

    test "a raise in `decide` is rescued" do
      command = scripted(decide: fn _state, _command -> raise "decide raised" end)

      assert {:error, %RuntimeError{message: "decide raised"}} = App.dispatch(command)
    end

    test "the task's own death answers `:decision_execution_failed`" do
      command = scripted(decide: fn _state, _command -> exit(:died) end)

      assert {:error, :decision_execution_failed} = App.dispatch(command)
    end

    test "a task past its timeout answers `:decision_execution_timeout`" do
      command = scripted(decide: fn _state, _command -> :timer.sleep(:infinity) end)

      assert {:error, :decision_execution_timeout} = App.dispatch(command, timeout: 50)
    end

    test "a route's own `timeout:` applies" do
      command = scripted(decide: fn _state, _command -> :timer.sleep(:infinity) end)

      assert {:error, :decision_execution_timeout} =
               Decree.Decision.DispatchTest.ShortRouter.dispatch(command, application: App)
    end

    test "appends at `:any_version`: on a new stream and on an existing one" do
      claim_id = UUID.uuid4()
      first = scripted(claim_id: claim_id)
      second = scripted(claim_id: claim_id)

      assert :ok = App.dispatch(first)
      assert :ok = App.dispatch(second)

      assert [1, 2] = "scripted-#{claim_id}" |> stored() |> Enum.map(& &1.stream_version)
    end
  end

  describe "F10, what `decide` may not answer" do
    setup do
      attach_telemetry([@append])
      :ok
    end

    for none <- [[], :ok, nil] do
      test "#{inspect(none)} is `:decision_returned_no_events`" do
        command = scripted(decide: fn _state, _command -> unquote(none) end)

        assert {:error, :decision_returned_no_events} = App.dispatch(command)
        refute_received {:telemetry, @append, _, _}
      end
    end

    test "`:duplicate` answers `:duplicate_command` and appends nothing" do
      command = scripted(decide: fn _state, _command -> :duplicate end)

      assert {:error, :duplicate_command} = App.dispatch(command)
      refute_received {:telemetry, @append, _, _}
      assert stored("scripted-" <> command.claim_id) == []
    end

    test "anything else is `:invalid_decision_return`" do
      for answer <- [
            {:ok, %Claimed{}},
            :nothing,
            1,
            [%Claimed{}, :other],
            [%Claimed{} | :x],
            {:streams, :all}
          ] do
        command = scripted(decide: fn _state, _command -> answer end)

        assert {:error, :invalid_decision_return} = App.dispatch(command), inspect(answer)
      end
    end
  end

  describe "items" do
    test "`query` answering an error stands" do
      command = scripted(query: fn _command, _context -> {:error, :cannot_ask} end)

      assert {:error, :cannot_ask} = App.dispatch(command)
    end

    test "a tag-only item refuses before any statement" do
      attach_telemetry([@read_query])

      command = scripted(query: fn _command, _context -> [[tags: ["scope:x"]]] end)

      assert {:error, :invalid_query} = App.dispatch(command)
      refute_received {:telemetry, @read_query, _, _}
    end

    test "a query of no items refuses" do
      command = scripted(query: fn _command, _context -> [] end)

      assert {:error, :invalid_query} = App.dispatch(command)
    end

    test "`followup_query` answering an error stands" do
      command = scripted(followup: fn _c, _t, _e -> {:error, :cannot_ask} end)

      assert {:error, :cannot_ask} = App.dispatch(command)
    end

    test "a follow-up's tag-only item refuses" do
      command = scripted(followup: fn _c, _t, _e -> [[tags: ["scope:x"]]] end)

      assert {:error, :invalid_query} = App.dispatch(command)
    end

    test "`followup_query` answering `[]` sends no second read" do
      attach_telemetry([@read_query])

      assert :ok = App.dispatch(scripted(followup: fn _c, _t, _e -> [] end))

      assert_received {:telemetry, @read_query, _, _}
      refute_received {:telemetry, @read_query, _, _}
    end

    test "a store refusal of an item passes through" do
      command =
        scripted(query: fn _c, _t -> [[types: ["T"], tags: [String.duplicate("a", 513)]]] end)

      assert {:error, :tag_too_long} = App.dispatch(command)
    end
  end

  describe "the fold" do
    test "shows `evolve` only what the decision's own items matched" do
      test = self()

      decide = fn state, _command ->
        send(test, {:state, state})

        %Claimed{}
      end

      command = scripted(decide: decide)
      scope = command.scope

      seed(%Claimed{scope: scope, note: "own"}, ["scope:" <> scope])
      seed(%Released{scope: "other"}, ["command:" <> command.command_id])
      seed(%Released{scope: "unrelated"}, ["scope:" <> scope])

      assert {:error, :reused_command_id} = App.dispatch(command)

      refute_received {:state, _state}

      command = scripted(decide: decide, query: fn _c, _t -> [claimed_item(scope)] end)

      assert :ok = App.dispatch(command)

      assert_received {:state, [%Claimed{note: "own"}]}
    end

    test "runs an upcaster on what `evolve` sees" do
      test = self()
      scope = scope()

      seed(%Stored{scope: scope}, ["scope:" <> scope])

      item = [types: [Atom.to_string(Stored)], tags: ["scope:" <> scope]]

      decide = fn state, _command ->
        send(test, {:state, state})

        %Claimed{}
      end

      assert :ok = App.dispatch(scripted(query: fn _c, _t -> [item] end, decide: decide))

      assert_received {:state, [%Claimed{note: "upcast", scope: ^scope}]}
    end

    test "merges both reads by position" do
      test = self()
      scope = scope()

      seed(%Released{scope: scope}, ["scope:" <> scope])
      seed(%Claimed{scope: scope, note: "second"}, ["scope:" <> scope])
      seed(%Released{scope: scope}, ["scope:" <> scope])

      released = [types: [Atom.to_string(Released)], tags: ["scope:" <> scope]]

      decide = fn state, _command ->
        send(test, {:state, state})

        %Claimed{}
      end

      command =
        scripted(
          query: fn _c, _t -> [released] end,
          followup: fn _c, _t, [%Released{}, %Released{}] -> [claimed_item(scope)] end,
          decide: decide
        )

      assert :ok = App.dispatch(command)

      assert_received {:state, [%Released{}, %Claimed{note: "second"}, %Released{}]}
    end
  end

  describe "step 7, a command match" do
    test "that is not this command's refuses, whatever `decide` would say" do
      attach_telemetry([@append, @read_tags])

      test = self()

      command =
        scripted(
          decide: fn _state, _command ->
            send(test, :decided)

            %Claimed{}
          end
        )

      position = seed(%Released{scope: "x"}, ["command:" <> command.command_id])

      assert {:error, :reused_command_id} = App.dispatch(command)

      refute_received :decided
      refute_received {:telemetry, @append, _, _}
      # The one fetch reads the facts with it.
      positions = fact_positions() ++ [position]
      assert_received {:telemetry, @read_tags, _, %{positions: ^positions}}
    end

    test "a resend of a command that landed is a duplicate" do
      command = claim()

      assert :ok = App.dispatch(command)
      assert {:error, :duplicate_command} = App.dispatch(command)
      assert [_one] = stored("claim-" <> command.claim_id)
    end

    test "no match reads no tags but the facts'" do
      attach_telemetry([@read_tags])

      assert :ok = App.dispatch(scripted())

      positions = fact_positions()
      assert_received {:telemetry, @read_tags, _, %{positions: ^positions}}
    end
  end

  describe "F8, `{:streams, members}`" do
    setup do
      user = UUID.uuid4()
      order = UUID.uuid4()

      decide = fn _state, command ->
        {:streams,
         [
           {:claim, command.claim_id,
            %Claimed{claim_id: command.claim_id, scope: command.scope, note: "one"}},
           {:order, order,
            [
              %Claimed{claim_id: order, scope: command.scope, note: "two"},
              %Claimed{claim_id: order, scope: command.scope, note: "three"}
            ]}
         ]}
      end

      [user: user, order: order, decide: decide]
    end

    test "tags each member's events with its own stream only", context do
      command = scripted(decide: context.decide)

      assert {:ok, %ExecutionResult{} = result} =
               App.dispatch(command, returning: :execution_result)

      route = "scripted-" <> command.claim_id

      [one] = positioned(route)
      [two, three] = positioned("scripted-" <> context.order)

      assert {:ok, tags} =
               EventStore.read_tags(App, [one.event_number, two.event_number, three.event_number])

      route_tag = "claim:" <> command.claim_id
      order_tag = "order:" <> context.order

      assert route_tag in tags[one.event_number]
      refute order_tag in tags[one.event_number]

      for event <- [two, three] do
        assert order_tag in tags[event.event_number]
        refute route_tag in tags[event.event_number]
      end

      for event <- [one, two, three] do
        assert ("command:" <> command.command_id) in tags[event.event_number]
      end

      assert result.stream_versions == %{route => 1, ("scripted-" <> context.order) => 2}

      assert result.aggregate_uuid == route
      assert result.aggregate_version == 1
      assert result.position == three.event_number

      assert [%Claimed{note: "one"}, %Claimed{note: "two"}, %Claimed{note: "three"}] =
               result.events

      assert [%Claimed{}, %Claimed{}, %Claimed{}] = result.aggregate_state
    end

    test "gives the members contiguous positions, in order", context do
      command = scripted(decide: context.decide)

      assert :ok = App.dispatch(command)

      [one] = positioned("scripted-" <> command.claim_id)
      [two, three] = positioned("scripted-" <> context.order)

      assert [two.event_number, three.event_number] ==
               [one.event_number + 1, one.event_number + 2]
    end

    test "a member's stream version carries on from what is there", context do
      first = scripted(decide: context.decide)

      assert :ok = App.dispatch(first)

      assert {:ok, %ExecutionResult{stream_versions: versions}} =
               App.dispatch(scripted(claim_id: first.claim_id, decide: context.decide),
                 returning: :execution_result
               )

      assert versions == %{
               ("scripted-" <> first.claim_id) => 2,
               ("scripted-" <> context.order) => 4
             }
    end

    test "one member is a batch too" do
      command =
        scripted(
          decide: fn _state, command ->
            {:streams, [{:claim, command.claim_id, %Claimed{claim_id: command.claim_id}}]}
          end
        )

      assert :ok = App.dispatch(command)
      assert [%{stream_version: 1}] = stored("scripted-" <> command.claim_id)
    end

    test "a batch with no event carrying the route's stream tag stores nothing", context do
      attach_telemetry([@append])

      decide = fn _state, _command ->
        {:streams, [{:user, context.user, %Claimed{}}, {:order, context.order, %Claimed{}}]}
      end

      command = scripted(decide: decide)

      assert {:error, :batch_lacks_scope_tag} = App.dispatch(command)

      refute_received {:telemetry, @append, _, _}
      assert stored("scripted-" <> context.user) == []
      assert stored("scripted-" <> context.order) == []
    end

    test "the route's stream tag on another kind is not the route's", context do
      decide = fn _state, command ->
        {:streams, [{:user, command.claim_id, %Claimed{}}, {:order, context.order, %Claimed{}}]}
      end

      assert {:error, :batch_lacks_scope_tag} = App.dispatch(scripted(decide: decide))
    end

    test "a non-canonical member identity refuses", context do
      for identity <- ["not-a-uuid", String.upcase(context.user), nil, 7] do
        decide = fn _state, _command -> {:streams, [{:user, identity, %Claimed{}}]} end

        assert {:error, :invalid_decision_identity} = App.dispatch(scripted(decide: decide))
      end
    end

    test "an invalid kind refuses", context do
      for kind <- [nil, true, false, "user", :"a:b", :Upper, :"", 1] do
        decide = fn _state, _command -> {:streams, [{kind, context.user, %Claimed{}}]} end

        assert {:error, :invalid_decision_stream} = App.dispatch(scripted(decide: decide))
      end
    end

    test "a malformed member refuses", context do
      for member <- [{:user, context.user}, :user, {:user, context.user, %Claimed{}, :extra}] do
        decide = fn _state, _command -> {:streams, [member]} end

        assert {:error, :invalid_decision_stream} = App.dispatch(scripted(decide: decide))
      end
    end

    test "a repeated member answers the store's atom" do
      event = %Claimed{}

      decide = fn _state, command ->
        {:streams, [{:claim, command.claim_id, event}, {:order, command.claim_id, event}]}
      end

      assert {:error, :repeated_stream} = App.dispatch(scripted(decide: decide))
    end

    test "`[]` for a member refuses, and so does no member", context do
      decide = fn _state, _command ->
        {:streams, [{:user, context.user, %Claimed{}}, {:order, context.order, []}]}
      end

      assert {:error, :decision_returned_no_events} = App.dispatch(scripted(decide: decide))

      assert {:error, :decision_returned_no_events} =
               App.dispatch(scripted(decide: fn _state, _command -> {:streams, []} end))
    end

    test "one member's bad `Tags` answer writes nothing on any member", context do
      decide = fn _state, _command ->
        {:streams,
         [
           {:user, context.user, %Claimed{}},
           {:order, context.order, %TagsAnswering{reason: :tagless}}
         ]}
      end

      assert {:error, :tagless} = App.dispatch(scripted(decide: decide))
      assert stored("scripted-" <> context.user) == []
    end
  end

  describe "F11, `max_items`" do
    setup do
      attach_telemetry([@read_query, [:decree, :decision, :refused]])
      :ok
    end

    defp items(count), do: Enum.map(1..count, &claimed_item("item-#{&1}"))

    test "14 own items, the command item and the index item pass" do
      command = scripted(query: fn _c, _t -> items(14) end)

      assert :ok = App.dispatch(command)
    end

    test "15 own items, the command item and the index item refuse with no statement" do
      command = scripted(query: fn _c, _t -> items(15) end)

      assert {:error, :decision_query_too_large} = App.dispatch(command)

      refute_received {:telemetry, @read_query, _, _}

      assert_received {:telemetry, [:decree, :decision, :refused], %{value: 17, limit: 16},
                       %{bound: :items}}
    end

    test "counts across both reads, the framework's items once" do
      command =
        scripted(query: fn _c, _t -> items(8) end, followup: fn _c, _t, _e -> items(6) end)

      assert :ok = App.dispatch(command)
    end

    test "over it across both reads, sends no second read" do
      command =
        scripted(query: fn _c, _t -> items(8) end, followup: fn _c, _t, _e -> items(7) end)

      assert {:error, :decision_query_too_large} = App.dispatch(command)

      assert_received {:telemetry, @read_query, _, _}
      refute_received {:telemetry, @read_query, _, _}
    end

    test "a lowered bound counts the same way" do
      assert :ok = App.dispatch(scripted([query: fn _c, _t -> items(2) end], Tight))

      assert {:error, :decision_query_too_large} =
               App.dispatch(scripted([query: fn _c, _t -> items(3) end], Tight))
    end
  end

  describe "`max_probes`" do
    setup do
      attach_telemetry([@read_query, [:decree, :decision, :refused]])
      :ok
    end

    defp types(count, from \\ 1) do
      Enum.map(from..(from + count - 1), &"Elixir.Type#{&1}")
    end

    test "one item of 46 types, the command item and the index item pass" do
      command = scripted(query: fn _c, _t -> [[types: types(46)]] end)

      assert :ok = App.dispatch(command)
    end

    test "one item of 47 types, the command item and the index item refuse with no statement" do
      command = scripted(query: fn _c, _t -> [[types: types(47)]] end)

      assert {:error, :decision_query_too_large} = App.dispatch(command)

      refute_received {:telemetry, @read_query, _, _}

      assert_received {:telemetry, [:decree, :decision, :refused], %{value: 49, limit: 48},
                       %{bound: :probes}}
    end

    test "one item of 100,000 types counts as what it costs, not as one item" do
      command = scripted(query: fn _c, _t -> [[types: types(100_000)]] end)

      assert {:error, :decision_query_too_large} = App.dispatch(command)

      refute_received {:telemetry, @read_query, _, _}

      assert_received {:telemetry, [:decree, :decision, :refused], %{value: 100_002, limit: 48},
                       %{bound: :probes}}
    end

    test "counts across both reads, the framework's items once" do
      command =
        scripted(
          query: fn _c, _t -> [[types: types(24)]] end,
          followup: fn _c, _t, _e -> [[types: types(22, 25)]] end
        )

      assert :ok = App.dispatch(command)
    end

    test "over it across both reads, sends no second read" do
      command =
        scripted(
          query: fn _c, _t -> [[types: types(24)]] end,
          followup: fn _c, _t, _e -> [[types: types(23, 25)]] end
        )

      assert {:error, :decision_query_too_large} = App.dispatch(command)

      assert_received {:telemetry, @read_query, _, _}
      refute_received {:telemetry, @read_query, _, _}

      assert_received {:telemetry, [:decree, :decision, :refused], %{value: 49, limit: 48},
                       %{bound: :probes}}
    end

    test "counts a repeated type once, as the store does" do
      repeated = List.duplicate("Elixir.Type1", 100)

      assert :ok = App.dispatch(scripted(query: fn _c, _t -> [[types: repeated]] end))
    end

    test "does not count an item's further tags" do
      tags = Enum.map(1..100, &"scope:#{&1}")

      assert :ok = App.dispatch(scripted(query: fn _c, _t -> [[types: types(1), tags: tags]] end))
    end

    test "a query the store refuses goes on to the read, and answers as it did" do
      long = String.duplicate("a", 513)

      for {item, answer} <- [
            {[types: [:not_a_string | types(100)]], :invalid_query},
            {[types: types(100), tags: [long]], :tag_too_long}
          ] do
        assert {:error, ^answer} = App.dispatch(scripted(query: fn _c, _t -> [item] end))

        assert_received {:telemetry, @read_query, _, _}
      end

      refute_received {:telemetry, [:decree, :decision, :refused], _, _}
    end

    test "a second read the store refuses answers as it did" do
      command =
        scripted(
          query: fn _c, _t -> [[types: types(1)]] end,
          followup: fn _c, _t, _e -> [[types: [:not_a_string | types(100)]]] end
        )

      assert {:error, :invalid_query} = App.dispatch(command)

      refute_received {:telemetry, [:decree, :decision, :refused], _, _}
    end

    test "a lowered bound counts the same way" do
      assert :ok = App.dispatch(scripted([query: fn _c, _t -> [[types: types(3)]] end], Tight))

      assert {:error, :decision_query_too_large} =
               App.dispatch(scripted([query: fn _c, _t -> [[types: types(4)]] end], Tight))
    end
  end

  describe "F11, `max_events`" do
    setup do
      attach_telemetry([[:decree, :decision, :read], [:decree, :decision, :refused]])
      :ok
    end

    defp seed_claims(scope, count) do
      for _ <- 1..count, do: seed(%Claimed{scope: scope}, ["scope:" <> scope])
    end

    test "exactly at the bound passes" do
      command = scripted([], Tight)

      seed_claims(command.scope, 3)

      assert :ok = App.dispatch(command)
    end

    test "one over refuses, on what was read, with the command match the newest row" do
      command = scripted([], Tight)

      seed_claims(command.scope, 3)
      seed(%Released{scope: "x"}, ["command:" <> command.command_id])

      assert {:error, :decision_query_too_large} = App.dispatch(command)

      value = 4 + fact_count()

      assert_received {:telemetry, [:decree, :decision, :refused], %{value: ^value, limit: 12},
                       %{bound: :events}}
    end

    test "well over it, the read cut at the bound plus one, still refuses" do
      command = scripted([], Tight)

      seed_claims(command.scope, 6)

      assert {:error, :decision_query_too_large} = App.dispatch(command)
    end

    test "at the bound, a command match as the newest row is seen" do
      command = scripted([], Tight)

      seed_claims(command.scope, 2)
      seed(%Released{scope: "x"}, ["command:" <> command.command_id])

      assert {:error, :reused_command_id} = App.dispatch(command)
    end

    test "both reads together, an event in both counted once" do
      scope = scope()
      [_one, _two, _three] = seed_claims(scope, 3)

      command =
        scripted(
          [
            scope: scope,
            followup: fn _c, _t, _e -> [claimed_item(scope)] end
          ],
          Tight
        )

      assert :ok = App.dispatch(command)
    end

    test "both reads together, over the bound, refuse" do
      scope = scope()

      seed_claims(scope, 2)
      seed(%Released{scope: scope}, ["scope:" <> scope])
      seed(%Released{scope: scope}, ["scope:" <> scope])

      released = [types: [Atom.to_string(Released)], tags: ["scope:" <> scope]]

      command =
        scripted([scope: scope, followup: fn _c, _t, _e -> [released] end], Tight)

      assert {:error, :decision_query_too_large} = App.dispatch(command)
    end

    test "emits the read event for a refused read, with no fold" do
      command = scripted([], Tight)

      seed_claims(command.scope, 4)

      assert {:error, :decision_query_too_large} = App.dispatch(command)

      matched = 4 + fact_count()

      assert_received {:telemetry, [:decree, :decision, :read],
                       %{matched_events: ^matched, fold_time: 0}, %{attempt: 1}}
    end
  end

  describe "F11, `max_batch_events`" do
    setup do
      attach_telemetry([@append, [:decree, :decision, :refused]])
      :ok
    end

    defp events(count), do: for(_ <- 1..count, do: %Claimed{})

    test "the bound passes, and one over refuses before the append" do
      assert :ok = App.dispatch(scripted(decide: fn _s, _c -> events(100) end))
      assert_received {:telemetry, @append, _, _}

      assert {:error, :decision_batch_too_large} =
               App.dispatch(scripted(decide: fn _s, _c -> events(101) end))

      refute_received {:telemetry, @append, _, _}

      assert_received {:telemetry, [:decree, :decision, :refused], %{value: 101, limit: 100},
                       %{bound: :batch_events}}
    end

    test "a lowered bound" do
      assert :ok = App.dispatch(scripted([decide: fn _s, _c -> events(2) end], Tight))

      assert {:error, :decision_batch_too_large} =
               App.dispatch(scripted([decide: fn _s, _c -> events(3) end], Tight))
    end

    test "counts across every member" do
      decide = fn _state, _command ->
        {:streams, [{:user, UUID.uuid4(), events(2)}, {:order, UUID.uuid4(), events(1)}]}
      end

      assert {:error, :decision_batch_too_large} =
               App.dispatch(scripted([decide: decide], Tight))
    end
  end

  describe "the answer" do
    test "`returning:` every value" do
      fresh = fn -> claim() end

      assert :ok = App.dispatch(fresh.(), returning: false)
      assert {:ok, 1} = App.dispatch(fresh.(), returning: :aggregate_version)
      assert {:ok, :claimed} = App.dispatch(fresh.(), returning: :aggregate_state)
      assert {:ok, [%Claimed{}]} = App.dispatch(fresh.(), returning: :events)

      assert {:ok, %ExecutionResult{aggregate_state: :claimed, aggregate_version: 1}} =
               App.dispatch(fresh.(), returning: :execution_result)
    end

    test "`position` and `stream_versions` are the event's, read back" do
      seed(%Released{scope: "earlier"}, [])

      command = claim()
      stream_uuid = "claim-" <> command.claim_id

      assert {:ok, %ExecutionResult{} = result} =
               App.dispatch(command, returning: :execution_result, metadata: %{"a" => "b"})

      seed(%Released{scope: "later"}, [])
      assert [event] = positioned(stream_uuid)

      assert result.aggregate_uuid == stream_uuid
      assert result.position == 2 + fact_count()
      assert result.position == event.event_number
      assert result.stream_versions == %{stream_uuid => event.stream_version}
      assert result.aggregate_version == event.stream_version
      assert [%Claimed{}] = result.events
      assert result.metadata == %{"a" => "b"}
    end
  end

  describe "the store's refusals, through the framework" do
    test "a plain append into a decision's stream" do
      data = %EventStore.EventData{event_type: "T", data: %Claimed{}, metadata: %{}}

      assert EventStore.append_to_stream(App, "claim-" <> UUID.uuid4(), :any_version, [data]) ==
               {:error, :plain_append_to_conditional_prefix}
    end

    test "a link into a decision's stream" do
      seed(%Released{scope: "s"}, [], App, "other-linked")
      [event] = positioned("other-linked")

      assert Decree.FrameworkEventStore.link_to_stream(
               "claim-" <> UUID.uuid4(),
               :any_version,
               [event.event_id],
               name: store()
             ) == {:error, :link_into_conditional_prefix}
    end

    test "an aggregate's function prefix landing in a decision's prefix" do
      command = %Decree.DecisionFixtures.StrayCommand{id: UUID.uuid4()}

      assert {:error, :plain_append_to_conditional_prefix} =
               StrayRouter.dispatch(command, application: App)
    end

    test "an aggregate's bare identity landing in a decision's prefix" do
      command = %Decree.DecisionFixtures.BareStrayCommand{id: "claim-" <> UUID.uuid4()}

      assert {:error, :plain_append_to_conditional_prefix} =
               StrayRouter.dispatch(command, application: App)
    end

    test "an undeclared prefix answers the store's atom" do
      start_supervised!({DeletableApp, []})

      assert {:error, :undeclared_conditional_target} =
               DeletableApp.dispatch(claim())
    end

    defp store, do: Module.concat([App, "FrameworkEventStore"])
  end

  describe "a resend, on a store that can erase tags" do
    setup do
      start_supervised!({HardDeletesApp, []})

      :ok
    end

    test "is still a duplicate: `command` tags cannot be erased" do
      command = scripted()
      stream_uuid = "scripted-" <> command.claim_id
      store = Module.concat([HardDeletesApp, "FrameworkEventStore"])

      assert :ok = HardDeletesApp.dispatch(command)

      assert {:error, :invalid_tag_kinds} =
               Decree.FrameworkEventStore.delete_stream_tags(stream_uuid, ["command"],
                 name: store
               )

      assert {:error, :duplicate_command} = HardDeletesApp.dispatch(command)

      assert [_only_one] = stored(stream_uuid, HardDeletesApp)
    end
  end

  describe "a store declaring deletable tags" do
    setup do
      attach_telemetry([[:decree, :decision, :retry]])
      start_supervised!({DeletableApp, []})

      :ok
    end

    test "`query_names_deletable` passes through unretried" do
      item = [types: ["T"], tags: ["secret:1"]]
      command = scripted(query: fn _c, _t -> [item] end)

      assert {:error, :query_names_deletable} = DeletableApp.dispatch(command)
      refute_received {:telemetry, [:decree, :decision, :retry], _, _}
    end

    test "`read_tags` leaves the deletable kind out" do
      position = seed(%Claimed{scope: "s"}, ["secret:1", "scope:s"], DeletableApp)

      assert {:ok, %{^position => ["scope:s"]}} = EventStore.read_tags(DeletableApp, [position])
    end

    test "a command match over a deletable event reads as any other" do
      command = scripted()

      seed(%Claimed{scope: "s"}, ["secret:1", "command:" <> command.command_id], DeletableApp)

      assert {:error, :reused_command_id} = DeletableApp.dispatch(command)
    end
  end
end
