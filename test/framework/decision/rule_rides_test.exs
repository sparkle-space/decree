defmodule Decree.Decision.RuleRidesTest do
  use Decree.RuleCase

  alias Decree.Aggregate.Multi
  alias Decree.Aggregates.ExecutionContext
  alias Decree.Event.Append
  alias Decree.EventStore.EventData
  alias Decree.PersonalFields.Envelope
  alias Decree.PersonalFields.Fixtures.Events.Registered, as: PersonalRegistered
  alias Decree.PersonalFields.Fixtures.Events.Unfolded
  alias Decree.PersonalFields.Fixtures.Keys
  alias Decree.RuleFixtures.{Personal, PlainNote}

  @stop [:decree, :aggregate, :execute, :stop]
  @marker "rule-rides-marker"

  defp noting(test) do
    fn %RuledAggregate{}, %{id: id, rule_result: %Result{} = result} ->
      send(test, {:rode, result})

      %Noted{id: id, note: length(result.events)}
    end
  end

  defp scope_item(scope), do: [types: [Atom.to_string(Read)], tags: ["scope:" <> scope]]

  describe "what `execute/2` gets" do
    test "is both reads' events, in position order" do
      scope = scope()
      other = scope()

      seed(%Read{scope: scope, note: "first"}, ["scope:" <> scope], App)
      seed(%Read{scope: other, note: "second"}, ["scope:" <> other], App)
      seed(%Read{scope: scope, note: "third"}, ["scope:" <> scope], App)

      command =
        ruled(
          scope: scope,
          followup: fn _command, _tag_context, _events -> [scope_item(other)] end,
          execute: noting(self())
        )

      assert :ok = App.dispatch(command)

      assert_received {:rode, %Result{events: events}}
      assert Enum.map(events, & &1.note) == ["first", "second", "third"]
    end

    test "an event both reads match rides once" do
      scope = scope()

      seed(%Read{scope: scope}, ["scope:" <> scope], App)

      command =
        ruled(
          scope: scope,
          followup: fn _command, _tag_context, _events -> [scope_item(scope)] end,
          execute: noting(self())
        )

      assert :ok = App.dispatch(command)
      assert_received {:rode, %Result{events: [%Read{}]}}
    end

    test "is only what the rule's own items matched: a rule has no command item" do
      command = ruled(execute: noting(self()))

      seed(%Read{scope: "elsewhere"}, ["command:" <> command.command_id], App)

      assert :ok = App.dispatch(command)
      assert_received {:rode, %Result{events: []}}
    end

    test "is empty for `[]`" do
      command = ruled(query: fn _command, _tag_context -> [] end, execute: noting(self()))

      assert :ok = App.dispatch(command)
      assert_received {:rode, %Result{events: []}}
    end

    test "overwrites a forged result" do
      scope = scope()

      seed(%Read{scope: scope, note: "real"}, ["scope:" <> scope], App)

      command =
        ruled(
          scope: scope,
          rule_result: %Result{events: [:forged]},
          execute: noting(self())
        )

      assert :ok = App.dispatch(command)
      assert_received {:rode, %Result{events: [%Read{note: "real"}]}}
    end

    test "overwrites a forged result of any shape, even with nothing to read" do
      command =
        ruled(
          rule_result: :forged,
          query: fn _command, _tag_context -> [] end,
          execute: noting(self())
        )

      assert :ok = App.dispatch(command)
      assert_received {:rode, %Result{events: []}}
    end

    test "on a route with no rule leaves the field as sent" do
      test = self()

      execute = fn %RuledAggregate{}, %PlainNote{id: id, rule_result: sent} ->
        send(test, {:sent, sent})

        %Noted{id: id, note: "plain"}
      end

      command = ruled([rule_result: :as_sent, execute: execute], PlainNote)

      assert :ok = App.dispatch(command)
      assert_received {:sent, :as_sent}
    end

    test "comes through a `Multi`" do
      scope = scope()

      seed(%Read{scope: scope}, ["scope:" <> scope], App)

      execute = fn %RuledAggregate{} = state, %{id: id, rule_result: %Result{events: events}} ->
        state
        |> Multi.new()
        |> Multi.execute(fn _state -> %Noted{id: id, note: length(events)} end)
        |> Multi.execute(fn %RuledAggregate{notes: notes} -> %Noted{id: id, note: notes} end)
      end

      command = ruled(scope: scope, execute: execute)

      assert :ok = App.dispatch(command)

      assert [%{data: %Noted{note: 1}}, %{data: %Noted{note: [1]}}] =
               stored(ruled_stream(command), App)
    end

    test "a `Multi`'s append is under the condition" do
      test = self()
      scope = scope()

      execute = fn %RuledAggregate{} = state, %{id: id, rule_result: %Result{events: events}} ->
        send(test, {:executing, length(events)})

        with [] <- events do
          seed(%Read{scope: scope}, ["scope:" <> scope], App)
        end

        state |> Multi.new() |> Multi.execute(fn _state -> %Noted{id: id, note: "multi"} end)
      end

      assert :ok = App.dispatch(ruled(scope: scope, execute: execute))

      assert_received {:executing, 0}
      assert_received {:executing, 1}
    end
  end

  describe "personal fields" do
    setup do
      start_supervised!(Keys)

      [user: Keys.mint(UUID.uuid4())]
    end

    # Writes an event as a command would have, encrypted, to a stream no aggregate owns.
    defp seed_encrypted(event, tag) do
      stream = "other-" <> UUID.uuid4()
      context = %ExecutionContext{command: %Personal{}, metadata: %{}}

      {:ok, prepared} = Append.prepare_events(App, stream, [event], context)

      prepared =
        Enum.map(prepared, fn %EventData{} = data -> Map.update!(data, :tags, &[tag | &1]) end)

      {:ok, _result} =
        {:ok, :ok = EventStore.append_to_stream(App, stream, :any_version, prepared)}
    end

    defp reading_personal(tag) do
      types = [Atom.to_string(PersonalRegistered), Atom.to_string(Unfolded)]

      fn _command, _tag_context -> [[types: types, tags: [tag]]] end
    end

    test "a listed event rides decrypted, and an unlisted one as stored", %{user: user} do
      tag = "scope:" <> scope()

      seed_encrypted(%PersonalRegistered{user_id: user, email: @marker}, tag)
      seed_encrypted(%Unfolded{user_id: user, email: @marker}, tag)

      command = ruled([query: reading_personal(tag), execute: noting(self())], Personal)

      assert :ok = App.dispatch(command)

      assert_received {:rode, %Result{events: [registered, unfolded]}}
      assert %PersonalRegistered{email: @marker} = registered
      assert Envelope.envelope?(unfolded.email)
    end

    test "with no listing every event rides as stored", %{user: user} do
      tag = "scope:" <> scope()

      seed_encrypted(%PersonalRegistered{user_id: user, email: @marker}, tag)

      command = ruled(query: reading_personal(tag), execute: noting(self()))

      assert :ok = App.dispatch(command)

      assert_received {:rode, %Result{events: [%PersonalRegistered{email: email}]}}
      assert Envelope.envelope?(email)
    end

    test "an erased subject's field rides nil", %{user: user} do
      tag = "scope:" <> scope()

      seed_encrypted(%PersonalRegistered{user_id: user, email: @marker}, tag)
      Keys.set(user, :erased)

      command = ruled([query: reading_personal(tag), execute: noting(self())], Personal)

      assert :ok = App.dispatch(command)
      assert_received {:rode, %Result{events: [%PersonalRegistered{email: nil}]}}
    end

    test "`followup_query/3` sees it decrypted too", %{user: user} do
      test = self()
      tag = "scope:" <> scope()

      seed_encrypted(%PersonalRegistered{user_id: user, email: @marker}, tag)

      followup = fn _command, _tag_context, events ->
        send(test, {:followup, events})

        []
      end

      command = ruled([query: reading_personal(tag), followup: followup], Personal)

      assert :ok = App.dispatch(command)
      assert_received {:followup, [%PersonalRegistered{email: @marker}]}
    end
  end

  describe "what rides goes nowhere else" do
    setup do
      attach_telemetry([@stop])

      scope = scope()
      seed(%Read{scope: scope, secret: @marker}, ["scope:" <> scope], App)

      [scope: scope]
    end

    defp holds_marker?(term) do
      inspect(term, limit: :infinity, printable_limit: :infinity, structs: false) =~ @marker
    end

    test "not the aggregate's state", %{scope: scope} do
      command = ruled(scope: scope)

      assert :ok = App.dispatch(command)

      state = App.aggregate_state(RuledAggregate, ruled_stream(command))

      assert %RuledAggregate{notes: [1]} = state
      refute holds_marker?(state)
    end

    test "not the execution context", %{scope: scope} do
      command = ruled(scope: scope)

      assert :ok = App.dispatch(command)

      assert_received {:telemetry, @stop, _,
                       %{execution_context: %ExecutionContext{command: ^command} = context}}

      assert command.rule_result == nil
      refute holds_marker?(context)
    end

    test "not the event, or its metadata", %{scope: scope} do
      command = ruled(scope: scope)

      assert :ok = App.dispatch(command)

      assert [recorded] = stored(ruled_stream(command), App)
      refute holds_marker?(recorded)
    end

    test "not a snapshot", %{scope: scope} do
      start_supervised!(
        {App, name: :snapshots_rule, snapshotting: %{RuledAggregate => [snapshot_every: 1]}}
      )

      command = ruled(scope: scope)

      assert :ok = App.dispatch(command, application: :snapshots_rule)

      # Behind the snapshot in the aggregate's mailbox.
      Decree.Aggregates.Aggregate.aggregate_state(
        :snapshots_rule,
        RuledAggregate,
        ruled_stream(command)
      )

      assert {:ok, snapshot} = EventStore.read_snapshot(:snapshots_rule, ruled_stream(command))
      assert %RuledAggregate{notes: [1]} = snapshot.data
      refute holds_marker?(snapshot)
    end
  end
end
