defmodule Decree.Decision.RuleRefusalsTest do
  use Decree.RuleCase

  import ExUnit.CaptureLog

  alias Decree.Aggregate.Multi
  alias Decree.Aggregates.ExecutionContext
  alias Decree.Event.Append
  alias Decree.EventStore.EventData
  alias Decree.PersonalFields.Fixtures.Events.Registered, as: PersonalRegistered
  alias Decree.PersonalFields.Fixtures.Keys
  alias Decree.RuleFixtures.{Gated, Personal, Tight}

  @read [:decree, :decision, :read]
  @refused [:decree, :decision, :refused]

  setup do
    attach_telemetry([@read, @refused])
    Process.register(self(), :rule_recorder)
    :ok
  end

  defp items(count, from \\ 0) do
    for index <- from..(from + count - 1), do: [types: ["T#{index}"], tags: ["scope:x"]]
  end

  defp querying(answer), do: fn _command, _tag_context -> answer end
  defp following(answer), do: fn _command, _tag_context, _events -> answer end

  defp assert_refused_cleanly(command, reason, opts \\ []) do
    stream = ruled_stream(command)

    assert {:error, ^reason} = App.dispatch(command, opts)

    refute_received {:append_to_stream, ^stream, _version, _count, _opts}
    assert stored(stream, App) == []
  end

  describe "items" do
    test "with no types answer `:invalid_query`, with nothing read" do
      for item <- [[tags: ["scope:x"]], [types: [], tags: ["scope:x"]], %{tags: ["scope:x"]}] do
        assert_refused_cleanly(ruled(query: querying([item])), :invalid_query)
      end

      refute_received {:read_query, _items, _opts}
    end

    test "anything but a list answers `:invalid_query`" do
      for answer <- [:nonsense, nil, %{}, [:item]] do
        assert_refused_cleanly(ruled(query: querying(answer)), :invalid_query)
      end
    end

    test "`replay: true` answers `:invalid_query`, in either read" do
      replay = [
        types: ["T"],
        tags: ["scope:x", "command_kind:Decree.RuleFixtures.Ruled"],
        replay: true
      ]

      assert_refused_cleanly(ruled(query: querying([replay])), :invalid_query)
      refute_received {:read_query, _items, _opts}

      assert_refused_cleanly(ruled(followup: following([replay])), :invalid_query)
    end

    test "from `followup_query/3` with no types answer `:invalid_query`" do
      assert_refused_cleanly(ruled(followup: following([[tags: ["scope:x"]]])), :invalid_query)
    end
  end

  describe "the bounds" do
    test "16 items and the index item refuse, with nothing read" do
      assert_refused_cleanly(ruled(query: querying(items(16))), :decision_query_too_large)

      refute_received {:read_query, _items, _opts}
      assert [{%{value: 17, limit: 16}, %{bound: :items}}] = received(@refused)
    end

    test "15 items and the index item pass" do
      assert :ok = App.dispatch(ruled(query: querying(items(15))))
    end

    test "48 probes from one item and the index item refuse: the bound counts the compiled form" do
      types = for index <- 1..48, do: "T#{index}"

      assert_refused_cleanly(
        ruled(query: querying([[types: types, tags: ["scope:x"]]])),
        :decision_query_too_large
      )

      refute_received {:read_query, _items, _opts}

      assert [{%{value: 49, limit: 48}, %{bound: :probes, decision: decision}}] =
               received(@refused)

      assert decision == Decree.RuleFixtures.ScriptedRule
    end

    test "47 probes from one item and the index item pass" do
      types = for index <- 1..47, do: "T#{index}"

      assert :ok = App.dispatch(ruled(query: querying([[types: types, tags: ["scope:x"]]])))
    end

    test "both reads count together" do
      command = ruled(query: querying(items(9)), followup: following(items(7, 9)))

      assert_refused_cleanly(command, :decision_query_too_large)

      assert [{%{value: 17}, %{bound: :items}}] = received(@refused)
    end

    test "`max_events + 1` refuses, and is reported" do
      scope = scope()

      for _event <- 1..4, do: seed(%Read{scope: scope}, ["scope:" <> scope], App)

      assert_refused_cleanly(ruled([scope: scope], Tight), :decision_query_too_large)

      matched = 4 + fact_count()

      assert [{%{value: ^matched, limit: 12}, %{bound: :events}}] = received(@refused)
      assert [{%{matched_events: ^matched, fold_time: 0}, _metadata}] = received(@read)
    end

    test "`max_events + 1` across both reads refuses" do
      scope = scope()
      other = scope()

      for _event <- 1..2 do
        seed(%Read{scope: scope}, ["scope:" <> scope], App)
        seed(%Read{scope: other}, ["scope:" <> other], App)
      end

      item = [types: [Atom.to_string(Read)], tags: ["scope:" <> other]]
      command = ruled([scope: scope, followup: following([item])], Tight)

      assert_refused_cleanly(command, :decision_query_too_large)
    end

    test "`max_batch_events + 1` refuses, with nothing appended and the state kept" do
      execute = fn %RuledAggregate{}, %{id: id} ->
        for note <- 1..3, do: %Noted{id: id, note: note}
      end

      command = ruled([execute: execute], Tight)

      assert_refused_cleanly(command, :decision_batch_too_large)

      assert [{%{value: 3, limit: 2}, %{bound: :batch_events}}] = received(@refused)

      assert %RuledAggregate{notes: []} =
               App.aggregate_state(RuledAggregate, ruled_stream(command))
    end

    test "`max_batch_events` passes" do
      execute = fn %RuledAggregate{}, %{id: id} ->
        for note <- 1..2, do: %Noted{id: id, note: note}
      end

      assert :ok = App.dispatch(ruled([execute: execute], Tight))
    end

    test "`max_batch_events + 1` after `[]` from `query/2` refuses, with nothing appended" do
      execute = fn %RuledAggregate{}, %{id: id} ->
        for note <- 1..3, do: %Noted{id: id, note: note}
      end

      command = ruled([query: querying([]), execute: execute], Tight)

      log = capture_log(fn -> assert_refused_cleanly(command, :decision_batch_too_large) end)

      assert log =~ "refused on the batch_events bound: 3 over 2"
      refute_received {:read_query, _items, _opts}
      assert [{%{value: 3, limit: 2}, %{bound: :batch_events}}] = received(@refused)

      assert %RuledAggregate{notes: []} =
               App.aggregate_state(RuledAggregate, ruled_stream(command))
    end

    test "`max_batch_events + 1` through a `Multi` after `[]` from `query/2` refuses" do
      execute = fn %RuledAggregate{} = state, %{id: id} ->
        for note <- 1..3, reduce: Multi.new(state) do
          multi -> Multi.execute(multi, fn _state -> %Noted{id: id, note: note} end)
        end
      end

      command = ruled([query: querying([]), execute: execute], Tight)

      log = capture_log(fn -> assert_refused_cleanly(command, :decision_batch_too_large) end)

      assert log =~ "refused on the batch_events bound: 3 over 2"
      refute_received {:read_query, _items, _opts}
      assert [{%{value: 3, limit: 2}, %{bound: :batch_events}}] = received(@refused)

      assert %RuledAggregate{notes: []} =
               App.aggregate_state(RuledAggregate, ruled_stream(command))
    end

    test "`max_batch_events` after `[]` from `query/2` is a plain append, with no condition" do
      execute = fn %RuledAggregate{}, %{id: id} ->
        for note <- 1..2, do: %Noted{id: id, note: note}
      end

      command = ruled([query: querying([]), execute: execute], Tight)
      stream = ruled_stream(command)

      assert :ok = App.dispatch(command)

      assert_received {:append_to_stream, ^stream, 0, 2, []}
      refute_received {:read_query, _items, _opts}
      assert received(@read) == []
      assert received(@refused) == []
      assert [%{data: %Noted{note: 1}}, %{data: %Noted{note: 2}}] = stored(stream, App)
    end
  end

  describe "a callback's answer" do
    test "from `query/2` stands, because it has read nothing" do
      assert_refused_cleanly(ruled(query: querying({:error, {:bad, "detail"}})), {:bad, "detail"})
    end

    test "from `followup_query/3` is reduced, because it has read something" do
      assert_refused_cleanly(
        ruled(followup: following({:error, {:bad, "detail"}})),
        {:bad, :redacted}
      )
    end

    test "from `followup_query/3` keeps an atom" do
      assert_refused_cleanly(ruled(followup: following({:error, :nope})), :nope)
    end
  end

  test "a `before_execute` refusal reads nothing" do
    assert {:error, :gated} = App.dispatch(ruled([], Gated))

    refute_received {:read_query, _items, _opts}
  end

  describe "a store that declares deletable names" do
    setup do
      event_store =
        Stores.store(
          deletable: [
            tag_kinds: ["secret"],
            event_types: ["Elixir.Declared"],
            stream_prefixes: ["gone-"]
          ]
        )

      start_supervised!({App, name: :deletable_rules, event_store: event_store})

      :ok
    end

    defp assert_names_deletable(command) do
      assert {:error, :query_names_deletable} =
               App.dispatch(command, application: :deletable_rules)
    end

    test "a query naming them answers `:query_names_deletable`" do
      assert_names_deletable(ruled(query: querying([[types: ["T"], tags: ["secret:x"]]])))
      assert_names_deletable(ruled(query: querying([[types: ["Elixir.Declared"]]])))
    end

    test "a second read naming them does too" do
      assert_names_deletable(ruled(followup: following([[types: ["T"], tags: ["secret:x"]]])))
    end
  end

  describe "an event no key can read" do
    setup do
      start_supervised!(Keys)

      [user: Keys.mint(UUID.uuid4())]
    end

    test "answers `:key_unknown`, with nothing appended and the aggregate well", %{user: user} do
      tag = "scope:" <> scope()
      stream = "other-" <> UUID.uuid4()

      event = %PersonalRegistered{user_id: user, email: "someone@secret.example"}
      context = %ExecutionContext{command: %Personal{}, metadata: %{}}

      {:ok, prepared} = Append.prepare_events(App, stream, [event], context)

      prepared =
        Enum.map(prepared, fn %EventData{} = data -> Map.update!(data, :tags, &[tag | &1]) end)

      :ok = EventStore.append_to_stream(App, stream, :any_version, prepared)

      Keys.set(user, :unknown)

      types = [Atom.to_string(PersonalRegistered)]
      command = ruled([query: querying([[types: types, tags: [tag]]])], Personal)

      assert_refused_cleanly(command, :key_unknown)

      assert [] = received(@refused)

      Keys.set(user, :key)

      assert :ok = App.dispatch(command)
    end
  end
end
