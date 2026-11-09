defmodule Decree.Aggregates.CommandWindowTest do
  use ExUnit.Case

  import Decree.DecisionStoreCase,
    only: [age_stamps: 2, attach_telemetry: 1, hold: 2, seed: 4, stored: 2]

  import Decree.RuleCase, only: [received: 1]
  import ExUnit.CaptureLog

  alias Decree.Aggregates.{Aggregate, ExecutionContext}
  alias Decree.CommandWindowFixtures, as: Fixtures
  alias Decree.CommandWindowFixtures.{Account, App, DeclaredWindowApp, FunctionIdentityApp}
  alias Decree.CommandWindowFixtures.{Note, Noted, OpaqueApp, Open, PlainAccount, PlainNote}
  alias Decree.CommandWindowFixtures.{PlainRuledNote, RuledNote, TaggedFunctionApp}
  alias Decree.CommandWindowFixtures.UndeclaredWindowApp
  alias Decree.Decision.Rule.Result
  alias Decree.Event.Append
  alias Decree.RuleFixtures.Read
  alias Decree.Store.TagIndex
  alias Decree.TagIndexFixtures
  alias Decree.UUID

  @read [:decree, :decision, :read]
  @retry [:decree, :decision, :retry]
  @an_hour 3_600

  # Every family is enabled unless a test is tagged `:no_index_facts`.
  setup context do
    Decree.FrameworkStorage.reset!()

    start_supervised!(App)

    if not Map.get(context, :no_index_facts, false), do: TagIndexFixtures.enable!(App)

    :ok
  end

  defp open(fields \\ []) do
    struct!(
      Open,
      [command_id: UUID.uuid4(), account_id: UUID.uuid4(), owner: "alice"] ++ fields
    )
  end

  defp note(account_id, fields \\ []) do
    struct!(Note, [command_id: UUID.uuid4(), account_id: account_id, actor: "alice"] ++ fields)
  end

  defp ruled_note(account_id, fields \\ []) do
    struct!(
      RuledNote,
      [
        command_id: UUID.uuid4(),
        account_id: account_id,
        actor: "alice",
        scope: Decree.DecisionStoreCase.scope()
      ] ++ fields
    )
  end

  defp opened! do
    %Open{account_id: account_id} = command = open()

    :ok = App.dispatch(command)

    account_id
  end

  defp stream(account_id) when is_binary(account_id), do: "account-" <> account_id

  defp notes(account_id) do
    account_id |> stream() |> stored(App) |> Enum.map(& &1.data.note)
  end

  # The events across the store that carry `command_id`'s tag.
  defp copies(command_id) do
    tag = "command:" <> command_id

    {:ok, %{events: events}} = Decree.EventStore.read_query(App, [[tags: [tag]]], count: 100)

    events
  end

  # An event on `account_id`'s stream carrying `command`'s framework tags, as its aggregate
  # would have appended it.
  defp rival(command, account_id) do
    tags = [Append.command_tag(command), Append.kind_tag(command), "account:" <> account_id]

    seed(%Noted{account_id: account_id, note: "rival"}, tags, App, stream(account_id))
  end

  # A script step that runs once, however often the script is called.
  defp once(step) do
    counter = :counters.new(1, [])

    fn ->
      if :counters.get(counter, 1) == 0 do
        :counters.add(counter, 1, 1)
        step.()
      end
    end
  end

  # An `execute/2` that tells the test it ran, then does what the aggregate does by default.
  defp telling(test) do
    fn %{} = _state, %{account_id: account_id} = command ->
      send(test, {:executed, command.command_id})

      %Noted{account_id: account_id, note: "noted"}
    end
  end

  defp executions(command_id, count \\ 0) do
    receive do
      {:executed, ^command_id} -> executions(command_id, count + 1)
    after
      0 -> count
    end
  end

  describe "the declaration" do
    test "`command_window_ms/1` answers the declared length" do
      assert Decree.Aggregate.command_window_ms(Account) == Fixtures.window_ms()
    end

    test "`command_window_ms/1` answers nil for a module that declares none" do
      assert Decree.Aggregate.command_window_ms(PlainAccount) == nil
      assert Decree.Aggregate.command_window_ms(Noted) == nil
    end

    test "`use Decree.Aggregate, command_window: [ms: n]` answers it, beside `snapshot:`" do
      [{module, _bytecode}] =
        Code.compile_string("""
        defmodule Decree.Aggregates.CommandWindowTest.Declared do
          use Decree.Aggregate, command_window: [ms: 60_000], snapshot: [every: 4]

          defstruct [:id]

          def authorize(_state, _command), do: :ok
        end
        """)

      assert module.__decree_aggregate__(:command_window) == [ms: 60_000]
      assert module.__decree_aggregate__(:snapshot) == [snapshot_every: 4, snapshot_version: 1]
      assert Decree.Aggregate.command_window_ms(module) == 60_000
    end

    test "a module with no `command_window:` declares none, and needs no `authorize/2`" do
      [{module, _bytecode}] =
        Code.compile_string("""
        defmodule Decree.Aggregates.CommandWindowTest.NoWindow do
          use Decree.Aggregate

          defstruct [:id]
        end
        """)

      assert Decree.Aggregate.command_window_ms(module) == nil
    end

    for {{name, options, word}, index} <-
          Enum.with_index([
            {"`ms: 0`", "command_window: [ms: 0]", "ms"},
            {"`ms: -1`", "command_window: [ms: -1]", "ms"},
            {"`ms: 1.5`", "command_window: [ms: 1.5]", "ms"},
            {~s(`ms: "5"`), ~s(command_window: [ms: "5"]), "ms"},
            {"a non-literal `ms`", "command_window: [ms: :timer.minutes(5)]", "ms"},
            {"no `ms`", "command_window: []", "ms"},
            {"an unknown `command_window:` key", "command_window: [ms: 1, every: 2]", "every"},
            {"a `command_window:` that is not a keyword list", "command_window: 5", "keyword"}
          ]) do
      test "fails to compile with #{name}" do
        module = "Decree.Aggregates.CommandWindowTest.Invalid#{unquote(index)}"

        error =
          assert_raise ArgumentError, fn ->
            Code.compile_string("""
            defmodule #{module} do
              use Decree.Aggregate, #{unquote(options)}

              defstruct [:id]

              def authorize(_state, _command), do: :ok
            end
            """)
          end

        assert error.message =~ module
        assert error.message =~ "command_window"
        assert error.message =~ unquote(word)
      end
    end

    for {{name, body}, index} <-
          Enum.with_index([
            {"no `authorize/2`", ""},
            {"a private `authorize/2`", "defp authorize(_state, _command), do: :ok"},
            {"an `authorize/1`", "def authorize(_state), do: :ok"}
          ]) do
      test "fails to compile with #{name}" do
        module = "Decree.Aggregates.CommandWindowTest.Unauthorized#{unquote(index)}"

        error =
          assert_raise ArgumentError, fn ->
            Code.compile_string("""
            defmodule #{module} do
              use Decree.Aggregate, command_window: [ms: 1_000]

              defstruct [:id]

              #{unquote(body)}
            end
            """)
          end

        assert error.message =~ module
        assert error.message =~ "authorize/2"
      end
    end
  end

  describe "a resend" do
    test "inside the window is a duplicate: nothing appended and `execute/2` not called" do
      account_id = opened!()
      command = note(account_id, script: %{execute: telling(self())})

      assert :ok = App.dispatch(command)
      assert executions(command.command_id) == 1

      assert {:error, :duplicate_command} = App.dispatch(command)
      assert executions(command.command_id) == 0

      assert notes(account_id) == ["opened", "noted"]
    end

    test "of a create inside the window is a duplicate" do
      command = open()

      assert :ok = App.dispatch(command)
      assert {:error, :duplicate_command} = App.dispatch(command)

      assert notes(command.account_id) == ["opened"]
    end

    test "after the window, by the database clock, is a new command and appends" do
      account_id = opened!()
      command = note(account_id)

      assert :ok = App.dispatch(command)

      age_stamps(command.command_id, @an_hour)

      assert :ok = App.dispatch(command)
      assert notes(account_id) == ["opened", "noted", "noted"]
    end

    test "with the id another command kind used on the same stream is reused" do
      %Open{account_id: account_id} = opening = open()
      :ok = App.dispatch(opening)

      assert {:error, :reused_command_id} =
               App.dispatch(note(account_id, command_id: opening.command_id))

      assert notes(account_id) == ["opened"]
    end

    test "with the id the same kind used on another stream is reused" do
      first = note(opened!())
      :ok = App.dispatch(first)

      other = opened!()

      assert {:error, :reused_command_id} =
               App.dispatch(note(other, command_id: first.command_id))

      assert notes(other) == ["opened"]
    end
  end

  describe "the order" do
    # The shape of sparklespace's `assert_unauthorized_never_duplicate/3`: a live id on an
    # existing stream, refused by `authorize/2` before the window is read.
    test "an `authorize/2` refusal wins over a live duplicate" do
      account_id = opened!()
      command = note(account_id)

      :ok = App.dispatch(command)

      attach_telemetry([@read])

      assert {:error, :unauthorized} = App.dispatch(%{command | actor: "mallory"})
      assert received(@read) == []

      # The premise: the id is live, so the owner's resend is the duplicate.
      assert {:error, :duplicate_command} = App.dispatch(command)
      assert notes(account_id) == ["opened", "noted"]
    end

    test "`authorize/2` is not called on an empty stream" do
      test = self()

      authorize = fn _state, _command ->
        send(test, :authorized)
        {:error, :refused}
      end

      command = open(script: %{authorize: authorize})

      assert :ok = App.dispatch(command)
      refute_received :authorized
    end

    test "`authorize/2` is called on an existing stream" do
      test = self()

      authorize = fn %Account{owner: "alice"}, %Note{} ->
        send(test, :authorized)
        :ok
      end

      assert :ok = App.dispatch(note(opened!(), script: %{authorize: authorize}))
      assert_received :authorized
    end

    test "the window runs on an empty stream" do
      first = open()
      :ok = App.dispatch(first)

      second = open(command_id: first.command_id)

      assert {:error, :reused_command_id} = App.dispatch(second)
      assert stored(stream(second.account_id), App) == []
    end

    test "an aggregate with no window never has `authorize/2` called" do
      test = self()

      authorize = fn _state, _command ->
        send(test, :authorized)
        {:error, :refused}
      end

      account_id = UUID.uuid4()
      plain = fn -> %PlainNote{command_id: UUID.uuid4(), account_id: account_id} end

      assert :ok = App.dispatch(plain.())
      assert :ok = App.dispatch(%PlainNote{plain.() | script: %{authorize: authorize}})
      refute_received :authorized
    end
  end

  describe "the tag index" do
    @tag :no_index_facts
    test "before the `command` family is enabled a command refuses" do
      command = open()

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(stream(command.account_id), App) == []
    end

    test "a disabled `command` family refuses a resend as a new command" do
      account_id = opened!()
      command = note(account_id)
      :ok = App.dispatch(command)

      TagIndexFixtures.disable!(App, "command")

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert {:error, :tag_index_not_enabled} = App.dispatch(note(account_id))
      assert notes(account_id) == ["opened", "noted"]
    end
  end

  describe "two writers with one `command_id`" do
    test "on the same stream: the loser rebuilds, re-reads, and is the duplicate" do
      account_id = opened!()
      test = self()
      command = note(account_id)

      # The winner's copy, appended between the loser's read and its append, as the winner's
      # aggregate would have appended it.
      lose_version = once(fn -> rival(command, account_id) end)

      execute = fn _state, %Note{} = command ->
        send(test, {:executed, command.command_id})
        lose_version.()
        %Noted{account_id: account_id, note: "noted"}
      end

      assert {:error, :duplicate_command} =
               App.dispatch(%{command | script: %{execute: execute}})

      assert executions(command.command_id) == 1
      assert notes(account_id) == ["opened", "rival"]
      assert length(copies(command.command_id)) == 1
    end

    test "on two streams of one kind: the loser's condition fails, and it is reused" do
      first = opened!()
      second = opened!()

      command = note(second)

      attach_telemetry([@read, @retry])

      # The winner, on the first stream, commits between the loser's read and its append.
      lose_condition = once(fn -> rival(command, first) end)

      execute = fn _state, %Note{} ->
        lose_condition.()
        %Noted{account_id: second, note: "noted"}
      end

      assert {:error, :reused_command_id} =
               App.dispatch(%{command | script: %{execute: execute}})

      assert notes(second) == ["opened"]
      assert notes(first) == ["opened", "rival"]
      assert length(copies(command.command_id)) == 1

      assert [{%{attempt: 1}, metadata}] = received(@retry)
      assert metadata == %{application: App, decision: Account, reason: :append_condition_failed}
      assert [%{attempt: 1}, %{attempt: 2}] = @read |> received() |> Enum.map(&elem(&1, 1))
    end

    test "between a rule's two reads: the second read's new match reads again, and is reused" do
      first = opened!()
      second = opened!()

      command = ruled_note(second)

      attach_telemetry([@retry])

      # The winner, on the first stream, commits between the loser's two reads.
      lose_between = once(fn -> rival(command, first) end)

      followup = fn command, _context, _events ->
        lose_between.()
        [[types: [Atom.to_string(Read)], tags: ["scope:" <> command.scope]]]
      end

      assert {:error, :reused_command_id} =
               App.dispatch(%{command | script: %{followup: followup}})

      assert notes(second) == ["opened"]
      assert length(copies(command.command_id)) == 1

      assert [{%{attempt: 1}, metadata}] = received(@retry)

      assert metadata == %{
               application: App,
               decision: Decree.RuleFixtures.ScriptedRule,
               reason: :new_command_match
             }
    end

    test "racing for real on two streams: one commits, the other is reused" do
      first = opened!()
      second = opened!()
      command_id = UUID.uuid4()
      test = self()

      held = fn label ->
        hold_once = once(fn -> hold(test, label) end)

        fn _state, %Note{account_id: account_id} ->
          hold_once.()
          %Noted{account_id: account_id, note: "noted"}
        end
      end

      winner = note(first, command_id: command_id, script: %{execute: held.(:winner)})
      loser = note(second, command_id: command_id, script: %{execute: held.(:loser)})

      winning = Task.async(fn -> App.dispatch(winner, timeout: 60_000) end)
      losing = Task.async(fn -> App.dispatch(loser, timeout: 60_000) end)

      # Both have read the window open, and neither has appended.
      assert_receive {:winner, winner_pid}, 5_000
      assert_receive {:loser, loser_pid}, 5_000

      send(winner_pid, :go)
      assert :ok = Task.await(winning, 60_000)

      send(loser_pid, :go)
      assert {:error, :reused_command_id} = Task.await(losing, 60_000)

      assert length(copies(command_id)) == 1
      assert notes(first) == ["opened", "noted"]
      assert notes(second) == ["opened"]
    end
  end

  describe "the read" do
    setup do
      Process.register(self(), :rule_recorder)
      attach_telemetry([@read])
      :ok
    end

    defp index_item, do: [tags: [TagIndex.fact_tag()]]

    defp read_item(scope), do: [types: [Atom.to_string(Read)], tags: ["scope:" <> scope]]

    test "a route with no rule reads the command item and the index item" do
      account_id = opened!()
      flush_recorded()

      command = note(account_id)
      command_tag = Append.command_tag(command)

      assert :ok = App.dispatch(command)

      assert_received {:read_query, items, opts}
      assert items == [[tags: [command_tag]], index_item()]
      assert opts[:command_tag] == command_tag
      refute_received {:read_query, _items, _opts}

      stream = stream(account_id)
      assert_received {:append_to_stream, ^stream, 1, 1, [condition: condition]}
      assert condition[:items] == items
      assert condition[:target] == Account

      assert [{%{items: 2}, metadata}] = received(@read)
      assert metadata == %{application: App, decision: Account, attempt: 1}
    end

    test "a rule route reads its items with the command item, in one read and one condition" do
      account_id = opened!()
      flush_recorded()

      test = self()

      execute = fn _state, %RuledNote{rule_result: %Result{events: events}} ->
        send(test, {:rode, events})
        %Noted{account_id: account_id, note: "ruled"}
      end

      command = ruled_note(account_id, script: %{execute: execute})
      command_tag = Append.command_tag(command)

      assert :ok = App.dispatch(command)
      assert_received {:rode, []}

      assert_received {:read_query, items, opts}
      assert items == [read_item(command.scope), [tags: [command_tag]], index_item()]
      assert opts[:command_tag] == command_tag
      refute_received {:read_query, _items, _opts}

      stream = stream(account_id)
      assert_received {:append_to_stream, ^stream, 1, 1, [condition: condition]}
      assert condition[:items] == items
      assert condition[:target] == Account

      assert [{_measurements, %{decision: Decree.RuleFixtures.ScriptedRule}}] = received(@read)

      assert {:error, :duplicate_command} = App.dispatch(command)
      assert notes(account_id) == ["opened", "ruled"]
    end

    test "a rule whose query answers `[]` still reads the window, under a condition" do
      account_id = opened!()
      flush_recorded()

      command = ruled_note(account_id, script: %{query: fn _command, _context -> [] end})
      command_tag = Append.command_tag(command)

      assert :ok = App.dispatch(command)

      assert_received {:read_query, items, _opts}
      assert items == [[tags: [command_tag]], index_item()]

      stream = stream(account_id)
      assert_received {:append_to_stream, ^stream, 1, 1, [condition: condition]}
      assert condition[:items] == items

      assert {:error, :duplicate_command} = App.dispatch(command)
    end

    test "a rule's `replay: true` item is still refused on a windowed aggregate" do
      account_id = opened!()

      query = fn command, _context ->
        [
          [
            types: [Atom.to_string(Noted)],
            tags: [Append.kind_tag(command), "account:" <> account_id],
            replay: true
          ]
        ]
      end

      assert {:error, :invalid_query} =
               App.dispatch(ruled_note(account_id, script: %{query: query}))
    end

    test "a route with no rule bounds its batch as a rule's default does" do
      account_id = opened!()

      execute = fn _state, %Note{} ->
        for n <- 1..101, do: %Noted{account_id: account_id, note: n}
      end

      log =
        capture_log(fn ->
          assert {:error, :decision_batch_too_large} =
                   App.dispatch(note(account_id, script: %{execute: execute}))
        end)

      assert log =~ "#{inspect(Account)} refused on the batch_events bound: 101 over 100"
      assert notes(account_id) == ["opened"]
    end

    test "a rule route on an aggregate with no window reads as it always has" do
      account_id = UUID.uuid4()
      scope = Decree.DecisionStoreCase.scope()

      command = %PlainRuledNote{command_id: UUID.uuid4(), account_id: account_id, scope: scope}

      assert :ok = App.dispatch(command)

      assert_received {:read_query, items, opts}
      assert items == [read_item(scope), index_item()]
      refute Keyword.has_key?(opts, :command_tag)

      stream = "plain-account-" <> account_id
      assert_received {:append_to_stream, ^stream, 0, 1, [condition: condition]}
      assert condition[:items] == items
      assert condition[:target] == PlainAccount
    end

    test "an aggregate with no window and no rule reads nothing and appends plainly" do
      account_id = UUID.uuid4()

      assert :ok = App.dispatch(%PlainNote{command_id: UUID.uuid4(), account_id: account_id})

      refute_received {:read_query, _items, _opts}

      stream = "plain-account-" <> account_id
      assert_received {:append_to_stream, ^stream, 0, 1, []}
      assert received(@read) == []
    end
  end

  defp flush_recorded do
    receive do
      {:read_query, _items, _opts} -> flush_recorded()
      {:read_tags, _positions} -> flush_recorded()
      {:append_to_stream, _stream, _version, _count, _opts} -> flush_recorded()
      {:telemetry, _event, _measurements, _metadata} -> flush_recorded()
    after
      0 -> :ok
    end
  end

  describe "the probe" do
    defp probe(command), do: Decree.Aggregate.check_command_window(App, command)

    # The probe first, since an open dispatch appends.
    defp parity(command) do
      probed = probe(command)
      dispatched = App.dispatch(command)

      {probed, dispatched}
    end

    test "answers what a dispatch answers when the window is open" do
      assert {:ok, :ok} = parity(note(opened!()))
    end

    test "answers what a dispatch answers for a duplicate" do
      command = note(opened!())
      :ok = App.dispatch(command)

      assert {{:error, :duplicate_command}, {:error, :duplicate_command}} = parity(command)
    end

    test "answers what a dispatch answers for an id another kind used on the stream" do
      %Open{account_id: account_id} = opening = open()
      :ok = App.dispatch(opening)

      assert {{:error, :reused_command_id}, {:error, :reused_command_id}} =
               parity(note(account_id, command_id: opening.command_id))
    end

    test "answers what a dispatch answers for an id the kind used on another stream" do
      first = note(opened!())
      :ok = App.dispatch(first)

      assert {{:error, :reused_command_id}, {:error, :reused_command_id}} =
               parity(note(opened!(), command_id: first.command_id))
    end

    test "answers what a dispatch answers on an empty stream" do
      first = open()
      :ok = App.dispatch(first)

      assert {{:error, :reused_command_id}, {:error, :reused_command_id}} =
               parity(open(command_id: first.command_id))
    end

    @tag :no_index_facts
    test "answers what a dispatch answers before the index is enabled" do
      assert {{:error, :tag_index_not_enabled}, {:error, :tag_index_not_enabled}} =
               parity(open())
    end

    test "answers what a dispatch answers for a bad `command_id` or identity" do
      assert {{:error, :command_id_required}, {:error, :command_id_required}} =
               parity(open(command_id: nil))

      assert {{:error, :invalid_command_id}, {:error, :invalid_command_id}} =
               parity(open(command_id: "not-a-uuid"))

      assert {{:error, :invalid_aggregate_identity}, {:error, :invalid_aggregate_identity}} =
               parity(open(account_id: "not-a-uuid"))
    end

    test "on a rule route reads the window alone" do
      command = ruled_note(opened!())
      :ok = App.dispatch(command)

      assert {{:error, :duplicate_command}, {:error, :duplicate_command}} = parity(command)
    end

    test "does not run `authorize/2`" do
      account_id = opened!()
      command = note(account_id)
      :ok = App.dispatch(command)

      mallory = %{command | actor: "mallory"}

      assert {{:error, :duplicate_command}, {:error, :unauthorized}} = parity(mallory)
    end

    test "raises for a command no route names" do
      assert_raise ArgumentError, ~r/no route/, fn ->
        probe(%Decree.RuleFixtures.Unrouted{command_id: UUID.uuid4(), id: UUID.uuid4()})
      end
    end

    test "raises for a route whose aggregate declares no window" do
      assert_raise ArgumentError, ~r/PlainAccount/, fn ->
        probe(%PlainNote{command_id: UUID.uuid4(), account_id: UUID.uuid4()})
      end
    end
  end

  describe "at start" do
    defp failed_start(app) do
      Process.flag(:trap_exit, true)

      assert {:error, {:shutdown, {:failed_to_start_child, id, failure}}} = app.start_link()
      assert id == {Decree.Application.Supervisor, :conditional_targets}
      assert {:EXIT, {%ArgumentError{message: message}, _stacktrace}} = failure

      message
    end

    test "a windowed aggregate the store does not declare raises, naming both" do
      message = failed_start(UndeclaredWindowApp)

      assert message =~ inspect(UndeclaredWindowApp)
      assert message =~ inspect(Account)
      assert message =~ "conditional_appends"
    end

    test "a windowed aggregate routed by a function identity with no `stream_tag:` raises" do
      message = failed_start(FunctionIdentityApp)

      assert message =~ inspect(Account)
      assert message =~ "stream_tag"
    end

    test "a windowed aggregate routed by an opaque identity raises" do
      message = failed_start(OpaqueApp)

      assert message =~ inspect(Account)
      assert message =~ "opaque"
    end

    test "a function identity with a `stream_tag:` starts" do
      start_supervised!(TaggedFunctionApp)
    end

    test "a declared windowed aggregate no rule route names draws no warning" do
      log = capture_log(fn -> start_supervised!(DeclaredWindowApp) end)

      refute log =~ "conditional target"
    end
  end

  describe "a direct call that bypassed the router" do
    defp direct(command, account_id) do
      stream = stream(account_id)

      {:ok, ^stream} = Decree.Aggregates.Supervisor.open_aggregate(App, Account, stream)

      context = %ExecutionContext{
        command: command,
        handler: Account,
        function: :execute,
        identity: :account_id,
        aggregate_identity: command.account_id,
        retry_attempts: 1,
        rule_retries: 1
      }

      Aggregate.execute(App, Account, stream, context)
    end

    test "with no `command_id` answers `:command_id_required`" do
      account_id = opened!()

      assert {:error, :command_id_required} =
               direct(%Note{account_id: account_id, command_id: nil}, account_id)

      assert notes(account_id) == ["opened"]
    end

    test "with no stream tag answers `:invalid_aggregate_identity`" do
      account_id = opened!()

      assert {:error, :invalid_aggregate_identity} =
               direct(%Note{account_id: nil, command_id: UUID.uuid4()}, account_id)

      assert notes(account_id) == ["opened"]
    end
  end
end
