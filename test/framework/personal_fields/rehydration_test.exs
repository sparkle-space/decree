defmodule Decree.PersonalFields.RehydrationTest do
  use Decree.PersonalFields.Case

  import ExUnit.CaptureLog

  alias Decree.Aggregates.Aggregate
  alias Decree.Aggregates.AggregateStateBuilder
  alias Decree.Aggregates.Supervisor, as: AggregateSupervisor
  alias Decree.Commands.ExecutionResult
  alias Decree.PersonalFields.Fixtures.Account

  alias Decree.PersonalFields.Fixtures.Commands.{
    ClauseOnState,
    DetailOnState,
    Emit,
    ExitNow,
    Hold,
    MatchState,
    RaiseNow
  }

  alias Decree.PersonalFields.Fixtures.Events.{
    Linked,
    MetadataOnly,
    Plain,
    PlainPoison,
    Poison,
    Registered,
    Unfolded
  }

  alias Decree.PersonalFields.Fixtures.UniqueViolation
  alias Decree.PersonalFields.RedactedError
  alias Decree.Registration
  alias Decree.Snapshotting

  setup do
    start_supervised!(App)

    :ok
  end

  defp resident(stream), do: Registration.whereis_name(App, {App, Account, stream})

  defp monitor_resident(stream) do
    pid = resident(stream)
    assert is_pid(pid)

    {pid, Process.monitor(pid)}
  end

  defp stop_resident(stream), do: :ok = Aggregate.shutdown(App, Account, stream)

  defp registered(stream, subject) do
    dispatch!(stream, %Registered{user_id: subject, email: email()},
      metadata: %{"password_hash" => "hash"}
    )
  end

  defp bare_aggregate(stream) do
    %Aggregate{
      application: App,
      aggregate_module: Account,
      aggregate_uuid: stream,
      snapshotting: Snapshotting.new(App, stream, [])
    }
  end

  describe "the fold" do
    test "decrypts what it lists, before `apply/2`", %{stream: stream, subject: subject} do
      registered(stream, subject)
      stop_resident(stream)

      assert %Account{email: "alice@secret.example", seen: 1} =
               Aggregate.aggregate_state(App, Account, stream)
    end

    test "leaves what it does not list as stored", %{stream: stream, subject: subject} do
      dispatch!(stream, %Unfolded{user_id: subject, email: email()})
      stop_resident(stream)

      assert %Account{unfolded: %{"$pf": 1}} = Aggregate.aggregate_state(App, Account, stream)
    end

    test "an unknown key on an event it does not list does not lock it", context do
      %{stream: stream, subject: subject} = context

      dispatch!(stream, %Unfolded{user_id: subject, email: email()})
      stop_resident(stream)
      Keys.set(subject, :unknown)

      assert %Account{seen: 1} = Aggregate.aggregate_state(App, Account, stream)
    end

    test "reads no metadata", %{stream: stream, subject: subject} do
      dispatch!(stream, %MetadataOnly{user_id: subject}, metadata: %{"password_hash" => "hash"})
      stop_resident(stream)
      before = Keys.lookups(subject)
      Keys.set(subject, :unknown)

      assert %Account{seen: 1} = Aggregate.aggregate_state(App, Account, stream)
      assert Keys.lookups(subject) == before
    end

    test "looks a subject up once, however many events", %{stream: stream, subject: subject} do
      dispatch!(stream, [
        %Registered{user_id: subject, email: email()},
        %Linked{user_id: subject, provider_uid: "a"},
        %Linked{user_id: subject, provider_uid: "b"}
      ])

      stop_resident(stream)
      before = Keys.lookups(subject)

      assert %Account{seen: 3} = Aggregate.aggregate_state(App, Account, stream)
      assert Keys.lookups(subject) == before + 1
    end

    test "populate/1 and rebuild_from_events/1 keep answering the struct", context do
      %{stream: stream, subject: subject} = context

      registered(stream, subject)

      assert %Aggregate{aggregate_version: 1, aggregate_state: %Account{email: email}} =
               AggregateStateBuilder.populate(bare_aggregate(stream))

      assert email == "alice@secret.example"

      assert %Aggregate{aggregate_version: 1, aggregate_state: %Account{}} =
               AggregateStateBuilder.rebuild_from_events(%Aggregate{
                 bare_aggregate(stream)
                 | aggregate_state: %Account{}
               })
    end

    test "populate/1 and rebuild_from_events/1 answer an unknown key", context do
      %{stream: stream, subject: subject} = context

      registered(stream, subject)
      Keys.set(subject, :unknown)

      assert AggregateStateBuilder.populate(bare_aggregate(stream)) == {:error, :key_unknown}

      assert AggregateStateBuilder.rebuild_from_events(%Aggregate{
               bare_aggregate(stream)
               | aggregate_state: %Account{}
             }) == {:error, :key_unknown}
    end

    test "an aggregate that lists nothing folds as stored", %{subject: subject} do
      stream = UUID.uuid4()

      dispatch!(stream, %Plain{user_id: subject, note: "a note"})

      assert Keys.lookups(subject) == 0
    end
  end

  describe "on dispatch" do
    test "an unknown key stops the aggregate and answers the caller", context do
      %{stream: stream, subject: subject} = context

      registered(stream, subject)
      stop_resident(stream)
      Keys.set(subject, :unknown)

      log =
        capture_log(fn ->
          assert App.dispatch(%Emit{account_id: stream, events: []}) == {:error, :key_unknown}
        end)

      refute log =~ "terminating"
      assert resident(stream) == :undefined

      Keys.set(subject, :key)

      assert :ok = App.dispatch(%Emit{account_id: stream, events: []})
    end

    test "an unknown key from preparing an append does not stop it", context do
      %{stream: stream, subject: subject} = context

      registered(stream, subject)
      pid = resident(stream)
      other = Keys.mint(UUID.uuid4())
      Keys.set(other, :unknown)

      assert {:error, :key_unknown} =
               App.dispatch(%Emit{
                 account_id: stream,
                 events: [%Registered{user_id: other, email: email()}]
               })

      assert resident(stream) == pid
      assert %Account{user_id: ^subject} = Aggregate.aggregate_state(App, Account, stream)
    end
  end

  describe "a retry after a version conflict" do
    test "meeting an unknown key answers, stops, appends nothing, then recovers", context do
      %{conn: conn, stream: stream, subject: subject} = context
      writer = Keys.mint(UUID.uuid4())

      registered(stream, subject)

      # Stored as the concurrent writer would.
      [concurrent] = prepare!(stream, %Linked{user_id: subject, provider_uid: "elsewhere"})

      task =
        Task.async(fn ->
          App.dispatch(%Hold{
            account_id: stream,
            events: [%Registered{user_id: writer, email: email("writer")}]
          })
        end)

      assert_receive {:executing, aggregate}
      ref = Process.monitor(aggregate)

      :ok = EventStore.append_to_stream(App, stream, 1, [concurrent])
      Keys.set(subject, :unknown)
      stored = rows(conn, "events", "data, metadata")

      send(aggregate, :release)

      assert Task.await(task) == {:error, :key_unknown}
      assert_receive {:DOWN, ^ref, :process, ^aggregate, {:shutdown, :key_unknown}}
      assert rows(conn, "events", "data, metadata") == stored

      Keys.set(subject, :key)

      assert :ok =
               App.dispatch(%Emit{
                 account_id: stream,
                 events: [%Registered{user_id: writer, email: email("writer")}]
               })

      assert [_registered, _linked, _third] = read(stream)
    end

    test "stops on the failed rebuild itself, not on the event it missed", context do
      %{stream: stream, subject: subject} = context

      registered(stream, subject)

      [concurrent] = prepare!(stream, %Linked{user_id: subject, provider_uid: "elsewhere"})

      holding = Task.async(fn -> App.dispatch(%Hold{account_id: stream, events: [%Plain{}]}) end)

      assert_receive {:executing, aggregate}
      ref = Process.monitor(aggregate)

      :ok = EventStore.append_to_stream(App, stream, 1, [concurrent])

      # Unknown for the rebuild alone, so the concurrent event's own delivery would read: only
      # the rebuild's failure can stop this aggregate.
      Keys.set(subject, :unknown_once)
      send(aggregate, :release)

      assert Task.await(holding) == {:error, :key_unknown}
      assert_receive {:DOWN, ^ref, :process, ^aggregate, {:shutdown, :key_unknown}}
    end

    test "a queued command answers the same, and the next dispatch succeeds", context do
      %{stream: stream, subject: subject} = context

      registered(stream, subject)

      [concurrent] = prepare!(stream, %Linked{user_id: subject, provider_uid: "elsewhere"})

      holding = Task.async(fn -> App.dispatch(%Hold{account_id: stream, events: [%Plain{}]}) end)

      assert_receive {:executing, aggregate}
      ref = Process.monitor(aggregate)

      queued = Task.async(fn -> App.dispatch(%Emit{account_id: stream, events: [%Plain{}]}) end)

      Wait.until(fn ->
        assert {:message_queue_len, length} = Process.info(aggregate, :message_queue_len)
        assert length >= 1
      end)

      :ok = EventStore.append_to_stream(App, stream, 1, [concurrent])
      Keys.set(subject, :unknown)
      send(aggregate, :release)

      assert Task.await(holding) == {:error, :key_unknown}
      assert Task.await(queued) == {:error, :key_unknown}
      assert_receive {:DOWN, ^ref, :process, ^aggregate, {:shutdown, :key_unknown}}

      Keys.set(subject, :key)

      assert :ok = App.dispatch(%Emit{account_id: stream, events: []})
    end
  end

  describe "an event on its own stream" do
    test "meeting an unknown key stops it with no crash report", context do
      %{stream: stream, subject: subject} = context

      registered(stream, subject)
      {pid, ref} = monitor_resident(stream)

      [concurrent] = prepare!(stream, %Linked{user_id: subject, provider_uid: "elsewhere"})
      Keys.set(subject, :unknown)

      log =
        capture_log(fn ->
          :ok = EventStore.append_to_stream(App, stream, 1, [concurrent])

          assert_receive {:DOWN, ^ref, :process, ^pid, {:shutdown, :key_unknown}}
        end)

      refute log =~ "terminating"
    end

    test "with a key is folded in plaintext", %{stream: stream, subject: subject} do
      registered(stream, subject)

      [concurrent] = prepare!(stream, %Linked{user_id: subject, provider_uid: "elsewhere"})
      :ok = EventStore.append_to_stream(App, stream, 1, [concurrent])

      Wait.until(fn ->
        assert %Account{links: [%{provider_uid: "elsewhere"}]} =
                 Aggregate.aggregate_state(App, Account, stream)
      end)
    end

    test "not on its list does not stop it", %{stream: stream, subject: subject} do
      registered(stream, subject)
      pid = resident(stream)

      [concurrent] = prepare!(stream, %Unfolded{user_id: subject, email: email()})
      Keys.set(subject, :unknown)
      :ok = EventStore.append_to_stream(App, stream, 1, [concurrent])

      Wait.until(fn ->
        assert %Account{seen: 2} = Aggregate.aggregate_state(App, Account, stream)
      end)

      assert resident(stream) == pid
    end
  end

  describe "a cold read" do
    test "exits with the tuple a stopping aggregate's call does", context do
      %{stream: stream, subject: subject} = context

      registered(stream, subject)
      stop_resident(stream)
      Keys.set(subject, :unknown)

      name = Registration.via_tuple(App, {App, Account, stream})

      assert catch_exit(Aggregate.aggregate_state(App, Account, stream)) ==
               {{:shutdown, :key_unknown}, {GenServer, :call, [name, :aggregate_state, 5_000]}}

      # The link did not crash this process, which is still here to say so.
      assert Process.alive?(self())
    end

    test "with a key answers", %{stream: stream, subject: subject} do
      registered(stream, subject)
      stop_resident(stream)

      assert %Account{email: "alice@secret.example"} =
               Aggregate.aggregate_state(App, Account, stream)
    end
  end

  describe "a raise from `apply/2` on a decrypted event" do
    defp store_poison(stream, subject, mode) do
      [event] = prepare!(stream, %Poison{user_id: subject, email: email(), mode: mode})

      append_raw!(stream, event)
    end

    for {mode, type} <- [
          {"match", MatchError},
          {"clause", FunctionClauseError},
          {"database", UniqueViolation}
        ] do
      test "(#{mode}) on the dispatch path is redacted", %{stream: stream, subject: subject} do
        store_poison(stream, subject, unquote(mode))
        Process.flag(:trap_exit, true)

        log =
          capture_log(fn ->
            {:ok, ^stream} = AggregateSupervisor.open_aggregate(App, Account, stream)
            pid = resident(stream)
            ref = Process.monitor(pid)

            assert_receive {:DOWN, ^ref, :process, ^pid, reason}

            assert {%RedactedError{type: unquote(type)}, stacktrace} = reason
            assert is_list(stacktrace)
            refute inspect(reason, limit: :infinity) =~ "secret.example"

            Process.sleep(50)
          end)

        assert log =~ "details redacted"
        refute log =~ "secret.example"
      end

      test "(#{mode}) on the cold path is redacted", %{stream: stream, subject: subject} do
        store_poison(stream, subject, unquote(mode))

        log =
          capture_log(fn ->
            {pid, ref} = spawn_monitor(fn -> Aggregate.aggregate_state(App, Account, stream) end)

            assert_receive {:DOWN, ^ref, :process, ^pid, _reason}

            Process.sleep(50)
          end)

        assert log =~ "details redacted"
        refute log =~ "secret.example"
      end
    end

    for {mode, reason} <- [
          {"exit", {:gave_up, :redacted}},
          {"throw", {:bad_return_value, {:error, :redacted}}}
        ] do
      test "(#{mode}) from `apply/2` on the dispatch path is reduced", context do
        %{stream: stream, subject: subject} = context
        store_poison(stream, subject, unquote(mode))

        log =
          capture_log(fn ->
            {:ok, ^stream} = AggregateSupervisor.open_aggregate(App, Account, stream)
            pid = resident(stream)
            ref = Process.monitor(pid)

            assert_receive {:DOWN, ^ref, :process, ^pid, reason}
            assert reason == unquote(Macro.escape(reason))

            Process.sleep(50)
          end)

        refute log =~ "secret.example"
      end

      test "(#{mode}) from `apply/2` on the cold path is reduced", context do
        %{stream: stream, subject: subject} = context
        store_poison(stream, subject, unquote(mode))

        log =
          capture_log(fn ->
            {pid, ref} = spawn_monitor(fn -> Aggregate.aggregate_state(App, Account, stream) end)

            assert_receive {:DOWN, ^ref, :process, ^pid, reason}
            refute inspect(reason, limit: :infinity) =~ "secret.example"

            Process.sleep(50)
          end)

        refute log =~ "secret.example"
      end
    end

    test "(exit) on its own stream is reduced" do
      fresh = UUID.uuid4()
      subject = Keys.mint(UUID.uuid4())

      registered(fresh, subject)
      {pid, ref} = monitor_resident(fresh)

      [poison] = prepare!(fresh, %Poison{user_id: subject, email: email(), mode: "exit"})

      log =
        capture_log(fn ->
          :ok = EventStore.append_to_stream(App, fresh, 1, [poison])

          assert_receive {:DOWN, ^ref, :process, ^pid, {:gave_up, :redacted}}

          Process.sleep(50)
        end)

      refute log =~ "secret.example"
    end

    test "on an event it did not decrypt stays exactly as it is", %{stream: stream} do
      store_raw!(stream, %PlainPoison{mode: "exit", note: "shown"})

      log =
        capture_log(fn ->
          {:ok, ^stream} = AggregateSupervisor.open_aggregate(App, Account, stream)
          pid = resident(stream)
          ref = Process.monitor(pid)

          assert_receive {:DOWN, ^ref, :process, ^pid, {:gave_up, "shown"}}

          Process.sleep(50)
        end)

      assert log =~ "shown"
    end

    test "on its own stream is redacted" do
      fresh = UUID.uuid4()
      subject = Keys.mint(UUID.uuid4())

      registered(fresh, subject)
      {pid, ref} = monitor_resident(fresh)

      [poison] = prepare!(fresh, %Poison{user_id: subject, email: email()})

      log =
        capture_log(fn ->
          :ok = EventStore.append_to_stream(App, fresh, 1, [poison])

          assert_receive {:DOWN, ^ref, :process, ^pid, {%RedactedError{type: MatchError}, _stack}}

          Process.sleep(50)
        end)

      refute log =~ "secret.example"
    end
  end

  describe "a raise while executing, in an aggregate holding decrypted state" do
    setup %{stream: stream, subject: subject} do
      registered(stream, subject)

      attach_telemetry([[:decree, :aggregate, :execute, :exception]])

      :ok
    end

    for {name, command, type} <- [
          {"a `MatchError` on the state", MatchState, MatchError},
          {"a `FunctionClauseError` taking the state", ClauseOnState, FunctionClauseError},
          {"a database error's detail", DetailOnState, UniqueViolation},
          {"a plain raise", RaiseNow, RuntimeError}
        ] do
      test "#{name} is redacted", %{stream: stream} do
        log =
          capture_log([level: :error], fn ->
            assert {:error, %RedactedError{type: unquote(type)}} =
                     App.dispatch(struct(unquote(command), account_id: stream))
          end)

        assert log =~ "details redacted"
        refute log =~ "secret.example"

        # Its metadata carries the state, and a telemetry handler must not log that: the
        # exception and the stacktrace are what decree adds.
        assert [{:telemetry, _event, _measurements, metadata}] = flush_telemetry()
        assert %RedactedError{type: unquote(type)} = metadata.reason
        refute inspect(metadata.stacktrace, limit: :infinity) =~ "secret.example"
      end
    end
  end

  describe "format_status/1" do
    test "a crash report holds neither the state nor the metadata", context do
      %{stream: stream, subject: subject} = context

      registered(stream, subject)
      {pid, ref} = monitor_resident(stream)

      log =
        capture_log(fn ->
          assert {:error, :aggregate_execution_failed} =
                   App.dispatch(%ExitNow{account_id: stream, reason: :boom},
                     metadata: %{"password_hash" => "hash-in-the-metadata"}
                   )

          assert_receive {:DOWN, ^ref, :process, ^pid, :boom}

          Process.sleep(50)
        end)

      assert log =~ "terminating"
      assert log =~ "execute_command"
      assert log =~ ":redacted"
      refute log =~ "secret.example"
      refute log =~ "hash-in-the-metadata"
    end

    test "`:sys.get_status/1` holds neither", %{stream: stream, subject: subject} do
      registered(stream, subject)

      pid = resident(stream)

      status = inspect(:sys.get_status(pid), limit: :infinity, printable_limit: :infinity)

      assert status =~ "aggregate_state: :redacted"
      refute status =~ "secret.example"
    end

    test "the logged events it formats hold neither", %{stream: stream, subject: subject} do
      registered(stream, subject)

      pid = resident(stream)

      # With its log on, the process keeps each message it was sent, and a status formats them.
      # That log is also in the status's raw debug options, which `:sys` prints beyond
      # `format_status/1`'s reach, and which only an operator's `:sys.log/2` turns on.
      :ok = :sys.log(pid, true)

      {:ok, %ExecutionResult{}} =
        App.dispatch(%Emit{account_id: stream},
          metadata: %{"password_hash" => "hash-in-the-metadata"},
          returning: :execution_result
        )

      {:status, ^pid, _module, [_process_dictionary, _status, _parent, _debug, formatted]} =
        :sys.get_status(pid)

      status = inspect(formatted, limit: :infinity, printable_limit: :infinity)

      assert status =~ "Decree.PersonalFields.Fixtures.Account"
      refute status =~ "secret.example"
      refute status =~ "hash-in-the-metadata"
    end

    test "clears the log" do
      assert %{log: []} =
               Aggregate.format_status(%{
                 state: %Aggregate{aggregate_state: %Account{email: email()}},
                 message: :other,
                 log: [{:in, "something"}]
               })
    end

    test "leaves other messages, and answers for a state it does not know" do
      assert %{message: :other, state: :other} =
               Aggregate.format_status(%{state: :other, message: :other})
    end
  end

  describe "an exit that reaches the dispatcher" do
    test "prints no task crash report with the command's context", context do
      %{stream: stream, subject: subject} = context

      registered(stream, subject)

      log =
        capture_log(fn ->
          assert {:error, :aggregate_execution_failed} =
                   App.dispatch(%ExitNow{account_id: stream, reason: :boom},
                     metadata: %{"email" => "in-the-context@secret.example"}
                   )

          Process.sleep(50)
        end)

      refute log =~ "in-the-context"
    end
  end

  test "an exception with a detail in its message shows no detail" do
    assert Exception.message(RedactedError.redact(%UniqueViolation{detail: "Key (email)=(x)"})) =~
             "UniqueViolation"

    refute Exception.message(RedactedError.redact(%UniqueViolation{detail: "Key (email)=(x)"})) =~
             "Key (email)"
  end
end
