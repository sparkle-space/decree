defmodule Decree.PersonalFields.DeliveryTest do
  use Decree.PersonalFields.Case

  import ExUnit.CaptureLog

  alias Decree.Event.FailureContext
  alias Decree.PersonalFields.Fixtures.{Handler, Leaky, LeakyReporting, OpaqueHandler}
  alias Decree.PersonalFields.Fixtures.{RetryingErrors, SkippingErrors}
  alias Decree.PersonalFields.Fixtures.Events.{Leak, Plain, Registered}
  alias Decree.PersonalFields.RedactedError

  @handler_name inspect(Handler)

  describe "a handler that names an event" do
    setup do
      start_supervised!(App)

      :ok
    end

    test "receives plaintext, and its metadata enriched from the copy", context do
      %{stream: stream, subject: subject} = context

      dispatch!(stream, %Registered{user_id: subject, email: email()},
        metadata: %{"email" => email(), "password_hash" => "hash", "who" => "alice"}
      )

      start_supervised!({Handler, start_from: :origin})

      assert_receive {:handled, %Registered{email: "alice@secret.example"}, metadata}

      assert %{
               :stream_id => ^stream,
               :handler_name => @handler_name,
               "email" => "alice@secret.example",
               "password_hash" => "hash",
               "who" => "alice"
             } = metadata
    end

    test "one lookup per subject per delivered event", context do
      %{stream: stream, subject: subject, conn: conn} = context

      dispatch!(stream, %Registered{user_id: subject, email: email()},
        metadata: %{"email" => email(), "password_hash" => "hash"}
      )

      before = Keys.lookups(subject)

      start_supervised!({Handler, start_from: :origin})
      assert_receive {:handled, %Registered{}, _metadata}
      Wait.until(fn -> assert acked(conn, @handler_name) == 1 end)

      assert Keys.lookups(subject) == before + 1
    end

    test "an event it does not name reaches the catch-all stored, unlooked up", context do
      %{conn: conn, stream: stream, subject: subject} = context

      dispatch!(stream, %Leak{user_id: subject, email: email()})
      Keys.set(subject, :unknown)
      before = Keys.lookups(subject)

      start_supervised!({Handler, start_from: :origin})

      Wait.until(fn -> assert acked(conn, @handler_name) == 1 end)

      assert Keys.lookups(subject) == before
      refute_received {:handled, _event, _metadata}
    end

    test "`:opaque` decrypts nothing", %{conn: conn, stream: stream, subject: subject} do
      dispatch!(stream, %Registered{user_id: subject, email: email()},
        metadata: %{"password_hash" => "hash"}
      )

      Keys.set(subject, :unknown)
      before = Keys.lookups(subject)

      start_supervised!({OpaqueHandler, start_from: :origin})

      assert_receive {:opaque, %Registered{email: %{"$pf": 1}}, metadata}
      assert %{"$pf" => 1} = metadata["password_hash"]
      assert Keys.lookups(subject) == before

      Wait.until(fn -> assert acked(conn, inspect(OpaqueHandler)) == 1 end)
    end
  end

  describe "a sustained unknown key" do
    test "holds the handler past its own `error/3`, and loses nothing", context do
      %{conn: conn, stream: stream, subject: subject} = context

      start_supervised!({App, on_event_handler_error: RetryingErrors})

      dispatch!(stream, %Registered{user_id: subject, email: nil},
        metadata: %{"password_hash" => "hash"}
      )

      stored = rows(conn, "events", "data, metadata")
      Keys.set(subject, :unknown)

      start_supervised!({Handler, start_from: :origin})

      for attempt <- 1..7 do
        assert_receive {:held, ^attempt, {:error, :key_unknown}}
        assert acked(conn, @handler_name) == 0
      end

      refute_received {:handled, _event, _metadata}
      refute_received {:module_error, _error}

      Keys.set(subject, :key)

      assert_receive {:handled, %Registered{}, %{"password_hash" => "hash"}}
      Wait.until(fn -> assert acked(conn, @handler_name) == 1 end)

      refute_receive {:handled, _event, _metadata}
      refute_received {:module_error, _error}
      assert rows(conn, "events", "data, metadata") == stored
    end

    for posture <- [nil, :stop, :backoff] do
      test "under `#{inspect(posture)}` stops the handler with nothing acked", context do
        %{conn: conn, stream: stream, subject: subject} = context

        start_supervised!({App, on_event_handler_error: unquote(posture)})

        dispatch!(stream, %Registered{user_id: subject, email: email()})
        Keys.set(subject, :unknown)
        Process.flag(:trap_exit, true)

        {:ok, handler} = Handler.start_link(start_from: :origin)

        assert_receive {:EXIT, ^handler, :key_unknown}
        assert acked(conn, @handler_name) == 0
        refute_received {:module_error, _error}
        refute_received {:handled, _event, _metadata}
      end
    end

    test "under a module that answers `:skip` stops the handler", context do
      %{conn: conn, stream: stream, subject: subject} = context

      start_supervised!({App, on_event_handler_error: SkippingErrors})

      dispatch!(stream, %Registered{user_id: subject, email: email()})
      Keys.set(subject, :unknown)
      Process.flag(:trap_exit, true)

      {:ok, handler} = Handler.start_link(start_from: :origin)

      assert_receive {:asked_to_skip, {:error, :key_unknown}}
      assert_receive {:EXIT, ^handler, :key_unknown}
      assert acked(conn, @handler_name) == 0
    end

    test "logs at warning, without the event", %{stream: stream, subject: subject} do
      start_supervised!({App, on_event_handler_error: RetryingErrors})

      dispatch!(stream, %Registered{user_id: subject, email: email()})
      Keys.set(subject, :unknown)

      attach_telemetry([[:decree, :event, :handle, :stop]])

      log =
        capture_log([level: :warning], fn ->
          start_supervised!({Handler, start_from: :origin})
          assert_receive {:held, 2, {:error, :key_unknown}}
        end)

      assert log =~ "can't read a personal field of event #1"
      refute log =~ "[error]"
      refute log =~ "$pf"
      refute log =~ "failed to handle event"

      assert [{:telemetry, _event, _measurements, metadata} | _rest] = flush_telemetry()
      assert metadata.error == :key_unknown
    end
  end

  describe "the set derived from the heads" do
    setup do
      start_supervised!(App)
      Code.put_compiler_option(:ignore_module_conflict, true)
      on_exit(fn -> Code.put_compiler_option(:ignore_module_conflict, false) end)

      :ok
    end

    @with_clause """
    defmodule Decree.PersonalFields.DeliveryTest.Compiled do
      use Decree.Event.Handler,
        application: Decree.PersonalFields.Fixtures.App,
        name: "compiled"

      alias Decree.PersonalFields.Fixtures.Events.{Plain, Registered}
      alias Decree.PersonalFields.Fixtures.Keys

      def handle(%Plain{}, _metadata), do: :ok

      def handle(%Registered{} = event, _metadata) do
        Keys.notify({:compiled, event})
        :ok
      end
    end
    """

    @without_clause """
    defmodule Decree.PersonalFields.DeliveryTest.Compiled do
      use Decree.Event.Handler,
        application: Decree.PersonalFields.Fixtures.App,
        name: "compiled"

      alias Decree.PersonalFields.Fixtures.Events.Plain

      def handle(%Plain{}, _metadata), do: :ok
    end
    """

    # Compiled at runtime by these tests, so the compiler can't see these calls.
    defp compiled, do: Decree.PersonalFields.DeliveryTest.Compiled

    defp personal_fields, do: apply(compiled(), :__personal_fields__, [])

    defp deliver(name) do
      {:ok, handler} = apply(compiled(), :start_link, [[name: name, start_from: :origin]])

      handler
    end

    test "a removed clause leaves that event undecrypted, and restoring it decrypts", context do
      %{conn: conn, stream: stream, subject: subject} = context

      dispatch!(stream, %Registered{user_id: subject, email: email()})

      Code.compile_string(@with_clause)

      assert personal_fields() == %{Plain => true, Registered => true}

      deliver("with-clause")
      assert_receive {:compiled, %Registered{email: "alice@secret.example"}}
      Wait.until(fn -> assert acked(conn, "with-clause") == 1 end)

      Code.compile_string(@without_clause)
      assert personal_fields() == %{Plain => true}

      before = Keys.lookups(subject)

      deliver("without-clause")
      Wait.until(fn -> assert acked(conn, "without-clause") == 1 end)

      assert Keys.lookups(subject) == before
      refute_received {:compiled, _event}

      Code.compile_string(@with_clause)
      deliver("restored")

      assert_receive {:compiled, %Registered{email: "alice@secret.example"}}
      assert Keys.lookups(subject) == before + 1
    end
  end

  describe "a failure on a decrypted event" do
    setup %{stream: stream, subject: subject} do
      start_supervised!(App)

      attach_telemetry([
        [:decree, :event, :handle, :stop],
        [:decree, :event, :handle, :exception]
      ])

      [
        plaintext: email(),
        leak: fn mode ->
          dispatch!(stream, %Leak{user_id: subject, email: email(), mode: mode})
        end
      ]
    end

    defp failing_run(handler, opts \\ []) do
      Process.flag(:trap_exit, true)

      capture_log(fn ->
        {:ok, pid} = handler.start_link([start_from: :origin] ++ opts)

        assert_receive {:EXIT, ^pid, reason}
        send(self(), {:stopped_with, reason})

        # A crash report is written after the exit.
        Process.sleep(50)
      end)
    end

    defp assert_nothing_shown(log, plaintext) do
      telemetry = flush_telemetry()

      refute log =~ plaintext
      refute log =~ "secret.example"
      refute inspect(telemetry, limit: :infinity, printable_limit: :infinity) =~ "secret.example"

      assert_received {:stopped_with, reason}
      refute inspect(reason, limit: :infinity) =~ "secret.example"

      reason
    end

    for {mode, type} <- [
          {"match", MatchError},
          {"clause", FunctionClauseError},
          {"database", Decree.PersonalFields.Fixtures.UniqueViolation}
        ] do
      test "a raise (#{mode}) carries nothing", %{leak: leak, plaintext: plaintext} do
        leak.(unquote(mode))

        log = failing_run(Leaky)

        assert_nothing_shown(log, plaintext)
        assert log =~ inspect(unquote(type))
        assert log =~ "details redacted"
      end
    end

    test "an `{:error, reason}` return shows reduced", %{leak: leak, plaintext: plaintext} do
      leak.("error")

      log = failing_run(Leaky)

      assert {:bad, :redacted} = assert_nothing_shown(log, plaintext)
    end

    test "an invalid return does not show its value", %{leak: leak, plaintext: plaintext} do
      leak.("invalid")

      log = failing_run(Leaky)

      assert :invalid_return_value = assert_nothing_shown(log, plaintext)
    end

    test "an exit carries nothing", %{leak: leak, plaintext: plaintext} do
      leak.("exit")

      log = failing_run(Leaky)

      assert {:gave_up, :redacted} = assert_nothing_shown(log, plaintext)
    end

    test "a throw carries nothing", %{leak: leak, plaintext: plaintext} do
      leak.("throw")

      log = failing_run(Leaky)

      assert :redacted = assert_nothing_shown(log, plaintext)
    end

    test "`error/3` gets a return as it stands, and ciphertext for the event", context do
      %{leak: leak, plaintext: plaintext, stream: stream} = context
      leak.("error")

      log = failing_run(LeakyReporting)

      assert_received {:error_handed, {:error, {:bad, ^plaintext}}, event, failure_context}
      assert %Leak{email: %{"$pf": 1}} = event
      assert %FailureContext{metadata: %{stream_id: ^stream}} = failure_context
      refute inspect(event, limit: :infinity) =~ "secret.example"

      assert_nothing_shown(log, plaintext)
    end

    test "`error/3` gets a raise redacted, with a scrubbed stacktrace", context do
      %{leak: leak, plaintext: plaintext} = context
      leak.("clause")

      log = failing_run(LeakyReporting)

      assert_received {:error_handed, {:error, %RedactedError{type: type} = error}, _event,
                       %FailureContext{stacktrace: stacktrace}}

      assert type == FunctionClauseError
      assert [{_module, _function, arity, _location} | _frames] = stacktrace
      assert is_integer(arity)
      refute inspect({error, stacktrace}, limit: :infinity) =~ "secret.example"

      assert_nothing_shown(log, plaintext)
    end
  end

  describe "a failure on an event that was not decrypted" do
    defmodule Failing do
      use Decree.Event.Handler,
        application: Decree.PersonalFields.Fixtures.App,
        name: __MODULE__

      alias Decree.PersonalFields.Fixtures.Events.Plain

      def handle(%Plain{note: "raise"}, _metadata), do: raise(ArgumentError, "shown")
      def handle(%Plain{note: note}, _metadata), do: {:error, {:bad, note}}
    end

    setup do
      start_supervised!(App)

      :ok
    end

    test "keeps today's shapes", %{stream: stream} do
      dispatch!(stream, %Plain{note: "visible"})
      Process.flag(:trap_exit, true)

      log =
        capture_log(fn ->
          {:ok, pid} = Failing.start_link(start_from: :origin)

          assert_receive {:EXIT, ^pid, {:bad, "visible"}}
        end)

      assert log =~ ~s({:bad, "visible"})
    end

    test "a raise stays that exception", %{stream: stream} do
      dispatch!(stream, %Plain{note: "raise"})
      Process.flag(:trap_exit, true)

      log =
        capture_log(fn ->
          {:ok, pid} = Failing.start_link(start_from: :origin)

          assert_receive {:EXIT, ^pid, %ArgumentError{message: "shown"}}
        end)

      assert log =~ "shown"
    end
  end
end
