defmodule Decree.Decision.RuleLeaksTest do
  use Decree.RuleCase

  import ExUnit.CaptureLog

  alias Decree.Aggregates.Aggregate
  alias Decree.Aggregates.Supervisor, as: AggregateSupervisor
  alias Decree.DecisionFixtures.Unreadable
  alias Decree.PersonalFields.RedactedError
  alias Decree.Registration
  alias Decree.RuleFixtures.{Adopted, AdoptedAggregate, Picky, PickyAggregate}

  @marker "rule-leak-marker"

  @event_store_spans [
    :ack_event,
    :append_to_stream,
    :append_to_streams,
    :delete_snapshot,
    :delete_subscription,
    :read_query,
    :read_snapshot,
    :read_tags,
    :record_snapshot,
    :stream_forward,
    :subscribe,
    :subscribe_to,
    :unsubscribe
  ]

  @spans [
           [:decree, :aggregate, :execute],
           [:decree, :aggregate, :populate],
           [:decree, :application, :dispatch],
           [:decree, :decision, :execute],
           [:decree, :event, :batch],
           [:decree, :event, :handle]
         ] ++ Enum.map(@event_store_spans, &[:decree, :event_store, &1])

  @every_event [
                 [:decree, :decision, :read],
                 [:decree, :decision, :retry],
                 [:decree, :decision, :refused],
                 [:decree, :personal_field, :key_unknown],
                 [:decree, :personal_field, :plaintext_read]
               ] ++ for(span <- @spans, stage <- [:start, :stop, :exception], do: span ++ [stage])

  setup do
    attach_telemetry(@every_event)

    scope = scope()
    seed(%Read{scope: scope, secret: @marker}, ["scope:" <> scope], App)

    [scope: scope]
  end

  defp secret([%Read{secret: secret} | _events]), do: secret
  defp secret(%{rule_result: %Result{events: events}}), do: secret(events)

  defp holds_marker?(term) do
    inspect(term, limit: :infinity, printable_limit: :infinity, structs: false) =~ @marker
  end

  defp telemetry(acc \\ []) do
    receive do
      {:telemetry, event, measurements, metadata} ->
        telemetry([{event, measurements, metadata} | acc])
    after
      0 -> acc
    end
  end

  # Runs a dispatch that fails with a marker in hand, and looks everywhere it could have gone.
  defp assert_no_leak(command, stream, appended \\ 0) do
    {reply, log} =
      with_log([level: :debug], fn ->
        App.dispatch(command, timeout: 60_000)
      end)

    assert {:error, _reason} = reply

    refute holds_marker?(reply)
    refute log =~ @marker

    events = telemetry()

    assert events != []
    refute holds_marker?(events)

    # An exit or a throw took the aggregate with it, and there is nothing left to ask.
    with pid when is_pid(pid) <- Registration.whereis_name(App, aggregate_name(command, stream)) do
      refute holds_marker?(:sys.get_status(pid))
    end

    assert length(stored(stream, App)) == appended

    reply
  end

  defp aggregate_name(command, stream), do: Aggregate.name(App, aggregate_module(command), stream)

  defp aggregate_module(%Adopted{}), do: AdoptedAggregate
  defp aggregate_module(%Picky{}), do: PickyAggregate
  defp aggregate_module(_command), do: RuledAggregate

  defp rebuilt(module, stream) do
    stream
    |> stored(App)
    |> Enum.reduce(struct(module), fn %{data: event}, state -> module.apply(state, event) end)
  end

  # Reduced, each is still a return `handle_call/3` may give.
  defp returned(:noreply, secret), do: {:noreply, secret}
  defp returned(:reply, secret), do: {:reply, :ok, secret}

  for {module, stream_of} <- [
        {Ruled, "ruled-"},
        {Decree.RuleFixtures.Adopted, "adopted-"}
      ] do
    describe "on #{inspect(module)}, which #{if module == Ruled, do: "does not list", else: "lists"} personal fields" do
      @module module
      @prefix stream_of

      defp unquote(:"command_#{stream_of}")(context, opts) do
        ruled([scope: context.scope] ++ opts, @module)
      end

      test "a raise from `execute/2` is redacted", context do
        execute = fn _state, command -> raise "failed on " <> secret(command) end
        command = unquote(:"command_#{stream_of}")(context, execute: execute)

        assert {:error, %RedactedError{type: RuntimeError}} =
                 assert_no_leak(command, @prefix <> command.id)
      end

      test "a `MatchError` from `execute/2` is redacted", context do
        execute = fn _state, %{rule_result: %Result{events: events}} -> [] = events end
        command = unquote(:"command_#{stream_of}")(context, execute: execute)

        assert {:error, %RedactedError{type: MatchError}} =
                 assert_no_leak(command, @prefix <> command.id)
      end

      test "an exit from `execute/2` is reduced", context do
        execute = fn _state, command -> exit({:gave_up, secret(command)}) end
        command = unquote(:"command_#{stream_of}")(context, execute: execute)

        assert {:error, :aggregate_execution_failed} =
                 assert_no_leak(command, @prefix <> command.id)
      end

      test "a throw from `execute/2` is reduced", context do
        execute = fn _state, command -> throw({:thrown, secret(command)}) end
        command = unquote(:"command_#{stream_of}")(context, execute: execute)

        assert {:error, :aggregate_execution_failed} =
                 assert_no_leak(command, @prefix <> command.id)
      end

      # Read as a return, the throw would leave the aggregate running with its state set to
      # `:redacted`, and the caller waiting for a reply that never comes. The aggregate opens on
      # an event appended outside it, so no delivery of one reaches it afterwards.
      for shape <- [:noreply, :reply] do
        test "a throw shaped as a `:#{shape}` return from `execute/2` stops the aggregate",
             context do
          execute = fn _state, command -> throw(returned(unquote(shape), secret(command))) end
          command = unquote(:"command_#{stream_of}")(context, execute: execute)
          stream = @prefix <> command.id
          module = aggregate_module(command)

          seed(%Noted{id: command.id, note: "noted"}, [], App, stream)
          {:ok, ^stream} = AggregateSupervisor.open_aggregate(App, module, stream)

          pid = Registration.whereis_name(App, aggregate_name(command, stream))
          ref = Process.monitor(pid)

          assert {:error, :aggregate_execution_failed} = assert_no_leak(command, stream, 1)

          assert_receive {:DOWN, ^ref, :process, ^pid, reason}
          assert reason == {:nocatch, {unquote(shape), :redacted}}

          # The next command opens a fresh aggregate, which rebuilds its state from the stream.
          note = fn _state, %{id: id} -> %Noted{id: id, note: "next"} end
          next = unquote(:"command_#{stream_of}")(context, id: command.id, execute: note)

          assert :ok = App.dispatch(next)
          assert Registration.whereis_name(App, aggregate_name(next, stream)) != pid

          assert %^module{} = state = Aggregate.aggregate_state(App, module, stream)
          assert state == rebuilt(module, stream)
        end
      end

      test "a raise from `followup_query/3` is redacted", context do
        followup = fn _command, _tag_context, events -> raise "failed on " <> secret(events) end
        command = unquote(:"command_#{stream_of}")(context, followup: followup)

        assert {:error, %RedactedError{type: RuntimeError}} =
                 assert_no_leak(command, @prefix <> command.id)
      end

      test "an exit from `followup_query/3` is reduced", context do
        followup = fn _command, _tag_context, events -> exit({:gave_up, secret(events)}) end
        command = unquote(:"command_#{stream_of}")(context, followup: followup)

        assert {:error, :aggregate_execution_failed} =
                 assert_no_leak(command, @prefix <> command.id)
      end

      test "a throw from `followup_query/3` is reduced", context do
        followup = fn _command, _tag_context, events -> throw({:thrown, secret(events)}) end
        command = unquote(:"command_#{stream_of}")(context, followup: followup)

        assert {:error, :aggregate_execution_failed} =
                 assert_no_leak(command, @prefix <> command.id)
      end

      test "an error from `followup_query/3` is reduced", context do
        followup = fn _command, _tag_context, events -> {:error, {:bad, secret(events)}} end
        command = unquote(:"command_#{stream_of}")(context, followup: followup)

        assert {:error, {:bad, :redacted}} = assert_no_leak(command, @prefix <> command.id)
      end
    end
  end

  # The read's span covers the store's answer and not the upcast, so an upcaster failing on a
  # stored event leaves through the aggregate, which redacts it, and through no span.
  for mode <- ["raise", "exit", "throw"] do
    test "an upcaster's #{mode} on an event a rule reads shows nothing", %{scope: scope} do
      seed(%Unreadable{secret: @marker, mode: unquote(mode)}, ["scope:" <> scope], App)

      query = fn command, _tag_context ->
        [[types: [Atom.to_string(Unreadable)], tags: ["scope:" <> command.scope]]]
      end

      command = ruled(scope: scope, query: query)

      assert {:error, reason} = assert_no_leak(command, ruled_stream(command))

      if unquote(mode) == "raise" do
        assert %RedactedError{type: RuntimeError} = reason
      else
        assert reason == :aggregate_execution_failed
      end
    end
  end

  test "a `FunctionClauseError` in `execute/2` is redacted, with no argument left", %{
    scope: scope
  } do
    command = ruled([scope: scope], Picky)

    assert {:error, %RedactedError{type: FunctionClauseError}} =
             assert_no_leak(command, "picky-" <> command.id)
  end

  test "a result written into an event raises, and the failure holds nothing", %{scope: scope} do
    execute = fn _state, %{id: id, rule_result: result} -> %Noted{id: id, note: result} end
    command = ruled(scope: scope, execute: execute)

    assert {:error, %RedactedError{type: ArgumentError}} =
             assert_no_leak(command, ruled_stream(command))
  end

  test "a failure's stacktrace reaches telemetry with no arguments", %{scope: scope} do
    execute = fn _state, command -> raise "failed on " <> secret(command) end
    command = ruled(scope: scope, execute: execute)

    assert {:error, %RedactedError{}} = App.dispatch(command)

    assert_received {:telemetry, [:decree, :aggregate, :execute, :exception], _,
                     %{reason: %RedactedError{}, stacktrace: stacktrace}}

    assert Enum.all?(stacktrace, fn frame -> is_integer(elem(frame, tuple_size(frame) - 2)) end)
  end

  describe "a success" do
    test "shows the marker nowhere", %{scope: scope} do
      command = ruled(scope: scope)

      {reply, log} = with_log([level: :debug], fn -> App.dispatch(command) end)

      assert reply == :ok
      refute log =~ @marker
      refute holds_marker?(telemetry())

      pid = Registration.whereis_name(App, aggregate_name(command, ruled_stream(command)))

      refute holds_marker?(:sys.get_status(pid))
    end
  end

  describe "an `{:error, reason}` from `execute/2`" do
    test "is not reduced: that is #7630's, and it is not widened here", %{scope: scope} do
      execute = fn _state, command -> {:error, {:refused, secret(command)}} end
      command = ruled(scope: scope, execute: execute)

      assert App.dispatch(command) == {:error, {:refused, @marker}}
    end
  end
end
