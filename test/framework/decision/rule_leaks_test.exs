defmodule Decree.Decision.RuleLeaksTest do
  use Decree.RuleCase

  import ExUnit.CaptureLog

  alias Decree.Aggregates.Aggregate
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
  defp assert_no_leak(command, stream) do
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

    assert stored(stream, App) == []

    reply
  end

  defp aggregate_name(%Adopted{}, stream), do: Aggregate.name(App, AdoptedAggregate, stream)
  defp aggregate_name(%Picky{}, stream), do: Aggregate.name(App, PickyAggregate, stream)
  defp aggregate_name(_command, stream), do: Aggregate.name(App, RuledAggregate, stream)

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
