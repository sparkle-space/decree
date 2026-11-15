defmodule Decree.Decision.TelemetryTest do
  use Decree.DecisionStoreCase

  import ExUnit.CaptureLog

  alias Decree.Aggregates.ExecutionContext
  alias Decree.DecisionFixtures.{ScriptedDecision, TightDecision}

  @start [:decree, :decision, :execute, :start]
  @stop [:decree, :decision, :execute, :stop]
  @exception [:decree, :decision, :execute, :exception]
  @read [:decree, :decision, :read]
  @retry [:decree, :decision, :retry]
  @refused [:decree, :decision, :refused]

  setup do
    attach_telemetry([@start, @stop, @exception, @read, @retry, @refused])
    :ok
  end

  describe "the span" do
    test "starts and stops with what it was given" do
      command = scripted()
      stream_uuid = "scripted-" <> command.claim_id

      assert :ok = App.dispatch(command)

      assert_received {:telemetry, @start, %{system_time: _},
                       %{
                         application: App,
                         decision: ScriptedDecision,
                         stream_uuid: ^stream_uuid,
                         execution_context: %ExecutionContext{command: ^command}
                       }}

      assert_received {:telemetry, @stop, %{duration: duration},
                       %{
                         application: App,
                         decision: ScriptedDecision,
                         stream_uuid: ^stream_uuid,
                         execution_context: %ExecutionContext{command: ^command},
                         attempts: 1,
                         events: [%Claimed{}]
                       } = metadata}

      assert is_integer(duration)
      refute Map.has_key?(metadata, :error)
    end

    test "stops with the error of a refusal" do
      command = scripted(decide: fn _state, _command -> {:error, :nope} end)

      assert {:error, :nope} = App.dispatch(command)

      assert_received {:telemetry, @stop, _, %{attempts: 1, error: :nope} = metadata}
      refute Map.has_key?(metadata, :events)
    end

    # A refusal in the middleware runs no decision, so it has no span; step 0's own refusals
    # are a backstop behind it on a routed dispatch.
    test "starts no span for a `command_id` the middleware refuses" do
      assert {:error, :command_id_required} = App.dispatch(scripted(command_id: nil))

      refute_received {:telemetry, @start, _, _}
      refute_received {:telemetry, @stop, _, _}
      refute_received {:telemetry, @read, _, _}
    end

    test "counts no attempt for a refusal before the first" do
      assert {:error, :invalid_scope_tags} = App.dispatch(scripted(scope_tags: fn _ -> [""] end))

      assert_received {:telemetry, @stop, _, %{attempts: 0, error: :invalid_scope_tags}}
      refute_received {:telemetry, @read, _, _}
    end

    test "counts every attempt" do
      test = self()

      decide = fn state, command ->
        if state == [] do
          seed(%Claimed{scope: command.scope}, ["scope:" <> command.scope])
        end

        send(test, :decided)

        %Claimed{scope: command.scope}
      end

      assert :ok = App.dispatch(scripted(decide: decide))

      assert_received {:telemetry, @stop, _, %{attempts: 2, events: [%Claimed{}]}}
    end

    test "ends in an exception for a raise" do
      command = scripted(decide: fn _state, _command -> raise "raised" end)

      assert {:error, %RuntimeError{}} = App.dispatch(command)

      assert_received {:telemetry, @exception, %{duration: _},
                       %{
                         application: App,
                         decision: ScriptedDecision,
                         kind: :error,
                         reason: %RuntimeError{message: "raised"},
                         stacktrace: [_ | _]
                       }}

      refute_received {:telemetry, @stop, _, _}
    end
  end

  describe "the event store's `append_to_streams` span" do
    @append_start [:decree, :event_store, :append_to_streams, :start]
    @append_stop [:decree, :event_store, :append_to_streams, :stop]

    test "names the streams and holds no event data" do
      attach_telemetry([@append_start, @append_stop])

      secret = "plaintext-" <> UUID.uuid4()
      user = UUID.uuid4()
      order = UUID.uuid4()

      decide = fn _state, _command ->
        {:streams,
         [
           {:claim, user, %Claimed{note: secret}},
           {:order, order, %Claimed{note: secret}}
         ]}
      end

      assert :ok =
               App.dispatch(scripted(claim_id: user, decide: decide),
                 metadata: %{"actor" => secret}
               )

      for event <- [@append_start, @append_stop] do
        assert_received {:telemetry, ^event, _measurements, metadata}

        assert Map.delete(metadata, :telemetry_span_context) == %{
                 application: App,
                 stream_uuids: ["scripted-" <> user, "scripted-" <> order]
               }

        refute inspect(metadata, limit: :infinity, printable_limit: :infinity) =~ secret
      end
    end
  end

  describe "the read event" do
    test "is one per completed cycle, with its measurements" do
      scope = scope()

      seed(%Claimed{scope: scope}, ["scope:" <> scope])
      seed(%Claimed{scope: scope}, ["scope:" <> scope])

      assert :ok = App.dispatch(scripted(scope: scope))

      # The index item is one item and one probe. Its facts are read by position, so they are
      # not matched events.
      matched = 2

      assert_received {:telemetry, @read,
                       %{
                         items: 3,
                         probes: 3,
                         matched_events: ^matched,
                         read_time: read_time,
                         fold_time: fold_time
                       }, %{application: App, decision: ScriptedDecision, attempt: 1}}

      assert is_integer(read_time) and read_time > 0
      assert is_integer(fold_time) and fold_time >= 0
      refute_received {:telemetry, @read, _, _}
    end

    test "counts both reads" do
      scope = scope()

      seed(%Released{scope: scope}, ["scope:" <> scope])

      released = [types: [Atom.to_string(Released)], tags: ["scope:" <> scope]]

      assert :ok = App.dispatch(scripted(scope: scope, followup: fn _c, _t, _e -> [released] end))

      matched = 1

      assert_received {:telemetry, @read, %{items: 4, probes: 4, matched_events: ^matched},
                       _metadata}
    end

    test "counts an item's types as probes, the framework's included" do
      scope = scope()
      types = [Atom.to_string(Claimed), Atom.to_string(Released), "Elixir.Other"]

      first = fn _c, _t -> [[types: types, tags: ["scope:" <> scope]]] end
      released = [types: tl(types), tags: ["scope:" <> scope, "scope:further"]]

      assert :ok =
               App.dispatch(
                 scripted(scope: scope, query: first, followup: fn _, _, _ -> [released] end)
               )

      assert_received {:telemetry, @read, %{items: 4, probes: 7}, _metadata}
    end

    test "numbers the attempt" do
      decide = fn state, command ->
        if state == [] do
          seed(%Claimed{scope: command.scope}, ["scope:" <> command.scope])
        end

        %Claimed{scope: command.scope}
      end

      assert :ok = App.dispatch(scripted(decide: decide))

      assert_received {:telemetry, @read, _, %{attempt: 1}}
      matched = 1

      assert_received {:telemetry, @read, %{matched_events: ^matched}, %{attempt: 2}}
    end
  end

  describe "the refused event and its log" do
    test "names the bound, as an alarm" do
      command = scripted([query: fn _c, _t -> Enum.map(1..3, &claimed_item("#{&1}")) end], Tight)

      log =
        capture_log(fn ->
          assert {:error, :decision_query_too_large} = App.dispatch(command)
        end)

      assert log =~ "refused on the items bound"
      assert log =~ "5 over 4"

      assert_received {:telemetry, @refused, %{value: 5, limit: 4},
                       %{application: App, decision: TightDecision, bound: :items}}
    end

    test "names the probes bound, as an alarm" do
      types = Enum.map(1..47, &"Elixir.Type#{&1}")
      command = scripted(query: fn _c, _t -> [[types: types]] end)

      log =
        capture_log(fn ->
          assert {:error, :decision_query_too_large} = App.dispatch(command)
        end)

      assert log =~ "refused on the probes bound"
      assert log =~ "49 over 48"

      assert_received {:telemetry, @refused, %{value: 49, limit: 48},
                       %{application: App, decision: ScriptedDecision, bound: :probes}}

      refute_received {:telemetry, @read, _, _}
    end
  end
end
