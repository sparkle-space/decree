defmodule Decree.Event.EventTagsTest do
  use Decree.PostgresCase

  alias Decree.Aggregates.Aggregate
  alias Decree.Commands.ExecutionResult

  alias Decree.Event.Tagging.Commands.{
    Emit,
    EmitAsRequester,
    EmitByFunction,
    EmitByFunctionTagged,
    EmitByRef,
    EmitIdentified,
    EmitMulti,
    EmitPrefixed,
    Race
  }

  alias Decree.Event.Tagging.Events.{
    Counted,
    FromContext,
    MemberAddSkipped,
    Raising,
    Refused,
    Tagged,
    Unimplemented
  }

  alias Decree.Event.Tagging.{App, Ledger, Recorder}
  alias Decree.EventStore
  alias Decree.EventStore.EventData
  alias Decree.FrameworkStorage
  alias Decree.Store.Config
  alias Decree.UUID

  @emit_kind "command_kind:Decree.Event.Tagging.Commands.Emit"

  setup do
    conn = start_supervised!({Postgrex, Config.default_postgrex_opts(FrameworkStorage.config())})

    start_supervised!(Recorder)
    start_supervised!(App)

    [conn: conn, user_id: UUID.uuid4(), command_id: UUID.uuid4()]
  end

  describe "a tagged append through a real dispatch" do
    test "an event with an implementation gets its own tags and the framework's", context do
      %{user_id: user_id, command_id: command_id} = context
      correlation_key = UUID.uuid4()

      :ok =
        App.dispatch(%Emit{
          user_id: user_id,
          command_id: command_id,
          correlation_id: correlation_key,
          events: [%Tagged{tags: ["order:1", "tenant:a"]}]
        })

      assert stored(context) == [
               {"Elixir.Decree.Event.Tagging.Events.Tagged",
                Enum.sort([
                  "order:1",
                  "tenant:a",
                  "command:" <> command_id,
                  @emit_kind,
                  "correlation:" <> correlation_key,
                  "user:" <> user_id
                ])}
             ]
    end

    test "an implementation reads the dispatch metadata and the command", context do
      %{user_id: user_id, command_id: command_id} = context

      :ok =
        App.dispatch(
          %Emit{user_id: user_id, command_id: command_id, events: [%FromContext{}]},
          metadata: %{"who" => "alice"}
        )

      assert [{_type, tags}] = stored(context)
      assert "who:alice" in tags
      assert ("field:" <> command_id) in tags
    end

    test "an event with no `command_id` field gets `command:` and keeps its shape", context do
      %{conn: conn, user_id: user_id, command_id: command_id} = context
      tenant_id = UUID.uuid4()
      event = %MemberAddSkipped{tenant_id: tenant_id, user_id: user_id}

      :ok = App.dispatch(%Emit{user_id: user_id, command_id: command_id, events: [event]})

      assert [{_type, tags}] = stored(context)
      assert ("command:" <> command_id) in tags
      assert ("tenant:" <> tenant_id) in tags

      assert [recorded] = App |> EventStore.stream_forward(user_id) |> Enum.to_list()
      assert recorded.data == event

      assert %{rows: [[data]]} = Postgrex.query!(conn, "SELECT data FROM events", [])
      assert Jason.decode!(data) == %{"tenant_id" => tenant_id, "user_id" => user_id}
    end

    test "an event with no implementation gets the framework's tags", context do
      %{user_id: user_id, command_id: command_id} = context

      :ok =
        App.dispatch(%Emit{user_id: user_id, command_id: command_id, events: [%Unimplemented{}]})

      assert stored_tags(context) == [
               Enum.sort(["command:" <> command_id, @emit_kind, "user:" <> user_id])
             ]
    end

    test "a tag an event repeats of the framework's is stored once", context do
      %{user_id: user_id, command_id: command_id} = context

      :ok =
        App.dispatch(%Emit{
          user_id: user_id,
          command_id: command_id,
          events: [%Tagged{tags: ["user:" <> user_id]}]
        })

      assert stored_tags(context) == [
               Enum.sort(["command:" <> command_id, @emit_kind, "user:" <> user_id])
             ]
    end

    test "every event of a multi-event command is tagged", context do
      %{user_id: user_id, command_id: command_id} = context

      :ok =
        App.dispatch(%Emit{
          user_id: user_id,
          command_id: command_id,
          events: [%Tagged{tags: ["n:1"]}, %Unimplemented{}, %Tagged{tags: ["n:3"]}]
        })

      framework = ["command:" <> command_id, @emit_kind, "user:" <> user_id]

      assert stored_tags(context) == [
               Enum.sort(["n:1" | framework]),
               Enum.sort(framework),
               Enum.sort(["n:3" | framework])
             ]
    end

    test "every event of a `Decree.Aggregate.Multi` command is tagged", context do
      %{user_id: user_id, command_id: command_id} = context

      :ok =
        App.dispatch(%EmitMulti{
          user_id: user_id,
          command_id: command_id,
          events: [%Tagged{tags: ["n:1"]}, %Tagged{tags: ["n:2"]}]
        })

      framework = [
        "command:" <> command_id,
        "command_kind:Decree.Event.Tagging.Commands.EmitMulti",
        "user:" <> user_id
      ]

      assert stored_tags(context) == [
               Enum.sort(["n:1" | framework]),
               Enum.sort(["n:2" | framework])
             ]
    end

    test "a no-event command builds no tags", %{user_id: user_id} = context do
      assert :ok = App.dispatch(%Emit{user_id: user_id, events: []})

      assert Recorder.all(:counted) == []
      assert stored(context) == []

      # The same implementation, reached.
      :ok = App.dispatch(%Emit{user_id: user_id, events: [%Counted{note: "once"}]})

      assert Recorder.all(:counted) == ["once"]
    end
  end

  describe "the `command:` tag" do
    test "is absent for a nil `command_id`, with no derived id", context do
      %{user_id: user_id} = context

      :ok =
        App.dispatch(%Emit{user_id: user_id, events: [%Unimplemented{}]},
          command_uuid: UUID.uuid4(),
          causation_id: UUID.uuid4()
        )

      assert stored_tags(context) == [Enum.sort([@emit_kind, "user:" <> user_id])]
    end
  end

  describe "the `correlation:` tag" do
    test "is never the dispatch's own correlation id", context do
      %{user_id: user_id} = context
      router_correlation = UUID.uuid4()

      :ok =
        App.dispatch(%Emit{user_id: user_id, events: [%Unimplemented{}]},
          correlation_id: router_correlation
        )

      assert stored_tags(context) == [Enum.sort([@emit_kind, "user:" <> user_id])]

      assert [recorded] = App |> EventStore.stream_forward(user_id) |> Enum.to_list()
      assert recorded.correlation_id == router_correlation
    end

    test "comes from the command's field when both are given", context do
      %{user_id: user_id} = context
      command_key = UUID.uuid4()

      :ok =
        App.dispatch(
          %Emit{user_id: user_id, correlation_id: command_key, events: [%Unimplemented{}]},
          correlation_id: UUID.uuid4()
        )

      assert stored_tags(context) == [
               Enum.sort([@emit_kind, "correlation:" <> command_key, "user:" <> user_id])
             ]
    end

    test "takes a key that is no UUID", %{user_id: user_id} = context do
      :ok =
        App.dispatch(%Emit{
          user_id: user_id,
          correlation_id: "stripe:evt_123",
          events: [%Unimplemented{}]
        })

      assert [tags] = stored_tags(context)
      assert "correlation:stripe:evt_123" in tags
    end
  end

  describe "the stream tag" do
    test "is the identity field's stem", %{user_id: user_id} = context do
      :ok = App.dispatch(%Emit{user_id: user_id, events: [%Unimplemented{}]})

      assert [tags] = stored_tags(context)
      assert ("user:" <> user_id) in tags
    end

    test "is the kind the route names", %{user_id: user_id} = context do
      :ok =
        App.dispatch(%EmitAsRequester{requester_user_id: user_id, events: [%Unimplemented{}]})

      assert [tags] = stored_tags(context)
      assert ("user:" <> user_id) in tags
      refute Enum.any?(tags, &String.starts_with?(&1, "requester_user:"))
    end

    test "keeps a field name with no `_id` suffix whole", context do
      content_ref = UUID.uuid4()

      :ok = App.dispatch(%EmitByRef{content_ref: content_ref, events: [%Unimplemented{}]})

      assert [tags] = stored_tags(context)
      assert ("content_ref:" <> content_ref) in tags
    end

    test "is absent for an identity that is no UUID", context do
      :ok = App.dispatch(%EmitByRef{content_ref: "cus_123", events: [%Unimplemented{}]})

      assert stored_tags(context) == [["command_kind:#{inspect(EmitByRef)}"]]
    end

    test "is absent for an uppercase UUID", context do
      content_ref = String.upcase(UUID.uuid4())

      :ok = App.dispatch(%EmitByRef{content_ref: content_ref, events: [%Unimplemented{}]})

      assert stored_tags(context) == [["command_kind:#{inspect(EmitByRef)}"]]
    end

    test "is the bare id under a prefixed `identify`", %{user_id: user_id} = context do
      :ok = App.dispatch(%EmitPrefixed{user_id: user_id, events: [%Unimplemented{}]})

      assert [tags] = stored_tags(context)
      assert ("user:" <> user_id) in tags

      assert [_event] = App |> EventStore.stream_forward("ledger-" <> user_id) |> Enum.to_list()
    end

    test "reads an identity `identify` resolves", %{user_id: user_id} = context do
      :ok = App.dispatch(%EmitIdentified{user_id: user_id, events: [%Unimplemented{}]})

      assert [tags] = stored_tags(context)
      assert ("member:" <> user_id) in tags
    end

    test "is absent for a function identity with no `stream_tag:`", context do
      :ok = App.dispatch(%EmitByFunction{key: UUID.uuid4(), events: [%Unimplemented{}]})

      assert stored_tags(context) == [["command_kind:#{inspect(EmitByFunction)}"]]
    end

    test "is the function's answer under a declared kind", context do
      key = UUID.uuid4()

      :ok = App.dispatch(%EmitByFunctionTagged{key: key, events: [%Unimplemented{}]})

      assert stored_tags(context) == [
               Enum.sort(["command_kind:#{inspect(EmitByFunctionTagged)}", "user:" <> key])
             ]
    end

    test "is absent for a function's answer that is no UUID", context do
      :ok = App.dispatch(%EmitByFunctionTagged{key: "cus_123", events: [%Unimplemented{}]})

      assert stored_tags(context) == [["command_kind:#{inspect(EmitByFunctionTagged)}"]]
    end
  end

  describe "a `:wrong_expected_version` retry" do
    test "derives the same tags afresh, and leaves the metadata alone", context do
      %{user_id: user_id, command_id: command_id} = context
      metadata = %{"who" => "alice"}

      command = %Race{
        user_id: user_id,
        command_id: command_id,
        events: [%FromContext{}, %Tagged{tags: ["n:2"]}]
      }

      assert {:ok, %ExecutionResult{metadata: ^metadata, aggregate_version: 3}} =
               App.dispatch(command, metadata: metadata, returning: :execution_result)

      assert [first, second] = attempts()
      assert first.version == 0
      assert second.version == 1
      assert tags_of(first) == tags_of(second)

      assert [first_tags, _second_tags] = tags_of(first)
      assert ("command:" <> command_id) in first_tags
      assert "who:alice" in first_tags

      assert Recorder.all(:attempt_metadata) == [metadata, metadata]

      for %{events: events} <- [first, second], %EventData{metadata: stored} <- events do
        assert stored == metadata
      end

      # The concurrent append carried no tags, so these are all the command's.
      assert stored_tags(context) == Enum.map(tags_of(second), &Enum.sort/1)
    end
  end

  describe "an implementation's `{:error, reason}`" do
    test "is the dispatch's answer, and nothing is appended", context do
      %{user_id: user_id} = context

      :ok = App.dispatch(%Emit{user_id: user_id, events: [%Tagged{note: "kept"}]})
      assert [_tags] = stored(context)

      events = [%Tagged{note: "first of two"}, %Refused{reason: "no"}]

      assert {:error, {:refused, "no"}} =
               App.dispatch(%Emit{user_id: user_id, events: events})

      assert [_tags] = stored(context)
      assert length(attempts()) == 1
      assert_state_holds_only_the_kept_event(user_id)

      # The aggregate is alive and its version is right: the next command appends.
      assert :ok = App.dispatch(%Emit{user_id: user_id, events: [%Tagged{note: "next"}]})
      assert [_kept, _next] = stored(context)
    end
  end

  describe "an implementation's raise" do
    test "is answered as an error, and the state is unchanged", context do
      %{user_id: user_id} = context

      :ok = App.dispatch(%Emit{user_id: user_id, events: [%Tagged{note: "kept"}]})

      assert {:error, %RuntimeError{message: "a raising implementation"}} =
               App.dispatch(%Emit{user_id: user_id, events: [%Raising{}]})

      assert [_tags] = stored(context)

      # `DefaultLifespan` stops an aggregate on an exception, as it does for a raise from
      # `execute/2`, so this is the state the stream rebuilds.
      assert %Ledger{seen: [%Tagged{note: "kept"}]} =
               Aggregate.aggregate_state(App, Ledger, user_id)

      assert :ok = App.dispatch(%Emit{user_id: user_id, events: [%Tagged{note: "next"}]})
      assert [_kept, _next] = stored(context)
    end
  end

  describe "a tag the store refuses" do
    for {name, tag, answer} <- [
          {"an over-long tag", quote(do: String.duplicate("a", 513)), :tag_too_long},
          {"a NUL byte", quote(do: <<"a", 0>>), :invalid_tag},
          {"invalid UTF-8", quote(do: <<0xFF>>), :invalid_tag},
          {"a tag that is no string", :an_atom, :invalid_tag}
        ] do
      test "answers without a retry: #{name}", context do
        %{user_id: user_id} = context

        :ok = App.dispatch(%Emit{user_id: user_id, events: [%Tagged{note: "kept"}]})

        event = %Tagged{tags: [unquote(tag)]}

        assert {:error, unquote(answer)} =
                 App.dispatch(%Emit{user_id: user_id, events: [event]})

        # The kept event's append, and one for this command.
        assert length(attempts()) == 2
        assert [_tags] = stored(context)
        assert_state_holds_only_the_kept_event(user_id)

        assert :ok = App.dispatch(%Emit{user_id: user_id, events: [%Tagged{note: "next"}]})
        assert [_kept, _next] = stored(context)
      end
    end
  end

  defp assert_state_holds_only_the_kept_event(user_id) do
    assert %Ledger{seen: [%Tagged{note: "kept"}]} =
             Aggregate.aggregate_state(App, Ledger, user_id)

    assert Aggregate.aggregate_version(App, Ledger, user_id) == 1
  end

  # The appends the command made: the race's own direct append is `:any_version`.
  defp attempts do
    :append |> Recorder.all() |> Enum.filter(&is_integer(&1.version))
  end

  defp tags_of(%{events: events}), do: Enum.map(events, & &1.tags)

  defp stored_tags(context), do: context |> stored() |> Enum.map(&elem(&1, 1))

  # Each event's type and sorted tags, by position. A test starts from an empty store.
  defp stored(%{conn: conn}) do
    %{rows: rows} =
      Postgrex.query!(
        conn,
        "SELECT position, event_type, tag FROM event_tags ORDER BY position, tag",
        []
      )

    rows
    |> Enum.chunk_by(fn [position, _type, _tag] -> position end)
    |> Enum.map(fn [[_position, type, _tag] | _rest] = chunk ->
      {type, Enum.map(chunk, fn [_position, _type, tag] -> tag end)}
    end)
  end
end
