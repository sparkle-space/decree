defmodule Decree.Decision.RequiredTagsTest do
  use Decree.DecisionStoreCase

  import Decree.PersonalFields.Fixtures, only: [email: 1]
  import Decree.RequiredTagsFixtures, only: [digest: 1]
  import ExUnit.CaptureLog
  import Mox

  alias Decree.Aggregates.ExecutionContext
  alias Decree.Event.Append
  alias Decree.EventStore.Adapters.Mock, as: MockEventStore
  alias Decree.EventStore.{EventData, RecordedEvent}
  alias Decree.PersonalFields.Envelope
  alias Decree.PersonalFields.Fixtures.Keys
  alias Decree.PersonalFields.RedactedError

  alias Decree.RequiredTagsFixtures.{
    Addressed,
    Contact,
    DecisionApp,
    Digested,
    Emit,
    Indexed,
    Injected,
    Malformed,
    MockedDecisionApp,
    PlainRaising,
    Unindexed,
    Unlisted,
    Unopted
  }

  alias Decree.RequiredTagsFixtures.App, as: PlainApp
  alias Decree.Store.{AppendResult, ReadResult}

  @addressed Atom.to_string(Addressed)
  @contact Atom.to_string(Contact)
  @digested Atom.to_string(Digested)
  @unopted Atom.to_string(Unopted)

  @not_lists [nil, :address, %{}]

  setup do
    start_supervised!(Keys)
    Keys.sink()
    start_supervised!(PlainApp)
    start_supervised!(DecisionApp)

    [user: Keys.mint(UUID.uuid4())]
  end

  defp emit(events, opts \\ []) do
    struct!(
      Emit,
      [ledger_id: UUID.uuid4(), command_id: UUID.uuid4(), events: List.wrap(events)] ++ opts
    )
  end

  defp stored_plain(%Emit{ledger_id: ledger_id}), do: stored(ledger_id, PlainApp)

  defp deciding(module, items, decide) when is_function(decide) do
    scripted([query: fn _command, _context -> items end, decide: decide], module)
  end

  defp deciding(module, items, decided) do
    deciding(module, items, fn _state, _command -> decided end)
  end

  defp stream_of(%Indexed{claim_id: claim_id}), do: "indexed-" <> claim_id
  defp stream_of(%Unindexed{claim_id: claim_id}), do: "unindexed-" <> claim_id

  defp stored_decided(command), do: stored(stream_of(command), DecisionApp)

  defp item(type, tags), do: [types: [type], tags: tags]

  defp read_nothing, do: [item(@digested, ["other:never"])]

  describe "a plain aggregate route, with an opted-in event" do
    test "an address with its tag passes" do
      command = emit(%Addressed{address: "here"})

      assert :ok = PlainApp.dispatch(command)
      assert [%RecordedEvent{data: %Addressed{address: "here"}}] = stored_plain(command)
    end

    test "an address without its tag refuses, and stores nothing" do
      command = emit(%Addressed{address: "here", forget_tag: true})

      assert {:error, :missing_required_tag} = PlainApp.dispatch(command)
      assert stored_plain(command) == []
    end

    test "no address and no tag passes" do
      command = emit(%Addressed{})

      assert :ok = PlainApp.dispatch(command)
      assert [%RecordedEvent{data: %Addressed{address: nil}}] = stored_plain(command)
    end

    test "one event of several refuses the command" do
      command = emit([%Addressed{address: "a"}, %Addressed{address: "b", forget_tag: true}])

      assert {:error, :missing_required_tag} = PlainApp.dispatch(command)
      assert stored_plain(command) == []
    end

    test "an answer that is not a list refuses, and stores nothing" do
      for answer <- @not_lists do
        command = emit(%Unlisted{answer: answer})

        assert {:error, :invalid_required_tag_kinds} = PlainApp.dispatch(command), inspect(answer)
        assert stored_plain(command) == []
      end
    end
  end

  describe "a decision route" do
    test "a conditional kind present with its tag passes" do
      command =
        deciding(Unindexed, [item(@addressed, ["address:here"])], %Addressed{address: "here"})

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{}] = stored_decided(command)
    end

    test "a conditional kind absent passes, though an item names it" do
      command = deciding(Unindexed, [item(@addressed, ["address:here"])], %Addressed{})

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{}] = stored_decided(command)
    end

    test "a conditional kind two items name is still conditional" do
      items = [item(@addressed, ["address:a"]), item(@addressed, ["address:b"])]
      command = deciding(Unindexed, items, %Addressed{})

      assert :ok = DecisionApp.dispatch(command)
    end

    test "a conditional kind present without its tag refuses" do
      command =
        deciding(Unindexed, [item(@addressed, ["address:here"])], %Addressed{
          address: "here",
          forget_tag: true
        })

      assert {:error, :missing_required_tag} = DecisionApp.dispatch(command)
      assert stored_decided(command) == []
    end

    test "a kind the event requires stays required with no item naming it" do
      command =
        deciding(Unindexed, read_nothing(), %Addressed{address: "here", forget_tag: true})

      assert {:error, :missing_required_tag} = DecisionApp.dispatch(command)
    end

    test "a non-conditional kind from an item is required of an opted-in event" do
      command = deciding(Unindexed, [item(@addressed, ["scope:s"])], %Addressed{})

      assert {:error, :missing_required_tag} = DecisionApp.dispatch(command)
      assert stored_decided(command) == []

      command = deciding(Unindexed, [item(@addressed, ["scope:s"])], %Addressed{scope: "s"})

      assert :ok = DecisionApp.dispatch(command)
    end

    test "a non-conditional kind from an item is required of an event with no exports" do
      command = deciding(Unindexed, [item(@unopted, ["scope:s"])], %Unopted{})

      assert {:error, :missing_required_tag} = DecisionApp.dispatch(command)
      assert stored_decided(command) == []

      command = deciding(Unindexed, [item(@unopted, ["scope:s"])], %Unopted{tags: ["scope:t"]})

      assert :ok = DecisionApp.dispatch(command)
    end

    test "a kind an item names goes for that item's type alone" do
      items = [item(@addressed, ["scope:s"]), item(@unopted, ["label:l"])]
      command = deciding(Unindexed, items, %Unopted{tags: ["label:l"]})

      assert :ok = DecisionApp.dispatch(command)
    end

    test "a second read's item counts" do
      command =
        scripted(
          [
            query: fn _command, _context -> read_nothing() end,
            followup: fn _command, _context, [] -> [item(@unopted, ["scope:s"])] end,
            decide: fn _state, _command -> %Unopted{} end
          ],
          Unindexed
        )

      assert {:error, :missing_required_tag} = DecisionApp.dispatch(command)
    end

    test "a member of several refuses the batch" do
      first = UUID.uuid4()
      second = UUID.uuid4()

      decided =
        {:streams,
         [
           {:claim, first, %Unopted{tags: ["scope:s"]}},
           {:claim, second, %Unopted{}}
         ]}

      command = deciding(Unindexed, [item(@unopted, ["scope:s"])], decided)

      assert {:error, :missing_required_tag} = DecisionApp.dispatch(command)
      assert stored("unindexed-" <> first, DecisionApp) == []
      assert stored("unindexed-" <> second, DecisionApp) == []
    end

    test "a malformed answer from an event reaches the store, and is refused" do
      command = deciding(Unindexed, read_nothing(), %Malformed{})

      assert {:error, :invalid_required_tag_kinds} = DecisionApp.dispatch(command)
    end

    test "an answer that is not a list refuses, and stores nothing" do
      for answer <- @not_lists do
        command = deciding(Unindexed, read_nothing(), %Unlisted{answer: answer})

        assert {:error, :invalid_required_tag_kinds} = DecisionApp.dispatch(command),
               inspect(answer)

        assert stored_decided(command) == []
      end
    end
  end

  describe "a decision route, as the store is asked" do
    setup [:set_mox_global, :stub_event_store]

    defp stub_event_store(context) do
      Decree.MockEventStoreCase.stub_event_store(context)

      test = self()

      stub(MockEventStore, :read_query, fn _meta, _items, _opts ->
        {:ok,
         %ReadResult{
           events: [],
           after: 0,
           read_at: DateTime.utc_now(),
           command_stamps: %{},
           matched_items: %{}
         }}
      end)

      stub(MockEventStore, :read_tags, fn _meta, _positions, _opts -> {:ok, %{}} end)

      stub(MockEventStore, :append_to_streams, fn _meta, appends, _opts ->
        send(test, {:appended, appends})

        versions = Map.new(appends, fn {stream, _version, events} -> {stream, length(events)} end)

        {:ok, %AppendResult{position: 1, stream_versions: versions}}
      end)

      start_supervised!(MockedDecisionApp)

      :ok
    end

    test "an event with no exports has `always/0` required of it" do
      command = deciding(Unindexed, read_nothing(), %Unopted{})

      assert :ok = MockedDecisionApp.dispatch(command)
      assert_received {:appended, [{_stream, :any_version, [%EventData{} = data]}]}
      assert data.required_tag_kinds == Decree.Event.RequiredTags.always()
      assert data.required_tag_kinds == ["command", "command_kind"]
    end
  end

  describe "an erased subject" do
    setup %{user: user} do
      Keys.set(user, :erased)

      :ok
    end

    test "on a plain aggregate reads as absent, not missing", %{user: user} do
      command = emit(%Contact{user_id: user, email: email("gone")})

      assert :ok = PlainApp.dispatch(command)
      assert [%RecordedEvent{data: %Contact{email: nil}}] = stored_plain(command)
    end

    test "on a decision reads as absent, and F7 passes on its empty side", %{user: user} do
      items = [item(@contact, ["email:" <> digest(email("gone"))])]
      command = deciding(Indexed, items, %Contact{user_id: user, email: email("gone")})

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{data: %Contact{email: nil}}] = stored_decided(command)
    end

    test "a subject with a key still has its tag required and compared", %{user: user} do
      Keys.set(user, :key)

      secret = email("kept")
      items = [item(@contact, ["email:" <> digest(secret)])]
      command = deciding(Indexed, items, %Contact{user_id: user, email: secret})

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{data: %Contact{email: envelope}}] = stored_decided(command)
      assert Envelope.envelope?(envelope)

      other = deciding(Indexed, items, %Contact{user_id: user, email: email("another")})

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(other)
      assert stored_decided(other) == []
    end
  end

  describe "the command's identity" do
    test "a nil `command_id` on an opted-in aggregate route refuses" do
      command = emit(%Addressed{}, command_id: nil)

      assert {:error, :missing_required_tag} = PlainApp.dispatch(command)
      assert stored_plain(command) == []
    end

    test "an empty `command_id` on an opted-in aggregate route refuses" do
      command = emit(%Addressed{}, command_id: "")

      assert {:error, :missing_required_tag} = PlainApp.dispatch(command)
      assert stored_plain(command) == []
    end

    test "a decision's step 0 still answers first" do
      for command_id <- [nil, ""] do
        command = %{deciding(Unindexed, read_nothing(), %Addressed{}) | command_id: command_id}

        assert {:error, :missing_command_id} = DecisionApp.dispatch(command)
      end
    end
  end

  describe "a plain route with an event that exports nothing" do
    test "appends as it did, with a nil `command_id`" do
      command = emit(%Unopted{}, command_id: nil)

      assert :ok = PlainApp.dispatch(command)
      assert [%RecordedEvent{data: %Unopted{}}] = stored_plain(command)
    end

    test "carries an empty list" do
      context = %ExecutionContext{command: emit(%Unopted{}, command_id: nil), metadata: %{}}

      assert {:ok, [%EventData{required_tag_kinds: []}]} =
               Append.prepare_events(PlainApp, UUID.uuid4(), [%Unopted{}], context)
    end
  end

  describe "F7" do
    defp digested(tags), do: %Digested{tags: tags}

    test "an equal digest passes" do
      command = deciding(Indexed, [item(@digested, ["email:a"])], digested(["email:a"]))

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{}] = stored_decided(command)
    end

    test "an unequal digest refuses with no retry, and stores nothing" do
      attach_telemetry([[:decree, :decision, :retry]])

      command = deciding(Indexed, [item(@digested, ["email:a"])], digested(["email:b"]))

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
      assert stored_decided(command) == []
      refute_received {:telemetry, [:decree, :decision, :retry], _measurements, _metadata}
    end

    test "a digest on the batch side alone refuses" do
      command = deciding(Indexed, read_nothing(), digested(["email:a"]))

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
    end

    test "a digest on the batch side passes only for all of the batch" do
      decided = [digested(["email:a"]), digested(["email:b"])]
      command = deciding(Indexed, [item(@digested, ["email:a"])], decided)

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
    end

    test "an empty batch side passes" do
      command = deciding(Indexed, [item(@digested, ["email:a"])], digested([]))

      assert :ok = DecisionApp.dispatch(command)
    end

    test "two digests of a kind, on both sides, pass" do
      items = [item(@digested, ["email:old"]), item(@digested, ["email:new"])]
      command = deciding(Indexed, items, digested(["email:new", "email:old"]))

      assert :ok = DecisionApp.dispatch(command)
    end

    test "two digests on the items and one on the batch refuse" do
      items = [item(@digested, ["email:old"]), item(@digested, ["email:new"])]
      command = deciding(Indexed, items, digested(["email:new"]))

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
    end

    test "a second read's digest item counts" do
      followup = fn _command, _context, _events -> [item(@digested, ["email:b"])] end

      for {batch, answer} <- [
            {["email:a", "email:b"], :ok},
            {["email:a"], {:error, :digest_tag_mismatch}}
          ] do
        command =
          scripted(
            [
              query: fn _command, _context -> [item(@digested, ["email:a"])] end,
              followup: followup,
              decide: fn _state, _command -> digested(batch) end
            ],
            Indexed
          )

        assert DecisionApp.dispatch(command) == answer
      end
    end

    test "the items count whatever type they name" do
      items = [item(@addressed, ["address:never"]), item(@unopted, ["email:a"])]
      command = deciding(Indexed, items, digested(["email:a"]))

      assert :ok = DecisionApp.dispatch(command)
    end

    test "a batch digest across two members is one set" do
      first = UUID.uuid4()
      second = UUID.uuid4()
      items = [item(@digested, ["email:a"]), item(@digested, ["email:b"])]

      decided =
        {:streams,
         [
           {:claim, first, digested(["email:a"])},
           {:claim, second, digested(["email:b"])}
         ]}

      assert :ok = DecisionApp.dispatch(deciding(Indexed, items, decided))
    end

    test "a kind marked `:plain` is not compared" do
      command = deciding(Indexed, [item(@digested, ["label:a"])], digested(["label:b"]))

      assert :ok = DecisionApp.dispatch(command)
    end

    test "a decision with no `index_families/0` compares nothing" do
      command = deciding(Unindexed, [item(@digested, ["email:a"])], digested(["email:b"]))

      assert :ok = DecisionApp.dispatch(command)
    end

    test "a decision whose `index_families/0` a later `use` injects compares it" do
      items = [item(@digested, ["email:a"])]

      command = deciding(Injected, items, digested(["email:b"]))

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
      assert stored("injected-" <> command.claim_id, DecisionApp) == []

      command = deciding(Injected, items, digested(["email:a"]))

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{}] = stored("injected-" <> command.claim_id, DecisionApp)
    end

    test "a tag that is no string is the store's to refuse" do
      command = deciding(Indexed, [item(@digested, ["email:a"])], digested([1]))

      assert {:error, :invalid_tag} = DecisionApp.dispatch(command)
    end

    test "comes before the required kinds" do
      items = [item(@unopted, ["scope:s", "email:a"])]
      command = deciding(Indexed, items, %Unopted{tags: ["email:b"]})

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
    end
  end

  describe "a failure of `__required_tag_kinds__/2`" do
    defp prepare(event) do
      context = %ExecutionContext{command: emit(event), metadata: %{}}

      Append.prepare_events(PlainApp, UUID.uuid4(), [event], context)
    end

    test "on a declared event, a raise is redacted as one from `tags/2` is", %{user: user} do
      secret = email("raised")

      error =
        assert_raise RedactedError, fn ->
          prepare(%Contact{user_id: user, email: secret, mode: "raise"})
        end

      assert error.type == MatchError
      refute inspect(error, limit: :infinity) =~ secret
      refute Exception.message(error) =~ secret
    end

    test "on a declared event, it reaches a dispatch redacted", %{user: user} do
      secret = email("dispatched")
      command = emit(%Contact{user_id: user, email: secret, mode: "raise"})

      # The debug line printing a command is not this path: commands are not redacted.
      log =
        capture_log([level: :info], fn ->
          assert {:error, %RedactedError{} = error} = PlainApp.dispatch(command)

          refute inspect(error, limit: :infinity) =~ secret
        end)

      refute log =~ secret
      assert stored_plain(command) == []
    end

    test "on a declared event, an error, an exit and a throw are reduced", %{user: user} do
      secret = email("reduced")
      event = &%Contact{user_id: user, email: secret, mode: &1}

      assert prepare(event.("error")) == {:error, {:refused, :redacted}}
      assert catch_exit(prepare(event.("exit"))) == {:gave_up, :redacted}
      assert catch_throw(prepare(event.("throw"))) == {:thrown, :redacted}
    end

    test "on an event declaring nothing, stays as it is" do
      assert_raise ArgumentError, "plain raise: shown", fn ->
        prepare(%PlainRaising{note: "shown"})
      end
    end
  end
end
