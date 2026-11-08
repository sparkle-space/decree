defmodule Decree.Decision.RequiredTagsTest do
  use Decree.DecisionStoreCase

  import Decree.PersonalFields.Fixtures, only: [email: 1]
  import Decree.RequiredTagsFixtures, only: [digest: 1]
  import ExUnit.CaptureLog
  import Mox

  alias Decree.Aggregates.ExecutionContext
  alias Decree.Event.Append
  alias Decree.EventStore
  alias Decree.EventStore.Adapters.Mock, as: MockEventStore
  alias Decree.EventStore.{EventData, RecordedEvent}
  alias Decree.PersonalFields.Envelope
  alias Decree.PersonalFields.Fixtures.Keys
  alias Decree.PersonalFields.RedactedError

  alias Decree.RequiredTagsFixtures.{
    Addressed,
    Contact,
    Creating,
    DecisionApp,
    Digested,
    Emit,
    Forged,
    Indexed,
    Injected,
    KeyedCreate,
    Malformed,
    MockedDecisionApp,
    PlainRaising,
    TwoDigested,
    TwoFamilies,
    Unindexed,
    Unlisted,
    Unopted
  }

  alias Decree.RequiredTagsFixtures.App, as: PlainApp
  alias Decree.Store.{AppendResult, ReadResult}

  @addressed Atom.to_string(Addressed)
  @contact Atom.to_string(Contact)
  @digested Atom.to_string(Digested)
  @two_digested Atom.to_string(TwoDigested)
  @unopted Atom.to_string(Unopted)

  @not_lists [nil, :address, %{}]

  # The fixture key's fingerprint, which a digest tag carries ahead of its digest.
  @fp Decree.TagIndexFixtures.fingerprint()

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

  # What preparing the events answers for `command`, before any store, as every append does.
  defp prepared(command, events) do
    context = %ExecutionContext{command: command, metadata: %{}}

    Append.prepare_events(PlainApp, command.ledger_id, List.wrap(events), context)
  end

  # An aggregate route answers no position, so the stream's entity tag finds the event.
  defp stored_tags(%Emit{ledger_id: ledger_id}) do
    assert {:ok, %ReadResult{events: [%{event_number: position}]}} =
             EventStore.read_query(PlainApp, [[tags: ["ledger:" <> ledger_id]]])

    assert {:ok, %{^position => tags}} = EventStore.read_tags(PlainApp, [position])

    tags
  end

  defp deciding(module, items, decide) when is_function(decide) do
    scripted([query: fn _command, _context -> items end, decide: decide], module)
  end

  defp deciding(module, items, decided) do
    deciding(module, items, fn _state, _command -> decided end)
  end

  defp stream_of(%Creating{claim_id: claim_id}), do: "creating-" <> claim_id
  defp stream_of(%Indexed{claim_id: claim_id}), do: "indexed-" <> claim_id
  defp stream_of(%KeyedCreate{claim_id: claim_id}), do: "keyed-" <> claim_id
  defp stream_of(%TwoFamilies{claim_id: claim_id}), do: "two-" <> claim_id
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
      second = UUID.uuid4()

      decide = fn _state, %{claim_id: route} ->
        {:streams,
         [
           {:claim, route, %Unopted{tags: ["scope:s"]}},
           {:claim, second, %Unopted{}}
         ]}
      end

      command = deciding(Unindexed, [item(@unopted, ["scope:s"])], decide)

      assert {:error, :missing_required_tag} = DecisionApp.dispatch(command)
      assert stored_decided(command) == []
      assert stored("unindexed-" <> second, DecisionApp) == []
    end

    test "the command item asks nothing of a type no item names" do
      command = deciding(Unindexed, read_nothing(), %Unopted{})

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{data: %Unopted{}}] = stored_decided(command)
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

      # Nothing but the facts that enable every family, their tags in the read. No `read_tags`
      # is stubbed, so a dispatch sending one fails.
      stub(MockEventStore, :read_query, fn _meta, items, opts ->
        {:ok, Decree.TagIndexFixtures.mocked_read(items, opts)}
      end)

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
    # A dispatch refuses a missing or blank `command_id` first, in the default middleware
    # (`Decree.Middleware.CommandId`), so the required `command` kind is a backstop there: it
    # still holds where events are prepared, which every append path goes through.
    for command_id <- [nil, ""] do
      test "a #{inspect(command_id)} `command_id` on an opted-in aggregate route refuses" do
        command = emit(%Addressed{}, command_id: unquote(command_id))

        assert {:error, :command_id_required} = PlainApp.dispatch(command)
        assert stored_plain(command) == []

        assert {:error, :missing_required_tag} = prepared(command, %Addressed{})
      end
    end

    test "a domain tag of the `command` kind does not stand in for a missing `command_id`" do
      for forged <- ["command:forged", "command"], command_id <- [nil, ""] do
        command = emit(%Forged{tags: [forged]}, command_id: command_id)

        assert {:error, :command_id_required} = PlainApp.dispatch(command),
               inspect({forged, command_id})

        assert stored_plain(command) == []

        assert {:error, :missing_required_tag} = prepared(command, %Forged{tags: [forged]}),
               inspect({forged, command_id})
      end
    end

    test "a domain tag equal to the framework's appends, and is stored once" do
      command_id = UUID.uuid4()
      tag = "command:" <> command_id
      command = emit(%Forged{tags: [tag]}, command_id: command_id)

      assert :ok = PlainApp.dispatch(command)
      assert Enum.count(stored_tags(command), &(&1 == tag)) == 1
    end

    test "a decision's `command_id` refusal still answers first" do
      for command_id <- [nil, ""] do
        command = %{deciding(Unindexed, read_nothing(), %Addressed{}) | command_id: command_id}

        assert {:error, :command_id_required} = DecisionApp.dispatch(command)
      end
    end
  end

  describe "a plain route with an event that exports nothing" do
    test "is refused with a nil `command_id`, by the default middleware" do
      command = emit(%Unopted{}, command_id: nil)

      assert {:error, :command_id_required} = PlainApp.dispatch(command)
      assert stored_plain(command) == []
    end

    test "keeps a `command` kind tag, with no usable `command_id`" do
      for forged <- ["command:forged", "command"], command_id <- [nil, ""] do
        command = emit(%Unopted{tags: [forged]}, command_id: command_id)
        context = %ExecutionContext{command: command, metadata: %{}}

        assert {:ok, [%EventData{tags: tags}]} =
                 Append.prepare_events(
                   PlainApp,
                   command.ledger_id,
                   [%Unopted{tags: [forged]}],
                   context
                 )

        assert forged in tags
      end
    end

    test "carries an empty list" do
      context = %ExecutionContext{command: emit(%Unopted{}, command_id: nil), metadata: %{}}

      assert {:ok, [%EventData{required_tag_kinds: []}]} =
               Append.prepare_events(PlainApp, UUID.uuid4(), [%Unopted{}], context)
    end
  end

  describe "F7" do
    # `label` is `Indexed`'s own family.
    setup do
      enable_index!("label")
      :ok
    end

    defp digested(tags), do: %Digested{tags: tags}

    test "an equal digest passes" do
      command =
        deciding(Indexed, [item(@digested, ["email:#{@fp}:a"])], digested(["email:#{@fp}:a"]))

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{}] = stored_decided(command)
    end

    test "an unequal digest refuses with no retry, and stores nothing" do
      attach_telemetry([[:decree, :decision, :retry]])

      command =
        deciding(Indexed, [item(@digested, ["email:#{@fp}:a"])], digested(["email:#{@fp}:b"]))

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
      assert stored_decided(command) == []
      refute_received {:telemetry, [:decree, :decision, :retry], _measurements, _metadata}
    end

    test "a digest on the batch side alone refuses" do
      command = deciding(Indexed, read_nothing(), digested(["email:#{@fp}:a"]))

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
    end

    test "a digest on the batch side passes only for all of the batch" do
      decided = [digested(["email:#{@fp}:a"]), digested(["email:#{@fp}:b"])]
      command = deciding(Indexed, [item(@digested, ["email:#{@fp}:a"])], decided)

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
    end

    test "an empty batch side passes" do
      command = deciding(Indexed, [item(@digested, ["email:#{@fp}:a"])], digested([]))

      assert :ok = DecisionApp.dispatch(command)
    end

    test "two digests of a kind, on both sides, pass" do
      items = [item(@digested, ["email:#{@fp}:old"]), item(@digested, ["email:#{@fp}:new"])]
      command = deciding(Indexed, items, digested(["email:#{@fp}:new", "email:#{@fp}:old"]))

      assert :ok = DecisionApp.dispatch(command)
    end

    test "two digests on the items and one on the batch refuse" do
      items = [item(@digested, ["email:#{@fp}:old"]), item(@digested, ["email:#{@fp}:new"])]
      command = deciding(Indexed, items, digested(["email:#{@fp}:new"]))

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
    end

    test "a second read's digest item counts" do
      followup = fn _command, _context, _events -> [item(@digested, ["email:#{@fp}:b"])] end

      for {batch, answer} <- [
            {["email:#{@fp}:a", "email:#{@fp}:b"], :ok},
            {["email:#{@fp}:a"], {:error, :digest_tag_mismatch}}
          ] do
        command =
          scripted(
            [
              query: fn _command, _context -> [item(@digested, ["email:#{@fp}:a"])] end,
              followup: followup,
              decide: fn _state, _command -> digested(batch) end
            ],
            Indexed
          )

        assert DecisionApp.dispatch(command) == answer
      end
    end

    test "the items count whatever type they name" do
      items = [item(@addressed, ["address:never"]), item(@unopted, ["email:#{@fp}:a"])]
      command = deciding(Indexed, items, digested(["email:#{@fp}:a"]))

      assert :ok = DecisionApp.dispatch(command)
    end

    test "a batch digest across two members is one set" do
      second = UUID.uuid4()
      items = [item(@digested, ["email:#{@fp}:a"]), item(@digested, ["email:#{@fp}:b"])]

      decide = fn _state, %{claim_id: route} ->
        {:streams,
         [
           {:claim, route, digested(["email:#{@fp}:a"])},
           {:claim, second, digested(["email:#{@fp}:b"])}
         ]}
      end

      assert :ok = DecisionApp.dispatch(deciding(Indexed, items, decide))
    end

    test "a kind marked `:plain` is not compared" do
      command = deciding(Indexed, [item(@digested, ["label:a"])], digested(["label:b"]))

      assert :ok = DecisionApp.dispatch(command)
    end

    test "a decision with no `index_families/0` compares nothing" do
      command =
        deciding(Unindexed, [item(@digested, ["email:#{@fp}:a"])], digested(["email:#{@fp}:b"]))

      assert :ok = DecisionApp.dispatch(command)
    end

    test "a decision whose `index_families/0` a later `use` injects compares it" do
      items = [item(@digested, ["email:#{@fp}:a"])]

      command = deciding(Injected, items, digested(["email:#{@fp}:b"]))

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
      assert stored("injected-" <> command.claim_id, DecisionApp) == []

      command = deciding(Injected, items, digested(["email:#{@fp}:a"]))

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{}] = stored("injected-" <> command.claim_id, DecisionApp)
    end

    test "a tag that is no string is the store's to refuse" do
      command = deciding(Indexed, [item(@digested, ["email:#{@fp}:a"])], digested([1]))

      assert {:error, :invalid_tag} = DecisionApp.dispatch(command)
    end

    test "comes before the required kinds" do
      items = [item(@unopted, ["scope:s", "email:#{@fp}:a"])]
      command = deciding(Indexed, items, %Unopted{tags: ["email:#{@fp}:b"]})

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
    end

    test "a digest kind in `scope_tags/1` off the batch answers ahead of the empty side" do
      command =
        scripted(
          [
            query: fn _command, _context -> [item(@digested, ["email:#{@fp}:a"])] end,
            scope_tags: fn _command -> ["email:#{@fp}:a"] end,
            decide: fn _state, _command -> digested([]) end
          ],
          Indexed
        )

      assert {:error, :batch_lacks_scope_tag} = DecisionApp.dispatch(command)
      assert stored_decided(command) == []
    end

    defp two_digested(tags), do: %TwoDigested{tags: tags}

    defp items_of(type, tags), do: Enum.map(tags, &item(type, [&1]))

    test "two families, each matching its items, pass" do
      items = items_of(@two_digested, ["email:#{@fp}:a", "oauth_uid:#{@fp}:u"])

      command =
        deciding(TwoFamilies, items, two_digested(["email:#{@fp}:a", "oauth_uid:#{@fp}:u"]))

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{}] = stored_decided(command)
    end

    test "two families on the one item are each compared" do
      items = [item(@two_digested, ["email:#{@fp}:a", "oauth_uid:#{@fp}:u"])]

      command =
        deciding(TwoFamilies, items, two_digested(["email:#{@fp}:a", "oauth_uid:#{@fp}:u"]))

      assert :ok = DecisionApp.dispatch(command)

      command =
        deciding(TwoFamilies, items, two_digested(["email:#{@fp}:a", "oauth_uid:#{@fp}:v"]))

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
      assert stored_decided(command) == []
    end

    test "a digest answering for the other family's refuses" do
      items = items_of(@two_digested, ["email:#{@fp}:a", "oauth_uid:#{@fp}:b"])

      command =
        deciding(TwoFamilies, items, two_digested(["email:#{@fp}:b", "oauth_uid:#{@fp}:a"]))

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
      assert stored_decided(command) == []
    end

    for {kind, other} <- [{"email", "oauth_uid"}, {"oauth_uid", "email"}] do
      test "#{kind} differing with #{other} matching refuses, and stores nothing" do
        kind = unquote(kind) <> ":" <> @fp <> ":"
        other = unquote(other) <> ":" <> @fp <> ":"
        items = items_of(@two_digested, [kind <> "a", other <> "o"])
        command = deciding(TwoFamilies, items, two_digested([kind <> "b", other <> "o"]))

        assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
        assert stored_decided(command) == []
      end

      test "#{kind} off the batch with #{other} matching is F7's empty side, and passes" do
        kind = unquote(kind) <> ":" <> @fp <> ":"
        other = unquote(other) <> ":" <> @fp <> ":"
        items = items_of(@two_digested, [kind <> "a", other <> "o"])
        command = deciding(TwoFamilies, items, two_digested([other <> "o"]))

        assert :ok = DecisionApp.dispatch(command)
        assert [%RecordedEvent{}] = stored_decided(command)

        # On the batch, the same kind and the same items are compared.
        command = deciding(TwoFamilies, items, two_digested([kind <> "b", other <> "o"]))

        assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)

        # Not conditional, the same batch gets past F7 and the required list refuses it.
        unopted = items_of(@unopted, [kind <> "a", other <> "o"])
        command = deciding(TwoFamilies, unopted, %Unopted{tags: [other <> "o"]})

        assert {:error, :missing_required_tag} = DecisionApp.dispatch(command)
      end

      test "#{kind} on the batch with no item, #{other} matching, refuses" do
        kind = unquote(kind) <> ":" <> @fp <> ":"
        other = unquote(other) <> ":" <> @fp <> ":"
        items = items_of(@two_digested, [other <> "o"])
        command = deciding(TwoFamilies, items, two_digested([kind <> "a", other <> "o"]))

        assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
        assert stored_decided(command) == []
      end
    end
  end

  describe "a create's fact item" do
    # Of another stream kind: a second `:claim` member would carry the route's kind anyway.
    defp on_two_kinds(other, first, second) do
      fn _state, %{claim_id: route} ->
        {:streams, [{:claim, route, first}, {:order, other, second}]}
      end
    end

    defp route_item(type),
      do: fn %{claim_id: route}, _context -> [item(type, ["claim:" <> route])] end

    test "asks nothing of a declared type on another member's stream kind" do
      other = UUID.uuid4()
      command = deciding(Creating, [], on_two_kinds(other, %Unopted{}, %Unopted{}))

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{data: %Unopted{}}] = stored_decided(command)
      assert [%RecordedEvent{data: %Unopted{}}] = stored("creating-" <> other, DecisionApp)
    end

    test "the exception is that item alone: the same item the decision wrote still asks" do
      other = UUID.uuid4()
      decide = on_two_kinds(other, %Unopted{}, %Unopted{})
      command = scripted([query: route_item(@unopted), decide: decide], Unindexed)

      assert {:error, :missing_required_tag} = DecisionApp.dispatch(command)
      assert stored_decided(command) == []
      assert stored("unindexed-" <> other, DecisionApp) == []
    end

    test "a create's own item naming the route's stream tag still asks" do
      other = UUID.uuid4()
      decide = on_two_kinds(other, %Unopted{}, %Unopted{})
      command = scripted([query: route_item(@unopted), decide: decide], Creating)

      assert {:error, :missing_required_tag} = DecisionApp.dispatch(command)
      assert stored_decided(command) == []
      assert stored("creating-" <> other, DecisionApp) == []
    end

    test "a second member of a type it does not name lands" do
      other = UUID.uuid4()
      command = deciding(Creating, [], on_two_kinds(other, %Unopted{}, %Digested{}))

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{data: %Unopted{}}] = stored_decided(command)
      assert [%RecordedEvent{data: %Digested{}}] = stored("creating-" <> other, DecisionApp)
    end

    test "of a route kind listed `:digest` refuses at step 3, before `decide/2`" do
      # F7's fact-item read is unreachable on a store dispatch here: step 3 refuses it first.
      test = self()

      command =
        deciding(KeyedCreate, [], fn _state, _command ->
          send(test, :decided)
          %Unopted{}
        end)

      assert {:error, :tag_index_not_enabled} = DecisionApp.dispatch(command)

      refute_received :decided
      assert stored_decided(command) == []
    end
  end

  describe "a `replay: true` item" do
    defp replay_query(type, tags) do
      fn %{claim_id: route} = command, _context ->
        [
          [
            types: [type],
            tags: ["claim:" <> route, Append.kind_tag(command) | tags],
            replay: true
          ]
        ]
      end
    end

    test "asks its kinds, as any item does" do
      query = replay_query(@unopted, ["label:l"])

      command = scripted([query: query, decide: fn _state, _c -> %Unopted{} end], Unindexed)

      assert {:error, :missing_required_tag} = DecisionApp.dispatch(command)
      assert stored_decided(command) == []

      decide = fn _state, _command -> %Unopted{tags: ["label:l"]} end
      command = scripted([query: query, decide: decide], Unindexed)

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{}] = stored_decided(command)
    end

    test "has its digest kind compared by F7" do
      query = replay_query(@digested, ["email:#{@fp}:a"])

      command =
        scripted([query: query, decide: fn _s, _c -> digested(["email:#{@fp}:a"]) end], Indexed)

      assert :ok = DecisionApp.dispatch(command)
      assert [%RecordedEvent{}] = stored_decided(command)

      command =
        scripted([query: query, decide: fn _s, _c -> digested(["email:#{@fp}:b"]) end], Indexed)

      assert {:error, :digest_tag_mismatch} = DecisionApp.dispatch(command)
      assert stored_decided(command) == []
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
      assert catch_exit(prepare(event.("throw"))) == {:nocatch, {:thrown, :redacted}}
    end

    test "on an event declaring nothing, stays as it is" do
      assert_raise ArgumentError, "plain raise: shown", fn ->
        prepare(%PlainRaising{note: "shown"})
      end
    end
  end
end
