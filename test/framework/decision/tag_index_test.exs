defmodule Decree.Decision.TagIndexTest do
  use Decree.DecisionStoreCase

  # Step 3 on a decision route: the index check, then the activation check.

  alias Decree.DecisionFixtures.{
    Activated,
    Activating,
    Burning,
    Digesting,
    DisableDeletableApp,
    HardDeletesApp,
    IndexDeletableApp
  }

  alias Decree.Event.Append
  alias Decree.FrameworkStorage
  alias Decree.Store.{Config, TagIndex}

  @retry [:decree, :decision, :retry]
  @read [:decree, :decision, :read]
  @read_query [:decree, :event_store, :read_query, :start]
  @read_tags [:decree, :event_store, :read_tags]

  setup do
    attach_telemetry([@retry, @read])
    :ok
  end

  defp stream(%Activating{claim_id: claim_id}), do: "activating-" <> claim_id
  defp stream(%Digesting{claim_id: claim_id}), do: "digesting-" <> claim_id
  defp stream(%{claim_id: claim_id}), do: "scripted-" <> claim_id

  # Tells the test about every callback that runs after step 3.
  defp reporting(test, opts \\ []) do
    scripted(
      [
        admit: fn _state, _command ->
          send(test, :admitted)
          :ok
        end,
        decide: fn _state, command ->
          send(test, :decided)
          %Claimed{claim_id: command.claim_id, scope: command.scope}
        end
      ] ++ opts
    )
  end

  defp refute_called do
    refute_received :admitted
    refute_received :decided
  end

  describe "a fresh store and a store with history" do
    @describetag :no_index_facts

    test "with no facts, the first dispatch refuses" do
      command = scripted()

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(stream(command)) == []
    end

    test "the enabling step enables a fresh store, its facts from position 1" do
      assert {:error, :tag_index_not_enabled} = App.dispatch(scripted())

      TagIndexFixtures.enable_fresh!(App)

      assert Enum.map(positioned(TagIndexFixtures.ledger_stream()), & &1.event_number) ==
               fact_positions()

      command = scripted()

      assert :ok = App.dispatch(command)
      assert [_one] = stored(stream(command))
    end

    test "on a store with history the enabling step does nothing, and dispatches refuse" do
      seed(%Released{scope: "history"}, [])

      TagIndexFixtures.enable_fresh!(App)

      assert stored(TagIndexFixtures.ledger_stream()) == []

      command = scripted()

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(stream(command)) == []
    end
  end

  describe "the repair path" do
    test "disable for repair, a repaired row, a new enable: the dispatch sees the row" do
      test = self()
      scope = scope()

      # An event whose `scope:` row is missing, as an index built before its kind was.
      position = seed(%Claimed{scope: scope, note: "untagged"}, [])

      decide = fn state, command ->
        send(test, {:state, state})
        %Claimed{claim_id: command.claim_id, scope: command.scope}
      end

      assert :ok = App.dispatch(scripted(scope: scope, decide: decide))
      assert_received {:state, []}

      disable_index!("entity")

      assert {:error, :tag_index_not_enabled} =
               App.dispatch(scripted(scope: scope, decide: decide))

      insert_tag!(position, "scope:" <> scope, Atom.to_string(Claimed))

      enable_index!("entity")

      assert :ok = App.dispatch(scripted(scope: scope, decide: decide))
      assert_received {:state, [%Claimed{note: "untagged"}, %Claimed{}]}
    end
  end

  describe "the order of step 3" do
    setup do
      disable_index!("command")
      :ok
    end

    test "a duplicate resend refuses before the fold, `admit/2` and `decide/2`" do
      enable_index!("command")

      command = reporting(self())
      assert :ok = App.dispatch(command)
      assert_received :admitted
      assert_received :decided

      disable_index!("command")

      # `evolve/2` raises on what this read folds, so a fold would answer the raise.
      seed(%Burning{scope: command.scope}, ["scope:" <> command.scope])

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)

      refute_called()
      assert [_only_the_first] = stored(stream(command))
    end

    test "a replay resend refuses before the fold, `admit/2` and `decide/2`" do
      enable_index!("command")

      gate = fn command, _context ->
        [
          [
            types: [Atom.to_string(Claimed)],
            tags: ["scope:" <> command.scope, Append.kind_tag(command)],
            replay: true
          ]
        ]
      end

      command =
        reporting(self(),
          query: gate,
          scope_tags: fn command -> ["scope:" <> command.scope] end
        )

      assert :ok = App.dispatch(command)
      assert_received :admitted
      assert_received :decided

      disable_index!("command")

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)

      refute_called()
      assert [_only_the_first] = stored(stream(command))
    end

    test "a create's first dispatch refuses before the fold, `admit/2` and `decide/2`" do
      test = self()

      # `evolve/2` raises on what this read folds, so a fold would answer the raise.
      burning = fn command, _context ->
        [[types: [Atom.to_string(Burning)], tags: ["scope:" <> command.scope]]]
      end

      command =
        scripted(
          [
            query: burning,
            admit: fn _state, _command ->
              send(test, :admitted)
              :ok
            end,
            decide: fn _state, _command -> send(test, :decided) end
          ],
          Create
        )

      seed(%Burning{scope: command.scope}, ["scope:" <> command.scope])

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)

      refute_called()
      assert stored("create-" <> command.claim_id) == []
    end
  end

  # The tags of the facts and command matches come with the read that found them, so no
  # dispatch sends `read_tags`.
  describe "the framework matches' tags" do
    setup do
      attach_telemetry([@read_query | for(stage <- [:start, :stop], do: @read_tags ++ [stage])])
      :ok
    end

    test "come with the read, and an enabled index decides with no `read_tags`" do
      command = reporting(self())

      assert :ok = App.dispatch(command)

      assert_received :decided
      assert [_one] = stored(stream(command))
      assert_received {:telemetry, @read_query, _, %{items: items, opts: opts}}
      assert Keyword.fetch!(opts, :tag_items) == framework_indexes(items)
      refute_read_tags()
    end

    test "a disabled index refuses with no `read_tags`" do
      disable_index!("entity")

      command = reporting(self())

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)

      refute_called()
      assert stored(stream(command)) == []
      refute_read_tags()
    end

    @tag :no_index_facts
    test "a missing index refuses with no `read_tags`" do
      command = reporting(self())

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)

      refute_called()
      assert stored(stream(command)) == []
      refute_read_tags()
    end

    test "a resend is a duplicate with no `read_tags`" do
      command = reporting(self())

      assert :ok = App.dispatch(command)
      assert_received :decided

      assert {:error, :duplicate_command} = App.dispatch(command)

      # Step 7 comes after `admit/2` and before `decide/2`.
      refute_received :decided
      assert [_only_the_first] = stored(stream(command))
      refute_read_tags()
    end

    test "a `command_id` on an event of another kind is reused, with no `read_tags`" do
      command = reporting(self())

      seed(%Released{scope: command.scope}, ["command:" <> command.command_id])

      assert {:error, :reused_command_id} = App.dispatch(command)

      refute_received :decided
      assert stored(stream(command)) == []
      refute_read_tags()
    end

    test "a replay resend reaches `decide/2` with no `read_tags`" do
      gate = fn command, _context ->
        [
          [
            types: [Atom.to_string(Claimed)],
            tags: ["scope:" <> command.scope, Append.kind_tag(command)],
            replay: true
          ]
        ]
      end

      command =
        reporting(self(),
          query: gate,
          scope_tags: fn command -> ["scope:" <> command.scope] end
        )

      assert :ok = App.dispatch(command)
      assert_received :decided

      # A replay whose `decide/2` answers events is reused, after `decide/2` ran.
      assert {:error, :reused_command_id} = App.dispatch(command)

      assert_received :decided
      assert [_only_the_first] = stored(stream(command))
      refute_read_tags()
    end
  end

  defp framework_indexes(items) do
    for {item, index} <- Enum.with_index(items),
        match?([tags: ["command:" <> _]], item) or item == Decree.Decision.Read.index_item(),
        do: index
  end

  defp refute_read_tags do
    refute_received {:telemetry, [:decree, :event_store, :read_tags | _stage], _, _}
  end

  describe "a family" do
    test "named in DOC-1's list resolves to itself, never to `entity`" do
      item = [types: [Atom.to_string(Released)], tags: ["tenant_slug:acme"]]
      query = fn _command, _context -> [item] end

      assert :ok = App.dispatch(scripted(query: query))

      disable_index!("tenant_slug")

      command = scripted(query: query)

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(stream(command)) == []
    end

    test "listed `:digest` with an unreadable fingerprint refuses, never reading as `-`" do
      for tag <- ["email:no-digest", "email:-:digest", "email::digest", "email"] do
        item = [types: [Atom.to_string(Released)], tags: [tag]]
        command = scripted([query: fn _command, _context -> [item] end], Digesting)

        assert {:error, :tag_index_not_enabled} = App.dispatch(command), tag
        assert stored(stream(command)) == []
      end
    end
  end

  describe "a fact the fold cannot attribute" do
    test "one whose state tags the read leaves out refuses, as a disable would" do
      start_supervised!({IndexDeletableApp, []})

      command = scripted()

      assert {:error, :tag_index_not_enabled} = IndexDeletableApp.dispatch(command)
      assert stored(stream(command), IndexDeletableApp) == []

      # The same store, with nothing declared deletable, reads the same facts as enabled.
      assert :ok = App.dispatch(scripted())
    end

    test "a later disable whose state tags the read leaves out refuses, though an enable shows" do
      start_supervised!({DisableDeletableApp, []})

      disable_index!("entity")

      # The enable before it still reads, so skipping the disable would read as enabled.
      command = scripted()

      assert {:error, :tag_index_not_enabled} = DisableDeletableApp.dispatch(command)
      assert stored(stream(command), DisableDeletableApp) == []
    end

    test "one naming a family both ways reads as disabled" do
      seed(%Released{scope: "both"}, [
        TagIndex.fact_tag(),
        TagIndex.state_tag("entity", nil, :enabled),
        TagIndex.state_tag("entity", nil, :disabled)
      ])

      command = scripted()

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(stream(command)) == []
    end
  end

  describe "the second read's items" do
    test "require their families too: a disabled one refuses" do
      disable_index!("tenant_slug")

      followup = fn _command, _context, _events ->
        [[types: [Atom.to_string(Released)], tags: ["tenant_slug:acme"]]]
      end

      # Every family the first read requires is enabled.
      assert :ok = App.dispatch(scripted())

      command = scripted(followup: followup)

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(stream(command)) == []
    end
  end

  describe "a hard delete" do
    setup do
      start_supervised!({HardDeletesApp, []})

      [store: Module.concat([HardDeletesApp, "FrameworkEventStore"])]
    end

    test "of a stream linking a disable is refused, and the decision still refuses", context do
      %{store: store} = context

      disable_index!("entity")

      {:ok, facts} =
        Decree.FrameworkEventStore.read_stream_forward(TagIndexFixtures.ledger_stream(), 0, 100,
          name: store
        )

      disable = List.last(facts)

      :ok =
        Decree.FrameworkEventStore.link_to_stream("fact-link", :any_version, [disable],
          name: store
        )

      assert {:error, :hard_delete_of_index_fact} =
               Decree.FrameworkEventStore.delete_stream("fact-link", :any_version, :hard,
                 name: store
               )

      assert {:error, :hard_delete_of_index_fact} =
               Decree.FrameworkEventStore.delete_stream(
                 TagIndexFixtures.ledger_stream(),
                 :any_version,
                 :hard,
                 name: store
               )

      command = scripted()

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(stream(command)) == []
    end
  end

  describe "a fact above `after`" do
    test "an enable between the two reads is not folded: the dispatch refuses" do
      test = self()
      held = make_ref()

      disable_index!("entity")

      followup = fn command, _context, _events ->
        if not Process.get(held, false) do
          Process.put(held, true)
          hold(test, :between_reads)
        end

        [claimed_item(command.scope)]
      end

      command = reporting(test, followup: followup)
      task = Task.async(fn -> App.dispatch(command, timeout: 60_000) end)

      assert_receive {:between_reads, pid}
      enable_index!("entity")
      send(pid, :go)

      assert {:error, :tag_index_not_enabled} = Task.await(task, :infinity)

      refute_called()
      refute_received {:telemetry, @retry, _, _}
      assert stored(stream(command)) == []
    end
  end

  describe "activation" do
    defp activate!, do: seed(%Activated{feature: "claims"}, ["feature:claims"])

    test "refuses `:decision_not_activated` until its event is there, then appends" do
      command = scripted([], Activating)

      assert {:error, :decision_not_activated} = App.dispatch(command)
      assert stored(stream(command)) == []

      seed(%Activated{feature: "other"}, ["feature:other"])
      assert {:error, :decision_not_activated} = App.dispatch(command)

      activate!()

      assert :ok = App.dispatch(command)
      assert [_one] = stored(stream(command))
    end

    test "an activation committed between the two reads is not counted: it refuses" do
      test = self()
      held = make_ref()

      followup = fn command, _context, _events ->
        if not Process.get(held, false) do
          Process.put(held, true)
          hold(test, :between_reads)
        end

        [claimed_item(command.scope)]
      end

      command = scripted([followup: followup], Activating)
      task = Task.async(fn -> App.dispatch(command, timeout: 60_000) end)

      assert_receive {:between_reads, pid}
      activate!()
      send(pid, :go)

      # An activation above `after` is not this read's state, as an index fact above it is not.
      assert {:error, :decision_not_activated} = Task.await(task, :infinity)

      refute_received {:telemetry, @retry, _, _}
      assert stored(stream(command)) == []
    end

    test "comes after the index check" do
      activate!()
      disable_index!("entity")

      assert {:error, :tag_index_not_enabled} = App.dispatch(scripted([], Activating))
    end

    test "an activation between the read and the append fails the condition, and the retry appends" do
      test = self()
      held = make_ref()

      decide = fn state, command ->
        if not Process.get(held, false) do
          Process.put(held, true)
          hold(test, :deciding)
        end

        send(test, {:state, state})
        %Claimed{claim_id: command.claim_id, scope: command.scope}
      end

      # A dispatch with no activation is refused at step 3 and never reaches its append, so the
      # window is a running one's, already activated, and a second activation lands in it.
      activate!()

      command = scripted([decide: decide], Activating)
      task = Task.async(fn -> App.dispatch(command, timeout: 60_000) end)

      assert_receive {:deciding, pid}
      activate!()
      send(pid, :go)

      assert :ok = Task.await(task, :infinity)

      assert_received {:telemetry, @retry, %{attempt: 1}, %{reason: :append_condition_failed}}

      # The activation item is never folded.
      assert_received {:state, []}
      assert_received {:state, []}
      assert [_one] = stored(stream(command))
    end

    test "its item is read after the index item, and counts against `max_items`" do
      attach_telemetry([[:decree, :event_store, :read_query, :start]])
      activate!()

      command = scripted([], Activating)
      assert :ok = App.dispatch(command)

      assert_received {:telemetry, [:decree, :event_store, :read_query, :start], _,
                       %{items: items}}

      assert Enum.take(items, -3) == [
               [tags: ["command:" <> command.command_id]],
               [tags: ["tag_index:fact"]],
               [types: [Atom.to_string(Activated)], tags: ["feature:claims"]]
             ]

      thirteen = Enum.map(1..13, &claimed_item("item-#{&1}"))
      fourteen = Enum.map(1..14, &claimed_item("item-#{&1}"))

      assert :ok = App.dispatch(scripted([query: fn _c, _t -> thirteen end], Activating))

      assert {:error, :decision_query_too_large} =
               App.dispatch(scripted([query: fn _c, _t -> fourteen end], Activating))
    end
  end

  # What a repair writes: a tag row for an event already stored, outside any append.
  defp insert_tag!(position, tag, event_type) do
    {:ok, conn} =
      FrameworkStorage.config() |> Config.default_postgrex_opts() |> Postgrex.start_link()

    try do
      Postgrex.query!(
        conn,
        "INSERT INTO event_tags (position, tag, event_type) VALUES ($1, $2, $3)",
        [position, tag, event_type]
      )
    after
      GenServer.stop(conn)
    end
  end
end
