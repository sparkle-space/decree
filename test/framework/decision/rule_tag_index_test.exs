defmodule Decree.Decision.RuleTagIndexTest do
  use Decree.RuleCase

  # Step 3 on a rule route: the index check, with no `command` requirement.

  @read [:decree, :decision, :read]
  @read_tags [:decree, :event_store, :read_tags]

  setup do
    attach_telemetry([@read | for(stage <- [:start, :stop], do: @read_tags ++ [stage])])
    :ok
  end

  defp refute_read_tags do
    refute_received {:read_tags, _positions}
    refute_received {:telemetry, [:decree, :event_store, :read_tags | _stage], _, _}
  end

  describe "a fresh store and a store with history" do
    @describetag :no_index_facts

    test "with no facts, the first dispatch refuses, with no `read_tags`" do
      Process.register(self(), :rule_recorder)

      command = ruled()

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(ruled_stream(command), App) == []
      refute_read_tags()
    end

    test "the enabling step enables a fresh store" do
      assert {:error, :tag_index_not_enabled} = App.dispatch(ruled())

      TagIndexFixtures.enable_fresh!(App)

      assert length(stored(TagIndexFixtures.ledger_stream(), App)) == fact_count()

      command = ruled()

      assert :ok = App.dispatch(command)
      assert [_one] = stored(ruled_stream(command), App)
    end

    test "on a store with history the enabling step does nothing, and dispatches refuse" do
      seed(%Read{scope: "history"}, [], App)

      TagIndexFixtures.enable_fresh!(App)

      assert stored(TagIndexFixtures.ledger_stream(), App) == []

      command = ruled()

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(ruled_stream(command), App) == []
    end
  end

  describe "`[]` from `query/2`" do
    # A registration with no address reads nothing, so it requires no index: a repair of the
    # address's family does not refuse it (DOC-1 §9.2).
    test "reads nothing, checks nothing, and appends with the index disabled" do
      families = [{"email", nil}, {"email", TagIndexFixtures.fingerprint()}, {"entity", nil}]

      for {family, key} <- families do
        disable_index!(family, key)
      end

      Process.register(self(), :rule_recorder)

      command = register(nil)

      assert :ok = App.dispatch(command)

      refute_received {:read_query, _items, _opts}
      refute_received {:read_tags, _positions}
      assert [%{data: %Registered{digest: nil}}] = stored(user_stream(command), App)
    end
  end

  describe "the second read's items" do
    test "require their families too: a disabled one refuses" do
      disable_index!("tenant_slug")

      followup = fn _command, _context, _events ->
        [[types: [Atom.to_string(Read)], tags: ["tenant_slug:acme"]]]
      end

      # Every family the first read requires is enabled.
      assert :ok = App.dispatch(ruled())

      command = ruled(followup: followup)

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(ruled_stream(command), App) == []
    end
  end

  describe "the index item" do
    test "is read after the rule's own items, and its facts' tags with it, with no `read_tags`" do
      Process.register(self(), :rule_recorder)

      command = ruled()

      assert :ok = App.dispatch(command)

      assert_received {:read_query, items, opts}
      assert List.last(items) == [tags: ["tag_index:fact"]]
      assert Keyword.fetch!(opts, :tag_items) == [length(items) - 1]

      assert [_one] = stored(ruled_stream(command), App)
      refute_read_tags()
    end

    test "disabled refuses, with no `read_tags`" do
      disable_index!("entity")

      Process.register(self(), :rule_recorder)

      command = ruled()

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert_received {:read_query, _items, _opts}
      assert stored(ruled_stream(command), App) == []
      refute_read_tags()
    end

    test "a refusal at step 3 calls no `execute/2` and emits no read event" do
      test = self()
      disable_index!("entity")

      execute = fn _state, _command -> send(test, :executed) end

      assert {:error, :tag_index_not_enabled} = App.dispatch(ruled(execute: execute))

      refute_received :executed
      assert received(@read) == []
    end
  end

  # A store gathers facts over its life, and the index item reads every one. `TightRule` bounds
  # `max_events` at 12, so its read asks for 13 rows: the facts must be neither cut by that
  # count nor counted by the bound (sparkle-space/sparklespace#8313).
  describe "more facts than `max_events + 1`" do
    test "a dispatch decides, and the read event counts its business events alone" do
      TagIndexFixtures.seed_facts!(App, 30)

      scope = scope()

      for _event <- 1..2, do: seed(%Read{scope: scope}, ["scope:" <> scope], App)

      command = ruled([scope: scope], Decree.RuleFixtures.Tight)

      assert :ok = App.dispatch(command)
      assert [_one] = stored(ruled_stream(command), App)

      assert [{%{matched_events: 2}, _metadata}] = received(@read)
    end

    test "every fact is folded: the last one a disable refuses" do
      TagIndexFixtures.seed_facts!(App, 30)

      disable_index!("entity")

      command = ruled([], Decree.RuleFixtures.Tight)

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(ruled_stream(command), App) == []
    end

    test "every fact is folded: a disable then an enable as the last two decides" do
      TagIndexFixtures.seed_facts!(App, 30)

      disable_index!("entity")
      enable_index!("entity")

      command = ruled([], Decree.RuleFixtures.Tight)

      assert :ok = App.dispatch(command)
      assert [_one] = stored(ruled_stream(command), App)
    end

    test "the read names the index item, and only it, as `position_items`" do
      Process.register(self(), :rule_recorder)

      assert :ok = App.dispatch(ruled())

      assert_received {:read_query, items, opts}
      assert Keyword.fetch!(opts, :position_items) == [length(items) - 1]
    end
  end
end
