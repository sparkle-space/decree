defmodule Decree.Decision.RuleTagIndexTest do
  use Decree.RuleCase

  # Step 3 on a rule route: the index check, with no `command` requirement.

  @read [:decree, :decision, :read]

  setup do
    attach_telemetry([@read])
    :ok
  end

  describe "a fresh store and a store with history" do
    @describetag :no_index_facts

    test "with no facts, the first dispatch refuses" do
      command = ruled()

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(ruled_stream(command), App) == []
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
    test "is read after the rule's own items, and its facts' tags in the one fetch" do
      Process.register(self(), :rule_recorder)

      command = ruled()

      assert :ok = App.dispatch(command)

      assert_received {:read_query, items, _opts}
      assert List.last(items) == [tags: ["tag_index:fact"]]

      positions = Enum.to_list(1..fact_count())
      assert_received {:read_tags, ^positions}
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
end
