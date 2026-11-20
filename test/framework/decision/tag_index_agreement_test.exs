defmodule Decree.Decision.TagIndexAgreementTest do
  use Decree.DecisionStoreCase

  # A decision's step 3 and `Decree.Store.TagIndex`'s public check fold the same facts the
  # same way: on each store below, a dispatch goes on exactly when `states/2` and `missing/2`
  # find its families enabled, and `bootstrap/3` verifies exactly then. An application's
  # copy of the fold is the contrast: it answered `:verified` on two of these stores, where
  # every dispatch refuses.

  alias Decree.DecisionFixtures.{Digesting, DisableDeletableApp, IndexDeletableApp}
  alias Decree.Store.{ReadResult, TagIndex}

  # A decision requires `command` for its command item, and `entity` for its scope tag.
  @requirements ["command", "entity"]

  describe "a store with history" do
    @describetag :no_index_facts

    for {name, _facts, _missing, _copy} <- TagIndexFixtures.agreement_cases() do
      test "agrees on #{name}" do
        {_name, facts, missing, copy_missing} =
          List.keyfind(TagIndexFixtures.agreement_cases(), unquote(name), 0)

        seed(%Released{scope: "history"})

        for fact <- facts do
          seed(%Released{scope: "fact"}, TagIndexFixtures.fact_tags(fact))
        end

        store = store(App)

        assert {:ok, states} = TagIndex.states(Decree.FrameworkEventStore, name: store)
        assert TagIndex.missing(states, @requirements) == missing

        bootstrap =
          TagIndex.bootstrap(Decree.FrameworkEventStore, @requirements, &never_enable/1,
            name: store
          )

        dispatch = App.dispatch(scripted())

        if missing == [] do
          assert bootstrap == :verified
          assert dispatch == :ok
        else
          assert bootstrap == {:error, {:tag_index_not_enabled, missing}}
          assert dispatch == {:error, :tag_index_not_enabled}
        end

        assert copy_missing(store, @requirements) == copy_missing
      end
    end
  end

  describe "a digest family under its key" do
    test "agrees while the key is enabled, and once it is disabled" do
      fingerprint = TagIndexFixtures.fingerprint()
      requirements = ["command", {"email", fingerprint}]

      item = [
        types: [Atom.to_string(Released)],
        tags: [TagIndexFixtures.digest_tag("email", "d1")]
      ]

      digesting = fn -> scripted([query: fn _command, _context -> [item] end], Digesting) end
      store = store(App)

      assert {:ok, states} = TagIndex.states(Decree.FrameworkEventStore, name: store)
      assert TagIndex.missing(states, requirements) == []

      assert TagIndex.bootstrap(Decree.FrameworkEventStore, requirements, &never_enable/1,
               name: store
             ) == :verified

      assert :ok = App.dispatch(digesting.())

      disable_index!("email", fingerprint)

      assert {:ok, states} = TagIndex.states(Decree.FrameworkEventStore, name: store)
      assert TagIndex.missing(states, requirements) == [{"email", fingerprint}]

      assert TagIndex.bootstrap(Decree.FrameworkEventStore, requirements, &never_enable/1,
               name: store
             ) == {:error, {:tag_index_not_enabled, [{"email", fingerprint}]}}

      assert {:error, :tag_index_not_enabled} = App.dispatch(digesting.())
    end
  end

  describe "a fact whose state tags the read leaves out" do
    test "an enable: every check refuses" do
      start_supervised!({IndexDeletableApp, []})

      assert_refused(IndexDeletableApp)

      # The copy missed every enable, so it refused too.
      assert copy_missing(store(IndexDeletableApp), @requirements) == @requirements
    end

    test "a later disable, with the enable before it showing: every check refuses" do
      start_supervised!({DisableDeletableApp, []})

      disable_index!("entity")

      assert_refused(DisableDeletableApp)

      # The copy read the enable alone, so it answered `:verified`.
      assert copy_missing(store(DisableDeletableApp), @requirements) == []
    end
  end

  defp assert_refused(application) do
    store = store(application)

    assert TagIndex.states(Decree.FrameworkEventStore, name: store) ==
             {:error, :unattributable_tag_index_fact}

    assert TagIndex.bootstrap(Decree.FrameworkEventStore, @requirements, &never_enable/1,
             name: store
           ) == {:error, :unattributable_tag_index_fact}

    assert application.dispatch(scripted()) == {:error, :tag_index_not_enabled}
  end

  defp store(application), do: Module.concat([application, "FrameworkEventStore"])

  defp never_enable(_requirements), do: flunk("bootstrap enabled a store with history")

  # A history check an application wrote before decree 0.2.0, with its own copy of the fold: it
  # folded the read's `tags` itself, checking each family's enabled tag first.
  defp copy_missing(store, families) do
    {:ok, %ReadResult{tags: tags}} =
      Decree.FrameworkEventStore.read_query([[tags: [TagIndex.fact_tag()]]],
        name: store,
        tag_items: [0],
        position_items: [0]
      )

    families -- copy_enabled_families(tags, families)
  end

  defp copy_enabled_families(tags, families) do
    enabled =
      tags
      |> Enum.sort_by(fn {position, _tags} -> position end)
      |> Enum.reduce(MapSet.new(), fn {_position, fact_tags}, acc ->
        Enum.reduce(families, acc, &copy_fold_state(&1, &2, fact_tags))
      end)

    Enum.filter(families, &MapSet.member?(enabled, &1))
  end

  defp copy_fold_state(family, enabled, fact_tags) do
    cond do
      TagIndex.state_tag(family, nil, :enabled) in fact_tags -> MapSet.put(enabled, family)
      TagIndex.state_tag(family, nil, :disabled) in fact_tags -> MapSet.delete(enabled, family)
      true -> enabled
    end
  end
end
