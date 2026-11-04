defmodule Decree.Decision.ConformanceTest do
  use Decree.DecisionStoreCase

  # The tag index cases of the conformance table, on a decision route. Their rule-route halves
  # are in `rule_conformance_test.exs`.

  alias Decree.DecisionFixtures.{Digesting, ParityCommand}
  alias Decree.Store.TagIndex
  alias Decree.TagIndexFixtures.{TagIndexDisabled, TagIndexEnabled}

  @retry [:decree, :decision, :retry]
  @append [:decree, :event_store, :append_to_streams, :start]
  @read_query [:decree, :event_store, :read_query, :start]

  setup do
    attach_telemetry([@retry, @append, @read_query])
    :ok
  end

  defp stream(%{claim_id: claim_id} = command) do
    case command do
      %Digesting{} -> "digesting-" <> claim_id
      _scripted -> "scripted-" <> claim_id
    end
  end

  # It reads `Released`, so the `Claimed` it decides is asked no `email` tag.
  defp digesting(digest, opts \\ []) do
    item = [types: [Atom.to_string(Released)], tags: [digest]]

    scripted([query: fn _command, _context -> [item] end] ++ opts, Digesting)
  end

  defp ledger, do: stored(TagIndexFixtures.ledger_stream())

  describe "CC-5, an unavailable digest key" do
    test "`query/2` answers an error, and nothing is read or stored" do
      command = scripted(query: fn _command, _context -> {:error, :digest_key_unavailable} end)

      assert {:error, :digest_key_unavailable} = App.dispatch(command)

      refute_received {:telemetry, @read_query, _, _}
      assert stored(stream(command)) == []
    end
  end

  describe "CC-6, a disable between the read and the append" do
    test "fails the condition on the index item, and the retry refuses" do
      test = self()

      command =
        scripted(
          decide: fn _state, command ->
            hold(test, :deciding)

            %Claimed{claim_id: command.claim_id, scope: command.scope}
          end
        )

      task = Task.async(fn -> App.dispatch(command, timeout: 60_000) end)

      assert_receive {:deciding, pid}
      disable_index!("entity")
      send(pid, :go)

      assert {:error, :tag_index_not_enabled} = Task.await(task, :infinity)

      assert_received {:telemetry, @retry, %{attempt: 1}, %{reason: :append_condition_failed}}
      assert stored(stream(command)) == []
    end

    test "between the two reads fails the same way" do
      test = self()
      held = make_ref()

      # Held on the first attempt only: the retry reads the disable and refuses at step 3.
      followup = fn command, _context, _events ->
        if not Process.get(held, false) do
          Process.put(held, true)
          hold(test, :between_reads)
        end

        [claimed_item(command.scope)]
      end

      command = scripted(followup: followup)
      task = Task.async(fn -> App.dispatch(command, timeout: 60_000) end)

      assert_receive {:between_reads, pid}
      disable_index!("entity")
      send(pid, :go)

      assert {:error, :tag_index_not_enabled} = Task.await(task, :infinity)

      assert_received {:telemetry, @retry, %{attempt: 1}, %{reason: :append_condition_failed}}
      assert stored(stream(command)) == []
    end
  end

  describe "CC-9, a repair of a family with no key" do
    test "disable and new enable both land; a dispatch refuses between and appends after" do
      disabled = disable_index!("entity")

      between = scripted()
      assert {:error, :tag_index_not_enabled} = App.dispatch(between)
      assert stored(stream(between)) == []

      enable_index!("entity")

      after_repair = scripted()
      assert :ok = App.dispatch(after_repair)
      assert [_one] = stored(stream(after_repair))

      assert [
               %TagIndexDisabled{index_id: ^disabled, family: "entity", key: nil},
               %TagIndexEnabled{family: "entity", key: nil, index_id: enabled}
             ] = ledger() |> Enum.map(& &1.data) |> Enum.take(-2)

      assert enabled != disabled
    end
  end

  describe "CC-12, a retired key" do
    setup do
      [
        fingerprint: TagIndexFixtures.fingerprint(),
        tag: TagIndexFixtures.digest_tag("email", "d")
      ]
    end

    test "is never enabled again, and a dispatch naming it refuses throughout", context do
      assert :ok = App.dispatch(digesting(context.tag))

      disable_index!("email", context.fingerprint, "key_retired")
      assert {:error, :tag_index_not_enabled} = App.dispatch(digesting(context.tag))

      assert {:error, :tag_index_key_retired} =
               App.dispatch(TagIndexFixtures.enable_command([{"email", context.fingerprint}]))

      assert {:error, :tag_index_not_enabled} = App.dispatch(digesting(context.tag))
    end

    test "retired while a repair holds it disabled, it is never enabled again", context do
      index_id = disable_index!("email", context.fingerprint, "repair")
      assert {:error, :tag_index_not_enabled} = App.dispatch(digesting(context.tag))

      assert :ok = App.dispatch(TagIndexFixtures.disable_command(index_id, "key_retired"))
      assert %TagIndexDisabled{reason: "key_retired"} = List.last(ledger()).data

      assert {:error, :tag_index_key_retired} =
               App.dispatch(TagIndexFixtures.enable_command([{"email", context.fingerprint}]))

      assert {:error, :tag_index_not_enabled} = App.dispatch(digesting(context.tag))
    end

    test "a restored key has the same fingerprint, and refuses", context do
      disable_index!("email", context.fingerprint, "key_retired")

      restored = TagIndex.fingerprint(TagIndexFixtures.key())
      assert restored == context.fingerprint

      command = digesting("email:" <> restored <> ":d")

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(stream(command)) == []
    end
  end

  describe "CC-14, the `command` family" do
    test "disabled, every decision refuses, though its query names no `command:` item" do
      disable_index!("command")

      for command <- [
            scripted(),
            claim(),
            %ParityCommand{command_id: UUID.uuid4(), id: UUID.uuid4(), scope: scope()}
          ] do
        assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      end

      refute_received {:telemetry, @append, _, _}
    end
  end

  defp claim do
    %Claim{command_id: UUID.uuid4(), claim_id: UUID.uuid4(), scope: scope()}
  end
end
