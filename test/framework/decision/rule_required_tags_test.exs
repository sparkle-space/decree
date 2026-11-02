defmodule Decree.Decision.RuleRequiredTagsTest do
  use Decree.RuleCase

  import ExUnit.CaptureLog

  alias Decree.RequiredTagsFixtures.Digested

  alias Decree.RuleFixtures.{
    AddMember,
    DigestChecked,
    MemberAdded,
    OAuthJoined,
    PlainNote,
    RegisterOAuth,
    StrictMemberAdded,
    StrictOAuthJoined
  }

  @retry [:decree, :decision, :retry]

  setup do
    attach_telemetry([@retry])
    Process.register(self(), :rule_recorder)
    :ok
  end

  defp naming(module, tag) do
    fn _command, _tag_context -> [[types: [Atom.to_string(module)], tags: [tag]]] end
  end

  defp appending(event) do
    fn _state, %{id: id} -> %{event | id: id} end
  end

  defp assert_refused(command, reason) do
    stream = ruled_stream(command)

    assert App.dispatch(command) == {:error, reason}

    assert stored(stream, App) == []
    assert received(@retry) == []

    assert %RuledAggregate{notes: []} = App.aggregate_state(RuledAggregate, stream)
  end

  describe "CC-13, the required-tag half" do
    test "a nil-email registration appends, the kind being conditional" do
      command =
        ruled(
          [query: naming(OAuthJoined, "email:probe"), execute: appending(%OAuthJoined{})],
          RegisterOAuth
        )

      assert :ok = App.dispatch(command)
      assert [%{data: %OAuthJoined{digest: nil}}] = stored(ruled_stream(command), App)
    end

    test "an unkeyed second command appends, the kind being conditional" do
      command =
        ruled(
          [query: naming(MemberAdded, "member_key:probe"), execute: appending(%MemberAdded{})],
          AddMember
        )

      assert :ok = App.dispatch(command)
      assert [%{data: %MemberAdded{key: nil}}] = stored(ruled_stream(command), App)
    end

    test "the same registration refuses, the kind not being conditional" do
      command =
        ruled(
          [
            query: naming(StrictOAuthJoined, "email:probe"),
            execute: appending(%StrictOAuthJoined{})
          ],
          RegisterOAuth
        )

      assert_refused(command, :missing_required_tag)
    end

    test "the same second command refuses, the kind not being conditional" do
      command =
        ruled(
          [
            query: naming(StrictMemberAdded, "member_key:probe"),
            execute: appending(%StrictMemberAdded{})
          ],
          AddMember
        )

      assert_refused(command, :missing_required_tag)
    end
  end

  describe "a kind an item names" do
    test "is required on a rule route, where the same event on a plain route is not refused" do
      plain = ruled([execute: appending(%StrictOAuthJoined{})], PlainNote)

      assert :ok = App.dispatch(plain)
      assert [%{data: %StrictOAuthJoined{}}] = stored(ruled_stream(plain), App)

      ruled =
        ruled(
          [
            query: naming(StrictOAuthJoined, "email:probe"),
            execute: appending(%StrictOAuthJoined{})
          ],
          RegisterOAuth
        )

      assert_refused(ruled, :missing_required_tag)
    end

    test "passes when the event carries it" do
      command =
        ruled(
          [
            query: naming(StrictOAuthJoined, "email:probe"),
            execute: appending(%StrictOAuthJoined{digest: "d"})
          ],
          RegisterOAuth
        )

      assert :ok = App.dispatch(command)
    end

    test "counts from the second read" do
      followup = fn _command, _tag_context, _events ->
        [[types: [Atom.to_string(StrictOAuthJoined)], tags: ["email:probe"]]]
      end

      command =
        ruled([followup: followup, execute: appending(%StrictOAuthJoined{})], RegisterOAuth)

      assert_refused(command, :missing_required_tag)
    end

    test "asks nothing of an event of a type the item doesn't name" do
      command =
        ruled(
          [query: naming(Read, "email:probe"), execute: appending(%StrictOAuthJoined{})],
          RegisterOAuth
        )

      assert :ok = App.dispatch(command)
    end

    test "asks every event of the batch" do
      execute = fn _state, %{id: id} ->
        [%StrictOAuthJoined{id: id, digest: "d"}, %StrictOAuthJoined{id: id}]
      end

      command =
        ruled(
          [query: naming(StrictOAuthJoined, "email:probe"), execute: execute],
          RegisterOAuth
        )

      assert_refused(command, :missing_required_tag)
    end
  end

  describe "the kinds a conditional append always requires" do
    test "refuse a command with no `command_id`, event opted in or not" do
      command = ruled(command_id: nil)

      assert_refused(command, :missing_required_tag)
    end

    test "are not a plain append's: `[]` from `query/2` has no condition" do
      command = ruled(command_id: nil, query: fn _command, _tag_context -> [] end)

      assert :ok = App.dispatch(command)
      assert [%{data: %Noted{}}] = stored(ruled_stream(command), App)
    end
  end

  describe "the event's own answer" do
    test "stays required though the kind is conditional" do
      command =
        ruled(
          [
            query: naming(OAuthJoined, "email:probe"),
            execute: appending(%OAuthJoined{digest: "d", forget_tag: true})
          ],
          RegisterOAuth
        )

      assert_refused(command, :missing_required_tag)
    end

    test "is all a plain append from `[]` gets" do
      nothing = fn _command, _tag_context -> [] end

      strict = ruled([query: nothing, execute: appending(%StrictOAuthJoined{})], RegisterOAuth)

      assert :ok = App.dispatch(strict)

      opted_in =
        ruled(
          [
            query: nothing,
            execute: appending(%OAuthJoined{digest: "d", forget_tag: true})
          ],
          RegisterOAuth
        )

      assert_refused(opted_in, :missing_required_tag)
    end
  end

  describe "a digest" do
    @digest "digest-that-may-not-be-logged"

    defp digested(item_tag, event_tags) do
      item = [types: [Atom.to_string(Digested)], tags: [item_tag]]

      ruled(
        [
          query: fn _command, _tag_context -> [item] end,
          execute: fn _state, _command -> %Digested{tags: event_tags} end
        ],
        DigestChecked
      )
    end

    test "unlike the condition's answers `:digest_tag_mismatch`, with nothing appended" do
      command = digested("email:" <> @digest, ["email:other-" <> @digest])
      stream = ruled_stream(command)

      {reply, log} = with_log([level: :debug], fn -> App.dispatch(command) end)

      assert reply == {:error, :digest_tag_mismatch}
      refute log =~ "other-" <> @digest

      refute_received {:append_to_stream, ^stream, _version, _count, _opts}
      assert stored(stream, App) == []
      assert received(@retry) == []
    end

    test "with none on the condition, the batch having one, is unequal" do
      item = [types: [Atom.to_string(Read)], tags: ["scope:x"]]

      command =
        ruled(
          [
            query: fn _command, _tag_context -> [item] end,
            execute: fn _state, _command -> %Digested{tags: ["email:" <> @digest]} end
          ],
          DigestChecked
        )

      assert_refused(command, :digest_tag_mismatch)
    end

    test "equal to the condition's appends" do
      assert :ok = App.dispatch(digested("email:" <> @digest, ["email:" <> @digest]))
    end

    test "dropped from the batch passes, as an erased subject's does" do
      assert :ok = App.dispatch(digested("email:" <> @digest, []))
    end

    test "of a `:plain` kind is not compared" do
      assert :ok = App.dispatch(digested("label:one", ["label:other"]))
    end

    test "on a rule with no `index_families/0` is not compared" do
      item = [types: [Atom.to_string(Digested)], tags: ["email:one"]]

      command =
        ruled(
          query: fn _command, _tag_context -> [item] end,
          execute: fn _state, _command -> %Digested{tags: ["email:other"]} end
        )

      assert :ok = App.dispatch(command)
    end
  end
end
