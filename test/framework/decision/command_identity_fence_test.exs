defmodule Decree.Decision.CommandIdentityFenceTest do
  use Decree.DecisionStoreCase

  # A decision route refuses a `command_id` that is not a canonical UUID, and a routed
  # identity that is not one, as an aggregate route does: before the decision is called, so
  # no statement is sent. The route's router registers no middleware.

  alias Decree.CommandIdCases

  @read_query [:decree, :event_store, :read_query, :start]
  @read_tags [:decree, :event_store, :read_tags, :start]
  @append [:decree, :event_store, :append_to_streams, :start]
  @decision_start [:decree, :decision, :execute, :start]

  setup do
    attach_telemetry([@read_query, @read_tags, @append, @decision_start])
    :ok
  end

  defp claim(fields) do
    struct!(Claim, [command_id: UUID.uuid4(), claim_id: UUID.uuid4(), scope: scope()] ++ fields)
  end

  # No statement, no decision, nothing on the route's stream.
  defp refute_decided(%Claim{claim_id: claim_id}) do
    refute_received {:telemetry, _event, _measurements, _metadata}

    if UUID.canonical?(claim_id), do: assert(stored("claim-" <> claim_id) == [])
  end

  describe "a decision route refuses a `command_id`" do
    for {name, value, refusal} <- CommandIdCases.refusals() do
      test "that is #{name}, with `#{inspect(refusal)}`" do
        command = claim(command_id: unquote(Macro.escape(value)))

        assert {:error, unquote(refusal)} = App.dispatch(command)

        refute_decided(command)
      end
    end

    test "that is nil, though the dispatch minted a `command_uuid`" do
      command = claim(command_id: nil)

      assert {:error, :command_id_required} = App.dispatch(command, command_uuid: UUID.uuid4())

      refute_decided(command)
    end

    test "before the identity" do
      command = claim(command_id: nil, claim_id: "not-a-uuid")

      assert {:error, :command_id_required} = App.dispatch(command)

      refute_decided(command)
    end
  end

  describe "a decision route refuses an identity" do
    for {name, identity} <- [
          {"not a UUID", "not-a-uuid"},
          {"an uppercase UUID", "6F9619FF-8B86-D011-B42D-00C04FC964FF"},
          {"a padded UUID", " 6f9619ff-8b86-d011-b42d-00c04fc964ff"},
          {"an integer", 7}
        ] do
      test "that is #{name}, with `:invalid_aggregate_identity`" do
        command = claim(claim_id: unquote(identity))

        assert {:error, :invalid_aggregate_identity} = App.dispatch(command)

        refute_decided(command)
      end
    end
  end

  test "a canonical `command_id` and identity are decided as they were sent" do
    command = claim([])
    %Claim{claim_id: claim_id} = command

    assert :ok = App.dispatch(command)

    assert [%{data: %Claimed{claim_id: ^claim_id}}] = stored("claim-" <> claim_id)
  end
end
