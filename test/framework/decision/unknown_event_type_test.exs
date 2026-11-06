defmodule Decree.Decision.UnknownEventTypeTest do
  use Decree.DecisionStoreCase

  # A decision whose read matches an event of a type this release does not know (one a
  # newer release appended) refuses with `{:error, :unknown_event_type}`, appending
  # nothing (sparkle-space/sparklespace#7804). The type names no module, so the framework
  # store's serializer does not know it.

  alias Decree.EventStore.EventData

  @unknown_type "Elixir.Decree.X16.NotYetReleasedDecisionEvent"

  test "a read matching an unknown type answers `{:error, :unknown_event_type}`" do
    tag = "scope:" <> scope()

    :ok =
      EventStore.append_to_stream(App, "other-" <> UUID.uuid4(), :any_version, [
        %EventData{
          event_type: @unknown_type,
          data: %{"note" => "from a newer release"},
          metadata: %{},
          tags: [tag]
        }
      ])

    decide = fn _state, _command -> raise "decided on a read it could not fold" end

    command =
      scripted(query: fn _c, _t -> [[types: [@unknown_type], tags: [tag]]] end, decide: decide)

    assert {:error, :unknown_event_type} = App.dispatch(command)

    # Nothing was appended for the command.
    assert stored("scripted-" <> command.claim_id) == []
  end
end
