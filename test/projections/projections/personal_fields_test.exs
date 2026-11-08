defmodule Decree.Projections.PersonalFieldsTest do
  use Decree.PersonalFields.Case

  alias Decree.Aggregates.Aggregate
  alias Decree.PersonalFields.Fixtures.{Account, Handler, RetryingErrors}
  alias Decree.PersonalFields.Fixtures.Commands.Emit
  alias Decree.PersonalFields.Fixtures.Events.Registered
  alias Decree.Projections.PersonalFieldsProjector
  alias Decree.Projections.{Projection, Repo}

  @projector "PersonalFieldsProjector"

  setup do
    :ok = Ecto.Adapters.SQL.Sandbox.checkout(Repo)
    Ecto.Adapters.SQL.Sandbox.mode(Repo, {:shared, self()})

    start_supervised!({App, on_event_handler_error: RetryingErrors})

    :ok
  end

  test "a sustained unknown key on metadata loses nothing", context do
    %{conn: conn, stream: stream, subject: subject} = context

    dispatch!(stream, %Registered{user_id: subject, email: nil},
      metadata: %{"password_hash" => "hash-of-a-password"}
    )

    # The rehydration: a fold reads the body, and never the metadata.
    folded = UUID.uuid4()

    dispatch!(folded, %Registered{user_id: subject, email: email()})

    :ok = Aggregate.shutdown(App, Account, stream)
    :ok = Aggregate.shutdown(App, Account, folded)
    stored = rows(conn, "events", "data, metadata")
    Keys.set(subject, :unknown)

    start_supervised!({PersonalFieldsProjector, start_from: :origin})
    start_supervised!({Handler, start_from: :origin})

    # Two handlers report, so seven attempts of each are at least fourteen messages.
    for _attempt <- 1..14 do
      assert_receive {:held, _failures, {:error, :key_unknown}}
    end

    refute_received :projected
    refute_received {:module_error, _error}
    refute_received {:handled, _event, _metadata}
    assert acked(conn, @projector) == 0
    assert acked(conn, inspect(Handler)) == 0
    assert Repo.all(Projection) == []

    assert {:error, :key_unknown} =
             App.dispatch(%Emit{command_id: UUID.uuid4(), account_id: folded})

    Keys.set(subject, :key)

    assert_receive :projected
    assert_receive :projected
    assert_receive {:handled, %Registered{email: nil}, %{"password_hash" => "hash-of-a-password"}}
    assert_receive {:handled, %Registered{email: "alice@secret.example"}, _metadata}

    Wait.until(fn -> assert acked(conn, @projector) == 2 end)
    Wait.until(fn -> assert acked(conn, inspect(Handler)) == 2 end)

    assert "hash-of-a-password" in Enum.map(Repo.all(Projection), & &1.name)

    refute_receive :projected
    refute_received {:handled, _event, _metadata}
    refute_received {:module_error, _error}
    assert rows(conn, "events", "data, metadata") == stored

    assert :ok = App.dispatch(%Emit{command_id: UUID.uuid4(), account_id: folded})
  end
end
