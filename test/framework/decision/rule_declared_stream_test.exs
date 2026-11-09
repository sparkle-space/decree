defmodule Decree.Decision.RuleDeclaredStreamTest do
  use Decree.RuleCase

  alias Decree.RuleFixtures.PlainApp

  @decision_events [
    [:decree, :decision, :read],
    [:decree, :decision, :retry],
    [:decree, :decision, :refused]
  ]

  defp store(application \\ App), do: Module.concat([application, "FrameworkEventStore"])

  setup do
    Process.register(self(), :rule_recorder)
    attach_telemetry(@decision_events)

    user = registered!(digest())

    # Only what the test does next.
    receive_all()

    [user: user]
  end

  defp receive_all do
    receive do
      _message -> receive_all()
    after
      0 -> :ok
    end
  end

  describe "on a declared aggregate's stream" do
    test "a plain command appends with no read and no condition", %{user: user} do
      assert :ok = App.dispatch(%RemoveUser{command_id: UUID.uuid4(), user_id: user})

      stream = user_stream(user)

      assert_received {:append_to_stream, ^stream, 1, 1, []}
      refute_received {:read_query, _items, _opts}
      refute_received {:telemetry, _event, _measurements, _metadata}

      assert [%{data: %Registered{}}, %{data: %Removed{}}] = stored(stream, App)
    end

    test "a link goes in as it would anywhere", %{user: user} do
      seed(%Noted{id: "linked"}, [], App, "linked-source")

      assert [%{event_id: event_id}] = stored("linked-source", App)

      assert :ok =
               Decree.FrameworkEventStore.link_to_stream(
                 user_stream(user),
                 :any_version,
                 [event_id],
                 name: store()
               )

      assert [%{data: %Registered{}}, %{data: %Noted{id: "linked"}}] =
               stored(user_stream(user), App)
    end

    test "a snapshot is taken as on any stream" do
      start_supervised!(
        {App, name: :snapshot_stream, snapshotting: %{User => [snapshot_every: 1]}}
      )

      command = register(digest())
      stream = user_stream(command)

      assert :ok = App.dispatch(command, application: :snapshot_stream)

      # Behind the snapshot in the aggregate's mailbox.
      Decree.Aggregates.Aggregate.aggregate_state(:snapshot_stream, User, stream)

      assert {:ok, %{data: %User{registered?: true}}} =
               EventStore.read_snapshot(:snapshot_stream, stream, 1)
    end

    test "a hard delete goes in, and is a release" do
      start_supervised!(
        {App, name: :hard_deletes, event_store: Stores.store(enable_hard_deletes: true)}
      )

      digest = digest()
      first = register(digest)
      stream = user_stream(first)

      assert :ok = App.dispatch(first, application: :hard_deletes)
      assert {:error, :email_taken} = App.dispatch(register(digest), application: :hard_deletes)

      assert :ok =
               Decree.FrameworkEventStore.delete_stream(stream, :any_version, :hard,
                 name: store(:hard_deletes)
               )

      assert :ok = App.dispatch(register(digest), application: :hard_deletes)
    end
  end

  describe "a route with no rule" do
    test "on a store with no declaration reaches the adapter as it always has" do
      start_supervised!(PlainApp)

      user = UUID.uuid4()

      assert :ok = PlainApp.dispatch(%EraseUser{command_id: UUID.uuid4(), user_id: user})

      stream = user_stream(user)

      assert_received {:append_to_stream, ^stream, 0, 1, []}
      refute_received {:read_query, _items, _opts}
      refute_received {:read_tags, _positions}
      refute_received {:telemetry, _event, _measurements, _metadata}
    end
  end

  test "a rule route's commit on a `returning: :append_result` store reads as success" do
    start_supervised!(
      {App, name: :returning_rules, event_store: Stores.store(returning: :append_result)}
    )

    command = register(digest())

    assert App.dispatch(command, application: :returning_rules, returning: :aggregate_version) ==
             {:ok, 1}

    assert [%{data: %Registered{}}] = stored(user_stream(command), :returning_rules)
  end

  test "a rule route's append carries the condition, naming its aggregate" do
    command = register(digest())

    assert :ok = App.dispatch(command)

    assert_received {:append_to_stream, "user-" <> _id, 0, 1, opts}
    assert [condition: condition] = opts
    assert condition[:target] == User
    assert [_part_one, [tags: ["tag_index:fact"]]] = condition[:items]
    assert is_integer(condition[:after])
  end
end
