defmodule Decree.Store.Storage.ConditionalPrefixTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{AppendResult, Config, EventData, LockSession}
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  @declared [stream_prefixes: ["decision-", "ruling-"], targets: [:declared_target]]
  @no_conn :a_connection_that_does_not_exist
  @expected_versions [:any_version, :no_stream, :stream_exists, 0, 3]

  defmodule PassThroughSerializer do
    @behaviour Decree.Store.Serializer

    def serialize(term), do: term
    def deserialize(term, _config), do: term
  end

  setup %{postgrex_config: postgrex_config} do
    stop_supervised!(TestEventStore)

    start_supervised!(
      {TestEventStore, conditional_appends: @declared, serializer: PassThroughSerializer}
    )

    [observer: LockSession.connect(postgrex_config)]
  end

  describe "a plain `append_to_stream`" do
    test "to a declared prefix sends no statement, at every size and expected version" do
      for stream_uuid <- ["decision-1", "ruling-1"],
          expected_version <- @expected_versions,
          count <- [1, 1_000, 1_001] do
        assert EventStore.append_to_stream(stream_uuid, expected_version, events(count),
                 conn: @no_conn
               ) == {:error, :plain_append_to_conditional_prefix},
               inspect({stream_uuid, expected_version, count})
      end
    end

    test "writes nothing", context do
      assert EventStore.append_to_stream("decision-1", 0, events(1)) ==
               {:error, :plain_append_to_conditional_prefix}

      assert all_version(context) == 0
      assert stream_id(context, "decision-1") == nil
    end

    test "with `returning:` is refused all the same" do
      assert EventStore.append_to_stream("decision-1", 0, events(1),
               conn: @no_conn,
               returning: :append_result
             ) == {:error, :plain_append_to_conditional_prefix}
    end

    test "outside a prefix passes, for a name that only resembles one", context do
      for stream_uuid <- ["decision", "Decision-1", "no-decision-1"] do
        assert :ok = EventStore.append_to_stream(stream_uuid, 0, events(1))
      end

      assert all_version(context) == 3
    end
  end

  describe "a plain `append_to_streams`" do
    test "with one such member refuses the batch, wherever it is" do
      for appends <- [
            [{"decision-1", 0, events(1)}],
            [{"decision-1", 0, events(1)}, {"plain", 0, events(1)}],
            [{"plain", 0, events(1)}, {"decision-1", 0, events(1)}],
            [{"a", 0, events(1)}, {"ruling-1", 0, events(1)}, {"z", 0, events(1)}]
          ] do
        assert EventStore.append_to_streams(appends, conn: @no_conn) ==
                 {:error, :plain_append_to_conditional_prefix},
               inspect(Enum.map(appends, &elem(&1, 0)))
      end
    end

    test "writes nothing on any member", context do
      assert EventStore.append_to_streams([
               {"plain", 0, events(1)},
               {"decision-1", 0, events(1)}
             ]) == {:error, :plain_append_to_conditional_prefix}

      assert all_version(context) == 0
      assert stream_id(context, "plain") == nil
    end

    test "with none passes", context do
      assert :ok = EventStore.append_to_streams([{"a", 0, events(1)}, {"b", 0, events(2)}])

      assert all_version(context) == 3
    end
  end

  describe "`link_to_stream`" do
    setup do
      :ok = EventStore.append_to_stream("source", 0, events(2))

      {:ok, linked} = EventStore.read_stream_forward("source")

      [linked: linked]
    end

    test "into a declared prefix sends no statement, at every expected version", context do
      for stream_uuid <- ["decision-1", "ruling-1"],
          expected_version <- @expected_versions do
        assert EventStore.link_to_stream(stream_uuid, expected_version, context.linked,
                 conn: @no_conn
               ) == {:error, :link_into_conditional_prefix},
               inspect({stream_uuid, expected_version})
      end
    end

    test "writes nothing", context do
      assert EventStore.link_to_stream("decision-1", :any_version, context.linked) ==
               {:error, :link_into_conditional_prefix}

      assert stream_id(context, "decision-1") == nil
    end

    test "out of one passes, into a stream outside the prefixes", context do
      condition = [items: [[tags: ["never"]]], after: 2]

      :ok = EventStore.append_to_stream("decision-1", 0, events(1), condition: condition)

      {:ok, linked} = EventStore.read_stream_forward("decision-1")

      assert :ok = EventStore.link_to_stream("plain", :any_version, linked)
      assert stream_versions(context, "plain") == [1]
    end

    test "into a name that only resembles a prefix passes", context do
      for stream_uuid <- ["decision", "no-decision-1"] do
        assert :ok = EventStore.link_to_stream(stream_uuid, :any_version, context.linked)
      end
    end

    test "answers the deletable refusal for a stream both declare", context do
      start_supervised!(
        {TestEventStore,
         name: :both_store,
         conditional_appends: [stream_prefixes: ["decision-"]],
         deletable: [stream_prefixes: ["decision-"], event_types: ["typed"]],
         serializer: PassThroughSerializer}
      )

      assert EventStore.link_to_stream("decision-1", :any_version, context.linked,
               conn: @no_conn,
               name: :both_store
             ) == {:error, :link_into_deletable_stream}
    end
  end

  describe "a condition" do
    test "writes to a declared prefix", context do
      assert :ok =
               EventStore.append_to_stream("decision-1", 0, events(1), condition: condition())

      assert stream_versions(context, "decision-1") == [1]
    end

    test "with a declared `target` on a prefixed stream is still a conditional append",
         context do
      assert :ok =
               EventStore.append_to_stream("decision-1", 0, events(1),
                 condition: condition(target: :declared_target)
               )

      assert stream_versions(context, "decision-1") == [1]
    end

    test "at any `target` on a prefixed stream is a conditional append", context do
      assert :ok =
               EventStore.append_to_stream("decision-1", 0, events(1),
                 condition: condition(target: :not_declared)
               )

      assert stream_versions(context, "decision-1") == [1]
    end

    test "with a declared `target` writes to members in and out of a prefix", context do
      assert :ok =
               EventStore.append_to_streams(
                 [{"decision-1", 0, events(1)}, {"plain", 0, events(1)}],
                 condition: condition(target: :declared_target)
               )

      assert stream_versions(context, "decision-1") == [1]
      assert stream_versions(context, "plain") == [1]
    end

    test "that fails stays a failed condition, and not a refusal" do
      :ok = EventStore.append_to_stream("source", 0, [event(["contested"])])

      assert EventStore.append_to_stream("decision-1", 0, events(1),
               condition: [items: [[tags: ["contested"]]], after: 0]
             ) == {:error, :append_condition_failed}
    end

    test "that cannot run answers its own refusal, not the plain one" do
      assert EventStore.append_to_stream("decision-1", 0, events(1),
               conn: @no_conn,
               condition: nil
             ) == {:error, :invalid_condition}
    end
  end

  describe "a hard delete" do
    setup do
      start_supervised!(
        {TestEventStore,
         name: :hard_store,
         enable_hard_deletes: true,
         conditional_appends: @declared,
         serializer: PassThroughSerializer}
      )

      :ok
    end

    test "of a declared prefix sends no statement, at every expected version" do
      for stream_uuid <- ["decision-1", "ruling-1"],
          expected_version <- [:any_version, :stream_exists, 0, 3] do
        assert EventStore.delete_stream(stream_uuid, expected_version, :hard,
                 conn: @no_conn,
                 name: :hard_store
               ) == {:error, :hard_delete_of_conditional_prefix},
               inspect({stream_uuid, expected_version})
      end
    end

    test "takes nothing, and a soft delete still goes through", context do
      :ok = decided("decision-1", :hard_store)

      assert EventStore.delete_stream("decision-1", :any_version, :hard, name: :hard_store) ==
               {:error, :hard_delete_of_conditional_prefix}

      assert stream_versions(context, "decision-1") == [1]
      assert all_version(context) == 1

      assert :ok = EventStore.delete_stream("decision-1", :any_version, :soft, name: :hard_store)
      assert stream_versions(context, "decision-1") == [1]
    end

    test "leaves a caller's transaction alive", context do
      :ok = decided("decision-1", :hard_store)

      session = LockSession.start(context.postgrex_config)

      LockSession.run(session, fn tx ->
        assert EventStore.delete_stream("decision-1", :any_version, :hard,
                 conn: tx,
                 name: :hard_store
               ) == {:error, :hard_delete_of_conditional_prefix}

        assert %Postgrex.Result{rows: [[1]]} = Postgrex.query!(tx, "SELECT 1", [])
      end)

      assert {:ok, :ok} = LockSession.commit(session)
      assert stream_versions(context, "decision-1") == [1]
    end

    test "of a stream outside the prefixes goes on, for a name that only resembles one",
         context do
      for stream_uuid <- ["plain", "decision", "no-decision-1"] do
        :ok = EventStore.append_to_stream(stream_uuid, 0, events(1), name: :hard_store)

        assert :ok = EventStore.delete_stream(stream_uuid, :any_version, :hard, name: :hard_store)
        assert stream_id(context, stream_uuid) == nil
      end
    end

    test "of a stream both declare goes through", context do
      start_supervised!(
        {TestEventStore,
         name: :both_hard_store,
         enable_hard_deletes: true,
         conditional_appends: [stream_prefixes: ["decision-"]],
         deletable: [stream_prefixes: ["decision-"], event_types: ["typed"]],
         serializer: PassThroughSerializer}
      )

      :ok = decided("decision-1", :both_hard_store)

      assert :ok =
               EventStore.delete_stream("decision-1", :any_version, :hard, name: :both_hard_store)

      assert stream_id(context, "decision-1") == nil
    end

    test "goes by the stream's own name when the prefixes overlap", context do
      start_supervised!(
        {TestEventStore,
         name: :overlap_store,
         enable_hard_deletes: true,
         conditional_appends: [stream_prefixes: ["decision-"]],
         deletable: [stream_prefixes: ["decision-secret-"], event_types: ["typed"]],
         serializer: PassThroughSerializer}
      )

      :ok = decided("decision-secret-1", :overlap_store)

      assert :ok =
               EventStore.delete_stream("decision-secret-1", :any_version, :hard,
                 name: :overlap_store
               )

      assert stream_id(context, "decision-secret-1") == nil

      assert EventStore.delete_stream("decision-1", :any_version, :hard,
               conn: @no_conn,
               name: :overlap_store
             ) == {:error, :hard_delete_of_conditional_prefix}
    end

    test "on a store without hard deletes keeps that store's answers", context do
      :ok = decided("decision-1", TestEventStore)

      assert EventStore.delete_stream("decision-1", :any_version, :hard) ==
               {:error, :not_supported}

      assert EventStore.delete_stream("decision-2", :stream_exists, :hard) ==
               {:error, :stream_not_found}

      assert stream_versions(context, "decision-1") == [1]
    end
  end

  describe "the store's declaration" do
    test "with no prefix refuses nothing", context do
      start_supervised!(
        {TestEventStore,
         name: :targets_store,
         conditional_appends: [targets: [:declared_target]],
         serializer: PassThroughSerializer}
      )

      assert :ok = EventStore.append_to_stream("decision-1", 0, events(1), name: :targets_store)
      assert stream_versions(context, "decision-1") == [1]
    end

    test "with nothing declared refuses nothing", context do
      start_supervised!(
        {TestEventStore, name: :undeclared_store, serializer: PassThroughSerializer}
      )

      assert :ok =
               EventStore.append_to_stream("decision-1", 0, events(1), name: :undeclared_store)

      assert stream_versions(context, "decision-1") == [1]
    end

    test "is no option of a call" do
      assert EventStore.append_to_stream("decision-1", 0, events(1),
               conn: @no_conn,
               conditional_appends: []
             ) == {:error, :plain_append_to_conditional_prefix}
    end

    test "is read from what `Stream` is given", %{conn: conn, schema: schema} do
      opts = [schema: schema, serializer: PassThroughSerializer]

      assert :ok = Stream.append_to_stream(conn, "decision-1", 0, events(1), opts)

      assert Stream.append_to_stream(
               conn,
               "decision-1",
               1,
               events(1),
               [conditional_appends: [stream_prefixes: ["decision-"]]] ++ opts
             ) == {:error, :plain_append_to_conditional_prefix}
    end
  end

  describe "the declaration, in the config" do
    test "is checked when the config is parsed" do
      for declared <- [
            :all,
            ["decision-"],
            [stream_prefixes: [""]],
            [stream_prefixes: ["decision-", ""]],
            [stream_prefixes: "decision-"],
            [stream_prefixes: [:decision]],
            [stream_prefixes: nil],
            [stream_prefix: ["decision-"]],
            [targets: :mine],
            [stream_prefixes: ["a-"], stream_prefixes: ["b-"]]
          ] do
        assert_raise ArgumentError, ~r/conditional_appends/, fn ->
          Config.parse(conditional_appends: declared)
        end
      end
    end

    test "takes what a store declares" do
      for declared <- [
            [],
            [stream_prefixes: []],
            [stream_prefixes: ["decision-"]],
            [targets: [:mine, MyApp.User, "any term"]],
            [stream_prefixes: ["decision-"], targets: [:mine]]
          ] do
        parsed = Config.parse(conditional_appends: declared)

        assert Keyword.fetch!(parsed, :conditional_appends) == declared
      end

      refute Keyword.has_key?(Config.parse([]), :conditional_appends)
    end
  end

  describe "the order of refusals" do
    test "tags and types answer before the plain refusal" do
      assert EventStore.append_to_stream("decision-1", 0, [event([String.duplicate("a", 513)])],
               conn: @no_conn
             ) == {:error, :tag_too_long}

      assert EventStore.append_to_streams([{"decision-1", 0, [event([<<0>>])]}], conn: @no_conn) ==
               {:error, :invalid_tag}
    end

    test "a bad `returning:` answers before the plain refusal" do
      assert EventStore.append_to_stream("decision-1", 0, events(1),
               conn: @no_conn,
               returning: :position
             ) == {:error, :invalid_returning}
    end
  end

  describe "a caller's transaction" do
    test "survives each refusal, and every one of them writes nothing", context do
      :ok = EventStore.append_to_stream("source", 0, events(1))
      {:ok, linked} = EventStore.read_stream_forward("source")

      session = LockSession.start(context.postgrex_config)

      LockSession.run(session, fn tx ->
        assert EventStore.append_to_stream("decision-1", 0, events(1), conn: tx) ==
                 {:error, :plain_append_to_conditional_prefix}

        assert EventStore.append_to_stream("decision-1", 0, events(1_001), conn: tx) ==
                 {:error, :plain_append_to_conditional_prefix}

        assert EventStore.append_to_streams(
                 [{"plain", 0, events(1)}, {"decision-1", 0, events(1)}],
                 conn: tx
               ) == {:error, :plain_append_to_conditional_prefix}

        assert EventStore.link_to_stream("decision-1", :any_version, linked, conn: tx) ==
                 {:error, :link_into_conditional_prefix}

        assert %Postgrex.Result{rows: [[1]]} = Postgrex.query!(tx, "SELECT 1", [])

        assert {:ok, %AppendResult{position: 2, stream_versions: %{"written" => 1}}} =
                 EventStore.append_to_stream("written", 0, events(1),
                   conn: tx,
                   returning: :append_result
                 )
      end)

      assert {:ok, :ok} = LockSession.commit(session)
      assert stream_versions(context, "written") == [1]
      assert all_version(context) == 2
    end
  end

  defp decided(stream_uuid, store) do
    EventStore.append_to_stream(stream_uuid, 0, events(1), condition: condition(), name: store)
  end

  defp condition(overrides \\ []) do
    Keyword.merge([items: [[tags: ["never"]]], after: 0], overrides)
  end

  defp event(tags) do
    %EventData{event_type: "typed", data: "{}", metadata: "{}", tags: tags}
  end

  defp events(count) do
    for _index <- 1..count, do: event([])
  end
end
