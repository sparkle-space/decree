defmodule Decree.Store.Storage.DeletableStreamsTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{Config, EventData, LockSession, ReadResult, UUID}
  alias TestEventStore, as: EventStore

  defmodule Captured do
    defstruct [:id]
  end

  defmodule Forgotten do
    defstruct [:id]
  end

  defmodule Serializer do
    @behaviour Decree.Store.Serializer

    def serialize(%_struct{}), do: "{}"
    def serialize(term), do: term
    def deserialize(term, _config), do: term
  end

  @type_name "xpost-captured"
  @kind "content_ref"
  @declared [
    stream_prefixes: ["XPost-"],
    event_types: [@type_name, "Elixir.Decree.Store.Storage.DeletableStreamsTest.Captured"],
    tag_kinds: [@kind]
  ]
  @all_stream 0
  # A connection that does not exist: any statement, a BEGIN included, would exit.
  @no_conn :a_connection_that_does_not_exist

  setup %{postgrex_config: postgrex_config} do
    stop_supervised!(TestEventStore)

    start_supervised!(
      {TestEventStore,
       deletable: @declared,
       enable_hard_deletes: true,
       conditional_appends: [stream_prefixes: ["decision-"]],
       serializer: Serializer}
    )

    [observer: LockSession.connect(postgrex_config)]
  end

  # Every shape of a name, and each of the mixed items that a `command:` tag does not save.
  @refused [
    {"a declared type", [[types: [@type_name]]]},
    {"a declared type among others", [[types: ["other", @type_name]]]},
    {"a declared kind as the first tag", [[tags: ["content_ref:1"]]]},
    {"a declared kind as a rest tag", [[tags: ["user:1", "content_ref:1"]]]},
    {"a declared kind with no value", [[tags: ["content_ref"]]]},
    {"a value holding a colon", [[tags: ["content_ref:1:2"]]]},
    {"a declared type beside a `command:` tag", [[types: [@type_name], tags: ["command:abc"]]]},
    {"a declared kind beside a `command:` tag", [[tags: ["command:abc", "content_ref:1"]]]},
    {"one item of two", [[tags: ["user:1"]], [types: [@type_name]]]}
  ]

  describe "the refusal, before any statement" do
    for {name, items} <- @refused do
      @items items

      test "in `read_query`, for #{name}" do
        assert EventStore.read_query(@items, conn: @no_conn) == {:error, :query_names_deletable}
      end

      test "in `stream_query`, for #{name}" do
        assert EventStore.stream_query(@items, conn: @no_conn) ==
                 {:error, :query_names_deletable}
      end

      test "in a condition, for #{name}" do
        assert condition("decision-x", @items, conn: @no_conn) ==
                 {:error, :query_names_deletable}
      end
    end

    test "in a multi-stream condition" do
      assert EventStore.append_to_streams(
               [{"decision-x", 0, [event("written", [])]}],
               conn: @no_conn,
               condition: [items: [[types: [@type_name]]], after: 0]
             ) == {:error, :query_names_deletable}
    end

    test "in a condition of 1,000 events or more" do
      assert EventStore.append_to_stream("decision-x", 0, events(1_001, "written"),
               conn: @no_conn,
               condition: [items: [[types: [@type_name]]], after: 0]
             ) == {:error, :query_names_deletable}
    end

    test "ahead of an undeclared target, which an allowed item still answers" do
      assert condition("undeclared", [[types: [@type_name]]], conn: @no_conn) ==
               {:error, :query_names_deletable}

      assert condition("undeclared", [[types: ["other"]]], conn: @no_conn) ==
               {:error, :undeclared_conditional_target}
    end

    test "with a caller's transaction still usable", %{conn: conn} do
      assert {:ok, :usable} =
               Postgrex.transaction(conn, fn tx ->
                 items = [[tags: ["content_ref:1"]]]

                 assert {:error, :query_names_deletable} = EventStore.read_query(items, conn: tx)

                 assert {:error, :query_names_deletable} =
                          EventStore.stream_query(items, conn: tx)

                 assert {:error, :query_names_deletable} =
                          condition("decision-x", items, conn: tx)

                 assert %Postgrex.Result{rows: [[1]]} = Postgrex.query!(tx, "SELECT 1", [])

                 :usable
               end)
    end
  end

  describe "the `command:` item passes" do
    @items [[tags: ["command:abc"]]]

    test "in `read_query`" do
      assert {:ok, %ReadResult{events: []}} = EventStore.read_query(@items)
    end

    test "in `stream_query`" do
      assert EventStore.stream_query(@items) |> Enum.to_list() == []
    end

    test "in a condition" do
      assert :ok = condition("decision-x", @items)
    end
  end

  describe "a query naming nothing declared" do
    test "passes, for a tag of a kind that only starts like a declared one" do
      items = [[tags: ["content_reference:1"]], [types: ["xpost"]]]

      assert {:ok, %ReadResult{events: []}} = EventStore.read_query(items)
      assert EventStore.stream_query(items) |> Enum.to_list() == []
      assert :ok = condition("decision-x", items)
    end

    test "passes on a store that declares nothing" do
      start_supervised!(
        {TestEventStore,
         name: :undeclared_store,
         conditional_appends: [stream_prefixes: ["decision-"]],
         serializer: Serializer}
      )

      items = [[types: [@type_name], tags: ["content_ref:1"]]]

      assert {:ok, %ReadResult{events: []}} =
               EventStore.read_query(items, name: :undeclared_store)

      assert EventStore.stream_query(items, name: :undeclared_store) |> Enum.to_list() == []
      assert :ok = condition("decision-x", items, name: :undeclared_store)
    end
  end

  describe "the declaration" do
    test "a caller cannot change it" do
      items = [[types: [@type_name]]]

      assert EventStore.read_query(items, deletable: []) == {:error, :query_names_deletable}
      assert EventStore.stream_query(items, deletable: []) == {:error, :query_names_deletable}

      assert condition("decision-x", items, deletable: []) ==
               {:error, :query_names_deletable}

      assert EventStore.append_to_stream("XPost-1", 0, [event("forgotten", [])], deletable: []) ==
               {:error, :undeclared_deletable_event_type}
    end

    test "round-trips through the parser" do
      parsed = Config.parse(deletable: @declared)

      assert Keyword.fetch!(parsed, :deletable) == @declared
    end

    test "takes each key alone, and none" do
      for declared <- [
            [stream_prefixes: ["XPost-"]],
            [event_types: ["typed"]],
            [tag_kinds: ["kind"]],
            []
          ] do
        assert declared |> then(&Config.parse(deletable: &1)) |> Keyword.fetch!(:deletable) ==
                 declared
      end
    end

    test "absent is absent" do
      refute Keyword.has_key?(Config.parse([]), :deletable)
    end

    test "raises on every malformed shape" do
      for declared <- [
            nil,
            "XPost-",
            :stream_prefixes,
            %{stream_prefixes: ["XPost-"]},
            ["XPost-"],
            [stream_prefix: ["XPost-"]],
            [targets: ["XPost-"]],
            [stream_prefixes: "XPost-"],
            [stream_prefixes: [""]],
            [stream_prefixes: [:xpost]],
            [stream_prefixes: ["XPost-" | "other"]],
            [stream_prefixes: ["a"], stream_prefixes: ["b"]],
            [event_types: [Captured]],
            [event_types: [""]],
            [event_types: nil],
            [tag_kinds: ["a:b"]],
            [tag_kinds: [""]],
            [tag_kinds: [:kind]],
            [tag_kinds: ["command"]],
            [tag_kinds: ["kind", "command"]],
            {:system, "DECREE_DELETABLE"}
          ] do
        assert_raise ArgumentError, fn -> Config.parse(deletable: declared) end
      end
    end

    test "a store will not start on a malformed one" do
      assert {:error, _reason} =
               start_supervised(
                 {TestEventStore, name: :malformed_store, deletable: [tag_kinds: ["command"]]}
               )
    end
  end

  describe "an append to a declared prefix" do
    test "of an undeclared type is refused before any statement, in every path" do
      for count <- [1, 1_000, 1_001] do
        events = events(count - 1, @type_name) ++ [event("forgotten", [])]

        assert EventStore.append_to_stream("XPost-1", 0, events, conn: @no_conn) ==
                 {:error, :undeclared_deletable_event_type},
               inspect(count)
      end

      assert EventStore.append_to_streams(
               [
                 {"plain", 0, [event("forgotten", [])]},
                 {"XPost-1", 0, [event(@type_name, []), event("forgotten", [])]}
               ],
               conn: @no_conn
             ) == {:error, :undeclared_deletable_event_type}
    end

    test "takes a struct's type from its module" do
      forgotten = %EventData{data: %Forgotten{id: 1}, metadata: "{}"}

      assert EventStore.append_to_stream("XPost-1", 0, [forgotten], conn: @no_conn) ==
               {:error, :undeclared_deletable_event_type}

      assert EventStore.append_to_streams([{"XPost-1", 0, [forgotten]}], conn: @no_conn) ==
               {:error, :undeclared_deletable_event_type}

      captured = %EventData{data: %Captured{id: 1}, metadata: "{}"}

      assert :ok = EventStore.append_to_stream("XPost-1", 0, [captured])
    end

    test "answers after a tag that is too long" do
      events = [event("forgotten", [String.duplicate("a", 513)])]

      assert EventStore.append_to_stream("XPost-1", 0, events, conn: @no_conn) ==
               {:error, :tag_too_long}
    end

    test "of a declared type passes, at either size" do
      assert :ok = EventStore.append_to_stream("XPost-1", 0, [event(@type_name, [])])
      assert :ok = EventStore.append_to_stream("XPost-2", 0, events(1_001, @type_name))

      assert :ok =
               EventStore.append_to_streams([{"XPost-3", 0, [event(@type_name, [])]}])
    end

    test "to a stream outside the prefixes passes, whatever its type" do
      assert :ok = EventStore.append_to_stream("Post-1", 0, [event("forgotten", [])])
      assert :ok = EventStore.append_to_streams([{"Post-2", 0, [event("forgotten", [])]}])
    end

    test "does not make a hard delete refuse a stream outside the prefixes" do
      :ok = EventStore.append_to_stream("plain", 0, [event("forgotten", [])])

      assert :ok = EventStore.delete_stream("plain", :any_version, :hard)
    end
  end

  # A hard delete of a stream holding links removes those events from every stream, so a link
  # into a deletable stream would hand it an event of another stream.
  describe "a link into a declared prefix" do
    test "is refused before any statement, at every expected version" do
      for expected_version <- [:any_version, :no_stream, :stream_exists, 0, 7] do
        for event_ids <- [[UUID.uuid4()], []] do
          assert EventStore.link_to_stream("XPost-1", expected_version, event_ids, conn: @no_conn) ==
                   {:error, :link_into_deletable_stream}
        end
      end
    end

    test "of a declared type is refused too" do
      :ok = EventStore.append_to_stream("XPost-1", 0, [event(@type_name, [])])

      {:ok, linked} = EventStore.read_stream_forward("XPost-1")

      assert EventStore.link_to_stream("XPost-2", :any_version, linked) ==
               {:error, :link_into_deletable_stream}

      assert {:error, :stream_not_found} = EventStore.read_stream_forward("XPost-2")
    end

    test "cannot be let through by a caller" do
      assert EventStore.link_to_stream("XPost-1", :any_version, [UUID.uuid4()],
               conn: @no_conn,
               deletable: []
             ) == {:error, :link_into_deletable_stream}
    end

    test "leaves a caller's transaction usable", %{conn: conn} do
      assert {:ok, :usable} =
               Postgrex.transaction(conn, fn tx ->
                 assert {:error, :link_into_deletable_stream} =
                          EventStore.link_to_stream("XPost-1", :any_version, [UUID.uuid4()],
                            conn: tx
                          )

                 assert %Postgrex.Result{rows: [[1]]} = Postgrex.query!(tx, "SELECT 1", [])

                 :usable
               end)
    end

    test "out of one passes, into a stream outside the prefixes" do
      :ok = EventStore.append_to_stream("XPost-1", 0, [event(@type_name, [])])

      {:ok, linked} = EventStore.read_stream_forward("XPost-1")

      assert :ok = EventStore.link_to_stream("Post-1", :any_version, linked)
    end

    test "passes for a name that only resembles a prefix" do
      :ok = EventStore.append_to_stream("plain", 0, [event("forgotten", [])])

      {:ok, linked} = EventStore.read_stream_forward("plain")

      for stream_uuid <- ["XPost", "xpost-1", "Post-XPost-1"] do
        assert :ok = EventStore.link_to_stream(stream_uuid, :any_version, linked)
      end
    end

    test "passes on a store that declares nothing, or no prefix" do
      :ok = EventStore.append_to_stream("plain", 0, [event("forgotten", [])])

      {:ok, linked} = EventStore.read_stream_forward("plain")

      start_supervised!({TestEventStore, name: :undeclared_store, serializer: Serializer})

      start_supervised!(
        {TestEventStore,
         name: :no_prefix_store, deletable: [event_types: [@type_name]], serializer: Serializer}
      )

      assert :ok =
               EventStore.link_to_stream("XPost-1", :any_version, linked, name: :undeclared_store)

      assert :ok =
               EventStore.link_to_stream("XPost-2", :any_version, linked, name: :no_prefix_store)
    end
  end

  # A hard delete takes no `$all`, so a condition can run across it. The refusal is what
  # makes that harmless: the one thing a condition can still name is a `command:` item.
  describe "a hard delete across a condition" do
    test "takes no `$all`", context do
      :ok = EventStore.append_to_stream("XPost-1", 0, [event(@type_name, ["content_ref:1"])])

      holder = LockSession.start(context.postgrex_config)
      LockSession.lock_row(holder, context.schema, @all_stream)

      assert :ok = EventStore.delete_stream("XPost-1", :any_version, :hard, timeout: 2_000)

      assert {:ok, :ok} = LockSession.commit(holder)
    end

    test "no condition can name the declared type on either side of it" do
      :ok = EventStore.append_to_stream("XPost-1", 0, [event(@type_name, [])])

      assert condition("decision-x", [[types: [@type_name]]]) == {:error, :query_names_deletable}

      :ok = EventStore.delete_stream("XPost-1", :any_version, :hard)

      assert condition("decision-x", [[types: [@type_name]]]) == {:error, :query_names_deletable}
    end

    test "a `command:` item sees it: deduplication ends early, and a read agrees" do
      items = [[tags: ["command:abc"]]]

      :ok = EventStore.append_to_stream("XPost-1", 0, [event(@type_name, ["command:abc"])])

      assert {:ok, %ReadResult{events: [_event]}} = EventStore.read_query(items)
      assert condition("decision-x", items) == {:error, :append_condition_failed}

      :ok = EventStore.delete_stream("XPost-1", :any_version, :hard)

      assert {:ok, %ReadResult{events: []}} = EventStore.read_query(items)
      assert :ok = condition("decision-x", items)
    end
  end

  defp condition(stream_uuid, items, opts \\ []) do
    opts = Keyword.put(opts, :condition, items: items, after: 0)

    EventStore.append_to_stream(stream_uuid, 0, [event("written", [])], opts)
  end

  defp events(0, _event_type), do: []

  defp events(count, event_type) do
    for _index <- 1..count, do: event(event_type, [])
  end

  defp event(event_type, tags) do
    %EventData{event_type: event_type, data: "{}", metadata: "{}", tags: tags}
  end
end
