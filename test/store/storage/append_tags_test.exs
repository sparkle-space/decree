defmodule Decree.Store.Storage.AppendTagsTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{AppendError, Config, EventData, EventFactory, LockSession}
  alias Decree.Store.Storage.{Appender, Initializer, Schema}
  alias Decree.Store.StoreRows
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  @all_stream 0

  setup %{postgrex_config: postgrex_config} do
    [observer: LockSession.connect(postgrex_config)]
  end

  describe "a tagged append" do
    test "writes each tag at the event's `$all` position, with the event's type", context do
      :ok = EventStore.append_to_stream("other", 0, EventFactory.create_events(1))

      events = [
        %{event("first") | tags: ["tenant:a", "command:one"]},
        %{event("second") | tags: ["tenant:a"]}
      ]

      assert :ok = EventStore.append_to_stream("stream", 0, events)

      assert tag_rows(context) == [
               [2, "command:one", "first"],
               [2, "tenant:a", "first"],
               [3, "tenant:a", "second"]
             ]

      assert type_rows(context) == [
               [1, "Elixir.Decree.Store.EventFactory.Event"],
               [2, "first"],
               [3, "second"]
             ]
    end

    test "numbers tags across the 1,000 event batches", context do
      :ok = EventStore.append_to_stream("other", 0, EventFactory.create_events(1))

      events =
        1_001
        |> EventFactory.create_events()
        |> List.update_at(0, &%{&1 | event_type: "first", tags: ["tag:first"]})
        |> List.update_at(999, &%{&1 | event_type: "thousandth", tags: ["tag:thousandth"]})
        |> List.update_at(1_000, &%{&1 | event_type: "last", tags: ["tag:last"]})

      assert :ok = EventStore.append_to_stream("stream", 0, events)

      assert tag_rows(context) == [
               [2, "tag:first", "first"],
               [1_001, "tag:thousandth", "thousandth"],
               [1_002, "tag:last", "last"]
             ]

      assert length(type_rows(context)) == 1_002
      assert all_version(context) == 1_002
    end

    test "numbers an appender call's second batch from its own first event", context do
      %{conn: conn, schema: schema} = context

      {:ok, stream_id} = StoreRows.insert_stream(conn, schema, "stream")
      events = EventFactory.create_recorded_events(1_001, "stream")
      tags = List.duplicate([], 1_000) ++ [["tag:last"]]

      assert :ok = Appender.append(conn, stream_id, events, schema: schema, tags: tags)

      assert [[1_001, "tag:last", _event_type]] = tag_rows(context)
    end

    test "stores a tag repeated on one event once", context do
      events = [%{event("typed") | tags: ["tenant:a", "tenant:b", "tenant:a"]}]

      assert :ok = EventStore.append_to_stream("stream", 0, events)

      assert tag_rows(context) == [[1, "tenant:a", "typed"], [1, "tenant:b", "typed"]]
    end

    test "stores the same tag on two events of a batch", context do
      events = [%{event("first") | tags: ["tenant:a"]}, %{event("second") | tags: ["tenant:a"]}]

      assert :ok = EventStore.append_to_stream("stream", 0, events)

      assert tag_rows(context) == [[1, "tenant:a", "first"], [2, "tenant:a", "second"]]
    end
  end

  describe "an event with no tags" do
    test "writes no tag row and one type row", context do
      assert :ok = EventStore.append_to_stream("stream", 0, [event("typed")])

      assert tag_rows(context) == []
      assert type_rows(context) == [[1, "typed"]]
    end

    # The framework's mapping copies each field of its own event data onto the store's, the
    # default `tags: []` included, so it builds this.
    test "is what the framework's mapper builds", context do
      event_data = %EventData{event_type: "typed", data: %{"key" => "value"}, metadata: %{}}

      assert %EventData{tags: []} = event_data
      assert :ok = EventStore.append_to_stream("stream", 0, [event_data])

      assert tag_rows(context) == []
      assert type_rows(context) == [[1, "typed"]]
    end

    test "beside a tagged one takes no ordinal from it", context do
      events = [event("first"), %{event("second") | tags: ["tenant:a"]}, event("third")]

      assert :ok = EventStore.append_to_stream("stream", 0, events)

      assert tag_rows(context) == [[2, "tenant:a", "second"]]
      assert length(type_rows(context)) == 3
    end

    test "is an appender call without the option", context do
      %{conn: conn, schema: schema} = context

      {:ok, stream_id} = StoreRows.insert_stream(conn, schema, "stream")
      events = EventFactory.create_recorded_events(2, "stream")

      assert :ok = Appender.append(conn, stream_id, events, schema: schema)

      assert tag_rows(context) == []
      assert length(type_rows(context)) == 2
    end
  end

  describe "the stamp" do
    test "goes on the `command:` row and no other", context do
      events = [
        %{
          event("typed")
          | tags: ["command:id", "command_kind:Module", "commander:x", "tenant:x"]
        }
      ]

      assert :ok = EventStore.append_to_stream("stream", 0, events)

      assert [
               ["command:id", %DateTime{}],
               ["command_kind:Module", nil],
               ["commander:x", nil],
               ["tenant:x", nil]
             ] = stamp_rows(context)
    end

    test "is shared by the events of one batch", context do
      events = [%{event("first") | tags: ["command:a"]}, %{event("second") | tags: ["command:b"]}]

      assert :ok = EventStore.append_to_stream("stream", 0, events)

      assert [["command:a", stamp], ["command:b", stamp]] = stamp_rows(context)
    end

    test "is monotone in position order across concurrent writers", context do
      %{postgrex_config: postgrex_config, schema: schema} = context

      writers = 8
      appends = 10

      1..writers
      |> Enum.map(fn writer ->
        conn = LockSession.connect(postgrex_config)

        Task.async(fn ->
          for append <- 1..appends do
            events = [
              %{event("typed") | tags: ["command:#{writer}-#{append}-1"]},
              %{event("typed") | tags: ["command:#{writer}-#{append}-2"]}
            ]

            :ok =
              EventStore.append_to_stream("stream-#{writer}", :any_version, events, conn: conn)
          end
        end)
      end)
      |> Task.await_many(30_000)

      rows =
        query!(
          context,
          ~s(SELECT position, stamped_at FROM "#{schema}".event_tags) <>
            " WHERE stamped_at IS NOT NULL ORDER BY position",
          []
        )

      assert length(rows) == writers * appends * 2
      assert Enum.map(rows, &hd/1) == Enum.to_list(1..(writers * appends * 2))

      rows
      |> Enum.map(fn [_position, stamp] -> stamp end)
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.each(fn [earlier, later] ->
        assert DateTime.compare(earlier, later) != :gt
      end)
    end

    test "is read after `$all` is granted, not when the append starts", context do
      %{observer: observer, postgrex_config: postgrex_config, schema: schema} = context

      holder = LockSession.start(postgrex_config)
      LockSession.lock_row(holder, schema, @all_stream)

      appender = LockSession.connect(postgrex_config)
      backend = LockSession.backend_pid(appender)
      events = [%{event("typed") | tags: ["command:id"]}]

      append =
        Task.async(fn -> EventStore.append_to_stream("stream", 0, events, conn: appender) end)

      LockSession.wait_until_blocked(observer, backend, holder.backend)

      %Postgrex.Result{rows: [[reading]]} =
        Postgrex.query!(observer, "SELECT clock_timestamp()", [])

      assert {:ok, :ok} = LockSession.commit(holder)
      assert :ok = Task.await(append)

      assert [["command:id", stamp]] = stamp_rows(context)
      assert DateTime.compare(stamp, reading) == :gt
    end
  end

  describe "a tag over 512 bytes" do
    test "is refused and writes nothing", context do
      events = [%{event("typed") | tags: [String.duplicate("a", 513)]}]

      assert {:error, :tag_too_long} = EventStore.append_to_stream("stream", 0, events)

      assert_nothing_written(context)
      assert {:error, :stream_not_found} = EventStore.read_stream_forward("stream")
    end

    test "answers before a stale expected version", context do
      :ok = EventStore.append_to_stream("stream", 0, EventFactory.create_events(1))

      events = [%{event("typed") | tags: [String.duplicate("a", 513)]}]

      assert {:error, :tag_too_long} = EventStore.append_to_stream("stream", 0, events)

      assert tag_rows(context) == []
      assert all_version(context) == 1
    end

    test "at exactly 512 bytes is stored", context do
      tag = String.duplicate("a", 512)

      assert :ok = EventStore.append_to_stream("stream", 0, [%{event("typed") | tags: [tag]}])

      assert tag_rows(context) == [[1, tag, "typed"]]
    end

    test "is counted in bytes, not characters", context do
      tag = String.duplicate("é", 300)
      assert String.length(tag) < 512

      assert {:error, :tag_too_long} =
               EventStore.append_to_stream("stream", 0, [%{event("typed") | tags: [tag]}])

      assert_nothing_written(context)
    end

    test "on any event refuses the whole append", context do
      events = [
        %{event("first") | tags: ["tenant:a"]},
        %{event("second") | tags: [String.duplicate("a", 513)]}
      ]

      assert {:error, :tag_too_long} = EventStore.append_to_stream("stream", 0, events)

      assert_nothing_written(context)
    end

    test "leaves a caller's transaction usable", context do
      %{postgrex_config: postgrex_config} = context

      session = LockSession.start(postgrex_config)
      long = [%{event("typed") | tags: [String.duplicate("a", 513)]}]
      short = [%{event("typed") | tags: ["tenant:a"]}]

      LockSession.run(session, fn tx ->
        assert {:error, :tag_too_long} = EventStore.append_to_stream("stream", 0, long, conn: tx)
        assert :ok = EventStore.append_to_stream("stream", 0, short, conn: tx)
      end)

      assert {:ok, :ok} = LockSession.commit(session)

      assert tag_rows(context) == [[1, "tenant:a", "typed"]]
    end

    test "in an append of 1,000 or more events writes nothing", context do
      events = long_tag_batch(1_001)

      assert {:error, :tag_too_long} = EventStore.append_to_stream("stream", 0, events)

      assert_nothing_written(context)
    end

    test "in an append of 1,000 or more events leaves a caller's transaction usable", context do
      %{postgrex_config: postgrex_config} = context

      session = LockSession.start(postgrex_config)
      long = long_tag_batch(1_001)
      short = [%{event("typed") | tags: ["tenant:a"]}]

      LockSession.run(session, fn tx ->
        assert {:error, :tag_too_long} = EventStore.append_to_stream("stream", 0, long, conn: tx)
        assert :ok = EventStore.append_to_stream("stream", 0, short, conn: tx)
      end)

      assert {:ok, :ok} = LockSession.commit(session)

      assert tag_rows(context) == [[1, "tenant:a", "typed"]]
    end

    test "gives way to `:invalid_tag` on another event" do
      events = [
        %{event("first") | tags: [String.duplicate("a", 513)]},
        %{event("second") | tags: [<<0>>]}
      ]

      assert {:error, :invalid_tag} = EventStore.append_to_stream("stream", 0, events)
    end

    test "through a direct appender call is refused by the table", context do
      %{conn: conn, schema: schema} = context

      {:ok, stream_id} = StoreRows.insert_stream(conn, schema, "stream")
      events = EventFactory.create_recorded_events(1, "stream")
      tags = [[String.duplicate("a", 513)]]

      assert {:error, %AppendError{class: :rejected, code: :check_violation}} =
               Appender.append(conn, stream_id, events, schema: schema, tags: tags)

      assert_nothing_written(context)
    end
  end

  describe "a tag that is not a string Postgres can hold" do
    # Refused before any statement, so no connection is ever asked for.
    @no_conn :a_connection_that_does_not_exist

    for {name, tag} <- [
          {"a lone NUL byte", quote(do: <<0>>)},
          {"holding a NUL byte", quote(do: <<"a", 0>>)},
          {"not UTF-8", quote(do: <<0xFF>>)},
          {"an atom", :atom},
          {"nil", nil},
          {"a number", 1}
        ] do
      test "is refused and writes nothing: #{name}", context do
        events = [%{event("typed") | tags: [unquote(tag)]}]

        assert {:error, :invalid_tag} =
                 EventStore.append_to_stream("stream", 0, events, conn: @no_conn)

        assert_nothing_written(context)
        assert {:error, :stream_not_found} = EventStore.read_stream_forward("stream")
      end
    end

    test "answers before a stale expected version", context do
      :ok = EventStore.append_to_stream("stream", 0, EventFactory.create_events(1))

      events = [%{event("typed") | tags: [<<0>>]}]

      assert {:error, :invalid_tag} = EventStore.append_to_stream("stream", 0, events)

      assert tag_rows(context) == []
      assert all_version(context) == 1
    end

    test "answers `:invalid_tag`, not `:tag_too_long`, when it is both" do
      events = [%{event("typed") | tags: [String.duplicate(<<0>>, 513)]}]

      assert {:error, :invalid_tag} = EventStore.append_to_stream("stream", 0, events)
    end

    test "on any event refuses the whole append", context do
      events = [
        %{event("first") | tags: ["tenant:a"]},
        %{event("second") | tags: [<<0xFF>>]}
      ]

      assert {:error, :invalid_tag} = EventStore.append_to_stream("stream", 0, events)

      assert_nothing_written(context)
    end

    test "leaves a caller's transaction usable", context do
      %{postgrex_config: postgrex_config} = context

      session = LockSession.start(postgrex_config)
      bad = [%{event("typed") | tags: [<<0>>]}]
      fine = [%{event("typed") | tags: ["tenant:a"]}]

      LockSession.run(session, fn tx ->
        assert {:error, :invalid_tag} = EventStore.append_to_stream("stream", 0, bad, conn: tx)
        assert :ok = EventStore.append_to_stream("stream", 0, fine, conn: tx)
      end)

      assert {:ok, :ok} = LockSession.commit(session)

      assert tag_rows(context) == [[1, "tenant:a", "typed"]]
    end

    test "in an append of 1,000 or more events leaves a caller's transaction usable", context do
      %{postgrex_config: postgrex_config} = context

      session = LockSession.start(postgrex_config)

      bad =
        1_001
        |> EventFactory.create_events()
        |> List.update_at(1_000, &%{&1 | tags: [<<0xFF>>]})

      fine = [%{event("typed") | tags: ["tenant:a"]}]

      LockSession.run(session, fn tx ->
        assert {:error, :invalid_tag} = EventStore.append_to_stream("stream", 0, bad, conn: tx)
        assert :ok = EventStore.append_to_stream("stream", 0, fine, conn: tx)
      end)

      assert {:ok, :ok} = LockSession.commit(session)

      assert tag_rows(context) == [[1, "tenant:a", "typed"]]
    end

    test "through `append_to_streams` is refused before any statement" do
      appends = [{"a", 0, [%{event("typed") | tags: [<<0>>]}]}]

      assert {:error, :invalid_tag} = EventStore.append_to_streams(appends, conn: @no_conn)
    end
  end

  describe "the tag predicate" do
    test "takes any valid UTF-8 up to and past the limit" do
      assert EventData.valid_tag?("tenant:a")
      assert EventData.valid_tag?("")
      assert EventData.valid_tag?("é")
      assert EventData.valid_tag?(String.duplicate("a", 513))
    end

    test "refuses what Postgres cannot hold, and anything that is not a binary" do
      refute EventData.valid_tag?(<<0>>)
      refute EventData.valid_tag?(<<"a", 0, "b">>)
      refute EventData.valid_tag?(<<0xFF>>)
      refute EventData.valid_tag?(nil)
      refute EventData.valid_tag?(:atom)
      refute EventData.valid_tag?(1)
      refute EventData.valid_tag?(~c"tag")
    end
  end

  # `Decree.Store.Streams.Stream` removes repeats because this is the answer it would get.
  test "a tag repeated on one event through a direct appender call reads as stale", context do
    %{conn: conn, schema: schema} = context

    {:ok, stream_id} = StoreRows.insert_stream(conn, schema, "stream")
    events = EventFactory.create_recorded_events(1, "stream")

    assert {:error, :wrong_expected_version} =
             Appender.append(conn, stream_id, events, schema: schema, tags: [["a", "a"]])

    assert_nothing_written(context)
  end

  describe "an append that does not stand writes no tag or type row" do
    test "a stale one", context do
      %{conn: conn, schema: schema} = context

      {:ok, stream_id} = StoreRows.insert_stream(conn, schema, "stream")
      stale = EventFactory.create_recorded_events(1, "stream", 1, 5)

      assert {:error, :wrong_expected_version} =
               Appender.append(conn, stream_id, stale, schema: schema, tags: [["tenant:a"]])

      assert_nothing_written(context)
    end

    test "one that repeats an event id", context do
      %{conn: conn, schema: schema} = context

      {:ok, stream_id} = StoreRows.insert_stream(conn, schema, "stream")
      [event] = EventFactory.create_recorded_events(1, "stream")

      :ok = Appender.append(conn, stream_id, [event], schema: schema)

      assert {:error, :duplicate_event} =
               Appender.append(conn, stream_id, [%{event | stream_version: 2}],
                 schema: schema,
                 tags: [["tenant:a"]]
               )

      assert tag_rows(context) == []
      assert length(type_rows(context)) == 1
    end

    test "one in a transaction that rolls back", context do
      %{postgrex_config: postgrex_config} = context

      session = LockSession.start(postgrex_config)
      events = [%{event("typed") | tags: ["tenant:a"]}]

      LockSession.run(session, fn tx ->
        assert :ok = EventStore.append_to_stream("stream", 0, events, conn: tx)
      end)

      assert {:error, :rolled_back} = LockSession.rollback(session)

      assert_nothing_written(context)
    end
  end

  describe "a reset" do
    test "empties both tables, so that position 1 can be written again", context do
      %{conn: conn, config: config} = context

      events = [%{event("typed") | tags: ["tenant:a"]}]

      :ok = EventStore.append_to_stream("stream", 0, events)

      Initializer.reset!(conn, config)

      assert_nothing_written(context)

      assert :ok = EventStore.append_to_stream("stream", 0, events)

      assert tag_rows(context) == [[1, "tenant:a", "typed"]]
      assert type_rows(context) == [[1, "typed"]]
    end
  end

  describe "a hard delete" do
    test "takes the deleted events' tag and type rows with them", context do
      %{postgrex_config: postgrex_config, schema: schema} = context

      events = [%{event("typed") | tags: ["tenant:a"]}]
      deleter = LockSession.connect(postgrex_config)

      :ok = EventStore.append_to_stream("deleted-stream", 0, events)

      assert :ok =
               Stream.delete(deleter, "deleted-stream", :any_version, :hard,
                 schema: schema,
                 enable_hard_deletes: true
               )

      assert {:error, :stream_not_found} = EventStore.read_stream_forward("deleted-stream")
      assert tag_rows(context) == []
      assert type_rows(context) == []

      # `$all` never goes back, so the next event takes a position of its own.
      assert :ok = EventStore.append_to_stream("other-stream", 0, events)

      assert tag_rows(context) == [[2, "tenant:a", "typed"]]
      assert type_rows(context) == [[2, "typed"]]
    end
  end

  describe "a named schema" do
    test "takes the rows, and the default schema none", context do
      schema = unique_schema("tagged")
      init_schema(context, schema)

      start_supervised!({TestEventStore, name: :tagged_store, schema: schema})

      events = [%{event("typed") | tags: ["tenant:a"]}]

      assert :ok = EventStore.append_to_stream("stream", 0, events, name: :tagged_store)

      assert tag_rows(context, schema) == [[1, "tenant:a", "typed"]]
      assert type_rows(context, schema) == [[1, "typed"]]

      assert_nothing_written(context)
    end

    test "is found past a session's temporary tables of the same names", context do
      %{postgrex_config: postgrex_config} = context

      schema = unique_schema("configured")
      init_schema(context, schema)

      conn = LockSession.connect(postgrex_config)
      {:ok, stream_id} = StoreRows.insert_stream(conn, schema, "stream")

      for table <- ["event_tags", "event_types"] do
        Postgrex.query!(conn, ~s[CREATE TEMP TABLE #{table} (LIKE "#{schema}".#{table})], [])
      end

      events = EventFactory.create_recorded_events(1, "stream")

      assert :ok =
               Appender.append(conn, stream_id, events, schema: schema, tags: [["tenant:a"]])

      assert [[1, "tenant:a", _event_type]] = tag_rows(context, schema)
      assert [[1, _event_type]] = type_rows(context, schema)

      for table <- ["event_tags", "event_types"] do
        assert %Postgrex.Result{rows: [[0]]} =
                 Postgrex.query!(conn, "SELECT count(*) FROM pg_temp.#{table}", [])
      end
    end
  end

  defp event(event_type) do
    %EventData{event_type: event_type, data: %{"key" => "value"}, metadata: %{}}
  end

  defp long_tag_batch(count) do
    count
    |> EventFactory.create_events()
    |> List.update_at(count - 1, &%{&1 | tags: [String.duplicate("a", 513)]})
  end

  defp assert_nothing_written(%{schema: schema} = context) do
    assert tag_rows(context) == []
    assert type_rows(context) == []
    assert all_version(context) == 0

    assert [[0]] =
             query!(context, ~s[SELECT count(*) FROM "#{schema}".events], [])
  end

  defp tag_rows(%{schema: schema} = context), do: tag_rows(context, schema)

  defp tag_rows(context, schema) do
    query!(
      context,
      ~s(SELECT position, tag, event_type FROM "#{schema}".event_tags ORDER BY position, tag),
      []
    )
  end

  defp type_rows(%{schema: schema} = context), do: type_rows(context, schema)

  defp type_rows(context, schema) do
    query!(
      context,
      ~s(SELECT position, event_type FROM "#{schema}".event_types ORDER BY position),
      []
    )
  end

  defp stamp_rows(%{schema: schema} = context) do
    query!(context, ~s(SELECT tag, stamped_at FROM "#{schema}".event_tags ORDER BY tag), [])
  end

  defp all_version(%{schema: schema} = context) do
    [[version]] =
      query!(context, ~s(SELECT stream_version FROM "#{schema}".streams WHERE stream_id = 0), [])

    version
  end

  defp query!(context, sql, params) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(context.observer, sql, params)

    rows
  end

  defp unique_schema(prefix) do
    "#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp init_schema(context, schema) do
    %{config: config} = context

    config = Keyword.put(config, :schema, schema)

    :ok = Schema.create(config)
    on_exit(fn -> Schema.drop(config) end)

    conn = config |> Config.default_postgrex_opts() |> LockSession.connect()

    {:ok, _} = Initializer.run!(conn, config)
    :ok = GenServer.stop(conn)
  end
end
