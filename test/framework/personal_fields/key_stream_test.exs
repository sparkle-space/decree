defmodule Decree.PersonalFields.KeyStreamTest do
  use Decree.PersonalFields.Case

  import ExUnit.CaptureLog

  alias Decree.Aggregates.Aggregate
  alias Decree.Event.{Mapper, PersonalFields, TagContext, TagCoverage, Tags}
  alias Decree.FrameworkEventStore, as: Store
  alias Decree.FrameworkStorage

  alias Decree.PersonalFields.Fixtures.{
    Account,
    AdmissionErased,
    AdmissionGranted,
    CatchAllHandler,
    DeclaredStore,
    FailingAdapter,
    KeyStreams,
    LedgerApp,
    LedgerKeys,
    RawSerializer,
    ShortTypes
  }

  alias Decree.PersonalFields.Fixtures.Commands.Emit
  alias Decree.PersonalFields.Fixtures.Events.{PassedThrough, Plain, Registered}
  alias Decree.PersonalFields.KeyStream
  alias Decree.PersonalFields.KeyStream.Head
  alias Decree.PersonalFields.{SubjectDataKeyMinted, SubjectDataKeyRewrapped}
  alias Decree.Store.{AppendError, AppendResult, Config, EventData, LockSession, TcpProxy}

  @app LedgerApp
  @store LedgerApp.FrameworkEventStore
  @plain_store :key_stream_plain_store

  @prefix KeyStreams.prefix()
  @tag_v1 KeyStreams.master_tag()
  @tag_v2 "master-v2"

  # Not text, and recognisable in a log. The first wrap is bytes JSON could not hold.
  @marker "PF2-WRAPPED-MARKER"
  @wrap <<0xFF, 0xFE, 0x00, @marker::binary, 0x80>>
  @second_wrap <<0xC0, 0x01, @marker::binary, "-TWO">>

  @telemetry for event <- [:start, :stop, :exception],
                 do: [:decree, :event_store, :append_to_stream, event]

  @head_spans for event <- [:start, :stop, :exception],
                  do: [:decree, :event_store, :read_stream_head, event]

  setup %{subject: subject} do
    start_supervised!(LedgerApp)

    key_stream = @prefix <> subject

    on_exit(fn ->
      FailingAdapter.unflag(key_stream)
      FailingAdapter.unflag(KeyStreams.admission_prefix() <> subject)
      FailingAdapter.unflag_append(key_stream)
      FailingAdapter.stop_pretending()
    end)

    [key_stream: key_stream]
  end

  defp mint!(subject, wrap \\ @wrap) do
    {:ok, key_id} = KeyStream.mint(@app, @prefix, subject, wrap, @tag_v1)

    key_id
  end

  defp head!(subject) do
    {:ok, %Head{} = head} = KeyStream.head(@app, @prefix, subject)

    head
  end

  defp hard_delete!(stream) do
    :ok = Store.delete_stream(stream, :any_version, :hard, name: @store)
  end

  defp minted(subject, key_id, wrap) do
    %SubjectDataKeyMinted{
      subject_id: subject,
      key_id: key_id,
      wrapped_key: Base.encode64(wrap),
      master_tag: @tag_v1,
      minted_at: "2026-11-01T00:00:00Z"
    }
  end

  defp minted_data(subject, key_id, wrap) do
    Mapper.map_to_event_data(minted(subject, key_id, wrap))
  end

  defp postgrex_config, do: Config.default_postgrex_opts(FrameworkStorage.config())

  defp scalar(conn, sql, params \\ []) do
    %{rows: [[value]]} = Postgrex.query!(conn, sql, params)

    value
  end

  defp event_count(conn), do: scalar(conn, "SELECT count(*) FROM events")

  defp stream_rows(conn, stream) do
    scalar(conn, "SELECT count(*) FROM streams WHERE stream_uuid = $1", [stream])
  end

  # Every session of this database that is waiting, with the sessions it is waiting on.
  defp blocked(observer) do
    %{rows: rows} =
      Postgrex.query!(
        observer,
        """
        SELECT pid, pg_blocking_pids(pid)
        FROM pg_stat_activity
        WHERE datname = current_database() AND cardinality(pg_blocking_pids(pid)) > 0
        """,
        []
      )

    Map.new(rows, fn [pid, blockers] -> {pid, blockers} end)
  end

  defp start_plain_store do
    start_supervised!({Store, name: @plain_store, serializer: RawSerializer})
  end

  defp plain_append!(stream, event_type, data) do
    event = %EventData{event_type: event_type, data: data, metadata: %{}}

    :ok = Store.append_to_stream(stream, :any_version, [event], name: @plain_store)
  end

  # `inspect/1` prints bytes that are not text as integers, so the marker alone proves nothing.
  defp refute_leak(text) when is_binary(text) do
    refute text =~ @marker
    refute text =~ Base.encode64(@wrap)
    refute text =~ Base.encode64(@second_wrap)
    refute text =~ printed(@wrap)
    refute text =~ printed(@second_wrap)
  end

  defp printed(wrap),
    do: wrap |> binary_part(0, 8) |> inspect() |> String.trim("<<") |> String.trim(">>")

  defp refute_leak_in_term(term) do
    binary = :erlang.term_to_binary(term)

    targets = [
      @marker,
      @wrap,
      @second_wrap,
      Base.encode64(@wrap),
      Base.encode64(@second_wrap)
    ]

    assert :binary.match(binary, targets) == :nomatch
  end

  describe "a minted key" do
    test "reads back from the head", %{subject: subject, key_stream: key_stream} do
      assert {:ok, key_id} = KeyStream.mint(@app, @prefix, subject, @wrap, @tag_v1)
      assert UUID.canonical?(key_id)

      assert {:ok,
              %Head{
                stream_uuid: ^key_stream,
                version: 1,
                subject_id: ^subject,
                key_id: ^key_id,
                wrapped_key: @wrap,
                master_tag: @tag_v1
              }} = KeyStream.head(@app, @prefix, subject)
    end

    test "is stored as base64, in the declared fields", %{conn: conn, subject: subject} do
      key_id = mint!(subject)

      data = scalar(conn, "SELECT data FROM events")

      assert %{
               "subject_id" => ^subject,
               "key_id" => ^key_id,
               "master_tag" => @tag_v1,
               "minted_at" => minted_at
             } = stored = Jason.decode!(data)

      assert stored["wrapped_key"] == Base.encode64(@wrap)
      assert {:ok, _at, 0} = DateTime.from_iso8601(minted_at)
      assert map_size(stored) == 5
      refute data =~ @marker
    end

    test "is stored under the types `event_types/0` says", %{conn: conn, subject: subject} do
      mint!(subject)
      :ok = KeyStream.rewrap(@app, head!(subject), @second_wrap, @tag_v2)

      %{rows: rows} =
        Postgrex.query!(conn, "SELECT event_type FROM events ORDER BY created_at", [])

      assert List.flatten(rows) == KeyStream.event_types()
    end

    test "answers each read from the store", %{conn: conn, subject: subject, key_stream: stream} do
      mint!(subject)

      before = Process.get()

      head!(subject)

      assert Process.get() == before

      hard_delete!(stream)

      assert KeyStream.head(@app, @prefix, subject) == {:error, :stream_not_found}
      assert event_count(conn) == 0
    end

    test "has no head while its stream does not exist", %{subject: subject} do
      assert KeyStream.head(@app, @prefix, subject) == {:error, :stream_not_found}
    end

    test "a soft-deleted stream answers `:stream_deleted`, and takes no write", context do
      %{subject: subject, key_stream: stream} = context

      mint!(subject)
      head = head!(subject)

      :ok = Store.delete_stream(stream, :any_version, :soft, name: @store)

      assert KeyStream.head(@app, @prefix, subject) == {:error, :stream_deleted}

      assert KeyStream.mint(@app, @prefix, subject, @wrap, @tag_v1) ==
               {:error, :stream_deleted}

      assert KeyStream.rewrap(@app, head, @second_wrap, @tag_v2) == {:error, :stream_deleted}
    end
  end

  describe "no tag" do
    test "lands on a minted or a rewrapped key", %{conn: conn, subject: subject} do
      mint!(subject)
      :ok = KeyStream.rewrap(@app, head!(subject), @second_wrap, @tag_v2)

      %{rows: positions} =
        Postgrex.query!(
          conn,
          """
          SELECT se.stream_version
          FROM stream_events se
          INNER JOIN events e ON e.event_id = se.event_id
          WHERE se.stream_id = 0 AND e.event_type = ANY($1)
          """,
          [KeyStream.event_types()]
        )

      positions = List.flatten(positions)

      assert length(positions) == 2

      assert scalar(
               conn,
               "SELECT count(*) FROM event_tags WHERE position = ANY($1)",
               [positions]
             ) == 0
    end

    test "comes from the protocol" do
      context = %TagContext{}

      assert Tags.tags(%SubjectDataKeyMinted{}, context) == {:ok, []}
      assert Tags.tags(%SubjectDataKeyRewrapped{}, context) == {:ok, []}

      assert TagCoverage.verify!([SubjectDataKeyMinted, SubjectDataKeyRewrapped]) == :ok
    end
  end

  describe "the declaration" do
    setup %{subject: subject} do
      mint!(subject)

      [event_id: scalar_event_id(subject)]
    end

    defp scalar_event_id(subject) do
      {:ok, event} = EventStore.read_stream_head(@app, @prefix <> subject)

      event.event_id
    end

    test "refuses a query naming a key event's type" do
      for type <- KeyStream.event_types() do
        assert Store.read_query([[types: [type]]], name: @store) ==
                 {:error, :query_names_deletable}
      end
    end

    test "refuses a link into a key stream", %{event_id: event_id} do
      other = @prefix <> UUID.uuid4()

      assert Store.link_to_stream(other, :any_version, [event_id], name: @store) ==
               {:error, :link_into_deletable_stream}
    end

    test "refuses an undeclared event type on a key stream", context do
      %{conn: conn, key_stream: stream} = context

      event = Mapper.map_to_event_data(%Plain{note: "stray"})

      assert EventStore.append_to_stream(@app, stream, 1, [event]) ==
               {:error, :undeclared_deletable_event_type}

      assert event_count(conn) == 1
    end
  end

  describe "a write refuses, with nothing written," do
    test "a prefix the store does not declare", %{conn: conn, subject: subject} do
      assert KeyStream.mint(@app, "Elsewhere-", subject, @wrap, @tag_v1) ==
               {:error, :undeclared_key_stream}

      assert KeyStream.mint(@app, "SubjectKey-Other-", subject, @wrap, @tag_v1) ==
               {:error, :undeclared_key_stream}

      assert KeyStream.mint(@app, "", subject, @wrap, @tag_v1) ==
               {:error, :undeclared_key_stream}

      assert KeyStream.mint(@app, :not_a_prefix, subject, @wrap, @tag_v1) ==
               {:error, :undeclared_key_stream}

      assert event_count(conn) == 0
    end

    test "a rewrap of a stream under an undeclared prefix", %{conn: conn, subject: subject} do
      start_plain_store()

      stream = "Elsewhere-" <> subject
      [type | _types] = KeyStream.event_types()

      plain_append!(stream, type, minted(subject, UUID.uuid4(), @wrap))

      head = %Head{
        stream_uuid: stream,
        version: 1,
        subject_id: subject,
        key_id: UUID.uuid4(),
        wrapped_key: @wrap,
        master_tag: @tag_v1
      }

      assert KeyStream.rewrap(@app, head, @second_wrap, @tag_v2) ==
               {:error, :undeclared_key_stream}

      assert event_count(conn) == 1
    end

    test "a subject that is not a canonical UUID", %{conn: conn, subject: subject} do
      for bad <- [String.upcase(subject), "not-a-uuid", "", nil, :subject, 1] do
        assert KeyStream.mint(@app, @prefix, bad, @wrap, @tag_v1) == {:error, :invalid_subject}
        assert KeyStream.head(@app, @prefix, bad) == {:error, :invalid_subject}
      end

      assert event_count(conn) == 0
    end

    test "a rewrap whose head names no canonical subject", %{conn: conn, subject: subject} do
      mint!(subject)
      head = head!(subject)

      for bad <- [String.upcase(subject), "not-a-uuid", nil] do
        assert KeyStream.rewrap(@app, %Head{head | subject_id: bad}, @second_wrap, @tag_v2) ==
                 {:error, :invalid_subject}
      end

      assert event_count(conn) == 1
    end

    test "a rewrap whose head's stream and subject disagree", %{conn: conn, subject: subject} do
      mint!(subject)
      head = head!(subject)

      other = %Head{head | subject_id: UUID.uuid4()}

      assert KeyStream.rewrap(@app, other, @second_wrap, @tag_v2) == {:error, :invalid_subject}

      no_stream = %Head{head | stream_uuid: nil}

      assert KeyStream.rewrap(@app, no_stream, @second_wrap, @tag_v2) ==
               {:error, :invalid_subject}

      assert event_count(conn) == 1
    end

    test "a head of no version, which would create a gone stream", context do
      %{conn: conn, subject: subject, key_stream: stream} = context

      mint!(subject)
      head = head!(subject)

      hard_delete!(stream)

      for version <- [0, -1, nil, :any_version, :no_stream, :stream_exists] do
        assert KeyStream.rewrap(@app, %Head{head | version: version}, @second_wrap, @tag_v2) ==
                 {:error, :invalid_head}
      end

      assert event_count(conn) == 0
      assert stream_rows(conn, stream) == 0
    end

    test "a head with no key id", %{conn: conn, subject: subject} do
      mint!(subject)
      head = head!(subject)

      for key_id <- [nil, "", <<0xFF>>] do
        assert KeyStream.rewrap(@app, %Head{head | key_id: key_id}, @second_wrap, @tag_v2) ==
                 {:error, :invalid_head}
      end

      assert event_count(conn) == 1
    end

    test "a wrap or a tag it can't store", %{conn: conn, subject: subject} do
      for {wrap, tag, reason} <- [
            {"", @tag_v1, :invalid_wrapped_key},
            {nil, @tag_v1, :invalid_wrapped_key},
            {:wrap, @tag_v1, :invalid_wrapped_key},
            {@wrap, "", :invalid_master_tag},
            {@wrap, nil, :invalid_master_tag},
            {@wrap, <<0xFF>>, :invalid_master_tag}
          ] do
        assert KeyStream.mint(@app, @prefix, subject, wrap, tag) == {:error, reason}
      end

      assert event_count(conn) == 0

      mint!(subject)
      head = head!(subject)

      assert KeyStream.rewrap(@app, head, "", @tag_v2) == {:error, :invalid_wrapped_key}
      assert KeyStream.rewrap(@app, head, @wrap, <<0xFF>>) == {:error, :invalid_master_tag}
      assert KeyStream.rewrap(@app, %{}, @wrap, @tag_v2) == {:error, :invalid_head}
      assert KeyStream.rewrap(@app, nil, @wrap, @tag_v2) == {:error, :invalid_head}

      assert event_count(conn) == 1
    end

    test "on a store that declares nothing", %{conn: conn, subject: subject} do
      start_supervised!({LedgerApp, name: :undeclared_ledger, event_store: undeclared_store()})

      assert KeyStream.mint(:undeclared_ledger, @prefix, subject, @wrap, @tag_v1) ==
               {:error, :undeclared_key_stream}

      assert event_count(conn) == 0
    end

    test "on a store that declares it all, and can't hard delete", context do
      %{conn: conn, subject: subject} = context

      start_supervised!({LedgerApp, name: :undeletable_ledger, event_store: undeletable_store()})

      mint!(subject)
      head = head!(subject)

      assert KeyStream.mint(:undeletable_ledger, @prefix, UUID.uuid4(), @wrap, @tag_v1) ==
               {:error, :undeclared_key_stream}

      assert KeyStream.rewrap(:undeletable_ledger, head, @second_wrap, @tag_v2) ==
               {:error, :undeclared_key_stream}

      assert event_count(conn) == 1
    end

    defp undeclared_store, do: FrameworkStorage.event_store_config(adapter: FailingAdapter)

    defp undeletable_store do
      FrameworkStorage.event_store_config(
        adapter: FailingAdapter,
        deletable: [stream_prefixes: [@prefix], event_types: KeyStream.event_types()]
      )
    end
  end

  describe "a store configured" do
    test "with `returning: :append_result` still answers a key write's success", context do
      %{subject: subject, stream: stream} = context

      event_store =
        Keyword.merge(undeletable_store(), enable_hard_deletes: true, returning: :append_result)

      start_supervised!({LedgerApp, name: :result_ledger, event_store: event_store})

      # What the option does to every append, so this test can't pass without it.
      assert {:ok, %AppendResult{}} =
               EventStore.append_to_stream(:result_ledger, stream, :no_stream, [
                 Mapper.map_to_event_data(%Plain{note: "first"})
               ])

      assert {:ok, key_id} = KeyStream.mint(:result_ledger, @prefix, subject, @wrap, @tag_v1)

      {:ok, %Head{version: 1, key_id: ^key_id} = head} =
        KeyStream.head(:result_ledger, @prefix, subject)

      assert KeyStream.rewrap(:result_ledger, head, @second_wrap, @tag_v2) == :ok

      assert {:ok,
              %Head{
                version: 2,
                key_id: ^key_id,
                wrapped_key: @second_wrap,
                master_tag: @tag_v2
              }} = KeyStream.head(:result_ledger, @prefix, subject)
    end

    test "with `enable_hard_deletes` as text mints, and hard deletes", context do
      %{conn: conn, subject: subject, key_stream: key_stream} = context

      event_store = Keyword.merge(undeletable_store(), enable_hard_deletes: "true")

      start_supervised!({LedgerApp, name: :text_ledger, event_store: event_store})

      assert {:ok, _key_id} = KeyStream.mint(:text_ledger, @prefix, subject, @wrap, @tag_v1)

      assert Store.delete_stream(key_stream, :any_version, :hard,
               name: Module.concat([:text_ledger, "FrameworkEventStore"])
             ) == :ok

      assert event_count(conn) == 0
    end
  end

  describe "a mint on an existing stream" do
    test "answers `:stream_exists` and changes nothing", %{conn: conn, subject: subject} do
      key_id = mint!(subject)
      head = head!(subject)

      assert KeyStream.mint(@app, @prefix, subject, @second_wrap, @tag_v2) ==
               {:error, :stream_exists}

      assert head!(subject) == head
      assert %Head{key_id: ^key_id, version: 1, wrapped_key: @wrap} = head
      assert event_count(conn) == 1
    end
  end

  describe "first events that race" do
    test "all lose to a held `:no_stream` append", context do
      %{conn: conn, subject: subject, key_stream: stream} = context

      winner = UUID.uuid4()
      observer = LockSession.connect(postgrex_config())
      session = LockSession.start(postgrex_config())

      :ok =
        LockSession.run(session, fn tx ->
          EventStore.append_to_stream(
            @app,
            stream,
            :no_stream,
            [minted_data(subject, winner, @wrap)],
            conn: tx
          )
        end)

      log =
        capture_log(fn ->
          tasks =
            for _mint <- 1..4 do
              Task.async(fn ->
                KeyStream.mint(@app, @prefix, subject, @second_wrap, @tag_v1)
              end)
            end

          # Each is held on the unique index of `stream_uuid`, by the session and nothing else.
          Wait.until(5_000, fn ->
            blocked = blocked(observer)

            assert map_size(blocked) == 4
            assert blocked |> Map.values() |> Enum.uniq() == [[session.backend]]
          end)

          {:ok, :ok} = LockSession.commit(session)

          for answer <- Task.await_many(tasks, 15_000) do
            assert answer == {:error, :stream_exists}
          end
        end)

      # Each went through the store's one retry: the race, and not a read of a stream that was
      # there.
      assert length(String.split(log, ":duplicate_stream_uuid")) == 5
      refute log =~ "deadlock_detected"

      assert %Head{version: 1, key_id: ^winner, wrapped_key: @wrap} = head!(subject)
      assert event_count(conn) == 1
    end

    test "two mints, neither held by a test: one wins and one reads its key", context do
      %{conn: conn, subject: subject} = context

      observer = LockSession.connect(postgrex_config())
      session = LockSession.start(postgrex_config())

      # With `$all` held, the first mint writes its row and stops at `$all`. The second is then
      # behind the first, in Postgres and not by timing.
      LockSession.lock_row(session, "public", 0)

      log =
        capture_log(fn ->
          tasks =
            for wrap <- [@wrap, @second_wrap] do
              Task.async(fn -> KeyStream.mint(@app, @prefix, subject, wrap, @tag_v1) end)
            end

          Wait.until(5_000, fn ->
            blocked = blocked(observer)

            assert map_size(blocked) == 2

            assert [{first, [holder]}] =
                     Enum.filter(blocked, fn {_pid, blockers} ->
                       blockers == [session.backend]
                     end)

            assert holder == session.backend
            assert [{_second, [^first]}] = Enum.reject(blocked, &(elem(&1, 0) == first))
          end)

          assert {:error, :rolled_back} = LockSession.rollback(session)

          answers = Task.await_many(tasks, 15_000)

          assert [{:ok, winner}] = Enum.filter(answers, &match?({:ok, _key_id}, &1))
          assert Enum.count(answers, &(&1 == {:error, :stream_exists})) == 1
          assert %Head{version: 1, key_id: ^winner} = head!(subject)
        end)

      assert length(String.split(log, ":duplicate_stream_uuid")) == 2
      refute log =~ "deadlock_detected"

      assert event_count(conn) == 1
    end

    test "a mint behind a writer on a row at version 0 answers `:stream_exists`", context do
      %{conn: conn, subject: subject, key_stream: stream} = context

      Postgrex.query!(
        conn,
        "INSERT INTO streams (stream_uuid, stream_version) VALUES ($1, 0)",
        [stream]
      )

      winner = UUID.uuid4()
      observer = LockSession.connect(postgrex_config())
      session = LockSession.start(postgrex_config())

      :ok =
        LockSession.run(session, fn tx ->
          EventStore.append_to_stream(
            @app,
            stream,
            :no_stream,
            [minted_data(subject, winner, @wrap)],
            conn: tx
          )
        end)

      task = Task.async(fn -> KeyStream.mint(@app, @prefix, subject, @second_wrap, @tag_v1) end)

      Wait.until(5_000, fn ->
        assert blocked(observer) |> Map.values() == [[session.backend]]
      end)

      {:ok, :ok} = LockSession.commit(session)

      assert Task.await(task) == {:error, :stream_exists}
      assert %Head{version: 1, key_id: ^winner} = head!(subject)
    end

    # The store retries once, and a second loss needs a stream gone and re-created inside that
    # retry, which no lock can hold: `Stream.maybe_retry_once` runs straight on. So this one is
    # injected.
    test "a second loss of the store's retry answers `:stream_exists`", context do
      %{subject: subject, key_stream: stream} = context

      FailingAdapter.flag_append(stream, fn ->
        {:error, {:already_retried_once, :duplicate_stream_uuid}}
      end)

      assert KeyStream.mint(@app, @prefix, subject, @wrap, @tag_v1) == {:error, :stream_exists}
    end
  end

  describe "a rewrap" do
    test "appends at the exact version and keeps the key id", %{subject: subject} do
      key_id = mint!(subject)

      assert KeyStream.rewrap(@app, head!(subject), @second_wrap, @tag_v2) == :ok

      assert %Head{version: 2, key_id: ^key_id, wrapped_key: @second_wrap, master_tag: @tag_v2} =
               head = head!(subject)

      assert KeyStream.rewrap(@app, head, @wrap, @tag_v1) == :ok

      assert %Head{version: 3, key_id: ^key_id, wrapped_key: @wrap, master_tag: @tag_v1} =
               head!(subject)
    end

    test "is stored as base64, in the declared fields", %{conn: conn, subject: subject} do
      key_id = mint!(subject)

      :ok = KeyStream.rewrap(@app, head!(subject), @second_wrap, @tag_v2)

      [_minted, type] = KeyStream.event_types()

      data = scalar(conn, "SELECT data FROM events WHERE event_type = $1", [type])

      assert %{
               "subject_id" => ^subject,
               "key_id" => ^key_id,
               "master_tag" => @tag_v2,
               "rewrapped_at" => rewrapped_at
             } = stored = Jason.decode!(data)

      assert stored["wrapped_key"] == Base.encode64(@second_wrap)
      assert {:ok, _at, 0} = DateTime.from_iso8601(rewrapped_at)
    end

    test "with a stale head answers `:wrong_expected_version`", %{conn: conn, subject: subject} do
      mint!(subject)
      stale = head!(subject)

      :ok = KeyStream.rewrap(@app, stale, @second_wrap, @tag_v2)

      assert KeyStream.rewrap(@app, stale, @wrap, @tag_v1) == {:error, :wrong_expected_version}

      assert %Head{version: 2, wrapped_key: @second_wrap} = head!(subject)
      assert event_count(conn) == 2
    end

    test "on a hard-deleted stream fails and writes nothing", context do
      %{conn: conn, subject: subject, key_stream: stream} = context

      mint!(subject)
      head = head!(subject)

      hard_delete!(stream)

      assert KeyStream.rewrap(@app, head, @second_wrap, @tag_v2) ==
               {:error, :wrong_expected_version}

      assert event_count(conn) == 0
      assert stream_rows(conn, stream) == 0
    end

    test "behind a hard delete answers `:not_found`, and leaves no orphan", context do
      %{conn: conn, subject: subject, key_stream: stream} = context

      mint!(subject)
      head = head!(subject)

      observer = LockSession.connect(postgrex_config())
      session = LockSession.start(postgrex_config())

      :ok =
        LockSession.run(session, fn tx ->
          Store.delete_stream(stream, :any_version, :hard, name: @store, conn: tx)
        end)

      task = Task.async(fn -> KeyStream.rewrap(@app, head, @second_wrap, @tag_v2) end)

      # The rewrap read the stream, which the delete has not yet committed, and waits on its row.
      Wait.until(5_000, fn ->
        assert blocked(observer) |> Map.values() == [[session.backend]]
      end)

      {:ok, :ok} = LockSession.commit(session)

      assert Task.await(task) == {:error, :not_found}

      # `append_events_v4` takes the row before its `INSERT INTO events`, so DOC-2 §5.5's orphan
      # does not exist: no row of either kind holds the wrap.
      assert event_count(conn) == 0
      assert stream_rows(conn, stream) == 0
      assert scalar(conn, "SELECT count(*) FROM stream_events") == 0
    end
  end

  describe "after a hard delete" do
    test "a fresh key can be minted, and the head shows it", context do
      %{subject: subject, key_stream: stream} = context

      first = mint!(subject)
      old = head!(subject)

      hard_delete!(stream)

      assert KeyStream.head(@app, @prefix, subject) == {:error, :stream_not_found}

      second = mint!(subject, @second_wrap)

      assert second != first
      assert %Head{version: 1, key_id: ^second, wrapped_key: @second_wrap} = head!(subject)

      # The known hole, DOC-2 §5.5: a hard delete resets a stream's version, so version 1 from
      # before it passes, and the old key is at the head again. PF-3's re-check after this write
      # is what closes it.
      assert KeyStream.rewrap(@app, old, @wrap, @tag_v1) == :ok
      assert %Head{version: 2, key_id: ^first, wrapped_key: @wrap} = head!(subject)

      stale = %Head{old | version: 5}

      assert KeyStream.rewrap(@app, stale, @wrap, @tag_v1) == {:error, :wrong_expected_version}
    end
  end

  describe "a read that fails" do
    defp assert_failed({:error, reason}) do
      refute reason in [:stream_not_found, :stream_deleted, :malformed_key_event, :erased]
      refute match?({:erased, _}, reason)

      reason
    end

    test "is no answer about the subject, when the connection drops", %{subject: subject} do
      %{hostname: hostname, port: port} = Map.new(postgrex_config())

      proxy = start_supervised!({TcpProxy, upstream: {hostname, port}})

      start_supervised!(
        {LedgerApp, name: :proxied_ledger, event_store: proxied_store(TcpProxy.port(proxy))}
      )

      assert {:ok, _key_id} =
               KeyStream.mint(:proxied_ledger, @prefix, subject, @wrap, @tag_v1)

      assert {:ok, %Head{}} = KeyStream.head(:proxied_ledger, @prefix, subject)

      log =
        capture_log(fn ->
          stop_supervised!(TcpProxy)

          for _read <- 1..2 do
            answer = KeyStream.head(:proxied_ledger, @prefix, subject)

            assert_failed(answer)
            refute_leak(inspect(answer, limit: :infinity))
          end
        end)

      refute_leak(log)
    end

    defp proxied_store(port) do
      FrameworkStorage.event_store_config(
        adapter: FailingAdapter,
        hostname: "127.0.0.1",
        port: port,
        pool_size: 2,
        enable_hard_deletes: true,
        deletable: [stream_prefixes: [@prefix], event_types: KeyStream.event_types()]
      )
    end

    test "is no answer about the subject, when the store is stopped", %{subject: subject} do
      mint!(subject)

      stop_supervised!(LedgerApp)

      assert_failed(KeyStream.head(@app, @prefix, subject))
    end

    for {name, failure} <- [
          {"a raise", quote(do: fn -> raise "down" end)},
          {"an exit", quote(do: fn -> exit({:timeout, :down}) end)},
          {"a throw", quote(do: fn -> throw({:down, :now}) end)},
          {"a reason", quote(do: fn -> {:error, %DBConnection.ConnectionError{}} end)},
          {"a term", quote(do: fn -> {:error, "down"} end)}
        ] do
      test "answers an error, for #{name}", %{subject: subject, key_stream: stream} do
        FailingAdapter.flag(stream, unquote(failure))

        assert_failed(KeyStream.head(@app, @prefix, subject))
      end
    end

    test "reduces what it holds", %{subject: subject, key_stream: stream} do
      for failure <- [
            fn -> raise @marker end,
            fn -> raise MatchError, term: @wrap end,
            fn -> exit({:gave_up, @marker}) end,
            fn -> throw({:thrown, @marker}) end,
            fn -> throw(@marker) end,
            fn -> {:error, {:failed, @marker}} end,
            fn -> {:error, @marker} end,
            fn -> {:error, %RuntimeError{message: @marker}} end
          ] do
        FailingAdapter.flag(stream, failure)

        answer = KeyStream.head(@app, @prefix, subject)

        assert_failed(answer)
        refute_leak_in_term(answer)
      end
    end

    test "of a head that is another event is malformed", %{subject: subject, key_stream: stream} do
      start_plain_store()

      plain_append!(stream, "Elixir.#{inspect(Plain)}", %Plain{note: @marker})

      assert KeyStream.head(@app, @prefix, subject) == {:error, :malformed_key_event}
    end

    test "of a head with a wrap that is not base64 is malformed", context do
      %{subject: subject, key_stream: stream} = context

      start_plain_store()

      [type | _types] = KeyStream.event_types()

      plain_append!(stream, type, %{minted(subject, UUID.uuid4(), @wrap) | wrapped_key: "%%%"})

      assert KeyStream.head(@app, @prefix, subject) == {:error, :malformed_key_event}
    end

    for {name, change} <- [
          {"no wrap", [wrapped_key: nil]},
          {"an empty wrap", [wrapped_key: ""]},
          {"a wrap that is no string", [wrapped_key: 1]},
          {"no key id", [key_id: nil]},
          {"an empty key id", [key_id: ""]},
          {"no master tag", [master_tag: nil]},
          {"another subject", [subject_id: "00000000-0000-4000-8000-000000000000"]}
        ] do
      test "of a head with #{name} is malformed", %{subject: subject, key_stream: stream} do
        start_plain_store()

        [_minted, type] = KeyStream.event_types()

        rewrapped = %SubjectDataKeyRewrapped{
          subject_id: subject,
          key_id: UUID.uuid4(),
          wrapped_key: Base.encode64(@wrap),
          master_tag: @tag_v1
        }

        plain_append!(stream, type, struct!(rewrapped, unquote(change)))

        assert KeyStream.head(@app, @prefix, subject) == {:error, :malformed_key_event}
      end
    end

    test "of a row that won't decode leaves no bytes anywhere", context do
      %{subject: subject, key_stream: stream} = context

      start_plain_store()
      attach_telemetry(@head_spans)

      [type | _types] = KeyStream.event_types()
      bytes = ~s({"wrapped_key": "#{Base.encode64(@wrap)}" not json #{@marker})

      plain_append!(stream, type, %RawSerializer.Raw{bytes: bytes})

      log =
        capture_log(fn ->
          answer = KeyStream.head(@app, @prefix, subject)

          assert answer == {:error, Jason.DecodeError}
          refute_leak_in_term(answer)
        end)

      refute_leak(log)
      assert flush_telemetry() == []
    end

    test "of an undecodable type leaves no bytes anywhere", context do
      %{subject: subject, key_stream: stream} = context

      start_plain_store()
      attach_telemetry(@head_spans)

      type = "Elixir.PF2.NoSuchEvent" <> String.replace(UUID.uuid4(), "-", "")

      plain_append!(stream, type, %{wrapped_key: Base.encode64(@wrap)})

      log =
        capture_log(fn ->
          answer = KeyStream.head(@app, @prefix, subject)

          # The store raises `UnknownEventTypeError` for a type it does not know (#7804),
          # reduced to its module: no bytes of the event.
          assert answer == {:error, Decree.Store.UnknownEventTypeError}
          refute_leak_in_term(answer)
        end)

      refute_leak(log)
      assert flush_telemetry() == []
    end

    test "of a stream with no events reads as no stream", %{conn: conn, subject: subject} do
      # What a hard delete between the store's two statements of one read leaves: a stream
      # whose row was read and whose events are gone.
      stream = @prefix <> subject

      Postgrex.query!(
        conn,
        "INSERT INTO streams (stream_uuid, stream_version) VALUES ($1, 0)",
        [stream]
      )

      assert EventStore.read_stream_head(@app, stream) == {:error, :stream_not_found}
      assert KeyStream.head(@app, @prefix, subject) == {:error, :stream_not_found}
    end
  end

  describe "a write that fails" do
    test "keeps only an append error's class", %{subject: subject, key_stream: stream} do
      mint!(subject)
      head = head!(subject)

      for class <- [:unavailable, :retryable, :rejected] do
        FailingAdapter.flag_append(stream, fn ->
          {:error, %AppendError{class: class, code: nil, reason: @marker}}
        end)

        assert KeyStream.mint(@app, @prefix, subject, @wrap, @tag_v1) ==
                 {:error, {:append_failed, class}}

        assert KeyStream.rewrap(@app, head, @wrap, @tag_v1) ==
                 {:error, {:append_failed, class}}
      end
    end

    test "reduces what it holds", %{subject: subject, key_stream: stream} do
      mint!(subject)
      head = head!(subject)

      for failure <- [
            fn -> raise @marker end,
            fn -> raise MatchError, term: @wrap end,
            fn -> exit({:gave_up, @marker}) end,
            fn -> throw({:thrown, @marker}) end,
            fn -> throw(@marker) end,
            fn -> {:error, {:failed, @marker}} end,
            fn -> {:error, @marker} end
          ] do
        FailingAdapter.flag_append(stream, failure)

        assert {:error, _reason} = mint = KeyStream.mint(@app, @prefix, subject, @wrap, @tag_v1)
        assert {:error, _reason} = rewrap = KeyStream.rewrap(@app, head, @wrap, @tag_v1)

        refute_leak_in_term(mint)
        refute_leak_in_term(rewrap)
      end
    end

    test "answers an error when the store is stopped", %{subject: subject} do
      mint!(subject)
      head = head!(subject)

      stop_supervised!(LedgerApp)

      assert {:error, _reason} = KeyStream.mint(@app, @prefix, subject, @wrap, @tag_v1)
      assert {:error, _reason} = KeyStream.rewrap(@app, head, @wrap, @tag_v1)
    end
  end

  describe "a host's key source" do
    defp provision!(subject) do
      key = :crypto.strong_rand_bytes(32)

      {:ok, key_id} =
        KeyStream.mint(@app, @prefix, subject, KeyStreams.wrap(key), @tag_v1)

      {key_id, key}
    end

    defp admit!(subject) do
      admission(subject, %AdmissionGranted{subject_id: subject})
    end

    defp erase!(subject) do
      admission(subject, %AdmissionErased{subject_id: subject})
    end

    defp admission(subject, event) do
      stream = KeyStreams.admission_prefix() <> subject

      :ok =
        EventStore.append_to_stream(
          @app,
          stream,
          :any_version,
          [Mapper.map_to_event_data(event)]
        )
    end

    defp dispatch_registered!(stream, subject) do
      :ok =
        @app.dispatch(%Emit{
          account_id: stream,
          events: [%Registered{user_id: subject, email: email()}]
        })
    end

    defp fold(stream) do
      :ok = Aggregate.shutdown(@app, Account, stream)

      Aggregate.aggregate_state(@app, Account, stream)
    end

    test "encrypts and reads with no handler started", %{conn: conn} = context do
      %{subject: subject, stream: stream} = context

      admit!(subject)
      {key_id, key} = provision!(subject)

      assert LedgerKeys.lookup(subject) == {:ok, key_id, key}

      dispatch_registered!(stream, subject)

      stored = rows(conn, "events", "data") |> Enum.join(" ")

      refute stored =~ email()

      assert [%RecordedEvent{data: %Registered{email: %{"$pf": 1, k: ^key_id}}} = event] =
               EventStore.stream_forward(@app, stream) |> Enum.to_list()

      assert %Account{email: read} = fold(stream)
      assert read == email()

      assert {:ok, %RecordedEvent{data: %Registered{email: decrypted}}} =
               Decree.PersonalFields.decrypt_recorded_event(@app, event)

      assert decrypted == email()
    end

    test "never uses a surviving key once admission says erased", context do
      %{subject: subject, stream: stream} = context

      admit!(subject)
      provision!(subject)
      dispatch_registered!(stream, subject)

      erase!(subject)

      assert LedgerKeys.lookup(subject) == :erased
      assert {:ok, %Head{}} = KeyStream.head(@app, @prefix, subject)

      assert %Account{email: nil} = fold(stream)

      [event] = EventStore.stream_forward(@app, stream) |> Enum.to_list()

      assert {:ok, %RecordedEvent{data: %Registered{email: nil}}} =
               Decree.PersonalFields.decrypt_recorded_event(@app, event)

      other_stream = UUID.uuid4()

      dispatch_registered!(other_stream, subject)

      assert [%RecordedEvent{data: %Registered{email: nil}}] =
               EventStore.stream_forward(@app, other_stream) |> Enum.to_list()
    end

    test "answers unknown for a key stream gone with admission not erased", context do
      %{subject: subject, stream: stream, key_stream: key_stream} = context

      admit!(subject)
      provision!(subject)
      dispatch_registered!(stream, subject)
      :ok = Aggregate.shutdown(@app, Account, stream)

      hard_delete!(key_stream)

      assert LedgerKeys.lookup(subject) == :unknown

      assert @app.dispatch(%Emit{
               account_id: UUID.uuid4(),
               events: [%Registered{user_id: subject, email: email()}]
             }) == {:error, :key_unknown}

      [event] = EventStore.stream_forward(@app, stream) |> Enum.to_list()

      assert Decree.PersonalFields.decrypt_recorded_event(@app, event) ==
               {:error, :key_unknown}
    end

    test "still reads old envelopes after a rewrap", context do
      %{subject: subject, stream: stream} = context

      admit!(subject)
      {key_id, key} = provision!(subject)
      dispatch_registered!(stream, subject)

      head = head!(subject)
      :ok = KeyStream.rewrap(@app, head, KeyStreams.wrap(key), @tag_v2)

      assert %Head{version: 2, key_id: ^key_id, master_tag: @tag_v2} = head!(subject)
      assert LedgerKeys.lookup(subject) == {:ok, key_id, key}

      assert [%RecordedEvent{data: %Registered{email: %{k: ^key_id}}} = event] =
               EventStore.stream_forward(@app, stream) |> Enum.to_list()

      assert %Account{email: read} = fold(stream)
      assert read == email()

      assert {:ok, %RecordedEvent{data: %Registered{email: decrypted}}} =
               Decree.PersonalFields.decrypt_recorded_event(@app, event)

      assert decrypted == email()
    end

    test "answers unknown, and never absent, when a read fails", context do
      %{subject: subject, stream: stream, key_stream: key_stream} = context

      admit!(subject)
      provision!(subject)
      dispatch_registered!(stream, subject)
      :ok = Aggregate.shutdown(@app, Account, stream)

      [event] = EventStore.stream_forward(@app, stream) |> Enum.to_list()

      for flagged <- [key_stream, KeyStreams.admission_prefix() <> subject],
          failure <- [
            fn -> {:error, %DBConnection.ConnectionError{}} end,
            fn -> raise "down" end,
            fn -> exit(:down) end
          ] do
        FailingAdapter.flag(flagged, failure)

        assert LedgerKeys.lookup(subject) == :unknown

        assert Decree.PersonalFields.decrypt_recorded_event(@app, event) ==
                 {:error, :key_unknown}

        assert @app.dispatch(%Emit{
                 account_id: UUID.uuid4(),
                 events: [%Registered{user_id: subject, email: email()}]
               }) == {:error, :key_unknown}

        FailingAdapter.unflag(flagged)
      end
    end
  end

  describe "no wrapped bytes reach" do
    test "`inspect/1` of an event, a head or a recorded event", %{subject: subject} do
      encoded = Base.encode64(@wrap)

      minted = %SubjectDataKeyMinted{subject_id: subject, wrapped_key: encoded}
      rewrapped = %SubjectDataKeyRewrapped{subject_id: subject, wrapped_key: encoded}
      head = %Head{subject_id: subject, wrapped_key: @wrap}

      for term <- [
            minted,
            rewrapped,
            head,
            %RecordedEvent{data: minted},
            %RecordedEvent{data: rewrapped},
            {:ok, head},
            [head]
          ] do
        text = inspect(term, limit: :infinity, printable_limit: :infinity)

        assert text =~ subject or match?(%RecordedEvent{}, term)
        refute_leak(text)
      end
    end

    test "a log, over every write and every failure", context do
      %{subject: subject, key_stream: stream} = context

      start_plain_store()

      log =
        capture_log(fn ->
          mint!(subject)
          head = head!(subject)

          {:error, :stream_exists} = KeyStream.mint(@app, @prefix, subject, @wrap, @tag_v1)
          :ok = KeyStream.rewrap(@app, head, @second_wrap, @tag_v2)
          {:error, _reason} = KeyStream.rewrap(@app, head, @wrap, @tag_v1)
          {:error, _reason} = KeyStream.mint(@app, "Elsewhere-", subject, @wrap, @tag_v1)
          {:error, _reason} = KeyStream.mint(@app, @prefix, "bad", @wrap, @tag_v1)
          {:error, _reason} = KeyStream.rewrap(@app, %{}, @wrap, @tag_v1)

          for failure <- [
                fn -> raise @marker end,
                fn -> exit({:gave_up, @marker}) end,
                fn -> throw({:thrown, @marker}) end,
                fn -> {:error, {:failed, @marker}} end,
                fn -> {:error, %AppendError{class: :rejected, reason: @marker}} end
              ] do
            FailingAdapter.flag(stream, failure)
            FailingAdapter.flag_append(stream, failure)

            {:error, _reason} = KeyStream.head(@app, @prefix, subject)
            {:error, _reason} = KeyStream.mint(@app, @prefix, subject, @wrap, @tag_v1)
            {:error, _reason} = KeyStream.rewrap(@app, head, @wrap, @tag_v1)
          end

          FailingAdapter.unflag(stream)
          FailingAdapter.unflag_append(stream)

          hard_delete!(stream)

          {:error, :stream_not_found} = KeyStream.head(@app, @prefix, subject)
          {:error, :wrong_expected_version} = KeyStream.rewrap(@app, head, @wrap, @tag_v1)
        end)

      refute_leak(log)
    end

    # The leak is refuted first, so a span that comes back fails on what it published.
    defp refute_append_telemetry(telemetry) do
      refute_leak_in_term(telemetry)

      assert for({:telemetry, event, _measurements, _metadata} <- telemetry, do: event) == []
    end

    test "telemetry, on success", %{subject: subject} do
      attach_telemetry(@telemetry ++ @head_spans)

      mint!(subject)
      head = head!(subject)
      :ok = KeyStream.rewrap(@app, head, @second_wrap, @tag_v2)

      refute_append_telemetry(flush_telemetry())
    end

    test "telemetry, when the append raises", %{subject: subject} do
      FailingAdapter.pretend_declared([@prefix])
      attach_telemetry(@telemetry)

      # The application stays up, and its store has gone, so the raise is the store's own.
      :ok = Supervisor.terminate_child(LedgerApp, @store)

      assert {:error, _reason} = KeyStream.mint(@app, @prefix, subject, @wrap, @tag_v1)

      refute_append_telemetry(flush_telemetry())
    end

    # What a serializer raising on the `EventData` does: the first frame lists the wrap's base64.
    test "telemetry, when a failed function clause holds the wrap", context do
      %{subject: subject, key_stream: stream} = context

      mint!(subject)
      head = head!(subject)

      attach_telemetry(@telemetry)

      FailingAdapter.flag_append(stream, fn -> needs_a_map!({Base.encode64(@wrap), @wrap}) end)

      assert KeyStream.mint(@app, @prefix, subject, @wrap, @tag_v1) ==
               {:error, :function_clause}

      assert KeyStream.rewrap(@app, head, @second_wrap, @tag_v2) == {:error, :function_clause}

      refute_append_telemetry(flush_telemetry())
    end

    test "a head read's telemetry: it has none", %{subject: subject} do
      attach_telemetry(@head_spans)

      mint!(subject)
      head!(subject)

      assert flush_telemetry() == []
    end

    test "an error tuple", %{subject: subject, key_stream: stream} do
      mint!(subject)
      head = head!(subject)

      answers = [
        KeyStream.mint(@app, @prefix, subject, @wrap, @tag_v1),
        KeyStream.mint(@app, "Elsewhere-", subject, @wrap, @tag_v1),
        KeyStream.mint(@app, @prefix, "bad", @wrap, @tag_v1),
        KeyStream.mint(@app, @prefix, subject, @wrap, <<0xFF>>),
        KeyStream.rewrap(@app, %Head{head | version: 7}, @wrap, @tag_v1),
        KeyStream.rewrap(@app, %Head{head | version: 0}, @wrap, @tag_v1),
        KeyStream.rewrap(@app, %{wrapped_key: @wrap}, @wrap, @tag_v1)
      ]

      for answer <- answers do
        assert {:error, reason} = answer
        assert is_atom(reason)
        refute_leak_in_term(answer)
      end

      hard_delete!(stream)

      refute_leak_in_term(KeyStream.head(@app, @prefix, subject))
    end

    test "a stacktrace, from a bad argument", %{subject: subject} do
      # Every argument is checked inside the function, so no failed clause lists it.
      for answer <- [
            KeyStream.mint(:no_such_app, @prefix, subject, @wrap, @tag_v1),
            KeyStream.mint(@app, @prefix, :bad, @wrap, @tag_v1),
            KeyStream.mint(@app, @prefix, subject, @wrap, :bad),
            KeyStream.mint(@app, @prefix, subject, {:wrapped, @wrap}, @tag_v1),
            KeyStream.rewrap(:no_such_app, %Head{}, @wrap, @tag_v1),
            KeyStream.rewrap(@app, :bad, @wrap, @tag_v1),
            KeyStream.rewrap(@app, %Head{wrapped_key: @wrap}, @wrap, :bad),
            KeyStream.head(:no_such_app, @prefix, subject),
            KeyStream.head(@app, :bad, subject)
          ] do
        assert {:error, _reason} = answer
        refute_leak_in_term(answer)
      end
    end

    test "a failed function clause under a call, in the answer", context do
      %{subject: subject, key_stream: stream} = context

      mint!(subject)
      head = head!(subject)

      FailingAdapter.flag(stream, fn -> needs_a_map!(head) end)
      FailingAdapter.flag_append(stream, fn -> needs_a_map!(head) end)

      for answer <- [
            KeyStream.head(@app, @prefix, subject),
            KeyStream.mint(@app, @prefix, subject, @wrap, @tag_v1),
            KeyStream.rewrap(@app, head, @wrap, @tag_v1)
          ] do
        assert answer == {:error, :function_clause}
      end
    end

    # By `apply/3`, because a head that no clause takes is the point.
    defp needs_a_map!(term), do: apply(KeyStreams, :needs_a_map, [term])

    test "a crash report of a process dying with a head", %{subject: subject} do
      mint!(subject)

      log =
        capture_log(fn ->
          {:ok, pid} =
            Task.start(fn ->
              head = head!(subject)

              needs_a_map!(head)
            end)

          ref = Process.monitor(pid)

          assert_receive {:DOWN, ^ref, :process, ^pid, {:function_clause, _stack}}
        end)

      assert log =~ "FunctionClauseError"
      refute_leak(log)
    end

    test "a GenServer's crash report, holding a head in its state", %{subject: subject} do
      mint!(subject)
      head = head!(subject)

      log =
        capture_log(fn ->
          {:ok, pid} = Agent.start(fn -> head end)
          ref = Process.monitor(pid)

          Agent.cast(pid, fn state -> needs_a_map!(state) end)

          assert_receive {:DOWN, ^ref, :process, ^pid, _reason}
        end)

      assert log =~ "State:"
      refute_leak(log)
    end
  end

  describe "a `$all` subscriber" do
    setup do
      {:ok, subscription} =
        EventStore.subscribe_to(@app, :all, "key-stream-all", self(), :origin)

      assert_receive {:subscribed, ^subscription}

      [subscription: subscription]
    end

    test "gets each event, wrapped and undecrypted", %{subject: subject} = context do
      key = :crypto.strong_rand_bytes(32)
      wrap = KeyStreams.wrap(key)
      encoded = Base.encode64(wrap)

      {:ok, key_id} = KeyStream.mint(@app, @prefix, subject, wrap, @tag_v1)

      assert_receive {:events, [%RecordedEvent{} = minted]}

      assert %SubjectDataKeyMinted{
               subject_id: ^subject,
               key_id: ^key_id,
               wrapped_key: ^encoded,
               master_tag: @tag_v1
             } = minted.data

      assert minted.metadata == %{}
      assert PersonalFields.declaration(minted.data) == nil
      assert :binary.match(:erlang.term_to_binary(minted), key) == :nomatch

      :ok = EventStore.ack_event(@app, context.subscription, minted)

      :ok = KeyStream.rewrap(@app, head!(subject), wrap, @tag_v2)

      assert_receive {:events, [%RecordedEvent{data: %SubjectDataKeyRewrapped{} = rewrapped}]}
      assert rewrapped.wrapped_key == encoded
      assert PersonalFields.declaration(rewrapped) == nil
      assert :binary.match(:erlang.term_to_binary(rewrapped), key) == :nomatch
    end

    # Not a leak this layer can close: delivery's telemetry holds the event, as DOC-2 §6.5 says
    # of every event. So a telemetry handler may inspect it, and must not serialize it.
    test "shows the wrap in telemetry's terms, and `Inspect` hides it", context do
      %{subject: subject, subscription: subscription} = context

      attach_telemetry([
        [:decree, :event_store, :ack_event, :start],
        [:decree, :event, :handle, :start]
      ])

      start_supervised!(CatchAllHandler)

      mint!(subject)

      assert_receive {:events, [%RecordedEvent{} = event]}
      assert_receive :absorbed

      :ok = EventStore.ack_event(@app, subscription, event)

      assert_receive {:telemetry, [:decree, :event_store, :ack_event, :start], _, ack}
      assert_receive {:telemetry, [:decree, :event, :handle, :start], _, handle}

      for metadata <- [ack, handle] do
        refute_leak(inspect(metadata, limit: :infinity, printable_limit: :infinity))
      end

      encoded = Base.encode64(@wrap)

      assert %RecordedEvent{data: %SubjectDataKeyMinted{wrapped_key: ^encoded}} = ack.event

      assert %RecordedEvent{data: %SubjectDataKeyMinted{wrapped_key: ^encoded}} =
               handle.recorded_event
    end

    test "with a catch-all clause absorbs them", %{subject: subject} do
      start_supervised!(CatchAllHandler)

      mint!(subject)

      assert_receive :absorbed

      :ok = KeyStream.rewrap(@app, head!(subject), @second_wrap, @tag_v2)

      assert_receive :absorbed
    end
  end

  describe "`event_types/0`" do
    test "is what the framework's mapping stores" do
      assert KeyStream.event_types() == [
               Mapper.map_to_event_data(%SubjectDataKeyMinted{}).event_type,
               Mapper.map_to_event_data(%SubjectDataKeyRewrapped{}).event_type
             ]
    end

    test "follows a configured type provider" do
      Elixir.Application.put_env(:decree, :type_provider, ShortTypes)
      on_exit(fn -> Elixir.Application.delete_env(:decree, :type_provider) end)

      assert KeyStream.event_types() == ["SubjectDataKeyMinted", "SubjectDataKeyRewrapped"]

      assert Mapper.map_to_event_data(%SubjectDataKeyMinted{}).event_type ==
               "SubjectDataKeyMinted"
    end

    test "goes in a store's `use` line, as the docs show" do
      assert DeclaredStore.config()[:deletable][:event_types] == KeyStream.event_types()
    end
  end

  describe "`Decree.EventStore.read_stream_head/2`" do
    test "answers the last event of a stream", %{stream: stream} do
      for note <- ["first", "second", "third"] do
        append_plain!(stream, Mapper.map_to_event_data(%Plain{note: note}))
      end

      {:ok, event} = read_head(stream)

      assert %RecordedEvent{
               stream_id: ^stream,
               stream_version: 3,
               data: %Plain{note: "third"},
               metadata: %{}
             } = event
    end

    test "answers `:stream_not_found`, and `:stream_deleted`", %{stream: stream} do
      assert read_head(stream) == {:error, :stream_not_found}

      append_plain!(stream, Mapper.map_to_event_data(%Plain{note: "first"}))
      :ok = Store.delete_stream(stream, :any_version, :soft, name: @store)

      assert read_head(stream) == {:error, :stream_deleted}
    end

    test "upcasts, as `stream_forward/4` does", %{stream: stream, subject: subject} do
      append_plain!(stream, Mapper.map_to_event_data(%PassedThrough{user_id: subject}))

      assert {:ok, %RecordedEvent{data: %PassedThrough{version: 2}}} = read_head(stream)
    end

    defp read_head(stream), do: EventStore.read_stream_head(@app, stream)

    defp append_plain!(stream, event) do
      :ok = EventStore.append_to_stream(@app, stream, :any_version, [event])
    end
  end
end
