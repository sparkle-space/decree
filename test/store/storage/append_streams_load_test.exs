defmodule Decree.Store.Storage.AppendStreamsLoadTest do
  use ExUnit.Case

  alias Decree.Store.{Config, EventData, LockSession, UUID}
  alias Decree.Store.Storage.{Database, Initializer}
  alias Decree.Store.Streams.Stream

  @sessions 8
  @operations_per_session 500
  @pool ~w(a-pool B-pool c-pool D-pool e-pool F-pool g-pool H-pool)
  @shared_names 40
  @link_source "link-source"
  @link_source_events 64
  @doomed_streams 8
  @append_opts [schema: "public", serializer: Decree.Store.JsonSerializer]
  @delete_opts [schema: "public", enable_hard_deletes: true]

  # The store retries a deadlock once, so a deadlock would not show in any answer. The
  # proof is `pg_stat_database.deadlocks`, and this database is the test's own, so no
  # other test's backend can move that counter. A backend flushes the counter when it
  # exits, so every session closes, and is seen gone, before the counter is read.
  #
  # Hard deletes run here over `doomed-` streams of their own, with links of one of their
  # events at a time. A link of several of a stream's events stays out: it can deadlock with
  # a hard delete, because the two lock event rows in different orders. That cycle is a
  # measured residual, which `Decree.Store.Storage.HardDeleteTest` pins, and it would move
  # this counter. So links of several events keep to `link-source`, which nothing deletes. No
  # doomed stream is a link's target, because two hard deletes meeting only through a link
  # stream can cycle the same way.
  setup do
    config =
      TestEventStore
      |> Config.parsed(:decree)
      |> Keyword.put(:database, "decree_load_#{System.unique_integer([:positive])}")

    :ok = Database.create(config)

    on_exit(fn -> Database.drop(config) end)

    postgrex_config = Config.default_postgrex_opts(config)
    observer = LockSession.connect(postgrex_config)

    {:ok, _} = Initializer.run!(observer, config)

    [config: config, postgrex_config: postgrex_config, observer: observer]
  end

  test "random batches, plain appends, links and hard deletes never deadlock", context do
    %{observer: observer, postgrex_config: postgrex_config} = context

    seed(observer)

    # Without this the unchanged counter below could be a counter that does not count.
    assert_counter_sees_a_deadlock(context)

    before_run = deadlocks(context)
    links = link_source_event_ids(observer)
    seed = ExUnit.configuration()[:seed]

    connections =
      for session <- 1..@sessions do
        conn = LockSession.connect(postgrex_config)

        {session, conn, LockSession.backend_pid(conn)}
      end

    results =
      connections
      |> Enum.map(fn {session, conn, _backend} ->
        Task.async(fn -> run_session(conn, session, seed, links) end)
      end)
      |> Task.await_many(120_000)
      |> List.flatten()

    close(context, connections)

    assert deadlocks(context) == before_run, "the run deadlocked, seed #{seed}"

    answers = Enum.frequencies_by(results, & &1.answer)

    assert Map.keys(answers) -- legitimate_answers() == [], inspect(answers)
    assert answers[:ok] > @sessions * @operations_per_session / 2, inspect(answers)

    assert answers[:hard_deleted] > 0, inspect(answers)

    assert_streams_gapless(observer)
    assert_all_consistent(observer)
    assert_batches_contiguous(observer, results)
  end

  defp legitimate_answers do
    [
      :ok,
      {:error, {:already_retried_once, :duplicate_stream_uuid}},
      # A link of an event already in the target.
      {:error, :duplicate_event},
      # A link that lost the race to create its target.
      {:error, :stream_exists},
      :hard_deleted,
      # An append, or a link, that a hard delete got to first.
      {:error, :not_found},
      # A hard delete another one, or the stream's absence, got to first.
      {:error, :stream_not_found},
      # A hard delete that a committed link of one of its events got to first.
      {:error, :foreign_key_violation}
    ]
  end

  defp seed(conn) do
    for stream_uuid <- @pool do
      :ok = Stream.append_to_stream(conn, stream_uuid, 0, [event([])], @append_opts)
    end

    events = for _index <- 1..@link_source_events, do: event([])

    :ok = Stream.append_to_stream(conn, @link_source, 0, events, @append_opts)
  end

  defp run_session(conn, session, seed, links) do
    :rand.seed(:exsss, {seed, session, 7620})

    for operation <- 1..@operations_per_session do
      case :rand.uniform(100) do
        roll when roll <= 45 -> multi_stream_append(conn, session, operation)
        roll when roll <= 70 -> plain_append(conn, session, operation)
        roll when roll <= 82 -> link(conn, session, operation, links)
        roll when roll <= 90 -> doomed_append(conn, session, operation)
        roll when roll <= 95 -> doomed_link(conn, operation)
        _roll -> hard_delete(conn)
      end
    end
  end

  # Members in the random order `Enum.shuffle/1` leaves them, and events taking their
  # positions in that order, one to three per member.
  defp multi_stream_append(conn, session, operation) do
    names = Enum.uniq(for _member <- 1..:rand.uniform(4), do: name(session, operation))

    appends =
      for stream_uuid <- Enum.shuffle(names) do
        {stream_uuid, :any_version, for(_event <- 1..:rand.uniform(3), do: event())}
      end

    answer = Stream.append_to_streams(conn, appends, @append_opts)

    %{
      answer: answer,
      event_ids:
        appends
        |> Enum.flat_map(fn {_stream_uuid, _version, events} -> events end)
        |> Enum.map(& &1.event_id)
    }
  end

  defp plain_append(conn, session, operation) do
    events = for _event <- 1..:rand.uniform(3), do: event()

    answer =
      Stream.append_to_stream(conn, name(session, operation), :any_version, events, @append_opts)

    %{answer: answer, event_ids: Enum.map(events, & &1.event_id)}
  end

  defp link(conn, session, operation, links) do
    event_ids = Enum.take_random(links, :rand.uniform(3))

    answer =
      Stream.link_to_stream(conn, name(session, operation), :any_version, event_ids, @append_opts)

    %{answer: answer, event_ids: []}
  end

  # Its events leave no position to check once deleted, so these report none, and neither
  # does a batch with a doomed member. The other members still keep all of theirs.
  defp doomed_append(conn, session, operation) do
    events = for _event <- 1..:rand.uniform(3), do: event()

    answer =
      case :rand.uniform(2) do
        1 ->
          Stream.append_to_stream(conn, doomed_name(), :any_version, events, @append_opts)

        2 ->
          appends = [
            {doomed_name(), :any_version, events},
            {name(session, operation), :any_version, [event()]}
          ]

          Stream.append_to_streams(conn, Enum.shuffle(appends), @append_opts)
      end

    %{answer: answer, event_ids: []}
  end

  # One event, and into a stream of its own: see the note at the top.
  defp doomed_link(conn, operation) do
    target = "linked-doomed-#{rem(operation, @doomed_streams)}"

    case doomed_event_id(conn) do
      nil ->
        %{answer: :ok, event_ids: []}

      event_id ->
        answer =
          Stream.link_to_stream(conn, target, :any_version, [event_id], @append_opts)

        %{answer: answer, event_ids: []}
    end
  end

  defp doomed_event_id(conn) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        """
        SELECT se.event_id
        FROM stream_events se
        JOIN streams s USING (stream_id)
        WHERE s.stream_uuid = $1
        ORDER BY se.stream_version DESC
        LIMIT 1
        """,
        [doomed_name()]
      )

    case rows do
      [[event_id]] -> UUID.binary_to_string!(event_id)
      [] -> nil
    end
  end

  defp hard_delete(conn) do
    answer =
      case Stream.delete(conn, doomed_name(), :any_version, :hard, @delete_opts) do
        :ok -> :hard_deleted
        {:error, %Postgrex.Error{postgres: %{code: code}}} -> {:error, code}
        {:error, _error} = reply -> reply
      end

    %{answer: answer, event_ids: []}
  end

  defp doomed_name, do: "doomed-#{:rand.uniform(@doomed_streams)}"

  # A name every session uses, one a few sessions race to create as `operation` moves
  # through the same values in each, or a name of this operation's own.
  defp name(session, operation) do
    case :rand.uniform(10) do
      roll when roll <= 5 -> Enum.random(@pool)
      roll when roll <= 8 -> "shared-#{rem(div(operation, 8), @shared_names)}-#{roll}"
      _roll -> "fresh-#{session}-#{operation}-#{:rand.uniform(1_000_000)}"
    end
  end

  defp event(tags \\ ["load"]) do
    %EventData{
      event_id: UUID.uuid4(),
      event_type: "typed",
      data: %{"key" => "value"},
      metadata: %{},
      tags: tags
    }
  end

  defp link_source_event_ids(conn) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        """
        SELECT se.event_id
        FROM stream_events se
        JOIN streams s USING (stream_id)
        WHERE s.stream_uuid = $1
        """,
        [@link_source]
      )

    Enum.map(rows, fn [event_id] -> UUID.binary_to_string!(event_id) end)
  end

  # A real one, so the counter is shown to move on this server, in this database, by the
  # same route the run below relies on.
  defp assert_counter_sees_a_deadlock(context) do
    %{observer: observer, postgrex_config: postgrex_config} = context

    before_deadlock = deadlocks(context)

    for stream_uuid <- ["control-a", "control-b"] do
      :ok = Stream.append_to_stream(observer, stream_uuid, 0, [event([])], @append_opts)
    end

    first = LockSession.start(postgrex_config)
    second = LockSession.start(postgrex_config)

    LockSession.run(first, &lock_row(&1, "control-a"))
    LockSession.run(second, &lock_row(&1, "control-b"))

    waiting = LockSession.start_run(first, &lock_row(&1, "control-b"))
    LockSession.wait_until_blocked(observer, first.backend, second.backend)

    closing = LockSession.start_run(second, &lock_row(&1, "control-a"))

    answers = [LockSession.await_run(waiting), LockSession.await_run(closing)]

    assert [%Postgrex.Error{postgres: %{code: :deadlock_detected}}] =
             Enum.filter(answers, &match?(%Postgrex.Error{}, &1))

    LockSession.rollback(first)
    LockSession.rollback(second)

    close(context, [{1, first.conn, first.backend}, {2, second.conn, second.backend}])

    assert deadlocks(context) == before_deadlock + 1
  end

  defp lock_row(tx, stream_uuid) do
    case Postgrex.query(
           tx,
           "SELECT 1 FROM streams WHERE stream_uuid = $1 FOR NO KEY UPDATE",
           [stream_uuid]
         ) do
      {:ok, %Postgrex.Result{num_rows: 1}} -> :ok
      {:error, error} -> error
    end
  end

  defp close(%{observer: observer}, connections) do
    for {_session, conn, _backend} <- connections, do: GenServer.stop(conn)

    backends = Enum.map(connections, fn {_session, _conn, backend} -> backend end)

    Decree.Store.Wait.until(5_000, fn ->
      assert %Postgrex.Result{rows: []} =
               Postgrex.query!(
                 observer,
                 "SELECT pid FROM pg_stat_activity WHERE pid = ANY($1::int[])",
                 [backends]
               )
    end)
  end

  # Read in autocommit: `stats_fetch_consistency` keeps a snapshot for a transaction.
  defp deadlocks(%{observer: observer, config: config}) do
    %Postgrex.Result{rows: [[deadlocks]]} =
      Postgrex.query!(
        observer,
        "SELECT deadlocks FROM pg_stat_database WHERE datname = $1",
        [config[:database]]
      )

    deadlocks
  end

  defp assert_streams_gapless(conn) do
    assert %Postgrex.Result{rows: []} =
             Postgrex.query!(
               conn,
               """
               SELECT s.stream_uuid, s.stream_version, count(*), min(se.stream_version),
                 max(se.stream_version)
               FROM streams s
               JOIN stream_events se USING (stream_id)
               WHERE s.stream_id <> 0 AND s.stream_uuid NOT LIKE '%doomed-%'
               GROUP BY s.stream_id, s.stream_uuid, s.stream_version
               HAVING count(*) <> s.stream_version
                 OR min(se.stream_version) <> 1
                 OR max(se.stream_version) <> s.stream_version
               """,
               []
             )
  end

  # A hard delete leaves holes in `$all`, and no position past its head. Every event still
  # has its one position, and no position has lost its type row or kept a tag row or a type
  # row with no event, which is what a hard delete's one statement owes.
  defp assert_all_consistent(conn) do
    assert %Postgrex.Result{rows: [[events, positions, head]]} =
             Postgrex.query!(
               conn,
               """
               SELECT
                 (SELECT count(*) FROM events),
                 (SELECT count(*) FROM stream_events WHERE stream_id = 0),
                 (SELECT stream_version FROM streams WHERE stream_id = 0)
               """,
               []
             )

    assert events == positions
    assert positions < head

    assert %Postgrex.Result{rows: [[0, 0, 0]]} =
             Postgrex.query!(
               conn,
               """
               SELECT
                 (SELECT count(*) FROM event_tags t WHERE NOT EXISTS (
                   SELECT 1 FROM stream_events se
                   WHERE se.stream_id = 0 AND se.stream_version = t.position
                 )),
                 (SELECT count(*) FROM event_types t WHERE NOT EXISTS (
                   SELECT 1 FROM stream_events se
                   WHERE se.stream_id = 0 AND se.stream_version = t.position
                 )),
                 (SELECT count(*) FROM stream_events se WHERE se.stream_id = 0 AND NOT EXISTS (
                   SELECT 1 FROM event_types t WHERE t.position = se.stream_version
                 ))
               """,
               []
             )
  end

  # A batch's events hold a contiguous run of positions in input order, which also says
  # that nothing interleaved with it. One that failed left nothing.
  defp assert_batches_contiguous(conn, results) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        conn,
        "SELECT event_id, stream_version FROM stream_events WHERE stream_id = 0",
        []
      )

    positions = Map.new(rows, fn [event_id, position] -> {event_id, position} end)

    for %{answer: answer, event_ids: event_ids} <- results, event_ids != [] do
      found =
        event_ids
        |> Enum.map(&Map.get(positions, UUID.string_to_binary!(&1)))
        |> Enum.reject(&is_nil/1)

      case answer do
        :ok ->
          [first | _rest] = found

          assert found == Enum.to_list(first..(first + length(event_ids) - 1)),
                 inspect(event_ids)

        {:error, _error} ->
          assert found == []
      end
    end
  end
end
