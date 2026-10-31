defmodule Decree.Store.Storage.ReadQuerySnapshotTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{EventFactory, LockSession, ReadResult}
  alias TestEventStore, as: EventStore

  @query [[tags: ["tag"]]]

  setup %{postgrex_config: postgrex_config} do
    [observer: LockSession.connect(postgrex_config), reader: LockSession.connect(postgrex_config)]
  end

  test "reads past an open append without waiting, and agrees with its own head", context do
    %{observer: observer, postgrex_config: postgrex_config, reader: reader} = context

    :ok = EventStore.append_to_stream("before", 0, [EventFactory.create_event(nil)])

    # Session A holds `$all`: its rows and its bump are both uncommitted.
    session = LockSession.start(postgrex_config)

    LockSession.run(session, fn tx ->
      :ok = EventStore.append_to_stream("first", 0, [event()], conn: tx)
    end)

    assert read(reader) == {1, []}

    second_writer = LockSession.connect(postgrex_config)
    second_backend = LockSession.backend_pid(second_writer)

    second =
      Task.async(fn ->
        EventStore.append_to_stream("second", 0, [event()], conn: second_writer)
      end)

    LockSession.wait_until_blocked(observer, second_backend, session.backend)

    # Neither append is visible, and neither is `$all`'s bump. The read did not queue
    # behind the blocked writer: it took no lock.
    assert read(reader) == {1, []}

    assert {:ok, :ok} = LockSession.commit(session)
    assert :ok = Task.await(second)

    assert read(reader) == {3, [2, 3]}
  end

  # The pool of `TestEventStore` has one connection, which would serialise reads and
  # appends and prove nothing, so every writer and reader here has its own.
  test "never sees a partial batch, a gap, or a head past its own rows", context do
    %{postgrex_config: postgrex_config} = context

    writers = 6
    appends = 25
    readers = 4
    total = writers * appends * 3

    writer_tasks =
      for writer <- 1..writers do
        conn = LockSession.connect(postgrex_config)

        Task.async(fn ->
          for _append <- 1..appends do
            events = [event(), event(), event()]

            :ok =
              EventStore.append_to_stream("stream-#{writer}", :any_version, events, conn: conn)
          end
        end)
      end

    reader_tasks =
      for _reader <- 1..readers do
        conn = LockSession.connect(postgrex_config)

        Task.async(fn -> read_until(conn, total) end)
      end

    Task.await_many(writer_tasks, 60_000)
    Task.await_many(reader_tasks, 60_000)
  end

  # Ends on the writers' last event, which every reader reaches once they are done.
  defp read_until(conn, total) do
    {head, positions} = read(conn)

    assert length(positions) == head
    assert rem(head, 3) == 0
    assert positions == Enum.to_list(1..head//1)
    assert List.last(positions, 0) == head

    if head < total, do: read_until(conn, total)
  end

  defp read(conn) do
    assert {:ok, %ReadResult{after: head, events: events}} =
             EventStore.read_query(@query, conn: conn)

    {head, Enum.map(events, & &1.event_number)}
  end

  defp event do
    %{EventFactory.create_event(nil) | tags: ["tag"]}
  end
end
