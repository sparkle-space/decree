defmodule Decree.Store.Notifications.PublisherUndecodableEventTest do
  use Decree.Store.StorageCase

  # An event whose stored bytes the store's serializer cannot decode is a raise in the
  # publisher's deserialize, not an error it is answered: the publisher stops that
  # notification's batch at the event, broadcasts the events before it and keeps running, so
  # the notifications tree (`one_for_all`) is not restarted, and a run of them does not
  # exhaust its restart intensity and restart the whole store tree under a concurrent append
  # (sparkle-space/sparklespace#8349).
  #
  # Each event is planted through `Appender.append/4` with bytes the JSON serializer cannot
  # decode, a real append, so the `streams` table's `event_notification` trigger fires and the
  # publisher reads it.

  import ExUnit.CaptureLog

  alias Decree.Store.{
    CrashingTypeProvider,
    EventData,
    EventFactory,
    ExitedAnswerEvent,
    ExitingSerializer,
    ExitOnAskEvent,
    FiniteTypeProvider,
    JsonSerializer,
    PubSub,
    RecordedEvent,
    SecondAskCrashingSerializer,
    UUID
  }

  alias Decree.Store.Notifications.Publisher
  alias Decree.Store.Storage.{Appender, Initializer}

  @marker "undecodable-payload-marker"
  @publisher Module.concat([TestEventStore, Publisher])

  test "an undecodable event is skipped and the publisher publishes the next append", %{
    conn: conn,
    schema: schema
  } do
    publisher = Process.whereis(@publisher)
    ref = Process.monitor(publisher)

    bad = UUID.uuid4()
    good = UUID.uuid4()
    :ok = PubSub.subscribe(TestEventStore, good)

    log =
      capture_log(fn ->
        plant_undecodable(conn, schema, bad)

        :ok = TestEventStore.append_to_stream(good, 0, EventFactory.create_events(1))

        # The publisher takes one notification at a time, in order: the good append's
        # broadcast comes after it handled both of the planted event's notifications.
        assert_receive {:events, [%RecordedEvent{stream_uuid: ^good}]}
      end)

    refute_received {:DOWN, ^ref, :process, _pid, _reason}
    assert Process.whereis(@publisher) == publisher

    assert log =~
             "Decree.Store notifications failed to deserialize event 1 of " <>
               inspect(suffix(bad)) <> " due to: Jason.DecodeError"

    refute log =~ @marker
    refute log =~ bad
  end

  test "the events before an undecodable one in the same append are broadcast", %{
    conn: conn,
    schema: schema
  } do
    publisher = Process.whereis(@publisher)
    ref = Process.monitor(publisher)

    stream_uuid = UUID.uuid4()
    :ok = PubSub.subscribe(TestEventStore, stream_uuid, notices: true)

    [first, %RecordedEvent{} = second, third] =
      EventFactory.create_recorded_events(3, stream_uuid)

    second = %RecordedEvent{second | data: <<0>> <> @marker}

    log =
      capture_log(fn ->
        # One append, so one notification for the stream, and no later append.
        :ok = Appender.append(conn, nil, [first, second, third], schema: schema)

        assert_receive {:events, [%RecordedEvent{stream_uuid: ^stream_uuid} = event]}
        assert %RecordedEvent{stream_version: 1, data: %EventFactory.Event{event: 1}} = event
      end)

    refute_receive {:events, _events}, 300
    refute_received {:events_pending, _event_number}

    refute_received {:DOWN, ^ref, :process, _pid, _reason}
    assert Process.whereis(@publisher) == publisher

    assert log =~
             "Decree.Store notifications failed to deserialize event 2 of " <>
               inspect(suffix(stream_uuid)) <> " due to: Jason.DecodeError"

    refute log =~ @marker
    refute log =~ stream_uuid
  end

  test "a run of undecodable events does not restart the store's tree", %{
    conn: conn,
    schema: schema
  } do
    # A restart of the store's tree reaches the test as an exit signal: trapped, so the
    # assertions below report what changed.
    Process.flag(:trap_exit, true)

    before = store_children()

    probe = UUID.uuid4()
    writer = UUID.uuid4()

    capture_log(fn ->
      appends =
        Task.async(fn ->
          for version <- 0..199 do
            try do
              TestEventStore.append_to_stream(writer, version, EventFactory.create_events(1))
            catch
              :exit, reason -> {:exit, reason}
            end
          end
        end)

      # Each plant lands once the notifications are delivered again (a probe append is
      # broadcast), so on code that crashes each is one restart of the notifications tree,
      # and five in well under five seconds exceed its intensity (3 in 5 s).
      for _plant <- 1..5 do
        plant_undecodable(conn, schema, UUID.uuid4())
        await_published(probe)
      end

      results = Task.await(appends, 30_000)

      assert Enum.uniq(results) == [:ok]
    end)

    assert store_children() == before

    assert :ok = TestEventStore.append_to_stream(UUID.uuid4(), 0, EventFactory.create_events(1))
  end

  # A row the read itself cannot turn into a `RecordedEvent` raises in the storage read, before
  # any deserialize. A store whose schema was initialised with `correlation_id_type: "text"`
  # and is run with the default `"uuid"` reads each text id as a uuid's 16 bytes, and an id of
  # any other length raises `FunctionClauseError` in the reader's id conversion. The publisher
  # skips that notification and keeps running.
  describe "a row the read cannot adapt" do
    @text_schema "publisher_text_ids"
    @text_store :publisher_text_ids_store
    @id_marker "id-marker-not-a-uuid"

    setup %{conn: conn, config: config} do
      drop = ~s[DROP SCHEMA IF EXISTS "#{@text_schema}" CASCADE]
      Postgrex.query!(conn, drop, [])
      Postgrex.query!(conn, ~s[CREATE SCHEMA "#{@text_schema}"], [])

      text_config =
        Keyword.merge(config,
          schema: @text_schema,
          correlation_id_type: "text",
          causation_id_type: "text"
        )

      {:ok, :ok} = Initializer.run!(conn, text_config)

      on_exit(fn ->
        {:ok, observer} = Postgrex.start_link(TestEventStore.config())
        Postgrex.query!(observer, drop, [])
        GenServer.stop(observer)
      end)

      start_supervised!({TestEventStore, name: @text_store, schema: @text_schema},
        id: @text_store
      )

      :ok
    end

    test "is skipped and the publisher publishes the next append", %{conn: conn} do
      publisher = Process.whereis(Module.concat([@text_store, Publisher]))
      ref = Process.monitor(publisher)

      bad = UUID.uuid4()
      good = UUID.uuid4()
      :ok = PubSub.subscribe(@text_store, good)

      [%RecordedEvent{} = event] = EventFactory.create_recorded_events(1, bad)
      event = %RecordedEvent{event | correlation_id: @id_marker, causation_id: nil}

      log =
        capture_log(fn ->
          :ok =
            Appender.append(conn, nil, [event],
              schema: @text_schema,
              correlation_id_type: "text",
              causation_id_type: "text"
            )

          # Read by the same publisher, so with no id for it to convert.
          %EventData{} = good_event = EventFactory.create_event(nil)
          good_event = %EventData{good_event | correlation_id: nil, causation_id: nil}

          :ok = TestEventStore.append_to_stream(good, 0, [good_event], name: @text_store)

          assert_receive {:events, [%RecordedEvent{stream_uuid: ^good}]}
        end)

      refute_received {:DOWN, ^ref, :process, _pid, _reason}
      assert Process.whereis(Module.concat([@text_store, Publisher])) == publisher

      assert log =~
               "Decree.Store notifications failed to read events due to: FunctionClauseError"

      refute log =~ @id_marker
      refute log =~ bad
    end
  end

  # A type provider that crashes in `known_type?/1` with an exception other than the ones it
  # reads as an unknown type is a crash, not an undecodable event, and still raises (decree
  # 772c21f): the publisher goes down rather than cutting the batch. The crash is confined to
  # a store instance of its own, run with the framework's serializer over the provider; the
  # provider is global, so this module is not async.
  describe "a type provider that crashes" do
    @crash_store :publisher_crashing_provider_store

    setup do
      FiniteTypeProvider.configure!(CrashingTypeProvider)

      start_supervised!(
        {TestEventStore, name: @crash_store, serializer: Decree.Serialization.JsonSerializer},
        id: @crash_store
      )

      :ok
    end

    @tag capture_log: true
    test "takes the publisher down", %{conn: conn, schema: schema} do
      publisher = Process.whereis(Module.concat([@crash_store, Publisher]))
      assert is_pid(publisher)
      ref = Process.monitor(publisher)

      [event] = EventFactory.create_recorded_events(1, UUID.uuid4())

      :ok = Appender.append(conn, nil, [event], schema: schema)

      assert_receive {:DOWN, ^ref, :process, ^publisher,
                      {%RuntimeError{message: "type provider crashed"}, _stacktrace}},
                     2_000
    end
  end

  # Whether an event's type is known is asked once, outside the rescue: a serializer whose
  # ask passes once and then crashes takes the publisher down at its second ask, the next
  # event's, rather than having that crash cut the batch as an undecodable event.
  describe "a serializer whose type check crashes on its second ask" do
    @second_ask_store :publisher_second_ask_store

    setup do
      SecondAskCrashingSerializer.arm!()

      start_supervised!(
        {TestEventStore, name: @second_ask_store, serializer: SecondAskCrashingSerializer},
        id: @second_ask_store
      )

      :ok
    end

    test "takes the publisher down", %{conn: conn, schema: schema} do
      publisher = Process.whereis(Module.concat([@second_ask_store, Publisher]))
      assert is_pid(publisher)
      ref = Process.monitor(publisher)

      events = EventFactory.create_recorded_events(2, UUID.uuid4())

      log =
        capture_log(fn ->
          :ok = Appender.append(conn, nil, events, schema: schema)

          assert_receive {:DOWN, ^ref, :process, ^publisher,
                          {%RuntimeError{message: "known_type? crashed on call 2"}, _stacktrace}},
                         2_000
        end)

      refute log =~ "failed to deserialize"
    end
  end

  # An exit in the serializer, as from a call to a dead or slow process, is not a crash of the
  # type check: whether it comes from the type check or from a known event's deserialize, the
  # batch is cut at that event as at an undecodable one, and the publisher keeps running. The
  # exits are confined to a store instance of its own, so the test subscribes there; the
  # store's own instance reads the same rows and decodes them.
  describe "a serializer that exits" do
    @exit_store :publisher_exiting_serializer_store

    setup do
      start_supervised!(
        {TestEventStore, name: @exit_store, serializer: ExitingSerializer},
        id: @exit_store
      )

      :ok
    end

    test "in a known event's deserialize cuts the batch there", %{conn: conn, schema: schema} do
      [first, %RecordedEvent{} = second, third] =
        EventFactory.create_recorded_events(3, UUID.uuid4())

      second = %RecordedEvent{
        second
        | data:
            JsonSerializer.serialize(%EventFactory.Event{event: ExitingSerializer.data_marker()})
      }

      assert_exit_cut(conn, schema, [first, second, third], ExitingSerializer.data_marker())
    end

    test "in the type check cuts the batch there", %{conn: conn, schema: schema} do
      [first, %RecordedEvent{} = second, third] =
        EventFactory.create_recorded_events(3, UUID.uuid4())

      second = %RecordedEvent{
        second
        | event_type: "Elixir.Decree.Store.ExitOnAskEvent",
          data: JsonSerializer.serialize(%ExitOnAskEvent{event: 2})
      }

      assert_exit_cut(conn, schema, [first, second, third], ExitingSerializer.type_marker())
    end

    # A type check's answer is data: a truthy answer shaped like a caught exit is a known
    # type, and the event is deserialized and broadcast like any other.
    test "in name only, as a type check's answer, does not cut the batch", %{
      conn: conn,
      schema: schema
    } do
      [first, %RecordedEvent{} = second] = EventFactory.create_recorded_events(2, UUID.uuid4())
      %RecordedEvent{stream_uuid: stream_uuid} = first

      second = %RecordedEvent{
        second
        | event_type: "Elixir.Decree.Store.ExitedAnswerEvent",
          data: JsonSerializer.serialize(%ExitedAnswerEvent{event: 2})
      }

      publisher = Process.whereis(Module.concat([@exit_store, Publisher]))
      assert is_pid(publisher)
      ref = Process.monitor(publisher)

      :ok = PubSub.subscribe(@exit_store, stream_uuid, notices: true)

      log =
        capture_log(fn ->
          :ok = Appender.append(conn, nil, [first, second], schema: schema)

          assert_receive {:events, [%RecordedEvent{stream_version: 1}, event]}
          assert %RecordedEvent{stream_version: 2, data: %ExitedAnswerEvent{event: 2}} = event
        end)

      refute_receive {:events, _events}, 300
      refute_received {:events_pending, _event_number}
      refute_received {:DOWN, ^ref, :process, _pid, _reason}
      assert Process.whereis(Module.concat([@exit_store, Publisher])) == publisher

      refute log =~ "failed to deserialize"
      refute log =~ "stopped a batch"
      refute log =~ "exited_answer_marker"
    end
  end

  # One append of three events whose second exits in the exiting store's serializer, and no
  # later append: the first is broadcast, the rest are not, no position notice is sent, the
  # publisher survives, and the cut is logged by the exit's reduced class.
  defp assert_exit_cut(conn, schema, [first | _rest] = events, marker) do
    %RecordedEvent{stream_uuid: stream_uuid} = first

    publisher = Process.whereis(Module.concat([@exit_store, Publisher]))
    assert is_pid(publisher)
    ref = Process.monitor(publisher)

    :ok = PubSub.subscribe(@exit_store, stream_uuid, notices: true)

    log =
      capture_log(fn ->
        :ok = Appender.append(conn, nil, events, schema: schema)

        assert_receive {:events, [%RecordedEvent{stream_uuid: ^stream_uuid} = event]}
        assert %RecordedEvent{stream_version: 1, data: %EventFactory.Event{event: 1}} = event
      end)

    refute_receive {:events, _events}, 300
    refute_received {:events_pending, _event_number}

    refute_received {:DOWN, ^ref, :process, _pid, _reason}
    assert Process.whereis(Module.concat([@exit_store, Publisher])) == publisher

    assert log =~
             "Decree.Store notifications failed to deserialize event 2 of " <>
               inspect(suffix(stream_uuid)) <> " due to: {:noproc, :redacted}"

    refute log =~ marker
    refute log =~ stream_uuid
  end

  defp suffix(stream_uuid), do: "..." <> String.slice(stream_uuid, -8, 8)

  # A real append of one event of a known type whose data is not JSON.
  defp plant_undecodable(conn, schema, stream_uuid) do
    [%RecordedEvent{} = event] = EventFactory.create_recorded_events(1, stream_uuid)

    :ok =
      Appender.append(conn, nil, [%RecordedEvent{event | data: <<0>> <> @marker}], schema: schema)
  end

  # Append to the probe stream until an append is broadcast, subscribing again each time: a
  # restarted store tree starts a new PubSub registry. Each probe event has its own id, so a
  # late broadcast of an earlier probe does not count. Gives up after three seconds, so a tree
  # that does not come back fails on the children below, not here.
  defp await_published(probe, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 3_000
    event_id = UUID.uuid4()

    _ = PubSub.subscribe(TestEventStore, probe)

    _ =
      TestEventStore.append_to_stream(probe, :any_version, [EventFactory.create_event(event_id)])

    receive do
      {:events, [%RecordedEvent{event_id: ^event_id}]} -> :ok
    after
      200 ->
        if System.monotonic_time(:millisecond) < deadline,
          do: await_published(probe, deadline),
          else: :gave_up
    end
  end

  # The store's top supervisor's children and the notifications supervisor's, by id: a
  # restart of either tree changes a pid.
  defp store_children do
    top = Supervisor.which_children(TestEventStore)

    notifications =
      Enum.find_value(top, fn {id, pid, _type, _modules} ->
        if id == Module.concat(TestEventStore, Decree.Store.Notifications.Supervisor), do: pid
      end)

    %{
      top: Map.new(top, fn {id, pid, _type, _modules} -> {id, pid} end),
      notifications:
        notifications
        |> Supervisor.which_children()
        |> Map.new(fn {id, pid, _type, _modules} -> {id, pid} end)
    }
  end
end
