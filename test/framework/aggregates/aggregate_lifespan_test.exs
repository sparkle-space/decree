defmodule Decree.Aggregates.AggregateLifespanTest do
  use Decree.PostgresCase

  alias Decree.Aggregates.{Aggregate, DefaultLifespanRouter, LifespanAggregate, LifespanRouter}
  alias Decree.Aggregates.LifespanAggregate.{Command, Event}
  alias Decree.{DefaultApp, EventStore}
  alias Decree.EventStore.RecordedEvent
  alias Decree.{Registration, UUID}
  alias Decree.Helpers.ProcessHelper

  describe "aggregate lifespan" do
    setup do
      aggregate_uuid = UUID.uuid4()

      start_supervised!(
        {DefaultApp,
         snapshotting: %{LifespanAggregate => [snapshot_every: 2, snapshot_version: 1]}}
      )

      {:ok, ^aggregate_uuid} =
        Decree.Aggregates.Supervisor.open_aggregate(
          DefaultApp,
          LifespanAggregate,
          aggregate_uuid
        )

      pid = Registration.whereis_name(DefaultApp, {DefaultApp, LifespanAggregate, aggregate_uuid})
      ref = Process.monitor(pid)
      reply_to = :erlang.pid_to_list(self())

      %{aggregate_uuid: aggregate_uuid, pid: pid, ref: ref, reply_to: reply_to}
    end

    test "should use default `after_event/1` lifespan when none specified'", %{
      aggregate_uuid: aggregate_uuid,
      ref: ref
    } do
      command = %Command{command_id: UUID.uuid4(), uuid: aggregate_uuid, action: :event}

      assert :ok = DefaultLifespanRouter.dispatch(command, application: DefaultApp)

      refute_receive {:DOWN, ^ref, :process, _pid, _reason}
    end

    test "should use default `after_command/1` lifespan when none specified'", %{
      aggregate_uuid: aggregate_uuid,
      ref: ref
    } do
      command = %Command{command_id: UUID.uuid4(), uuid: aggregate_uuid, action: :noop}

      assert :ok = DefaultLifespanRouter.dispatch(command, application: DefaultApp)

      refute_receive {:DOWN, ^ref, :process, _pid, _reason}
    end

    test "should use default `after_error/1` lifespan when none specified'", %{
      aggregate_uuid: aggregate_uuid,
      ref: ref
    } do
      command = %Command{command_id: UUID.uuid4(), uuid: aggregate_uuid, action: :error}

      {:error, {:failed, nil, nil}} =
        DefaultLifespanRouter.dispatch(command, application: DefaultApp)

      refute_receive {:DOWN, ^ref, :process, _pid, _reason}
    end

    test "should call `after_event/1` callback function", %{
      aggregate_uuid: aggregate_uuid,
      reply_to: reply_to
    } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        action: :event,
        reply_to: reply_to,
        lifespan: :stop
      }

      :ok = LifespanRouter.dispatch(command, application: DefaultApp)

      assert_receive :after_event
      refute_received :after_command
      refute_received :after_error
    end

    test "should shutdown after `after_event/1` timeout", %{
      aggregate_uuid: aggregate_uuid,
      ref: ref,
      reply_to: reply_to
    } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        action: :event,
        reply_to: reply_to,
        lifespan: :stop
      }

      :ok = LifespanRouter.dispatch(command, application: DefaultApp)

      assert_receive {:DOWN, ^ref, :process, _pid, :normal}
    end

    test "honours lifespan after a call to aggregate_state", %{
      aggregate_uuid: aggregate_uuid,
      ref: ref,
      reply_to: reply_to
    } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        reply_to: reply_to,
        action: :event,
        lifespan: 500
      }

      :ok = LifespanRouter.dispatch(command, application: DefaultApp)

      %{lifespan: 500} = Aggregate.aggregate_state(DefaultApp, LifespanAggregate, aggregate_uuid)

      assert_receive {:DOWN, ^ref, :process, _pid, :normal}
    end

    test "honours lifespan after a call to aggregate_version", %{
      aggregate_uuid: aggregate_uuid,
      ref: ref,
      reply_to: reply_to
    } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        reply_to: reply_to,
        action: :event,
        lifespan: 500
      }

      :ok = LifespanRouter.dispatch(command, application: DefaultApp)

      1 = Aggregate.aggregate_version(DefaultApp, LifespanAggregate, aggregate_uuid)

      assert_receive {:DOWN, ^ref, :process, _pid, :normal}
    end

    test "honours lifespan after an info message is handled", %{
      aggregate_uuid: aggregate_uuid,
      pid: pid,
      ref: ref,
      reply_to: reply_to
    } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        reply_to: reply_to,
        action: :event,
        lifespan: 500
      }

      :ok = LifespanRouter.dispatch(command, application: DefaultApp)

      send(pid, :unexpected_message)

      assert_receive {:DOWN, ^ref, :process, _pid, :normal}
    end

    test "should call `after_command/1` callback function when no domain events", %{
      aggregate_uuid: aggregate_uuid,
      reply_to: reply_to
    } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        action: :noop,
        reply_to: reply_to,
        lifespan: :stop
      }

      :ok = LifespanRouter.dispatch(command, application: DefaultApp)

      assert_receive :after_command
      refute_received :after_event
      refute_received :after_error
    end

    test "should shutdown after `after_command/1` timeout", %{
      aggregate_uuid: aggregate_uuid,
      ref: ref,
      reply_to: reply_to
    } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        action: :noop,
        reply_to: reply_to,
        lifespan: :stop
      }

      :ok = LifespanRouter.dispatch(command, application: DefaultApp)

      assert_receive {:DOWN, ^ref, :process, _pid, :normal}
    end

    test "should call `after_error/1` callback function on error", %{
      aggregate_uuid: aggregate_uuid,
      reply_to: reply_to
    } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        action: :error,
        reply_to: reply_to,
        lifespan: :stop
      }

      {:error, {:failed, _reply_to, :stop}} =
        LifespanRouter.dispatch(command, application: DefaultApp)

      assert_receive :after_error
      refute_received :after_event
      refute_received :after_command
    end

    test "should shutdown after `after_error/1` timeout", %{
      aggregate_uuid: aggregate_uuid,
      ref: ref,
      reply_to: reply_to
    } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        action: :error,
        reply_to: reply_to,
        lifespan: :stop
      }

      {:error, {:failed, _reply_to, :stop}} =
        LifespanRouter.dispatch(command, application: DefaultApp)

      assert_receive {:DOWN, ^ref, :process, _pid, :normal}
    end

    # Mailbox ordering replaces wall-clock timing: both commands are queued while the
    # aggregate is suspended, so the second is already waiting when the first returns
    # its 100 ms lifespan, and that timeout cannot fire. Each finite lifespan replaces
    # the last one's: the second's 60 s replaces the first's 100 ms, and the third's
    # 100 ms replaces the 60 s. The 60 s is far past any stall, so the refute and the
    # same-pid assert cannot race it; no elapsed time is measured.
    test "should not shutdown if another command executed", %{
      aggregate_uuid: aggregate_uuid,
      pid: pid,
      ref: ref,
      reply_to: reply_to
    } do
      dispatch = fn lifespan ->
        fn ->
          command = %Command{
            command_id: UUID.uuid4(),
            uuid: aggregate_uuid,
            action: :noop,
            reply_to: reply_to,
            lifespan: lifespan
          }

          LifespanRouter.dispatch(command, application: DefaultApp)
        end
      end

      [first, second] = ProcessHelper.queue_commands(pid, [dispatch.(100), dispatch.(60_000)])

      :ok = Task.await(first)
      :ok = Task.await(second)

      assert_receive :after_command
      assert_receive :after_command

      # The fence's stalled host: past the first command's lifespan.
      Process.sleep(150)

      name = {DefaultApp, LifespanAggregate, aggregate_uuid}
      assert Registration.whereis_name(DefaultApp, name) == pid

      refute_receive {:DOWN, ^ref, :process, _pid, _reason}, 200

      :ok = dispatch.(100).()

      assert_receive :after_command
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
    end

    test "should reopen the aggregate to execute command if it is stopped by the previous command",
         %{
           aggregate_uuid: aggregate_uuid,
           ref: ref
         } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        action: :noop,
        lifespan: :stop
      }

      tasks =
        for _i <- 1..3 do
          Task.async(fn ->
            :ok = LifespanRouter.dispatch(command, application: DefaultApp)
          end)
        end

      Task.yield_many(tasks)

      assert_receive {:DOWN, ^ref, :process, _pid, :normal}
    end

    test "should reopen the aggregate to execute command if it is stopped by an error",
         %{
           aggregate_uuid: aggregate_uuid,
           ref: ref
         } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        action: :error,
        lifespan: :stop
      }

      tasks =
        for _i <- 1..3 do
          Task.async(fn ->
            {:error, {:failed, nil, :stop}} =
              LifespanRouter.dispatch(command, application: DefaultApp)
          end)
        end

      Task.yield_many(tasks)

      assert_receive {:DOWN, ^ref, :process, _pid, :normal}
    end

    test "should stop process when requested", %{
      aggregate_uuid: aggregate_uuid,
      ref: ref
    } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        action: :noop,
        lifespan: :stop
      }

      :ok = LifespanRouter.dispatch(command, application: DefaultApp)

      assert_receive {:DOWN, ^ref, :process, _pid, :normal}
    end

    test "should adhere to aggregate lifespan after taking snapshot",
         %{
           aggregate_uuid: aggregate_uuid,
           ref: ref
         } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        action: :event,
        lifespan: :infinity
      }

      :ok = LifespanRouter.dispatch(command, application: DefaultApp)

      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        action: :event,
        lifespan: :stop
      }

      :ok = LifespanRouter.dispatch(command, application: DefaultApp)

      assert_receive {:DOWN, ^ref, :process, _pid, :normal}
      assert {:ok, _snapshot} = EventStore.read_snapshot(DefaultApp, aggregate_uuid, 1)
    end

    test "should adhere to aggregate lifespan when receiving published events after taking snapshot",
         %{
           aggregate_uuid: aggregate_uuid,
           pid: pid,
           ref: ref
         } do
      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        action: :event,
        lifespan: :infinity
      }

      :ok = LifespanRouter.dispatch(command, application: DefaultApp)

      command = %Command{
        command_id: UUID.uuid4(),
        uuid: aggregate_uuid,
        action: :event,
        lifespan: 1
      }

      :ok = LifespanRouter.dispatch(command, application: DefaultApp)

      assert Process.alive?(pid)

      events = EventStore.stream_forward(DefaultApp, aggregate_uuid) |> Enum.to_list()

      # Publish events to aggregate after taking snapshot
      send(pid, {:events, events})

      assert_receive {:DOWN, ^ref, :process, _pid, :normal}
      assert {:ok, _snapshot} = EventStore.read_snapshot(DefaultApp, aggregate_uuid, 1)
    end

    test "should use `:infinity` lifespan by default", %{
      aggregate_uuid: aggregate_uuid,
      pid: pid,
      ref: ref
    } do
      events = [
        %RecordedEvent{
          event_id: UUID.uuid4(),
          stream_version: 1,
          data: %Event{uuid: aggregate_uuid}
        }
      ]

      # Simulate sending an event directly to the aggregate process
      send(pid, {:events, events})

      refute_receive {:DOWN, ^ref, :process, ^pid, _}
    end
  end
end
