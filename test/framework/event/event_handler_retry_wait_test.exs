defmodule Decree.Event.EventHandlerRetryWaitTest do
  # `{:retry, delay, context}` makes the handler wait for a timer, not sleep: while it waits it
  # keeps serving its mailbox (#7861).
  use Decree.MockEventStoreCase

  import ExUnit.CaptureLog

  alias Decree.Event.Handler
  alias Decree.Event.RetryWaitBatchHandler
  alias Decree.Event.RetryWaitEvent
  alias Decree.Event.RetryWaitHandler
  alias Decree.EventStore.RecordedEvent
  alias Decree.EventStore.Subscription
  alias Decree.Helpers.EventFactory
  alias Decree.MockedApp

  @moduletag :capture_log

  # A wait long enough that a sleeping handler visibly misses every bound below.
  @long_delay 5_000
  # A wait short enough to watch it end.
  @short_delay 300
  # How soon something must happen while the handler waits `@long_delay`.
  @prompt 1_000
  # How long to watch for a retry that must not come after `@short_delay` (about 2x it).
  @no_retry_window 700

  describe "fences (red against :timer.sleep)" do
    setup :start_retry_wait_handler

    test "a DOWN from the subscription during the wait stops the handler at once", ctx do
      %{handler: handler, ref: ref, subscription_ref: subscription_ref, subscription_pid: pid} =
        ctx

      id = deliver(handler, fail_times: 1, delay: @long_delay)
      assert_receive {:attempt, ^id, 1}, @prompt

      send(handler, {:DOWN, subscription_ref, :process, pid, :test_down})

      assert_receive {:DOWN, ^ref, :process, ^handler, :test_down},
                     @prompt,
                     "the handler did not stop within #{@prompt} ms of its subscription's DOWN"
    end

    test "a shutdown exit during the wait stops the handler at once", ctx do
      %{handler: handler, ref: ref} = ctx

      id = deliver(handler, fail_times: 1, delay: @long_delay)
      assert_receive {:attempt, ^id, 1}, @prompt

      Process.exit(handler, :shutdown)

      assert_receive {:DOWN, ^ref, :process, ^handler, :shutdown},
                     @prompt,
                     "the handler did not stop within #{@prompt} ms of a :shutdown exit"
    end

    test "stopping the handler's supervisor child during the wait is not delayed by it", ctx do
      %{handler: handler} = ctx

      id = deliver(handler, fail_times: 1, delay: @long_delay)
      assert_receive {:attempt, ^id, 1}, @prompt

      {micros, :ok} = :timer.tc(fn -> stop_supervised(RetryWaitHandler.child_spec([]).id) end)
      elapsed = div(micros, 1_000)

      assert elapsed < @prompt,
             "stop_supervised/1 took #{elapsed} ms during the wait, expected under #{@prompt} ms"

      refute Process.alive?(handler)
    end

    # Reading taken: a wait cut short runs no retry, so the event's redelivery is its only
    # next attempt and pays the only next wait.
    test "after a lock loss cuts the wait short, only the redelivery retries, with a fresh context",
         ctx do
      %{handler: handler, subscription_pid: pid} = ctx

      event = retry_wait_event(fail_times: 2, delay: @short_delay)
      [recorded] = EventFactory.map_to_recorded_events([event])
      id = event.id

      send(handler, {:events, [recorded]})
      assert_receive {:attempt, ^id, 1}, @prompt
      assert_receive {:retrying, ^id, %{failures: 1}}, @prompt

      send(handler, {:subscription_lock_lost, pid})

      refute_receive {:attempt, ^id, 2},
                     @no_retry_window,
                     "the cut-short wait still ran its retry"

      # The subscription takes its lock back and redelivers from the last acknowledged event.
      send(handler, {:subscribed, pid})
      send(handler, {:events, [recorded]})

      assert_receive {:attempt, ^id, 2}, @prompt
      assert_receive {:retrying, ^id, context}, @prompt

      assert context == %{failures: 1},
             "the redelivery's failure was reported with #{inspect(context)}, " <>
               "expected a fresh context (%{failures: 1})"

      assert_receive {:handled, ^id}, @prompt
    end

    test "after a DOWN cuts the wait short, the retry never runs", ctx do
      %{handler: handler, ref: ref, subscription_ref: subscription_ref, subscription_pid: pid} =
        ctx

      id = deliver(handler, fail_times: 1, delay: @short_delay)
      assert_receive {:attempt, ^id, 1}, @prompt

      send(handler, {:DOWN, subscription_ref, :process, pid, :test_down})

      assert_receive {:DOWN, ^ref, :process, ^handler, :test_down}, @prompt

      refute_received {:attempt, ^id, 2}, "the handler ran the retry before it read the DOWN"
    end

    test "events delivered during the wait are handled after the retried batch, in order", ctx do
      %{handler: handler} = ctx

      a = retry_wait_event(fail_times: 1, delay: @short_delay)
      b = retry_wait_event()
      c = retry_wait_event()

      send(handler, {:events, EventFactory.map_to_recorded_events([a, b], 1)})

      a_id = a.id
      assert_receive {:attempt, ^a_id, 1}, @prompt

      send(handler, {:events, EventFactory.map_to_recorded_events([c], 3)})

      get_state_during_wait!(handler)

      assert collect(:handled, 3) == [a.id, b.id, c.id]
    end

    test "a system message is answered during the wait, and the state shows the wait", ctx do
      %{handler: handler} = ctx

      id = deliver(handler, fail_times: 1, delay: @long_delay)
      assert_receive {:attempt, ^id, 1}, @prompt
      assert_receive {:retrying, ^id, _context}, @prompt

      state = get_state_during_wait!(handler)

      refute is_nil(Map.get(state, :retry_wait)),
             "the handler's state shows no pending retry wait"
    end

    test "a batch handler waits without blocking, retries the batch once, then handles later deliveries" do
      %{handler: handler} = start_handler(RetryWaitBatchHandler)

      a = retry_wait_event(fail_times: 1, delay: @short_delay)
      b = retry_wait_event()
      c = retry_wait_event()
      first_batch = [a.id, b.id]
      later_batch = [c.id]

      send(handler, {:events, EventFactory.map_to_recorded_events([a, b], 1)})

      assert_receive {:batch_attempt, ^first_batch, 1}, @prompt
      assert_receive {:batch_retrying, ^first_batch, %{failures: 1}}, @prompt

      send(handler, {:events, EventFactory.map_to_recorded_events([c], 3)})

      get_state_during_wait!(handler)

      assert collect(:batch_handled, 2) == [first_batch, later_batch]

      assert_received {:batch_attempt, ^first_batch, 2}
      refute_receive {:batch_attempt, ^first_batch, 3}, @no_retry_window
      refute_received {:batch_handled, ^first_batch}
    end

    test "each scheduled retry emits [:decree, :event, :retry, :scheduled] once", ctx do
      %{handler: handler} = ctx

      attach_retry_scheduled()
      %Handler{handler_name: handler_name} = :sys.get_state(handler)

      id = deliver(handler, fail_times: 2, delay: @short_delay)

      for failures <- [1, 2] do
        assert_receive {:retry_scheduled, measurements, metadata},
                       @prompt,
                       "no [:decree, :event, :retry, :scheduled] event for failure #{failures}"

        assert %{delay: @short_delay} = measurements
        assert is_integer(measurements[:system_time])

        assert %{
                 application: MockedApp,
                 handler_module: RetryWaitHandler,
                 handler_name: ^handler_name,
                 context: %{failures: ^failures},
                 recorded_event: %RecordedEvent{data: %RetryWaitEvent{id: ^id}}
               } = metadata
      end

      assert_receive {:handled, ^id}, @prompt
      refute_receive {:retry_scheduled, _measurements, _metadata}, @no_retry_window
    end
  end

  describe "regressions" do
    setup :start_retry_wait_handler

    test "a stale retry-wait timer message sent to an idle handler changes nothing", ctx do
      %{handler: handler, ref: ref} = ctx

      before = :sys.get_state(handler)

      capture_log(fn ->
        send(handler, {:retry_wait, make_ref()})

        assert :sys.get_state(handler) == before
      end)

      id = deliver(handler)
      assert_receive {:handled, ^id}, @prompt
      refute_received {:DOWN, ^ref, :process, ^handler, _reason}
    end

    test "a reset during the wait drops the pending retry", ctx do
      %{handler: handler, ref: ref} = ctx

      stub(MockEventStore, :unsubscribe, fn _event_store_meta, _subscription -> :ok end)

      id = deliver(handler, fail_times: 1, delay: @short_delay)
      assert_receive {:attempt, ^id, 1}, @prompt
      assert_receive {:retrying, ^id, _context}, @prompt

      send(handler, :reset)

      refute_receive {:attempt, ^id, 2}, @no_retry_window, "the reset kept the pending retry"
      refute_received {:DOWN, ^ref, :process, ^handler, _reason}

      assert is_nil(Map.get(:sys.get_state(handler), :retry_wait))
    end

    test "a zero-delay retry still retries", ctx do
      %{handler: handler} = ctx

      id = deliver(handler, fail_times: 2, delay: 0)

      assert_receive {:attempt, ^id, 1}, @prompt
      assert_receive {:retrying, ^id, %{failures: 1}}, @prompt
      assert_receive {:attempt, ^id, 2}, @prompt
      assert_receive {:retrying, ^id, %{failures: 2}}, @prompt
      assert_receive {:attempt, ^id, 3}, @prompt
      assert_receive {:handled, ^id}, @prompt
    end
  end

  defp start_retry_wait_handler(_context), do: start_handler(RetryWaitHandler)

  # The mock subscribes the handler to itself: its subscription pid is the handler, and the
  # monitor ref is the handler's own. Read both before any wait, which a sleep would block.
  defp start_handler(module) do
    handler = start_supervised!(module)
    true = Process.unlink(handler)
    ref = Process.monitor(handler)

    %Handler{
      subscription: %Subscription{subscription_ref: subscription_ref, subscription_pid: pid}
    } =
      :sys.get_state(handler)

    %{handler: handler, ref: ref, subscription_ref: subscription_ref, subscription_pid: pid}
  end

  defp retry_wait_event(opts \\ []) do
    %RetryWaitEvent{
      id: System.unique_integer([:positive]),
      reply_to: :erlang.pid_to_list(self()),
      fail_times: Keyword.get(opts, :fail_times, 0),
      delay: Keyword.get(opts, :delay, 0)
    }
  end

  # Delivers one event, numbered 1, and returns its id.
  defp deliver(handler, opts \\ []) do
    %RetryWaitEvent{id: id} = event = retry_wait_event(opts)

    send(handler, {:events, EventFactory.map_to_recorded_events([event])})

    id
  end

  defp get_state_during_wait!(handler) do
    :sys.get_state(handler, 100)
  catch
    :exit, {:timeout, _call} ->
      flunk("the handler did not answer :sys.get_state/2 within 100 ms during the retry wait")
  end

  # The next `n` replies tagged `tag`, in arrival order.
  defp collect(tag, n) do
    for index <- 1..n do
      receive do
        {^tag, value} -> value
      after
        @prompt ->
          flunk(
            "reply #{index} of #{n} tagged #{inspect(tag)} did not arrive within #{@prompt} ms"
          )
      end
    end
  end

  defp attach_retry_scheduled do
    handler_id = "#{inspect(__MODULE__)}-retry-scheduled-#{System.unique_integer([:positive])}"

    :ok =
      :telemetry.attach(
        handler_id,
        [:decree, :event, :retry, :scheduled],
        fn _event_name, measurements, metadata, reply_to ->
          send(reply_to, {:retry_scheduled, measurements, metadata})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end
end
