defmodule Decree.Event.RetryWaitBatchHandler do
  @moduledoc false

  # A batch handler (`batch_size`, no `batch_timeout`, so every delivery is handled at once)
  # whose batches fail while the first event's `fail_times` is not used up, and are retried
  # with `{:retry, delay, context}`, `delay` taken from the first event.
  #
  # Replies to the first event's `reply_to`, with `ids` the batch's event ids in order:
  #   * `{:batch_attempt, ids, n}` on every `handle_batch/1` call (n per batch, in this process)
  #   * `{:batch_handled, ids}` when an attempt succeeds
  #   * `{:batch_retrying, ids, context}` from `error/3`, with the context it answers

  use Decree.Event.Handler,
    application: Decree.MockedApp,
    name: __MODULE__,
    batch_size: 10,
    personal_fields: :opaque

  alias Decree.Event.FailureContext
  alias Decree.Event.RetryWaitEvent

  @impl true
  def handle_batch([{%RetryWaitEvent{} = first, _metadata} | _rest] = events) do
    %RetryWaitEvent{reply_to: reply_to, fail_times: fail_times} = first
    ids = Enum.map(events, fn {%RetryWaitEvent{id: id}, _metadata} -> id end)

    attempt = Process.get({:retry_wait_batch_attempts, ids}, 0) + 1
    Process.put({:retry_wait_batch_attempts, ids}, attempt)

    reply(reply_to, {:batch_attempt, ids, attempt})

    if attempt <= fail_times do
      {:error, :not_yet}
    else
      reply(reply_to, {:batch_handled, ids})

      :ok
    end
  end

  @impl true
  def error(
        {:error, :not_yet},
        [%RetryWaitEvent{reply_to: reply_to, delay: delay} | _rest] = events,
        %FailureContext{} = failure_context
      ) do
    %FailureContext{context: context} = failure_context

    ids = Enum.map(events, fn %RetryWaitEvent{id: id} -> id end)
    context = Map.update(context, :failures, 1, &(&1 + 1))

    reply(reply_to, {:batch_retrying, ids, context})

    {:retry, delay, context}
  end

  defp reply(reply_to, message), do: send(:erlang.list_to_pid(reply_to), message)
end
