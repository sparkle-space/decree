defmodule Decree.Event.RetryWaitHandler do
  @moduledoc false

  # A single-event handler whose events fail `fail_times` times and are retried with
  # `{:retry, delay, context}` (always the delayed answer, `delay` 0 included).
  #
  # Replies to the event's `reply_to`:
  #   * `{:attempt, id, n}` on every `handle/2` call (n counts from 1, per id, in this process)
  #   * `{:handled, id}` when an attempt succeeds
  #   * `{:retrying, id, context}` from `error/3`, with the context it answers

  use Decree.Event.Handler,
    application: Decree.MockedApp,
    name: __MODULE__

  alias Decree.Event.FailureContext
  alias Decree.Event.RetryWaitEvent

  def handle(%RetryWaitEvent{} = event, _metadata) do
    %RetryWaitEvent{id: id, reply_to: reply_to, fail_times: fail_times} = event

    attempt = Process.get({:retry_wait_attempts, id}, 0) + 1
    Process.put({:retry_wait_attempts, id}, attempt)

    reply(reply_to, {:attempt, id, attempt})

    if attempt <= fail_times do
      {:error, :not_yet}
    else
      reply(reply_to, {:handled, id})

      :ok
    end
  end

  def error({:error, :not_yet}, %RetryWaitEvent{} = event, %FailureContext{} = failure_context) do
    %RetryWaitEvent{id: id, reply_to: reply_to, delay: delay} = event
    %FailureContext{context: context} = failure_context

    context = Map.update(context, :failures, 1, &(&1 + 1))

    reply(reply_to, {:retrying, id, context})

    {:retry, delay, context}
  end

  defp reply(reply_to, message), do: send(:erlang.list_to_pid(reply_to), message)
end
