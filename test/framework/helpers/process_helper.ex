defmodule Decree.Helpers.ProcessHelper do
  @moduledoc false

  import ExUnit.Assertions

  alias Decree.Helpers.Wait
  alias Decree.Registration

  @doc """
  Stop the given process with a non-normal exit reason.
  """
  def shutdown(pid, reason \\ :shutdown)

  def shutdown(pid, reason) when is_pid(pid) do
    ref = Process.monitor(pid)

    Process.unlink(pid)
    Process.exit(pid, reason)

    assert_receive {:DOWN, ^ref, _, _, _}, 5_000
  end

  def shutdown(name, reason) when is_atom(name) do
    case Process.whereis(name) do
      nil -> :ok
      pid -> shutdown(pid, reason)
    end
  end

  @doc """
  Stop a given aggregate process.
  """
  def shutdown_aggregate(application, aggregate_module, aggregate_uuid) do
    name = {application, aggregate_module, aggregate_uuid}

    Registration.whereis_name(application, name) |> shutdown()

    # Wait until process removed from registry
    Wait.until(fn ->
      assert Registration.whereis_name(application, name) == :undefined
    end)
  end

  @doc """
  Start each function as a task while the aggregate is suspended, waiting for the
  task's command to sit in the aggregate's mailbox before starting the next, so the
  aggregate runs the commands in the order given once resumed. Returns the tasks in
  the same order.
  """
  def queue_commands(pid, funs) when is_pid(pid) and is_list(funs) do
    :ok = :sys.suspend(pid)

    try do
      funs
      |> Enum.with_index(1)
      |> Enum.map(fn {fun, count} ->
        task = Task.async(fun)

        wait_for_queued_commands(pid, count)

        task
      end)
    after
      :sys.resume(pid)
    end
  end

  @doc """
  Wait until `count` commands sit in the aggregate's mailbox.
  """
  def wait_for_queued_commands(pid, count) do
    Wait.until(fn ->
      {:messages, messages} = Process.info(pid, :messages)

      assert Enum.count(messages, &match?({:"$gen_call", _from, {:execute_command, _}}, &1)) ==
               count
    end)
  end
end
