defmodule Decree.Store.LockSession do
  @moduledoc false

  # One database session holding one transaction open, driven a step at a time, so
  # a test can say which locks are held while another session runs. State is read
  # from Postgres (`pg_blocking_pids`), never from timing.

  import ExUnit.Assertions

  alias Decree.Store.Wait

  defstruct [:conn, :backend, :task]

  @await_timeout 15_000

  def start(postgrex_config) do
    conn = connect(postgrex_config)
    backend = backend_pid(conn)

    task =
      Task.async(fn ->
        Postgrex.transaction(conn, &loop/1, timeout: :infinity)
      end)

    %__MODULE__{conn: conn, backend: backend, task: task}
  end

  # A connection of its own, so that an append can block while the test goes on.
  def connect(postgrex_config) do
    {:ok, conn} = Postgrex.start_link(Keyword.put(postgrex_config, :pool_size, 1))

    conn
  end

  def backend_pid(conn) do
    %Postgrex.Result{rows: [[pid]]} = Postgrex.query!(conn, "SELECT pg_backend_pid()", [])

    pid
  end

  # Runs `fun` with the open transaction and answers what it returned.
  def run(%__MODULE__{} = session, fun) do
    session |> start_run(fun) |> await_run()
  end

  def start_run(%__MODULE__{task: task}, fun) do
    ref = make_ref()

    send(task.pid, {:run, self(), ref, fun})

    ref
  end

  def await_run(ref) do
    assert_receive {^ref, reply}, @await_timeout

    reply
  end

  def lock_row(%__MODULE__{} = session, schema, stream_id) do
    run(session, fn tx ->
      Postgrex.query!(
        tx,
        ~s(SELECT 1 FROM "#{schema}".streams WHERE stream_id = $1 FOR NO KEY UPDATE),
        [stream_id]
      )
    end)
  end

  def commit(%__MODULE__{task: task}) do
    send(task.pid, :commit)

    Task.await(task, @await_timeout)
  end

  def rollback(%__MODULE__{task: task}) do
    send(task.pid, :rollback)

    Task.await(task, @await_timeout)
  end

  # Waits until the session `backend` is blocked by the session `blocker`.
  def wait_until_blocked(observer, backend, blocker) do
    Wait.until(5_000, fn ->
      %Postgrex.Result{rows: [[blockers]]} =
        Postgrex.query!(observer, "SELECT pg_blocking_pids($1)", [backend])

      assert blocker in blockers
    end)
  end

  defp loop(tx) do
    receive do
      {:run, from, ref, fun} ->
        send(from, {ref, fun.(tx)})
        loop(tx)

      :commit ->
        :ok

      :rollback ->
        Postgrex.rollback(tx, :rolled_back)
    end
  end
end
