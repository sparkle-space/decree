defmodule Decree.Store.Storage.Lock do
  @moduledoc false

  alias Decree.Store.Sql.Statements

  @doc """
  Use two 32-bit key values for advisory locks where the first key acts as the
  namespace.

  The namespace key is derived from the unique `oid` value for the `subscriptions`
  table. The `oid` is unique within a database and differs for identically named
  tables defined in different schemas and on repeat table definitions.

  This change aims to prevent lock collision with application level
  advisory lock usage and other libraries using Postgres advisory locks. Now
  there is a 1 in 2,147,483,647 chance of colliding with other locks.
  """
  def try_acquire_exclusive_lock(conn, key, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.try_advisory_lock(schema)

    case Postgrex.query(conn, query, [key], opts) do
      {:ok, %Postgrex.Result{rows: [[true]]}} -> :ok
      {:ok, %Postgrex.Result{rows: [[false]]}} -> {:error, :lock_already_taken}
      {:error, _error} = reply -> reply
    end
  end

  @doc """
  Like `try_acquire_exclusive_lock/3`, and also returns the backend process id of
  the database session that ran the query (`pg_backend_pid()`), whether or not the
  lock was acquired.

  Returns `{:ok, :acquired, backend_pid}`, `{:ok, :lock_already_taken, backend_pid}`
  or `{:error, reason}`.
  """
  def try_acquire_exclusive_session_lock(conn, key, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.try_session_advisory_lock(schema)

    case Postgrex.query(conn, query, [key], opts) do
      {:ok, %Postgrex.Result{rows: [[true, backend_pid]]}} ->
        {:ok, :acquired, backend_pid}

      {:ok, %Postgrex.Result{rows: [[false, backend_pid]]}} ->
        {:ok, :lock_already_taken, backend_pid}

      {:error, _error} = reply ->
        reply
    end
  end

  @doc """
  Which of the given keys the session running the query holds an advisory lock on
  (as taken by `try_acquire_exclusive_lock/3`), read from `pg_locks`.

  Returns `{:ok, MapSet.t(key)}` or `{:error, reason}`.
  """
  def held_keys(conn, keys, opts) when is_list(keys) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.query_held_advisory_locks(schema)

    case Postgrex.query(conn, query, [keys], opts) do
      {:ok, %Postgrex.Result{rows: rows}} -> {:ok, MapSet.new(rows, fn [key] -> key end)}
      {:error, _error} = reply -> reply
    end
  end

  @doc """
  For each subscription id, whether its `subscriptions` row exists and whether the
  session running the query holds the subscription's advisory lock.

  Returns `{:ok, backend_pid, %{subscription_id => %{row_exists: boolean(),
  lock_held: boolean()}}}`, where `backend_pid` is the session's backend process id
  (`nil` for an empty list of ids), or `{:error, reason}`.
  """
  def subscription_locks(conn, subscription_ids, opts) when is_list(subscription_ids) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.query_subscription_locks(schema)

    case Postgrex.query(conn, query, [subscription_ids], opts) do
      {:ok, %Postgrex.Result{rows: rows}} ->
        backend_pid =
          case rows do
            [[_id, _row_exists, _lock_held, backend_pid] | _rows] -> backend_pid
            [] -> nil
          end

        locks =
          Map.new(rows, fn [id, row_exists, lock_held, _backend_pid] ->
            {id, %{row_exists: row_exists, lock_held: lock_held}}
          end)

        {:ok, backend_pid, locks}

      {:error, _error} = reply ->
        reply
    end
  end

  def unlock(conn, key, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.advisory_unlock(schema)

    case Postgrex.query(conn, query, [key], opts) do
      {:ok, %Postgrex.Result{rows: [[true]]}} -> :ok
      {:ok, %Postgrex.Result{rows: [[false]]}} -> :ok
      {:error, _error} = reply -> reply
    end
  end
end
