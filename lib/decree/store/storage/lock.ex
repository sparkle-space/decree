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
  The backend process id of the database session the connection is on.
  """
  def session_backend_pid(conn, opts) do
    {_schema, opts} = Keyword.pop(opts, :schema)

    case Postgrex.query(conn, Statements.query_session_backend(), [], opts) do
      {:ok, %Postgrex.Result{rows: [[backend_pid]]}} -> {:ok, backend_pid}
      {:error, _error} = reply -> reply
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
