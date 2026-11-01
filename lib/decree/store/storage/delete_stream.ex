defmodule Decree.Store.Storage.DeleteStream do
  @moduledoc false

  require Logger

  alias Decree.Store.Sql.Statements
  alias Decree.Store.Streams.StreamInfo

  @all_stream_id 0

  def soft_delete(conn, stream_id, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.soft_delete_stream(schema)

    case Postgrex.query(conn, query, [stream_id], opts) do
      {:ok, %Postgrex.Result{num_rows: 1}} ->
        Logger.debug("Soft deleted stream #{inspect(stream_id)}")

        :ok

      {:ok, %Postgrex.Result{num_rows: 0}} ->
        Logger.warning(
          "Failed to soft delete stream #{inspect(stream_id)} due to: stream not found"
        )

        {:error, :stream_not_found}

      {:error, error} = reply ->
        Logger.warning(
          "Failed to soft delete stream #{inspect(stream_id)} due to: " <> inspect(error)
        )

        reply
    end
  end

  # Three statements in the caller's transaction, each with a snapshot of its own.
  #
  # The lock comes first and alone, so the delete's snapshot starts after it is granted and
  # sees everything an append committed while holding the row. NO KEY UPDATE, as an append
  # takes it, serialises against appends and leaves a link's KEY SHARE on this row alone. The
  # expected version is checked again here: `Stream.delete/5` read it before this transaction,
  # and an append that landed in between would otherwise be erased unseen.
  #
  # The stream's row goes last, and on its own because the order of a statement's CTEs is
  # undocumented. A link takes KEY SHARE on an event's row and then on the original stream's,
  # so a key-level delete of the stream's row before the event rows would be a cycle.
  #
  # One cycle stays. A link of several of this stream's events locks event rows in its own
  # row order, and the delete in its scan's, so each can hold a row the other wants. Only one
  # order on both sides would remove that, and it is the link's to take. The victim's
  # transaction rolls back whole, and the delete answers the raw `deadlock_detected` error
  # and can be retried. `hard_delete_test.exs` pins this. Two deletes that meet only through a
  # link stream can cycle on event rows the same way.
  def hard_delete(conn, stream_id, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)
    {expected_version, opts} = Keyword.pop(opts, :expected_version, :any_version)

    with :ok <- lock_at_version(conn, stream_id, expected_version, schema, opts),
         {:ok, %Postgrex.Result{}} <-
           Postgrex.query(conn, Statements.hard_delete_stream(schema), [stream_id], opts),
         {:ok, %Postgrex.Result{num_rows: 1, rows: [[^stream_id]]}} <-
           Postgrex.query(conn, Statements.delete_stream_row(schema), [stream_id], opts) do
      Logger.debug("Hard deleted stream #{inspect(stream_id)}")

      :ok
    else
      {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}} = error} ->
        log_failed_hard_delete(stream_id, inspect(error))

        {:error, :not_supported}

      {:ok, %Postgrex.Result{num_rows: 0}} ->
        log_failed_hard_delete(stream_id, "stream not found")

        {:error, :stream_not_found}

      {:error, error} = reply ->
        log_failed_hard_delete(stream_id, inspect(error))

        reply
    end
  end

  # Two locks and the delete, each a statement of its own, so the delete's snapshot starts
  # after both. This is an append's lock order, the stream's row and then `$all`, and a holder
  # of `$all` never waits on a stream's row, so neither lock can cycle with an append.
  #
  # The subject's row is there so this and a hard delete of the same stream serialise. A hard
  # delete of a link stream holding the subject's events takes no such row and meets this on
  # tag rows, and the shared `position, tag` lock order keeps that from a cycle. `$all` is the
  # append lock (TM-6): with it held, a conditional append either saw these rows before this
  # began, or starts after the commit and cannot see them. It is a lock and never an UPDATE,
  # because `event_notification` fires on UPDATE and would broadcast a phantom `$all`
  # notification.
  def delete_tags(conn, stream_id, tag_kinds, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    lock = Statements.lock_stream(schema)
    delete = Statements.delete_stream_tags(schema)

    with {:ok, %Postgrex.Result{num_rows: 1}} <- Postgrex.query(conn, lock, [stream_id], opts),
         {:ok, %Postgrex.Result{num_rows: 1}} <-
           Postgrex.query(conn, lock, [@all_stream_id], opts),
         {:ok, %Postgrex.Result{num_rows: count}} <-
           Postgrex.query(conn, delete, [stream_id, tag_kinds], opts) do
      Logger.debug("Deleted #{count} tag row(s) of stream #{inspect(stream_id)}")

      {:ok, count}
    else
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:error, :stream_not_found}

      {:error, %Postgrex.Error{postgres: %{code: :feature_not_supported}}} ->
        {:error, :not_supported}

      {:error, error} = reply ->
        Logger.warning(
          "Failed to delete tags of stream #{inspect(stream_id)} due to: " <> inspect(error)
        )

        reply
    end
  end

  # A row that is gone is `stream_not_found` before any version check, because a `StreamInfo`
  # with no id passes one for `:any_version` and for 0.
  defp lock_at_version(conn, stream_id, expected_version, schema, opts) do
    case Postgrex.query(conn, Statements.lock_stream(schema), [stream_id], opts) do
      {:ok, %Postgrex.Result{rows: [[stream_version, deleted_at]]}} ->
        stream = %StreamInfo{
          stream_id: stream_id,
          stream_version: stream_version,
          status: if(is_nil(deleted_at), do: :created, else: :deleted)
        }

        StreamInfo.validate_expected_version(stream, expected_version)

      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:error, :stream_not_found}

      {:error, _error} = reply ->
        reply
    end
  end

  defp log_failed_hard_delete(stream_id, reason) do
    Logger.warning("Failed to hard delete stream #{inspect(stream_id)} due to: " <> reason)
  end
end
