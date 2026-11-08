defmodule Decree.Store.Storage.DeleteStream do
  @moduledoc false

  require Logger

  alias Decree.Store.Sql.Statements
  alias Decree.Store.Storage.Stream, as: StreamStorage
  alias Decree.Store.TagIndex

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

  # Six statements in the caller's transaction, each with a snapshot of its own.
  #
  # The stream's row comes first and alone, so everything after it sees what an append or a
  # link committed while holding the row. NO KEY UPDATE, as an append takes it, serialises
  # against appends and links into this stream. The expected version is checked again here:
  # `Stream.delete/5` read it before this transaction, and an append that landed in between
  # would otherwise be erased unseen.
  #
  # The event rows come next and alone, in `event_id` order, FOR UPDATE because NO KEY UPDATE
  # does not conflict with a link's KEY SHARE. A link takes the same rows in the same order
  # after its own stream's row, so the two cannot cycle, and neither can two hard deletes that
  # meet on events through a link stream. An event a concurrent delete removed is skipped.
  #
  # Then the check for a tag index fact, held or linked, which refuses the delete: a disable it
  # removed would leave an earlier enable standing. There is no window. The stream's row holds
  # off any append or link into this stream, the event rows any link of them, and the check's
  # snapshot starts after both waits, so it sees every one that committed.
  #
  # The delete's snapshot then starts after that wait, so a link that committed during it is
  # seen and removed with the event, and every `stream_events` row of an event is touched only
  # while that event's row is held. With the event rows held no link of them is in flight, so
  # the stream's row, after them and on its own because the order of a statement's CTEs is
  # undocumented, finds no link's KEY SHARE on it and nothing still referencing it but the
  # stream's `versioned_snapshots` rows. The one KEY SHARE it can still wait on is a versioned
  # snapshot write's.
  #
  # Those rows go with the stream's row, in any pod: their foreign key to `streams` cascades
  # its DELETE, which is how an old pod's hard delete, with no statement for that table,
  # leaves none behind. A versioned write holds the stream's row FOR KEY SHARE, the lock its
  # foreign key's check also takes, and the row's DELETE conflicts with it. So a write that
  # holds the row commits before the DELETE goes on, and the cascade deletes its row; a write
  # that comes to the row after the DELETE waits for this transaction and then finds no
  # stream (`Snapshot.record_snapshot/3`).
  #
  # The last statement deletes the stream's legacy `snapshots` row, by the `stream_uuid` the
  # deleted row returns, the `source_uuid` a snapshot of it is recorded under. That table
  # has no foreign key, because an old pod writes a row there for any source, stream or not,
  # and takes no lock to do it. The statement also deletes any versioned row, which the
  # cascade has already removed.
  def hard_delete(conn, stream_id, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)
    {expected_version, opts} = Keyword.pop(opts, :expected_version, :any_version)

    with :ok <- StreamStorage.lock_at_version(conn, stream_id, expected_version, schema, opts),
         {:ok, %Postgrex.Result{}} <-
           Postgrex.query(conn, Statements.lock_deleted_events(schema), [stream_id], opts),
         :ok <- no_index_facts(conn, stream_id, schema, opts),
         {:ok, %Postgrex.Result{}} <-
           Postgrex.query(conn, Statements.hard_delete_stream(schema), [stream_id], opts),
         {:ok, %Postgrex.Result{num_rows: 1, rows: [[^stream_id, stream_uuid]]}} <-
           Postgrex.query(conn, Statements.delete_stream_row(schema), [stream_id], opts),
         {:ok, %Postgrex.Result{}} <-
           Postgrex.query(conn, Statements.delete_snapshot(schema), [stream_uuid], opts) do
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

  defp no_index_facts(conn, stream_id, schema, opts) do
    query = Statements.query_index_facts(schema)

    case Postgrex.query(conn, query, [stream_id, TagIndex.fact_tag()], opts) do
      {:ok, %Postgrex.Result{num_rows: 0}} -> :ok
      {:ok, %Postgrex.Result{}} -> {:error, :hard_delete_of_index_fact}
      {:error, _error} = reply -> reply
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

  defp log_failed_hard_delete(stream_id, reason) do
    Logger.warning("Failed to hard delete stream #{inspect(stream_id)} due to: " <> reason)
  end
end
