defmodule Decree.Store.Storage.Stream do
  @moduledoc false

  alias Decree.Store.Sql.Statements
  alias Decree.Store.Streams.StreamInfo

  def stream_info(conn, stream_uuid, opts) do
    {schema, opts} = Keyword.pop(opts, :schema)

    query = Statements.query_stream_info(schema)

    case Postgrex.query(conn, query, [stream_uuid], opts) do
      {:ok, %Postgrex.Result{num_rows: 0}} ->
        {:ok, StreamInfo.new(stream_uuid)}

      {:ok, %Postgrex.Result{rows: [row]}} ->
        {:ok, to_stream_info(row)}

      {:error, _error} = reply ->
        reply
    end
  end

  @doc """
  Take the stream's row in FOR NO KEY UPDATE, a statement of its own, and check the expected
  version on what it holds. Whatever read the version before this may be stale, and a stale
  writer must not pass. A soft delete since that read answers `{:error, :stream_deleted}`.

  A row that is gone is `{:error, :stream_not_found}` before any version check, because a
  `StreamInfo` with no id passes one for `:any_version` and for 0. A failure of the statement
  is returned as `{:error, error}`, for the caller to classify.
  """
  def lock_at_version(conn, stream_id, expected_version, schema, opts) do
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

  defp to_stream_info(row) do
    [stream_id, stream_uuid, stream_version, created_at, deleted_at] = row

    stream_version = if is_nil(stream_version), do: 0, else: stream_version
    status = if is_nil(deleted_at), do: :created, else: :deleted

    %StreamInfo{
      stream_uuid: stream_uuid,
      stream_id: stream_id,
      stream_version: stream_version,
      created_at: created_at,
      deleted_at: deleted_at,
      status: status
    }
  end
end
