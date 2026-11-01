defmodule Decree.Store.StoreRows do
  @moduledoc false

  # What a store holds, read from Postgres through `observer`, a connection of the test's
  # own. Each function takes a context with `:observer` and `:schema`.

  import ExUnit.Assertions

  def stream_id(context, stream_uuid) do
    context
    |> rows(~s(SELECT stream_id FROM "#{context.schema}".streams WHERE stream_uuid = $1), [
      stream_uuid
    ])
    |> List.first([nil])
    |> hd()
  end

  def stream_version(context, stream_uuid) do
    [[version]] =
      rows(
        context,
        ~s(SELECT stream_version FROM "#{context.schema}".streams WHERE stream_uuid = $1),
        [stream_uuid]
      )

    version
  end

  def stream_versions(context, stream_uuid) do
    context
    |> rows(
      ~s(SELECT se.stream_version FROM "#{context.schema}".stream_events se) <>
        ~s[ JOIN "#{context.schema}".streams s USING (stream_id)] <>
        " WHERE s.stream_uuid = $1 ORDER BY se.stream_version",
      [stream_uuid]
    )
    |> Enum.map(&hd/1)
  end

  def all_version(context) do
    [[version]] =
      rows(
        context,
        ~s(SELECT stream_version FROM "#{context.schema}".streams WHERE stream_id = 0),
        []
      )

    version
  end

  def count(context, table) do
    [[count]] = rows(context, ~s[SELECT count(*) FROM "#{context.schema}".#{table}], [])

    count
  end

  # The stream each `$all` position came from, and the event's version there, in position
  # order. Every position is a row, so a gap shows in `all_positions/1`.
  def all_events(context) do
    rows(
      context,
      ~s(SELECT s.stream_uuid, se.original_stream_version) <>
        ~s( FROM "#{context.schema}".stream_events se) <>
        ~s[ JOIN "#{context.schema}".streams s ON s.stream_id = se.original_stream_id] <>
        " WHERE se.stream_id = 0 ORDER BY se.stream_version",
      []
    )
    |> Enum.map(&List.to_tuple/1)
  end

  def all_positions(context) do
    context
    |> rows(
      ~s(SELECT stream_version FROM "#{context.schema}".stream_events) <>
        " WHERE stream_id = 0 ORDER BY stream_version",
      []
    )
    |> Enum.map(&hd/1)
  end

  def assert_row_locked(context, stream_id) do
    assert {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}} =
             try_row_lock(context, stream_id)
  end

  def assert_row_free(context, stream_id) do
    assert {:ok, %Postgrex.Result{num_rows: 1}} = try_row_lock(context, stream_id)
  end

  defp try_row_lock(context, stream_id) do
    Postgrex.query(
      context.observer,
      ~s(SELECT 1 FROM "#{context.schema}".streams WHERE stream_id = $1) <>
        " FOR NO KEY UPDATE NOWAIT",
      [stream_id]
    )
  end

  def rows(context, sql, params) do
    %Postgrex.Result{rows: rows} = Postgrex.query!(context.observer, sql, params)

    rows
  end
end
