defmodule Decree.Store.AppendError do
  @moduledoc """
  An append or link that the database did not accept, classified by what a
  caller can do about it.

  `append_to_stream/4` and `link_to_stream/4` return four failures of the
  appender's statement as plain atoms, because they are domain answers:
  `{:error, :wrong_expected_version}`, `{:error, :duplicate_event}`,
  `{:error, :not_found}` and (retried once inside the store, see below)
  `{:error, :duplicate_stream_uuid}`. Every other failure of that statement is
  returned as `{:error, %Decree.Store.AppendError{}}`, with these fields:

    - `class` — what a caller can do:
      - `:unavailable` — the database could not be reached or stopped serving:
        a `DBConnection.ConnectionError` (a refused or dropped connection, a
        checkout that timed out in the pool queue, a client-side query timeout)
        or SQLSTATE class 08 (connection exception), 53 (insufficient
        resources, such as `too_many_connections` or `disk_full`) or 57
        (operator intervention: `admin_shutdown`, `crash_shutdown`,
        `cannot_connect_now`, `query_canceled` (which includes a
        `statement_timeout`), `idle_session_timeout`, `database_dropped`), and
        `idle_in_transaction_session_timeout` (25P03), where the server ended
        the session. Retry after a wait.
      - `:retryable` — the same request may succeed if sent again: SQLSTATE
        class 40 (transaction rollback: `serialization_failure`,
        `deadlock_detected`) or 55 (object not in prerequisite state:
        `lock_not_available`, `object_in_use`).
      - `:rejected` — any other error: the request itself was refused
        (a constraint or data error, an undefined table, a query error).
        Retrying the same request will not help.
    - `code` — the Postgres error code name (`Postgrex.Error`'s
      `postgres.code`, such as `:serialization_failure`), or `nil` when the
      error carries no SQLSTATE.
    - `reason` — the original error term, for logging.

  A connection lost during the statement (`:unavailable`) or a
  `transaction_resolution_unknown` / `statement_completion_unknown` code does
  not say whether the events were written. Retrying an append with the same
  event ids answers `{:error, :duplicate_event}` if they were.

  An aggregate stays running after any `AppendError`: it is a plain struct, not
  an exception, so `Decree.Aggregates.DefaultLifespan` answers `:infinity`. The
  aggregate's in-memory version is still the one before the append; if a lost
  append did commit, the next command's append answers
  `{:error, :wrong_expected_version}`, and the aggregate rebuilds its state from
  the stream before it executes the command again. (Before this struct existed,
  a `DBConnection.ConnectionError` reached the lifespan as an exception and
  stopped the aggregate.)

  `{:error, :duplicate_stream_uuid}` means two appends raced to create the
  same stream; `append_to_stream/4` retries it once and then answers
  `{:error, {:already_retried_once, :duplicate_stream_uuid}}`.
  """

  @type class :: :unavailable | :retryable | :rejected

  @type t :: %__MODULE__{
          class: class,
          code: atom | nil,
          reason: term
        }

  defstruct [:class, :code, :reason]

  # The SQLSTATE classes, by their two-character prefix.
  @unavailable_classes ["08", "53", "57"]
  @retryable_classes ["40", "55"]

  # Codes outside those classes whose class says less than the code: the
  # server ended the session.
  @unavailable_codes ["25P03"]

  @doc """
  Classify an error the appender's statement returned.
  """
  @spec from_error(term) :: t
  def from_error(%Postgrex.Error{postgres: %{pg_code: pg_code} = postgres} = error)
      when is_binary(pg_code) do
    %__MODULE__{class: classify(pg_code), code: Map.get(postgres, :code), reason: error}
  end

  def from_error(%DBConnection.ConnectionError{} = error) do
    %__MODULE__{class: :unavailable, code: nil, reason: error}
  end

  # Any other term: a `Postgrex.Error` with no SQLSTATE, a `Postgrex.QueryError`,
  # a `DBConnection.OwnershipError` (an ownership pool) or anything else.
  def from_error(error) do
    %__MODULE__{class: :rejected, code: nil, reason: error}
  end

  @doc """
  A one-line description for a log line: the class, the code, and the original
  error's message.
  """
  @spec message(t) :: String.t()
  def message(%__MODULE__{class: class, code: code, reason: reason}) do
    code = if code, do: " (#{code})", else: ""

    "append #{class}#{code}: " <> describe(reason)
  end

  defp classify(pg_code) when pg_code in @unavailable_codes, do: :unavailable

  defp classify(<<class::binary-size(2), _rest::binary>>) when class in @unavailable_classes,
    do: :unavailable

  defp classify(<<class::binary-size(2), _rest::binary>>) when class in @retryable_classes,
    do: :retryable

  defp classify(_pg_code), do: :rejected

  defp describe(%{__exception__: true} = exception), do: Exception.message(exception)
  defp describe(reason), do: inspect(reason)
end
