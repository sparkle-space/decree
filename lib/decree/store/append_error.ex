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
      - `:unavailable` — the database could not be reached, stopped serving, or
        cannot say whether the statement took effect. Retry after a wait, if the
        append is safe to retry (see below).
      - `:retryable` — the same request may succeed if sent again.
      - `:rejected` — the request itself was refused. Retrying the same request
        will not help.
    - `code` — the Postgres error code name (`Postgrex.Error`'s
      `postgres.code`, such as `:serialization_failure`), or `nil` when the
      error carries no SQLSTATE.
    - `reason` — the original error term, for logging.

  ## Classes

  | class | SQLSTATE classes | SQLSTATE codes | other errors |
  |---|---|---|---|
  | `:unavailable` | 08 (connection exception), 53 (insufficient resources), 57 (operator intervention) | 25P03 `idle_in_transaction_session_timeout`, 25006 `read_only_sql_transaction`, 40003 `statement_completion_unknown`, 58000 `system_error`, 58030 `io_error` | `DBConnection.ConnectionError`; a `Postgrex.Error` without SQLSTATE fields (Postgrex's disconnect when the connection breaks mid-query) |
  | `:retryable` | 40 (transaction rollback), 55 (object not in prerequisite state) | | |
  | `:rejected` | any other | 40002 `transaction_integrity_constraint_violation` | `Postgrex.QueryError`, `DBConnection.OwnershipError`, any other term |

  A code listed in the codes column overrides its class. Notes on some entries:

    - class 57 includes `query_canceled` (57014), which a `statement_timeout`
      also raises, and `admin_shutdown`, `crash_shutdown`, `cannot_connect_now`,
      `idle_session_timeout`, `database_dropped`;
    - `read_only_sql_transaction` is a write that reached a primary a failover
      has demoted;
    - `idle_in_transaction_session_timeout` means the server ended the session;
    - `statement_completion_unknown` leaves the outcome unknown, as a lost
      connection does;
    - `transaction_integrity_constraint_violation` is a deferred constraint
      failure, which fails again on a retry;
    - `58P01` `undefined_file` and `58P02` `duplicate_file` stay `:rejected`.

  ## Retrying

  An `:unavailable` failure does not say whether the events were written. A
  retry is safe only when the append is idempotent: the events have pinned
  event ids (a retry of a written append then answers
  `{:error, :duplicate_event}`), or the append names an expected version that a
  committed earlier attempt has moved past (it then answers
  `{:error, :wrong_expected_version}`). An event without an `event_id` gets a
  fresh one on every call, so a retry with `:any_version` can write the events
  twice.

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

  # Codes whose class says less than the code. Each overrides its class.
  @unavailable_codes ["25P03", "25006", "40003", "58000", "58030"]
  @rejected_codes ["40002"]

  @doc """
  Classify an error the appender's statement returned.
  """
  @spec from_error(term) :: t
  def from_error(%Postgrex.Error{postgres: %{pg_code: pg_code} = postgres} = error)
      when is_binary(pg_code) do
    %__MODULE__{class: classify(pg_code), code: Map.get(postgres, :code), reason: error}
  end

  # Postgrex's disconnect when the connection breaks mid-query (`sync_error/2`,
  # "unexpected postgres status") carries a message and no SQLSTATE fields.
  def from_error(%Postgrex.Error{postgres: nil} = error) do
    %__MODULE__{class: :unavailable, code: nil, reason: error}
  end

  def from_error(%DBConnection.ConnectionError{} = error) do
    %__MODULE__{class: :unavailable, code: nil, reason: error}
  end

  # Any other term: a `Postgrex.QueryError`, a `DBConnection.OwnershipError` (an
  # ownership pool) or anything else.
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
  defp classify(pg_code) when pg_code in @rejected_codes, do: :rejected

  defp classify(<<class::binary-size(2), _rest::binary>>) when class in @unavailable_classes,
    do: :unavailable

  defp classify(<<class::binary-size(2), _rest::binary>>) when class in @retryable_classes,
    do: :retryable

  defp classify(_pg_code), do: :rejected

  defp describe(%{__exception__: true} = exception), do: Exception.message(exception)
  defp describe(reason), do: inspect(reason)
end
