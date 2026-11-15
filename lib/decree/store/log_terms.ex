defmodule Decree.Store.LogTerms do
  @moduledoc false

  # What the store's log lines may show of an error term and of a stream id
  # (sparkle-space/sparklespace#8352). A `Postgrex.Error` carries the values of the row it
  # failed on in its detail and often its message (a unique violation's key), a
  # `DBConnection.ConnectionError` can carry a host and port, a `Jason.DecodeError` the bytes it
  # could not decode, and a stream id can be a user's own id. A line names the failure by its
  # class and the stream by its last eight characters.
  #
  # `reduce/1` reads as `Decree.PersonalFields.RedactedError.reduce/1`, which the store may
  # not call (it is the framework's), with the class an operator needs kept: a Postgres error
  # code name, or a connection error's reason.

  @doc """
  Reduces an error term to what a log line may show: an atom stays; a `Postgrex.Error` is
  `{Postgrex.Error, code}` with its code name (its SQLSTATE when the code has no name), or
  `Postgrex.Error` without SQLSTATE fields; an exception with an atom `reason` (a
  `DBConnection.ConnectionError`, `:queue_timeout` or `:error`) is `{module, reason}`; any
  other exception is its module; `{:error, reason}` is `{:error, reduce(reason)}`; a tuple
  with an atom head is `{head, :redacted}`; anything else is `:redacted`.
  """
  @spec reduce(term) :: atom | {atom, atom | String.t()} | {:error, term}
  def reduce(reason) when is_atom(reason), do: reason

  def reduce(%{__exception__: true, __struct__: type, postgres: %{code: code}})
      when is_atom(type) and is_atom(code) and not is_nil(code),
      do: {type, code}

  def reduce(%{__exception__: true, __struct__: type, postgres: %{pg_code: pg_code}})
      when is_atom(type) and is_binary(pg_code),
      do: {type, pg_code}

  def reduce(%{__exception__: true, __struct__: type, reason: reason})
      when is_atom(type) and is_atom(reason) and not is_nil(reason),
      do: {type, reason}

  def reduce(%{__exception__: true, __struct__: type}) when is_atom(type), do: type

  def reduce({:error, reason}), do: {:error, reduce(reason)}

  def reduce(reason)
      when is_tuple(reason) and tuple_size(reason) > 0 and is_atom(elem(reason, 0)),
      do: {elem(reason, 0), :redacted}

  def reduce(_reason), do: :redacted

  @doc """
  A stream id as a log line may show it: its last eight characters after `...`. `$all` is not
  an id and is shown whole; a term that is not a string shows `...(none)`.
  """
  @spec suffix(term) :: String.t()
  def suffix("$all"), do: "$all"

  def suffix(id) when is_binary(id) do
    length = String.length(id)

    "..." <> String.slice(id, max(length - 8, 0), 8)
  end

  def suffix(_id), do: "...(none)"
end
