defmodule Decree.PersonalFields.RedactedError do
  @moduledoc """
  Stands in for an exception raised while a decrypted value was in hand.

  An exception can carry the value it failed on: `MatchError` holds its term, a database error
  its detail, `FunctionClauseError`'s stack frame the arguments. So it never reaches a log or
  telemetry from such a path. This one holds only the module of the original, as `type`.

  `scrub_stacktrace/1` and `reduce/1` do the same for a stacktrace and for an error reason.
  """

  defexception [:type]

  @type t :: %__MODULE__{type: module}

  @impl true
  def message(%__MODULE__{type: type}) do
    "#{inspect(type)} raised with a decrypted personal field in hand, details redacted"
  end

  @doc """
  Answers the stand-in for `exception`.
  """
  @spec redact(Exception.t()) :: t
  def redact(%{__struct__: type}), do: %__MODULE__{type: type}

  @doc """
  Answers `stacktrace` with each frame's argument list reduced to its arity, and each frame's
  location reduced to its file and line.
  """
  @spec scrub_stacktrace(Exception.stacktrace()) :: Exception.stacktrace()
  def scrub_stacktrace(stacktrace) when is_list(stacktrace) do
    Enum.map(stacktrace, &scrub_frame/1)
  end

  defp scrub_frame({module, function, arguments, location}) do
    {module, function, arity(arguments), scrub_location(location)}
  end

  defp scrub_frame({function, arguments, location}) do
    {function, arity(arguments), scrub_location(location)}
  end

  defp scrub_frame(_frame), do: :redacted

  defp arity(arguments) when is_list(arguments), do: length(arguments)
  defp arity(arity), do: arity

  defp scrub_location(location) when is_list(location) do
    Keyword.take(location, [:file, :line])
  end

  defp scrub_location(_location), do: []

  @doc """
  Reduces an error reason to what decree's own logs and telemetry may show: an atom stays, an
  exception becomes its type, a tuple with an atom head becomes `{head, :redacted}`, and
  anything else `:redacted`.
  """
  @spec reduce(term) :: atom | {atom, :redacted}
  def reduce(reason) when is_atom(reason), do: reason
  def reduce(%{__exception__: true, __struct__: type}) when is_atom(type), do: type

  def reduce(reason) when is_tuple(reason) and is_atom(elem(reason, 0)),
    do: {elem(reason, 0), :redacted}

  def reduce(_reason), do: :redacted
end
