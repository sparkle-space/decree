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

  @doc """
  What decree's own log lines may show of a command or an event: a struct's module, and
  `reduce/1` of any other term.
  """
  @spec module_or_reduce(term) :: atom | {atom, :redacted}
  def module_or_reduce(%{__struct__: module}) when is_atom(module), do: module
  def module_or_reduce(term), do: reduce(term)

  @doc """
  What decree's own log lines may show of an exception raised with `stacktrace`:
  `** (Module)` and the stacktrace scrubbed by `scrub_stacktrace/1`, never the exception's
  message or fields. A `Decree.PersonalFields.RedactedError` keeps its own message, which
  names only the module it stands in for; a term that is not an exception is shown by
  `reduce/1`.
  """
  @spec format_reduced(term, Exception.stacktrace()) :: String.t()
  def format_reduced(exception, stacktrace) when is_list(stacktrace) do
    "** " <>
      banner(exception) <> "\n" <> Exception.format_stacktrace(scrub_stacktrace(stacktrace))
  end

  defp banner(%__MODULE__{} = exception),
    do: "(" <> inspect(__MODULE__) <> ") " <> message(exception)

  defp banner(exception), do: "(" <> inspect(reduce(exception)) <> ")"

  @doc false
  # How a throw leaves code that held a decrypted value: as the exit `{:nocatch, reduced}`, the
  # stacktrace scrubbed. Thrown on out of a `gen_server` callback, a reduced term is read as the
  # callback's return, and a reduced `{:noreply, _}` is a valid one: the process would carry on
  # with its state set to `:redacted`.
  @spec leave_throw(term, Exception.stacktrace()) :: no_return
  def leave_throw(value, stacktrace) do
    :erlang.raise(:exit, {:nocatch, reduce(value)}, scrub_stacktrace(stacktrace))
  end
end
