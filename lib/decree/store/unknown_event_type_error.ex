defmodule Decree.Store.UnknownEventTypeError do
  @moduledoc """
  Raised by a read (`read_*`, `stream_*`, `read_query`) that meets an event whose type the
  event store's serializer does not know (see `c:Decree.Store.Serializer.known_type?/1`).
  Typically an event appended by a newer release of the application, read by an older one.

  The error names the event's type and number, never its data. A subscription does not raise
  it: it holds before the event and retries until an instance that knows the type delivers
  it (see `c:Decree.Store.subscribe_to_stream/4`).
  """

  defexception [:type, :event_number]

  @type t :: %__MODULE__{type: String.t(), event_number: non_neg_integer()}

  @impl Exception
  def message(%__MODULE__{type: type, event_number: event_number}) do
    "unknown event type #{inspect(type)} at event number #{inspect(event_number)}"
  end
end
