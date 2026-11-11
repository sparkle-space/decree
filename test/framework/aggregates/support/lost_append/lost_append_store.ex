defmodule Decree.Aggregates.LostAppendStore do
  @moduledoc false

  # The Postgres event store, with one append failure injected per flagged stream
  # (`flag/2`):
  #
  #   - `:unavailable` performs the append, then answers an `:unavailable`
  #     `AppendError`: the events were written, but the response was lost;
  #   - `:retryable` and `:rejected` answer the error without appending, as a
  #     statement Postgres rolled back.
  #
  # `subscribe/2` (an aggregate's subscription to its own stream) is a no-op, as a
  # lost notification, so an aggregate cannot catch up on the written event that
  # way and only a rebuild from the stream shows it.

  use Decree.DelegatingStore

  alias Decree.FrameworkEventStore, as: Store
  alias Decree.Store.AppendError

  def flag(stream_uuid, class), do: :persistent_term.put(key(stream_uuid), class)

  def unflag(stream_uuid), do: :persistent_term.erase(key(stream_uuid))

  @impl Decree.Store
  def append_to_stream(stream_uuid, expected_version, events, opts) do
    case :persistent_term.get(key(stream_uuid), nil) do
      nil ->
        Store.append_to_stream(stream_uuid, expected_version, events, opts)

      :unavailable ->
        unflag(stream_uuid)

        :ok = Store.append_to_stream(stream_uuid, expected_version, events, opts)

        {:error, error(:unavailable, nil)}

      class when class in [:retryable, :rejected] ->
        unflag(stream_uuid)

        code = if class == :retryable, do: :serialization_failure, else: :check_violation

        {:error, error(class, code)}
    end
  end

  @impl Decree.Store
  def subscribe(_stream_uuid, _opts), do: :ok

  defp error(class, code), do: %AppendError{class: class, code: code, reason: :injected}

  defp key(stream_uuid), do: {__MODULE__, stream_uuid}
end
