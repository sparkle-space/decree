defmodule Decree.Aggregates.LostAppendAdapter do
  @moduledoc false

  # The Postgres event store adapter, with one append failure injected per flagged
  # stream (`flag/2`):
  #
  #   - `:unavailable` performs the append, then answers an `:unavailable`
  #     `AppendError`: the events were written, but the response was lost;
  #   - `:retryable` and `:rejected` answer the error without appending, as a
  #     statement Postgres rolled back.
  #
  # `subscribe/2` (an aggregate's subscription to its own stream) is a no-op, as a
  # lost notification, so an aggregate cannot catch up on the written event that
  # way and only a rebuild from the stream shows it.

  @behaviour Decree.EventStore.Adapter

  alias Decree.EventStore.Adapters.EventStore, as: Adapter
  alias Decree.Store.AppendError

  def flag(stream_uuid, class), do: :persistent_term.put(key(stream_uuid), class)

  def unflag(stream_uuid), do: :persistent_term.erase(key(stream_uuid))

  @impl Decree.EventStore.Adapter
  def append_to_stream(adapter_meta, stream_uuid, expected_version, events, opts \\ []) do
    case :persistent_term.get(key(stream_uuid), nil) do
      nil ->
        Adapter.append_to_stream(adapter_meta, stream_uuid, expected_version, events, opts)

      :unavailable ->
        unflag(stream_uuid)

        :ok = Adapter.append_to_stream(adapter_meta, stream_uuid, expected_version, events, opts)

        {:error, error(:unavailable, nil)}

      class when class in [:retryable, :rejected] ->
        unflag(stream_uuid)

        code = if class == :retryable, do: :serialization_failure, else: :check_violation

        {:error, error(class, code)}
    end
  end

  @impl Decree.EventStore.Adapter
  def subscribe(_adapter_meta, _stream_uuid), do: :ok

  @impl Decree.EventStore.Adapter
  defdelegate append_to_streams(adapter_meta, appends, opts), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate read_query(adapter_meta, items, opts), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate read_tags(adapter_meta, positions, opts), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate child_spec(application, config), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate stream_forward(adapter_meta, stream_uuid, start_version, read_batch_size),
    to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate read_stream_head(adapter_meta, stream_uuid), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate deletable_stream_prefixes(adapter_meta), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate conditional_targets(adapter_meta), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate subscribe_to(adapter_meta, stream, name, subscriber, start_from, opts),
    to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate ack_event(adapter_meta, subscription, event), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate unsubscribe(adapter_meta, subscription), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate delete_subscription(adapter_meta, stream, name), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate read_snapshot(adapter_meta, source_uuid, snapshot_version), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate record_snapshot(adapter_meta, snapshot), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate delete_snapshot(adapter_meta, source_uuid), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate prune_snapshots(adapter_meta, source_type, snapshot_version), to: Adapter

  defp error(class, code), do: %AppendError{class: class, code: code, reason: :injected}

  defp key(stream_uuid), do: {__MODULE__, stream_uuid}
end
