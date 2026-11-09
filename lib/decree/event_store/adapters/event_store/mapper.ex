defmodule Decree.EventStore.Adapters.EventStore.Mapper do
  @moduledoc false

  def to_event_data(%Decree.EventStore.EventData{} = event_data) do
    %Decree.EventStore.EventData{
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: data,
      metadata: metadata,
      tags: tags,
      required_tag_kinds: required_tag_kinds
    } = event_data

    %Decree.Store.EventData{
      causation_id: causation_id,
      correlation_id: correlation_id,
      event_type: event_type,
      data: data,
      metadata: metadata,
      tags: tags,
      required_tag_kinds: required_tag_kinds
    }
  end

  def from_recorded_event(%Decree.Store.RecordedEvent{} = event) do
    %Decree.Store.RecordedEvent{
      event_id: event_id,
      event_number: event_number,
      stream_uuid: stream_uuid,
      stream_version: stream_version,
      correlation_id: correlation_id,
      causation_id: causation_id,
      event_type: event_type,
      data: data,
      metadata: metadata,
      created_at: created_at
    } = event

    %Decree.EventStore.RecordedEvent{
      event_id: event_id,
      event_number: event_number,
      stream_id: stream_uuid,
      stream_version: stream_version,
      correlation_id: correlation_id,
      causation_id: causation_id,
      event_type: event_type,
      data: data,
      metadata: metadata,
      created_at: created_at
    }
  end

  def to_snapshot_data(%Decree.EventStore.SnapshotData{} = snapshot) do
    %Decree.EventStore.SnapshotData{
      source_uuid: source_uuid,
      source_version: source_version,
      source_type: source_type,
      data: data,
      metadata: metadata,
      created_at: created_at,
      snapshot_version: snapshot_version
    } = snapshot

    %Decree.Store.Snapshots.SnapshotData{
      source_uuid: source_uuid,
      source_version: source_version,
      source_type: source_type,
      data: data,
      metadata: metadata,
      created_at: created_at,
      snapshot_version: snapshot_version
    }
  end

  def from_snapshot_data(%Decree.Store.Snapshots.SnapshotData{} = snapshot) do
    %Decree.Store.Snapshots.SnapshotData{
      source_uuid: source_uuid,
      source_version: source_version,
      source_type: source_type,
      data: data,
      metadata: metadata,
      created_at: created_at,
      snapshot_version: snapshot_version
    } = snapshot

    %Decree.EventStore.SnapshotData{
      source_uuid: source_uuid,
      source_version: source_version,
      source_type: source_type,
      data: data,
      metadata: metadata,
      created_at: created_at,
      snapshot_version: snapshot_version
    }
  end
end
