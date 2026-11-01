defmodule Decree.Commands.ExecutionResult do
  @moduledoc """
  Contains the aggregate, events, and metadata created by a successfully
  executed command.

  The available fields are:

    - `aggregate_uuid` - identity of the aggregate instance. For a decision, the route's
      stream, or the first member's in a multi-stream batch.

    - `aggregate_state` - resultant state of the aggregate after executing
      the command. For a decision, the state its fold reached, with the new events applied.

    - `aggregate_version` - resultant version of the aggregate after executing
      the command. For a decision, the version of the stream `aggregate_uuid` names.

    - `events` - a list of the created events, it may be an empty list.

    - `metadata` - a map containing the metadata associated with the command
      dispatch.

    - `position` - for a decision, the last `$all` position its append wrote. `nil` for an
      aggregate.

    - `stream_versions` - for a decision, `%{stream_uuid => version}`, each stream's version
      after its append. `nil` for an aggregate.

  """

  @type t :: %__MODULE__{
          aggregate_uuid: String.t(),
          aggregate_state: struct,
          aggregate_version: non_neg_integer(),
          events: list(struct()),
          metadata: struct(),
          position: non_neg_integer() | nil,
          stream_versions: %{String.t() => non_neg_integer()} | nil
        }

  @enforce_keys [:aggregate_uuid, :aggregate_state, :aggregate_version, :events, :metadata]

  defstruct [
    :aggregate_uuid,
    :aggregate_state,
    :aggregate_version,
    :events,
    :metadata,
    :position,
    :stream_versions
  ]
end
