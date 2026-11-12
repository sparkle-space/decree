defmodule Decree.Event.Tagging do
  @moduledoc false

  # Fixtures for the tag tests. A command here carries the events it should produce, so a
  # test states its events and reads what the store kept.
end

defmodule Decree.Event.Tagging.Recorder do
  @moduledoc false

  # What the aggregate process saw, by key. The tests start it, and an event store, a
  # `Decree.Event.Tags` implementation and a `before_execute` write to it from the aggregate
  # process, where a test's own pid cannot be reached through metadata.

  def start_link(_opts), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  def record(key, value) do
    Agent.update(__MODULE__, fn state ->
      Map.update(state, key, [value], &(&1 ++ [value]))
    end)
  end

  def all(key), do: Agent.get(__MODULE__, &Map.get(&1, key, []))
end

defmodule Decree.Event.Tagging.Store do
  @moduledoc false

  # The Postgres event store, recording each append it is given, as the store's event data. A
  # failed attempt leaves no row, so this is the only sight of what it carried.

  use Decree.DelegatingStore

  alias Decree.Event.Tagging.Recorder
  alias Decree.FrameworkEventStore, as: Store

  @impl Decree.Store
  def append_to_stream(stream_uuid, expected_version, events, opts) do
    Recorder.record(:append, %{version: expected_version, events: events})

    Store.append_to_stream(stream_uuid, expected_version, events, opts)
  end
end

defmodule Decree.Event.Tagging.Events do
  @moduledoc false

  defmodule Tagged do
    @moduledoc false
    # Its implementation answers the tags it carries.
    @derive Jason.Encoder
    defstruct [:note, tags: []]
  end

  defmodule MemberAddSkipped do
    @moduledoc false
    # The drift audit's M-1 shape: it has no `command_id` field.
    @derive Jason.Encoder
    defstruct [:tenant_id, :user_id]
  end

  defmodule Unimplemented do
    @moduledoc false
    # Takes the `Any` fallback.
    @derive Jason.Encoder
    defstruct [:note]
  end

  defmodule FromContext do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:note]
  end

  defmodule Counted do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:note]
  end

  defmodule Refused do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:reason]
  end

  defmodule Raising do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:note]
  end

  defmodule Answering do
    @moduledoc false
    # Its implementation fails the append with the bare `reason`.
    @derive Jason.Encoder
    defstruct [:reason]
  end

  defmodule Reloaded do
    @moduledoc false
    # Opted in. One test unloads it, so nothing else may use it.
    @derive Jason.Encoder
    defstruct [:note]

    def __required_tag_kinds__(%__MODULE__{}, _tag_context), do: ["reloaded"]
    def __conditional_tag_kinds__, do: ["reloaded"]
    def __tag_kinds__, do: ["reloaded"]
  end
end

defmodule Decree.Event.Tagging.EntityRef do
  @moduledoc false

  # An identity that is no string and renders as one.
  defstruct [:uuid]

  defimpl String.Chars do
    def to_string(%Decree.Event.Tagging.EntityRef{uuid: uuid}), do: uuid
  end
end

alias Decree.Event.Tagging.Events
alias Decree.Event.Tagging.Recorder

defimpl Decree.Event.Tags, for: Events.Tagged do
  def tags(%Events.Tagged{tags: tags}, _context), do: {:ok, tags}
end

defimpl Decree.Event.Tags, for: Events.MemberAddSkipped do
  def tags(%Events.MemberAddSkipped{tenant_id: tenant_id}, _context) do
    {:ok, ["tenant:" <> tenant_id]}
  end
end

defimpl Decree.Event.Tags, for: Events.FromContext do
  def tags(%Events.FromContext{}, %Decree.Event.TagContext{} = context) do
    %Decree.Event.TagContext{metadata: metadata, command: command} = context

    {:ok, ["who:" <> Map.fetch!(metadata, "who"), "field:" <> command.command_id]}
  end
end

defimpl Decree.Event.Tags, for: Events.Counted do
  def tags(%Events.Counted{note: note}, _context) do
    Recorder.record(:counted, note)

    {:ok, []}
  end
end

defimpl Decree.Event.Tags, for: Events.Refused do
  def tags(%Events.Refused{reason: reason}, _context), do: {:error, {:refused, reason}}
end

defimpl Decree.Event.Tags, for: Events.Raising do
  def tags(%Events.Raising{}, _context), do: raise("a raising implementation")
end

defimpl Decree.Event.Tags, for: Events.Answering do
  def tags(%Events.Answering{reason: reason}, _context), do: {:error, reason}
end

defmodule Decree.Event.Tagging.Commands do
  @moduledoc false

  defmodule Emit do
    @moduledoc false
    defstruct [:user_id, :command_id, :correlation_id, events: []]
  end

  defmodule EmitAsRequester do
    @moduledoc false
    defstruct [:requester_user_id, :command_id, events: []]
  end

  defmodule EmitByRef do
    @moduledoc false
    defstruct [:content_ref, :command_id, events: []]
  end

  defmodule EmitPrefixed do
    @moduledoc false
    defstruct [:user_id, :command_id, events: []]
  end

  defmodule EmitIdentified do
    @moduledoc false
    defstruct [:user_id, :command_id, events: []]
  end

  defmodule EmitByFunction do
    @moduledoc false
    defstruct [:key, :command_id, events: []]
  end

  defmodule EmitByFunctionTagged do
    @moduledoc false
    defstruct [:key, :command_id, events: []]
  end

  defmodule EmitMulti do
    @moduledoc false
    defstruct [:user_id, :command_id, events: []]
  end

  defmodule Race do
    @moduledoc false
    defstruct [:user_id, :command_id, events: []]
  end

  defmodule RaceByFunction do
    @moduledoc false
    # `keys` are the answers the identity function gives, one per call.
    defstruct [:keys, :command_id, events: []]
  end

  defmodule EmitRewritten do
    @moduledoc false
    # A middleware moves `rewrite_to` into `user_id` before the identity is extracted.
    defstruct [:user_id, :command_id, :rewrite_to, events: []]
  end

  defmodule Attempt do
    @moduledoc false
    defstruct [:user_id, :command_id, events: []]
  end

  defmodule AttemptMulti do
    @moduledoc false
    defstruct [:user_id, :command_id, events: []]
  end
end

defmodule Decree.Event.Tagging.Ledger do
  @moduledoc false

  # Keeps the events applied to it, so a test can see what its state holds.

  alias Decree.Aggregate.Multi
  alias Decree.Event.Tagging.Commands.EmitMulti

  defstruct seen: []

  def execute(%__MODULE__{} = ledger, %EmitMulti{events: events}) do
    Enum.reduce(events, Multi.new(ledger), fn event, multi ->
      Multi.execute(multi, fn %__MODULE__{} -> event end)
    end)
  end

  def execute(%__MODULE__{}, %{events: events}), do: events

  def apply(%__MODULE__{seen: seen} = ledger, event) do
    %__MODULE__{ledger | seen: seen ++ [event]}
  end
end

defmodule Decree.Event.Tagging.PrefixedLedger do
  @moduledoc false
  defstruct [:unused]

  def execute(%__MODULE__{}, %{events: events}), do: events
  def apply(%__MODULE__{} = ledger, _event), do: ledger
end

defmodule Decree.Event.Tagging.IdentifiedLedger do
  @moduledoc false
  defstruct [:unused]

  def execute(%__MODULE__{}, %{events: events}), do: events
  def apply(%__MODULE__{} = ledger, _event), do: ledger
end

defmodule Decree.Event.Tagging.FunctionLedger do
  @moduledoc false
  defstruct [:unused]

  def execute(%__MODULE__{}, %{events: events}), do: events
  def apply(%__MODULE__{} = ledger, _event), do: ledger
end

defmodule Decree.Event.Tagging.FunctionTaggedLedger do
  @moduledoc false
  defstruct [:unused]

  def execute(%__MODULE__{}, %{events: events}), do: events
  def apply(%__MODULE__{} = ledger, _event), do: ledger
end

defmodule Decree.Event.Tagging.FunctionRaceLedger do
  @moduledoc false
  defstruct [:unused]

  def apply(%__MODULE__{} = ledger, _event), do: ledger
end

defmodule Decree.Event.Tagging.RaceHandler do
  @moduledoc false
  @behaviour Decree.Commands.Handler

  # Races the command it precedes: the first attempt's `before_execute` appends straight
  # to the stream, after the aggregate read its version, so that attempt's own append
  # answers `:wrong_expected_version` from Postgres.

  alias Decree.Aggregates.ExecutionContext
  alias Decree.Event.Mapper
  alias Decree.Event.Tagging.Commands.{Race, RaceByFunction}
  alias Decree.Event.Tagging.Events.Tagged
  alias Decree.Event.Tagging.{App, FunctionRaceLedger, Ledger, Recorder}
  alias Decree.EventStore

  def race(%Ledger{}, %ExecutionContext{metadata: metadata} = context) do
    %ExecutionContext{command: %Race{user_id: user_id}} = context

    Recorder.record(:attempt_metadata, metadata)

    race_first_attempt(user_id)
  end

  def race_by_function(%FunctionRaceLedger{}, %ExecutionContext{metadata: metadata} = context) do
    %ExecutionContext{command: %RaceByFunction{keys: [routed | _others]}} = context

    Recorder.record(:attempt_metadata, metadata)

    race_first_attempt(routed)
  end

  def handle(%Ledger{}, %Race{events: events}), do: events
  def handle(%FunctionRaceLedger{}, %RaceByFunction{events: events}), do: events

  defp race_first_attempt(stream_uuid) do
    if length(Recorder.all(:attempt_metadata)) == 1 do
      event_data = Mapper.map_to_event_data([%Tagged{note: "concurrent"}], metadata: %{})

      :ok = EventStore.append_to_stream(App, stream_uuid, :any_version, event_data)
    end

    :ok
  end
end

defmodule Decree.Event.Tagging.AttemptHandler do
  @moduledoc false
  @behaviour Decree.Commands.Handler

  # Counts how often a command starts executing, by its `before_execute`.

  alias Decree.Aggregates.ExecutionContext
  alias Decree.Event.Tagging.Commands.{Attempt, AttemptMulti, EmitMulti}
  alias Decree.Event.Tagging.{Ledger, Recorder}

  def count(%Ledger{}, %ExecutionContext{}) do
    Recorder.record(:before_execute, :called)

    :ok
  end

  def handle(%Ledger{}, %Attempt{events: events}), do: events

  def handle(%Ledger{} = ledger, %AttemptMulti{events: events}) do
    Ledger.execute(ledger, %EmitMulti{events: events})
  end
end

defmodule Decree.Event.Tagging.RewriteIdentity do
  @moduledoc false
  @behaviour Decree.Middleware

  alias Decree.Event.Tagging.Commands.EmitRewritten
  alias Decree.Middleware.Pipeline

  def before_dispatch(%Pipeline{command: %EmitRewritten{} = command} = pipeline) do
    %EmitRewritten{rewrite_to: rewrite_to} = command

    %Pipeline{pipeline | command: %EmitRewritten{command | user_id: rewrite_to}}
  end

  def before_dispatch(%Pipeline{} = pipeline), do: pipeline

  def after_dispatch(%Pipeline{} = pipeline), do: pipeline

  def after_failure(%Pipeline{} = pipeline), do: pipeline
end

defmodule Decree.Event.Tagging.Router do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.Event.Tagging.Commands.{
    Attempt,
    AttemptMulti,
    Emit,
    EmitAsRequester,
    EmitByFunction,
    EmitByFunctionTagged,
    EmitByRef,
    EmitIdentified,
    EmitMulti,
    EmitPrefixed,
    EmitRewritten,
    Race,
    RaceByFunction
  }

  alias Decree.Event.Tagging.{
    AttemptHandler,
    FunctionLedger,
    FunctionRaceLedger,
    FunctionTaggedLedger,
    IdentifiedLedger,
    Ledger,
    PrefixedLedger,
    RaceHandler,
    Recorder
  }

  middleware Decree.Event.Tagging.RewriteIdentity

  identify PrefixedLedger, by: :user_id, prefix: "ledger-"
  identify IdentifiedLedger, by: :user_id
  identify FunctionLedger, by: &__MODULE__.key/1
  # Opaque, so an answer that is no UUID routes, and shows the tag absent for it.
  identify FunctionTaggedLedger, by: &__MODULE__.key/1, opaque: true
  identify FunctionRaceLedger, by: &__MODULE__.next_key/1

  dispatch Emit, to: Ledger, identity: :user_id
  dispatch EmitAsRequester, to: Ledger, identity: :requester_user_id, stream_tag: :user
  dispatch EmitByRef, to: Ledger, identity: :content_ref, opaque: true
  dispatch EmitPrefixed, to: PrefixedLedger
  dispatch EmitIdentified, to: IdentifiedLedger, stream_tag: :member
  dispatch EmitByFunction, to: FunctionLedger
  dispatch EmitByFunctionTagged, to: FunctionTaggedLedger, stream_tag: :user
  dispatch EmitMulti, to: Ledger, identity: :user_id

  dispatch Race,
    to: RaceHandler,
    aggregate: Ledger,
    identity: :user_id,
    before_execute: :race

  dispatch RaceByFunction,
    to: RaceHandler,
    aggregate: FunctionRaceLedger,
    stream_tag: :user,
    before_execute: :race_by_function

  dispatch EmitRewritten, to: Ledger, identity: :user_id

  dispatch [Attempt, AttemptMulti],
    to: AttemptHandler,
    aggregate: Ledger,
    identity: :user_id,
    before_execute: :count

  def key(%{key: key}), do: key

  # Answers differently at each call, and records the process that called it.
  def next_key(%RaceByFunction{keys: keys}) do
    Recorder.record(:identity_call, self())

    Enum.at(keys, length(Recorder.all(:identity_call)) - 1)
  end
end

defmodule Decree.Event.Tagging.App do
  @moduledoc false
  use Decree.Application,
    otp_app: :decree,
    event_store:
      Decree.FrameworkStorage.event_store_config(event_store: Decree.Event.Tagging.Store),
    pubsub: :local,
    registry: :local

  router(Decree.Event.Tagging.Router)

  # Its own event store process name, as `Decree.FrameworkStorage.name_event_store/1`
  # gives the other applications.
  def init(config) do
    instance = Keyword.get(config, :name) || Keyword.fetch!(config, :application)

    {:ok,
     Keyword.update(config, :event_store, [], fn event_store ->
       Keyword.put_new(event_store, :name, Module.concat([instance, "FrameworkEventStore"]))
     end)}
  end
end
