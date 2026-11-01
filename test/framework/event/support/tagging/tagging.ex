defmodule Decree.Event.Tagging do
  @moduledoc false

  # Fixtures for the tag tests. A command here carries the events it should produce, so a
  # test states its events and reads what the store kept.
end

defmodule Decree.Event.Tagging.Recorder do
  @moduledoc false

  # What the aggregate process saw, by key. The tests start it, and an event store
  # adapter, a `Decree.Event.Tags` implementation and a `before_execute` write to it from
  # the aggregate process, where a test's own pid cannot be reached through metadata.

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

defmodule Decree.Event.Tagging.Adapter do
  @moduledoc false

  # The Postgres event store adapter, recording each append it is given. A failed attempt
  # leaves no row, so this is the only sight of what it carried.

  @behaviour Decree.EventStore.Adapter

  alias Decree.Event.Tagging.Recorder
  alias Decree.EventStore.Adapters.EventStore, as: Adapter

  @impl Decree.EventStore.Adapter
  def append_to_stream(adapter_meta, stream_uuid, expected_version, events, opts \\ []) do
    Recorder.record(:append, %{version: expected_version, events: events})

    Adapter.append_to_stream(adapter_meta, stream_uuid, expected_version, events, opts)
  end

  @impl Decree.EventStore.Adapter
  defdelegate subscribe(adapter_meta, stream_uuid), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate child_spec(application, config), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate stream_forward(adapter_meta, stream_uuid, start_version, read_batch_size),
    to: Adapter

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
  defdelegate read_snapshot(adapter_meta, source_uuid), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate record_snapshot(adapter_meta, snapshot), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate delete_snapshot(adapter_meta, source_uuid), to: Adapter
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
    defstruct [:content_ref, events: []]
  end

  defmodule EmitPrefixed do
    @moduledoc false
    defstruct [:user_id, :command_id, events: []]
  end

  defmodule EmitIdentified do
    @moduledoc false
    defstruct [:user_id, events: []]
  end

  defmodule EmitByFunction do
    @moduledoc false
    defstruct [:key, events: []]
  end

  defmodule EmitByFunctionTagged do
    @moduledoc false
    defstruct [:key, events: []]
  end

  defmodule EmitMulti do
    @moduledoc false
    defstruct [:user_id, :command_id, events: []]
  end

  defmodule Race do
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

defmodule Decree.Event.Tagging.RaceHandler do
  @moduledoc false
  @behaviour Decree.Commands.Handler

  # Races the command it precedes: the first attempt's `before_execute` appends straight
  # to the stream, after the aggregate read its version, so that attempt's own append
  # answers `:wrong_expected_version` from Postgres.

  alias Decree.Aggregates.ExecutionContext
  alias Decree.Event.Mapper
  alias Decree.Event.Tagging.Commands.Race
  alias Decree.Event.Tagging.Events.Tagged
  alias Decree.Event.Tagging.{App, Ledger, Recorder}
  alias Decree.EventStore

  def race(%Ledger{}, %ExecutionContext{metadata: metadata} = context) do
    %ExecutionContext{command: %Race{user_id: user_id}} = context

    Recorder.record(:attempt_metadata, metadata)

    if length(Recorder.all(:attempt_metadata)) == 1 do
      event_data = Mapper.map_to_event_data([%Tagged{note: "concurrent"}], metadata: %{})

      :ok = EventStore.append_to_stream(App, user_id, :any_version, event_data)
    end

    :ok
  end

  def handle(%Ledger{}, %Race{events: events}), do: events
end

defmodule Decree.Event.Tagging.Router do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.Event.Tagging.Commands.{
    Emit,
    EmitAsRequester,
    EmitByFunction,
    EmitByFunctionTagged,
    EmitByRef,
    EmitIdentified,
    EmitMulti,
    EmitPrefixed,
    Race
  }

  alias Decree.Event.Tagging.{
    FunctionLedger,
    FunctionTaggedLedger,
    IdentifiedLedger,
    Ledger,
    PrefixedLedger,
    RaceHandler
  }

  identify PrefixedLedger, by: :user_id, prefix: "ledger-"
  identify IdentifiedLedger, by: :user_id
  identify FunctionLedger, by: &__MODULE__.key/1
  identify FunctionTaggedLedger, by: &__MODULE__.key/1

  dispatch Emit, to: Ledger, identity: :user_id
  dispatch EmitAsRequester, to: Ledger, identity: :requester_user_id, stream_tag: :user
  dispatch EmitByRef, to: Ledger, identity: :content_ref
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

  def key(%{key: key}), do: key
end

defmodule Decree.Event.Tagging.App do
  @moduledoc false
  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: Decree.Event.Tagging.Adapter,
      event_store: Decree.FrameworkEventStore
    ],
    pubsub: :local,
    registry: :local

  router(Decree.Event.Tagging.Router)

  # Its own event store process name, as `Decree.FrameworkStorage.name_event_store/1`
  # gives the applications on the unwrapped adapter.
  def init(config) do
    instance = Keyword.get(config, :name) || Keyword.fetch!(config, :application)

    {:ok,
     Keyword.update(config, :event_store, [], fn event_store ->
       Keyword.put_new(event_store, :name, Module.concat([instance, "FrameworkEventStore"]))
     end)}
  end
end
