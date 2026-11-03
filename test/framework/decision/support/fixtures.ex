defmodule Decree.DecisionFixtures do
  @moduledoc false

  # Fixtures for the decision tests. A `Scripted` command carries the functions its decision's
  # callbacks run, so a test states the behaviour it needs, and can block a callback on a
  # message to interleave another writer.

  @doc """
  The window every fixture decision answers. A test moves it with `with_window/1`.
  """
  def command_window_ms, do: Application.get_env(:decree, :decision_test_window_ms, 300_000)
end

defmodule Decree.DecisionFixtures.Claimed do
  @moduledoc false
  @derive Jason.Encoder
  defstruct [:claim_id, :scope, :note]

  # Its implementation answers no tag for no scope.
  def __conditional_tag_kinds__, do: ["scope"]

  defimpl Decree.Event.Tags do
    def tags(%{scope: nil}, _context), do: {:ok, []}
    def tags(%{scope: scope}, _context), do: {:ok, ["scope:" <> scope]}
  end
end

defmodule Decree.DecisionFixtures.Released do
  @moduledoc false
  @derive Jason.Encoder
  defstruct [:claim_id, :scope]

  defimpl Decree.Event.Tags do
    def tags(%{scope: scope}, _context), do: {:ok, ["scope:" <> scope]}
  end
end

defmodule Decree.DecisionFixtures.Stored do
  @moduledoc false

  # Upcast to `Claimed`, with a note saying so.
  @derive Jason.Encoder
  defstruct [:claim_id, :scope]

  defimpl Decree.Event.Upcaster do
    def upcast(%{claim_id: claim_id, scope: scope}, _metadata) do
      %Decree.DecisionFixtures.Claimed{claim_id: claim_id, scope: scope, note: "upcast"}
    end
  end
end

defmodule Decree.DecisionFixtures.Unreadable do
  @moduledoc false

  # Its upcaster fails as `mode` says, with `secret` in hand.
  @derive Jason.Encoder
  defstruct [:secret, :mode]

  defimpl Decree.Event.Upcaster do
    def upcast(%{mode: "raise", secret: secret}, _metadata), do: raise("cannot upcast " <> secret)
    def upcast(%{mode: "exit", secret: secret}, _metadata), do: exit({:gave_up, secret})
    def upcast(%{mode: "throw", secret: secret}, _metadata), do: throw({:thrown, secret})
    def upcast(event, _metadata), do: event
  end
end

defmodule Decree.DecisionFixtures.Burning do
  @moduledoc false

  # `evolve/2` raises on it.
  @derive Jason.Encoder
  defstruct [:scope]
end

defmodule Decree.DecisionFixtures.Secret do
  @moduledoc false

  # Carries a tag of the kind `DeletableApp` declares.
  @derive Jason.Encoder
  defstruct [:secret]

  defimpl Decree.Event.Tags do
    def tags(%{secret: secret}, _context), do: {:ok, ["secret:" <> secret]}
  end
end

defmodule Decree.DecisionFixtures.TagsAnswering do
  @moduledoc false

  # A `Decree.Event.Tags` implementation answering whatever the event says.
  @derive Jason.Encoder
  defstruct [:reason]

  defimpl Decree.Event.Tags do
    def tags(%{reason: reason}, _context), do: {:error, reason}
  end
end

defmodule Decree.DecisionFixtures.TagsRaising do
  @moduledoc false
  @derive Jason.Encoder
  defstruct [:claim_id]

  defimpl Decree.Event.Tags do
    def tags(_event, _context), do: raise("tags raised")
  end
end

defmodule Decree.DecisionFixtures.Activated do
  @moduledoc false

  # What switches `ActivatedDecision` on.
  @derive Jason.Encoder
  defstruct [:feature]

  defimpl Decree.Event.Tags do
    def tags(%{feature: feature}, _context), do: {:ok, ["feature:" <> feature]}
  end
end

defmodule Decree.DecisionFixtures.Claim do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope]
end

defmodule Decree.DecisionFixtures.Scripted do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, :correlation_id, script: %{}]
end

defmodule Decree.DecisionFixtures.Tight do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, script: %{}]
end

defmodule Decree.DecisionFixtures.Create do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, script: %{}]
end

defmodule Decree.DecisionFixtures.Activating do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, script: %{}]
end

defmodule Decree.DecisionFixtures.Digesting do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, script: %{}]
end

defmodule Decree.DecisionFixtures.ByFunction do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, script: %{}]

  def identity(%{claim_id: claim_id}), do: claim_id
end

defmodule Decree.DecisionFixtures.Recorded do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, :refuse]
end

defmodule Decree.DecisionFixtures.ClaimDecision do
  @moduledoc false

  # A plain decision with no optional callback: one claim per scope.
  use Decree.Decision, create: false

  alias Decree.DecisionFixtures.{Claim, Claimed}

  @impl Decree.Decision
  def command_window_ms, do: Decree.DecisionFixtures.command_window_ms()

  @impl Decree.Decision
  def query(%Claim{scope: scope}, %Decree.Event.TagContext{}) do
    [[types: [Atom.to_string(Claimed)], tags: ["scope:" <> scope]]]
  end

  @impl Decree.Decision
  def initial_state, do: :free

  @impl Decree.Decision
  def evolve(_state, %Claimed{}), do: :claimed

  @impl Decree.Decision
  def decide(:claimed, %Claim{}), do: {:error, :already_claimed}

  def decide(:free, %Claim{} = command) do
    %Claimed{claim_id: command.claim_id, scope: command.scope}
  end
end

defmodule Decree.DecisionFixtures.Scripting do
  @moduledoc false

  # A decision whose callbacks do what the command's script says, and by default what
  # `ClaimDecision` does. The state is the list of events folded.
  defmacro __using__(opts) do
    quote do
      use Decree.Decision, unquote(Keyword.put_new(opts, :create, false))

      alias Decree.DecisionFixtures.{Burning, Claimed}

      def command_window_ms, do: Decree.DecisionFixtures.command_window_ms()

      def scope_tags(command) do
        case command.script do
          %{scope_tags: scope_tags} -> scope_tags.(command)
          _script -> []
        end
      end

      def query(command, tag_context) do
        case command.script do
          %{query: query} ->
            query.(command, tag_context)

          _script ->
            [[types: [Atom.to_string(Claimed)], tags: ["scope:" <> command.scope]]]
        end
      end

      def followup_query(command, tag_context, business_events) do
        case command.script do
          %{followup: followup} -> followup.(command, tag_context, business_events)
          _script -> []
        end
      end

      def initial_state, do: []

      def evolve(_state, %Burning{}), do: raise("evolve raised")
      def evolve(state, event), do: state ++ [event]

      def admit(state, command) do
        case command.script do
          %{admit: admit} -> admit.(state, command)
          _script -> :ok
        end
      end

      def decide(state, command) do
        case command.script do
          %{decide: decide} -> decide.(state, command)
          _script -> %Claimed{claim_id: command.claim_id, scope: command.scope}
        end
      end

      defoverridable evolve: 2
    end
  end
end

defmodule Decree.DecisionFixtures.ScriptedDecision do
  @moduledoc false
  use Decree.DecisionFixtures.Scripting
end

defmodule Decree.DecisionFixtures.TightDecision do
  @moduledoc false

  # Three of its own past the framework's: the index item is one item and one probe, and the
  # store-backed cases' nine facts are nine matched events.
  use Decree.DecisionFixtures.Scripting,
    max_items: 4,
    max_probes: 5,
    max_events: 12,
    max_batch_events: 2
end

defmodule Decree.DecisionFixtures.ActivatedDecision do
  @moduledoc false

  # Off until an `Activated` event for "claims" is there.
  use Decree.DecisionFixtures.Scripting,
    activation: [types: ["Elixir.Decree.DecisionFixtures.Activated"], tags: ["feature:claims"]]
end

defmodule Decree.DecisionFixtures.DigestDecision do
  @moduledoc false

  # Reads `email` as a digest, so its family is keyed by a fingerprint.
  use Decree.DecisionFixtures.Scripting

  def index_families, do: %{"email" => :digest}
end

defmodule Decree.DecisionFixtures.CreateDecision do
  @moduledoc false

  # Makes a `Claimed` fact per route identity.
  use Decree.DecisionFixtures.Scripting,
    create: [types: ["Elixir.Decree.DecisionFixtures.Claimed"]]
end

defmodule Decree.DecisionFixtures.RecordingDecision do
  @moduledoc false

  # Tells the process registered as `:decision_recorder` about every callback, in order.
  use Decree.Decision, create: false

  alias Decree.DecisionFixtures.{Claimed, Released, Scripted}

  @impl Decree.Decision
  def command_window_ms, do: Decree.DecisionFixtures.command_window_ms()

  @impl Decree.Decision
  def query(%Scripted{scope: scope}, _tag_context) do
    record({:query, scope})

    [[types: [Atom.to_string(Claimed)], tags: ["scope:" <> scope]]]
  end

  @impl Decree.Decision
  def followup_query(%Scripted{scope: scope}, _tag_context, business_events) do
    record({:followup_query, business_events})

    [[types: [Atom.to_string(Released)], tags: ["scope:" <> scope]]]
  end

  @impl Decree.Decision
  def initial_state, do: []

  @impl Decree.Decision
  def evolve(state, event) do
    record({:evolve, event})

    state ++ [event]
  end

  @impl Decree.Decision
  def admit(state, %Scripted{script: script}) do
    record({:admit, state})

    Map.get(script, :admit, :ok)
  end

  @impl Decree.Decision
  def decide(state, %Scripted{claim_id: claim_id, scope: scope}) do
    record({:decide, state})

    %Claimed{claim_id: claim_id, scope: scope, note: "decided"}
  end

  defp record(call), do: send(:decision_recorder, {:callback, call})
end

defmodule Decree.DecisionFixtures.Router do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.DecisionFixtures.{
    ActivatedDecision,
    Activating,
    ByFunction,
    Claim,
    ClaimDecision,
    Create,
    CreateDecision,
    DigestDecision,
    Digesting,
    Scripted,
    ScriptedDecision,
    Tight,
    TightDecision
  }

  decide(Claim, to: ClaimDecision, identity: :claim_id, identity_prefix: "claim-")

  decide(Activating,
    to: ActivatedDecision,
    identity: :claim_id,
    identity_prefix: "activating-"
  )

  decide(Digesting, to: DigestDecision, identity: :claim_id, identity_prefix: "digesting-")

  decide(Scripted,
    to: ScriptedDecision,
    identity: :claim_id,
    identity_prefix: "scripted-"
  )

  decide(Tight, to: TightDecision, identity: :claim_id, identity_prefix: "tight-")
  decide(Create, to: CreateDecision, identity: :claim_id, identity_prefix: "create-")

  decide(ByFunction,
    to: Decree.DecisionFixtures.FunctionDecision,
    identity: &ByFunction.identity/1,
    identity_prefix: "function-",
    stream_tag: :claim
  )
end

defmodule Decree.DecisionFixtures.FunctionDecision do
  @moduledoc false
  use Decree.DecisionFixtures.Scripting
end

defmodule Decree.DecisionFixtures.ParityEvent do
  @moduledoc false
  @derive Jason.Encoder
  defstruct [:id, :scope]

  defimpl Decree.Event.Tags do
    def tags(%{scope: scope}, _context), do: {:ok, ["parity:" <> scope]}
  end
end

defmodule Decree.DecisionFixtures.ParityCommand do
  @moduledoc false

  # One command shape for both routes. `refuse:` makes either refuse, and `halt:` makes the
  # middleware halt.
  defstruct [:command_id, :id, :scope, :refuse, :halt]
end

defmodule Decree.DecisionFixtures.ParityAggregateCommand do
  @moduledoc false
  defstruct [:command_id, :id, :scope, :refuse, :halt]
end

defmodule Decree.DecisionFixtures.ParityAggregate do
  @moduledoc false

  alias Decree.DecisionFixtures.{ParityAggregateCommand, ParityEvent}

  defstruct events: []

  def execute(%__MODULE__{}, %ParityAggregateCommand{refuse: reason}) when not is_nil(reason) do
    {:error, reason}
  end

  def execute(%__MODULE__{}, %ParityAggregateCommand{id: id, scope: scope}) do
    %ParityEvent{id: id, scope: scope}
  end

  def apply(%__MODULE__{events: events} = state, event) do
    %__MODULE__{state | events: events ++ [event]}
  end
end

defmodule Decree.DecisionFixtures.ParityDecision do
  @moduledoc false
  use Decree.Decision, create: false

  alias Decree.DecisionFixtures.{ParityCommand, ParityEvent}

  @impl Decree.Decision
  def command_window_ms, do: Decree.DecisionFixtures.command_window_ms()

  @impl Decree.Decision
  def query(%ParityCommand{scope: scope}, _tag_context) do
    [[types: [Atom.to_string(ParityEvent)], tags: ["parity:" <> scope]]]
  end

  @impl Decree.Decision
  def initial_state, do: []

  @impl Decree.Decision
  def evolve(state, event), do: state ++ [event]

  @impl Decree.Decision
  def decide(_state, %ParityCommand{refuse: reason}) when not is_nil(reason), do: {:error, reason}
  def decide(_state, %ParityCommand{id: id, scope: scope}), do: %ParityEvent{id: id, scope: scope}
end

defmodule Decree.DecisionFixtures.RecordingMiddleware do
  @moduledoc false

  # Sends the dispatching process each stage it sees, with the pipeline as it stood.
  @behaviour Decree.Middleware

  alias Decree.Middleware.Pipeline

  def before_dispatch(%Pipeline{command: %{halt: true}} = pipeline) do
    pipeline = pipeline |> Pipeline.respond({:error, :halted_here}) |> Pipeline.halt()

    record(:before_dispatch, pipeline)
  end

  def before_dispatch(%Pipeline{} = pipeline), do: record(:before_dispatch, pipeline)

  def after_dispatch(%Pipeline{} = pipeline), do: record(:after_dispatch, pipeline)

  def after_failure(%Pipeline{} = pipeline), do: record(:after_failure, pipeline)

  defp record(stage, %Pipeline{} = pipeline) do
    send(self(), {:middleware, stage, pipeline})

    pipeline
  end
end

defmodule Decree.DecisionFixtures.ParityRouter do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.DecisionFixtures.{
    ParityAggregate,
    ParityAggregateCommand,
    ParityCommand,
    ParityDecision,
    RecordingMiddleware
  }

  middleware RecordingMiddleware

  identify ParityAggregate, by: :id, prefix: "parity-aggregate-"
  identify ParityDecision, by: :id, prefix: "parity-decision-"

  dispatch ParityAggregateCommand, to: ParityAggregate
  decide(ParityCommand, to: ParityDecision)
end

defmodule Decree.DecisionFixtures.StrayCommand do
  @moduledoc false
  defstruct [:command_id, :id]
end

defmodule Decree.DecisionFixtures.BareStrayCommand do
  @moduledoc false
  defstruct [:command_id, :id]
end

defmodule Decree.DecisionFixtures.StrayAggregate do
  @moduledoc false

  defstruct [:id]

  def execute(%__MODULE__{}, %{id: id}) do
    %Decree.DecisionFixtures.ParityEvent{id: id, scope: "stray"}
  end

  def apply(%__MODULE__{} = state, _event), do: state
end

defmodule Decree.DecisionFixtures.StrayRouter do
  @moduledoc false

  # What the build cannot check: an aggregate's function prefix and bare identity landing in
  # a decision's prefix. The store's refusal holds them.
  use Decree.Commands.Router

  alias Decree.DecisionFixtures.{BareStrayCommand, StrayAggregate, StrayCommand}

  def prefix, do: "claim-"

  dispatch StrayCommand, to: StrayAggregate, identity: :id, identity_prefix: &__MODULE__.prefix/0
  dispatch BareStrayCommand, to: StrayAggregate, identity: :id
end

defmodule Decree.DecisionFixtures.App do
  @moduledoc false

  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: Decree.EventStore.Adapters.EventStore,
      event_store: Decree.FrameworkEventStore,
      conditional_appends: [
        stream_prefixes: [
          "claim-",
          "scripted-",
          "tight-",
          "create-",
          "function-",
          "parity-decision-",
          "activating-",
          "digesting-"
        ]
      ]
    ],
    pubsub: :local,
    registry: :local

  router Decree.DecisionFixtures.Router
  router Decree.DecisionFixtures.ParityRouter
  router Decree.DecisionFixtures.StrayRouter
  router Decree.TagIndexFixtures.Router

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.DecisionFixtures.DeletableApp do
  @moduledoc false

  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: Decree.EventStore.Adapters.EventStore,
      event_store: Decree.FrameworkEventStore,
      conditional_appends: [stream_prefixes: ["scripted-"]],
      deletable: [tag_kinds: ["secret"]]
    ],
    pubsub: :local,
    registry: :local

  router Decree.DecisionFixtures.Router

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.DecisionFixtures.IndexDeletableApp do
  @moduledoc false

  # The fixture enable declared deletable, so the one tag fetch leaves its tags out.
  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: Decree.EventStore.Adapters.EventStore,
      event_store: Decree.FrameworkEventStore,
      conditional_appends: [stream_prefixes: ["scripted-"]],
      deletable: [event_types: ["Elixir.Decree.TagIndexFixtures.TagIndexEnabled"]]
    ],
    pubsub: :local,
    registry: :local

  router Decree.DecisionFixtures.Router

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.DecisionFixtures.DisableDeletableApp do
  @moduledoc false

  # The fixture disable declared deletable, so the fetch leaves out a disable's tags and keeps
  # the enable before it.
  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: Decree.EventStore.Adapters.EventStore,
      event_store: Decree.FrameworkEventStore,
      conditional_appends: [stream_prefixes: ["scripted-"]],
      deletable: [event_types: ["Elixir.Decree.TagIndexFixtures.TagIndexDisabled"]]
    ],
    pubsub: :local,
    registry: :local

  router Decree.DecisionFixtures.Router

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.DecisionFixtures.DeletableStreamsApp do
  @moduledoc false

  # A prefix both conditional and deletable, so a hard delete can reach a decision's stream.
  # `Claimed` is declared, so a query here must not name it.
  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: Decree.EventStore.Adapters.EventStore,
      event_store: Decree.FrameworkEventStore,
      conditional_appends: [stream_prefixes: ["scripted-"]],
      deletable: [
        stream_prefixes: ["scripted-"],
        event_types: ["Elixir.Decree.DecisionFixtures.Claimed"]
      ],
      enable_hard_deletes: true
    ],
    pubsub: :local,
    registry: :local

  router Decree.DecisionFixtures.Router

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.DecisionFixtures.HardDeletesApp do
  @moduledoc false

  use Decree.Application,
    otp_app: :decree,
    event_store: [
      adapter: Decree.EventStore.Adapters.EventStore,
      event_store: Decree.FrameworkEventStore,
      conditional_appends: [stream_prefixes: ["scripted-"]],
      enable_hard_deletes: true
    ],
    pubsub: :local,
    registry: :local

  router Decree.DecisionFixtures.Router

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.DecisionFixtures.StrongHandler do
  @moduledoc false

  # Strongly consistent. Holds any event whose scope is "held", so that stream never acks, and
  # takes the milliseconds in its note over one whose scope is "late".
  use Decree.Event.Handler,
    application: Decree.DecisionFixtures.App,
    name: "DecisionStrongHandler",
    consistency: :strong

  alias Decree.DecisionFixtures.Claimed

  def handle(%Claimed{scope: "held"}, _metadata), do: :timer.sleep(:infinity)
  def handle(%Claimed{scope: "late", note: note}, _metadata), do: :timer.sleep(note)
  def handle(_event, _metadata), do: :ok
end
