defmodule Decree.DecisionFixtures.Personal do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, script: %{}]
end

defmodule Decree.DecisionFixtures.Bare do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, script: %{}]
end

defmodule Decree.DecisionFixtures.PersonalTight do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, script: %{}]
end

defmodule Decree.DecisionFixtures.Failing do
  @moduledoc false

  # `evolve/2` fails on `StateFails` as its mode says, with the state in hand.
  alias Decree.PersonalFields.Fixtures.Events.StateFails

  def evolve(state, %StateFails{mode: "match"}) do
    :never = state
  end

  def evolve(state, %StateFails{mode: "clause"}), do: only_empty(state)
  def evolve(state, %StateFails{mode: "exit"}), do: exit({:gave_up, state})
  def evolve(state, %StateFails{mode: "throw"}), do: throw({:thrown, state})
  def evolve(state, event), do: state ++ [event]

  defp only_empty([]), do: []
end

defmodule Decree.DecisionFixtures.PersonalDecision do
  @moduledoc false

  # A decision that lists what it reads. `Unfolded` is declared and off the list.
  use Decree.DecisionFixtures.Scripting
  use Decree.PersonalFields.Aggregate

  alias Decree.PersonalFields.Fixtures.Events.{Registered, Renamed, Tagged}

  @personal_fold [Registered, Renamed, Tagged]

  def evolve(state, event), do: Decree.DecisionFixtures.Failing.evolve(state, event)
end

defmodule Decree.DecisionFixtures.BareDecision do
  @moduledoc false

  # The same callbacks, with no `Decree.PersonalFields.Aggregate`.
  use Decree.DecisionFixtures.Scripting

  def evolve(state, event), do: Decree.DecisionFixtures.Failing.evolve(state, event)
end

defmodule Decree.DecisionFixtures.PersonalTightDecision do
  @moduledoc false
  use Decree.DecisionFixtures.Scripting, max_batch_events: 1
  use Decree.PersonalFields.Aggregate

  alias Decree.PersonalFields.Fixtures.Events.Registered

  @personal_fold [Registered]
end

defmodule Decree.DecisionFixtures.PersonalRouter do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.DecisionFixtures.{
    Bare,
    BareDecision,
    Personal,
    PersonalDecision,
    PersonalTight,
    PersonalTightDecision,
    RecordingMiddleware
  }

  middleware RecordingMiddleware

  decide(Personal, to: PersonalDecision, identity: :claim_id, identity_prefix: "personal-")
  decide(Bare, to: BareDecision, identity: :claim_id, identity_prefix: "bare-")

  decide(PersonalTight,
    to: PersonalTightDecision,
    identity: :claim_id,
    identity_prefix: "tight-personal-"
  )
end

defmodule Decree.DecisionFixtures.PersonalApp do
  @moduledoc false

  use Decree.Application,
    otp_app: :decree,
    key_source: Decree.PersonalFields.Fixtures.Keys,
    personal_metadata_keys: [:email],
    event_store: [
      event_store: Decree.FrameworkEventStore,
      conditional_appends: [stream_prefixes: ["personal-", "bare-", "tight-personal-"]]
    ],
    pubsub: :local,
    registry: :local

  router Decree.DecisionFixtures.PersonalRouter

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end
