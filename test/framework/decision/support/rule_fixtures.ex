defmodule Decree.RuleFixtures do
  @moduledoc false

  # Fixtures for the rule tests. The worked one is the design's two-part email rule, on a
  # `User` aggregate: part 1 finds an address's registrations, part 2 reads each candidate's
  # releases, and no address is on a release. A `Ruled` command carries the functions its rule
  # and its aggregate run, so a test states the behaviour it needs, and a function can block on
  # a message to let another writer in.
end

defmodule Decree.RuleFixtures.Registered do
  @moduledoc false

  # Part 1. A nil digest holds nothing and is tagged nothing.
  @derive Jason.Encoder
  defstruct [:user_id, :digest, :note]

  def __required_tag_kinds__(%__MODULE__{digest: nil}, _tag_context), do: []
  def __required_tag_kinds__(%__MODULE__{}, _tag_context), do: ["email"]

  def __conditional_tag_kinds__, do: ["email"]

  def __tag_kinds__, do: ["email", "user"]

  defimpl Decree.Event.Tags do
    def tags(%{digest: nil}, _context), do: {:ok, []}
    def tags(%{digest: digest}, _context), do: {:ok, ["email:" <> digest]}
  end
end

defmodule Decree.RuleFixtures.Erased do
  @moduledoc false

  # Part 2, and dominant. `erased_by:` is its own kind, so erasing another user is not a read
  # of the one who did it.
  @derive Jason.Encoder
  defstruct [:user_id, :erased_by]

  def __tag_kinds__, do: ["erased_by", "user"]

  defimpl Decree.Event.Tags do
    def tags(%{erased_by: nil}, _context), do: {:ok, []}
    def tags(%{erased_by: erased_by}, _context), do: {:ok, ["erased_by:" <> erased_by]}
  end
end

defmodule Decree.RuleFixtures.Removed do
  @moduledoc false

  # Part 2's other release.
  @derive Jason.Encoder
  defstruct [:user_id]

  def __tag_kinds__, do: ["user"]
end

defmodule Decree.RuleFixtures.Noted do
  @moduledoc false

  # What a `Ruled` command appends by default.
  @derive Jason.Encoder
  defstruct [:id, :note]
end

defmodule Decree.RuleFixtures.Read do
  @moduledoc false

  # What a `Ruled` command's rule reads.
  @derive Jason.Encoder
  defstruct [:scope, :secret, :note]
end

defmodule Decree.RuleFixtures.OAuthJoined do
  @moduledoc false

  # A registration with no address holds nothing: `email` is conditional, and asked for by the
  # event itself when it has one. `forget_tag` is the buggy implementation that loses it.
  @derive Jason.Encoder
  defstruct [:id, :digest, :forget_tag]

  def __required_tag_kinds__(%__MODULE__{digest: nil}, _tag_context), do: []
  def __required_tag_kinds__(%__MODULE__{}, _tag_context), do: ["email"]

  def __conditional_tag_kinds__, do: ["email"]

  def __tag_kinds__, do: ["email"]

  defimpl Decree.Event.Tags do
    def tags(%{digest: nil}, _context), do: {:ok, []}
    def tags(%{forget_tag: true}, _context), do: {:ok, []}
    def tags(%{digest: digest}, _context), do: {:ok, ["email:" <> digest]}
  end
end

defmodule Decree.RuleFixtures.StrictOAuthJoined do
  @moduledoc false

  # `OAuthJoined` with `email` not conditional, and opted in to nothing.
  @derive Jason.Encoder
  defstruct [:id, :digest]

  def __tag_kinds__, do: ["email"]

  defimpl Decree.Event.Tags do
    def tags(%{digest: nil}, _context), do: {:ok, []}
    def tags(%{digest: digest}, _context), do: {:ok, ["email:" <> digest]}
  end
end

defmodule Decree.RuleFixtures.MemberAdded do
  @moduledoc false

  # An unkeyed member has no `member_key:` tag, and the kind is conditional.
  @derive Jason.Encoder
  defstruct [:id, :key]

  def __conditional_tag_kinds__, do: ["member_key"]

  def __tag_kinds__, do: ["member_key"]

  defimpl Decree.Event.Tags do
    def tags(%{key: nil}, _context), do: {:ok, []}
    def tags(%{key: key}, _context), do: {:ok, ["member_key:" <> key]}
  end
end

defmodule Decree.RuleFixtures.StrictMemberAdded do
  @moduledoc false

  # `MemberAdded` with the kind not conditional.
  @derive Jason.Encoder
  defstruct [:id, :key]

  def __tag_kinds__, do: ["member_key"]

  defimpl Decree.Event.Tags do
    def tags(%{key: nil}, _context), do: {:ok, []}
    def tags(%{key: key}, _context), do: {:ok, ["member_key:" <> key]}
  end
end

defmodule Decree.RuleFixtures.RegisterUser do
  @moduledoc false
  defstruct [:command_id, :user_id, :digest, :rule_result, script: %{}]
end

defmodule Decree.RuleFixtures.EraseUser do
  @moduledoc false

  # The plain command on the aggregate a rule route also reaches.
  defstruct [:command_id, :user_id, :erased_by]
end

defmodule Decree.RuleFixtures.RemoveUser do
  @moduledoc false
  defstruct [:command_id, :user_id]
end

defmodule Decree.RuleFixtures.Ruled do
  @moduledoc false
  defstruct [:command_id, :id, :scope, :rule_result, script: %{}]
end

defmodule Decree.RuleFixtures.Tight do
  @moduledoc false
  defstruct [:command_id, :id, :scope, :rule_result, script: %{}]
end

defmodule Decree.RuleFixtures.Personal do
  @moduledoc false
  defstruct [:command_id, :id, :scope, :rule_result, script: %{}]
end

defmodule Decree.RuleFixtures.RegisterOAuth do
  @moduledoc false
  defstruct [:command_id, :id, :scope, :rule_result, script: %{}]
end

defmodule Decree.RuleFixtures.AddMember do
  @moduledoc false
  defstruct [:command_id, :id, :scope, :rule_result, script: %{}]
end

defmodule Decree.RuleFixtures.DigestChecked do
  @moduledoc false

  # On a route whose rule says `email` holds digests.
  defstruct [:command_id, :id, :scope, :rule_result, script: %{}]
end

defmodule Decree.RuleFixtures.Unrouted do
  @moduledoc false

  # Routed only by `OutsideRouter`, which no application includes.
  defstruct [:command_id, :id, :scope, :rule_result, script: %{}]
end

defmodule Decree.RuleFixtures.PlainNote do
  @moduledoc false

  # A command with the field and no rule: it stays as sent.
  defstruct [:command_id, :id, :scope, :rule_result, script: %{}]
end

defmodule Decree.RuleFixtures.Stopped do
  @moduledoc false

  # On a route whose aggregate stops after every event.
  defstruct [:command_id, :id, :scope, :rule_result, script: %{}]
end

defmodule Decree.RuleFixtures.Gated do
  @moduledoc false

  # On a route with a `before_execute` that refuses.
  defstruct [:command_id, :id, :scope, :rule_result, script: %{}]
end

defmodule Decree.RuleFixtures.Adopted do
  @moduledoc false
  defstruct [:command_id, :id, :scope, :rule_result, script: %{}]
end

defmodule Decree.RuleFixtures.Picky do
  @moduledoc false

  # For an `execute/2` with no clause for what rode.
  defstruct [:command_id, :id, :scope, :rule_result, script: %{}]
end

defmodule Decree.RuleFixtures.EmailRule do
  @moduledoc false

  # Design doc §4's two-part query, and the fold `execute/2` runs over what rides.
  use Decree.Decision.Rule

  alias Decree.RuleFixtures.{Erased, Registered, RegisterUser, Removed}

  @registered Atom.to_string(Registered)
  @releases [Atom.to_string(Erased), Atom.to_string(Removed)]

  @impl Decree.Decision.Rule
  def query(%RegisterUser{digest: nil}, _tag_context), do: []

  def query(%RegisterUser{digest: digest}, _tag_context) do
    [[types: [@registered], tags: ["email:" <> digest]]]
  end

  @impl Decree.Decision.Rule
  def followup_query(%RegisterUser{script: script}, _tag_context, registrations) do
    with %{between_reads: between_reads} <- script, do: between_reads.(registrations)

    for candidate <- registrations |> holders() |> Enum.uniq() do
      [types: @releases, tags: ["user:" <> candidate]]
    end
  end

  @doc """
  Who holds the address, once the events are folded in position order.
  """
  def holders(events) do
    Enum.reduce(events, [], fn
      %Registered{user_id: user_id}, holders -> [user_id | holders]
      %Erased{user_id: user_id}, holders -> List.delete(holders, user_id)
      %Removed{user_id: user_id}, holders -> List.delete(holders, user_id)
    end)
  end
end

defmodule Decree.RuleFixtures.User do
  @moduledoc false

  alias Decree.Decision.Rule.Result
  alias Decree.RuleFixtures.{EmailRule, EraseUser, Erased, Registered, RegisterUser}
  alias Decree.RuleFixtures.{Removed, RemoveUser}

  @derive Jason.Encoder
  defstruct [:user_id, :digest, registered?: false, released?: false]

  def execute(%__MODULE__{registered?: true}, %RegisterUser{}), do: {:error, :already_registered}

  def execute(%__MODULE__{}, %RegisterUser{rule_result: %Result{events: events}} = command) do
    with %{deciding: deciding} <- command.script, do: deciding.(events)

    case EmailRule.holders(events) do
      [] -> %Registered{user_id: command.user_id, digest: command.digest}
      [_holder | _holders] -> {:error, :email_taken}
    end
  end

  def execute(%__MODULE__{}, %EraseUser{user_id: user_id, erased_by: erased_by}) do
    %Erased{user_id: user_id, erased_by: erased_by}
  end

  def execute(%__MODULE__{}, %RemoveUser{user_id: user_id}), do: %Removed{user_id: user_id}

  def apply(%__MODULE__{} = state, %Registered{user_id: user_id, digest: digest}) do
    %__MODULE__{state | user_id: user_id, digest: digest, registered?: true}
  end

  def apply(%__MODULE__{} = state, %{user_id: _user_id}) do
    %__MODULE__{state | released?: true}
  end
end

defmodule Decree.RuleFixtures.Scripting do
  @moduledoc false

  # A rule whose callbacks do what the command's script says. By default it reads the scope's
  # `Read` events.
  defmacro __using__(opts) do
    quote do
      use Decree.Decision.Rule, unquote(opts)

      alias Decree.RuleFixtures.Read

      @impl Decree.Decision.Rule
      def query(command, tag_context) do
        case command.script do
          %{query: query} -> query.(command, tag_context)
          _script -> [[types: [Atom.to_string(Read)], tags: ["scope:" <> command.scope]]]
        end
      end

      @impl Decree.Decision.Rule
      def followup_query(command, tag_context, events) do
        case command.script do
          %{followup: followup} -> followup.(command, tag_context, events)
          _script -> []
        end
      end
    end
  end
end

defmodule Decree.RuleFixtures.ScriptedRule do
  @moduledoc false
  use Decree.RuleFixtures.Scripting
end

defmodule Decree.RuleFixtures.TightRule do
  @moduledoc false
  # Three events of its own past the store-backed cases' nine facts.
  use Decree.RuleFixtures.Scripting, max_events: 12, max_batch_events: 2
end

defmodule Decree.RuleFixtures.DigestRule do
  @moduledoc false
  use Decree.RuleFixtures.Scripting

  @impl Decree.Decision.Rule
  def index_families, do: %{"email" => :digest, "label" => :plain}
end

defmodule Decree.RuleFixtures.PersonalRule do
  @moduledoc false

  use Decree.RuleFixtures.Scripting
  use Decree.PersonalFields.Aggregate

  alias Decree.PersonalFields.Fixtures.Events.Registered

  @personal_fold [Registered]
end

defmodule Decree.RuleFixtures.RuledAggregate do
  @moduledoc false

  # Holds what it applied. It does what the command's script says, and by default notes how many
  # events rode with the command.
  alias Decree.Decision.Rule.Result
  alias Decree.RuleFixtures.Noted

  @derive Jason.Encoder
  defstruct notes: []

  def execute(%__MODULE__{} = state, %{script: %{execute: execute}} = command) do
    execute.(state, command)
  end

  def execute(%__MODULE__{}, %{id: id, rule_result: %Result{events: events}}) do
    %Noted{id: id, note: length(events)}
  end

  def execute(%__MODULE__{}, %{id: id}), do: %Noted{id: id, note: "plain"}

  def refuse(%__MODULE__{}, _context), do: {:error, :gated}

  def apply(%__MODULE__{notes: notes} = state, %Noted{note: note}) do
    %__MODULE__{state | notes: notes ++ [note]}
  end

  def apply(%__MODULE__{} = state, _event), do: state
end

defmodule Decree.RuleFixtures.AdoptedAggregate do
  @moduledoc false

  # What `RuledAggregate` does, for an aggregate that lists personal fields.
  use Decree.PersonalFields.Aggregate

  defstruct notes: []

  def execute(%__MODULE__{}, %{script: %{execute: execute}} = command) do
    execute.(%Decree.RuleFixtures.RuledAggregate{}, command)
  end

  def apply(%__MODULE__{} = state, _event), do: state
end

defmodule Decree.RuleFixtures.PickyAggregate do
  @moduledoc false

  alias Decree.Decision.Rule.Result
  alias Decree.RuleFixtures.{Noted, Picky}

  defstruct [:id]

  def execute(%__MODULE__{}, %Picky{id: id, rule_result: %Result{events: []}}) do
    %Noted{id: id}
  end

  def apply(%__MODULE__{} = state, _event), do: state
end

defmodule Decree.RuleFixtures.Stopping do
  @moduledoc false

  # An aggregate that stops after every event.
  @behaviour Decree.Aggregates.AggregateLifespan

  def after_event(_event), do: :stop
  def after_command(_command), do: :infinity
  def after_error(_error), do: :infinity
end

defmodule Decree.RuleFixtures.Router do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.RuleFixtures.{AddMember, Adopted, AdoptedAggregate, DigestChecked, DigestRule}
  alias Decree.RuleFixtures.{EmailRule, EraseUser, Gated, Personal, RegisterOAuth}
  alias Decree.RuleFixtures.{PersonalRule, Picky, PickyAggregate, PlainNote, RegisterUser}
  alias Decree.RuleFixtures.{RemoveUser, Ruled, RuledAggregate, ScriptedRule, Stopped, Stopping}
  alias Decree.RuleFixtures.{Tight, TightRule, User}

  identify User, by: :user_id, prefix: "user-"
  identify RuledAggregate, by: :id, prefix: "ruled-"
  identify AdoptedAggregate, by: :id, prefix: "adopted-"
  identify PickyAggregate, by: :id, prefix: "picky-"

  dispatch RegisterUser, to: User, rule: EmailRule
  dispatch [EraseUser, RemoveUser], to: User

  dispatch Ruled, to: RuledAggregate, rule: ScriptedRule
  dispatch Tight, to: RuledAggregate, rule: TightRule
  dispatch Personal, to: RuledAggregate, rule: PersonalRule
  dispatch Stopped, to: RuledAggregate, rule: ScriptedRule, lifespan: Stopping
  dispatch Gated, to: RuledAggregate, rule: ScriptedRule, before_execute: :refuse
  dispatch PlainNote, to: RuledAggregate

  dispatch [RegisterOAuth, AddMember], to: RuledAggregate, rule: ScriptedRule
  dispatch DigestChecked, to: RuledAggregate, rule: DigestRule

  dispatch Adopted, to: AdoptedAggregate, rule: ScriptedRule
  dispatch Picky, to: PickyAggregate, rule: ScriptedRule
end

defmodule Decree.RuleFixtures.OutsideRouter do
  @moduledoc false

  # A router no application includes, so nothing checked its declaration at start.
  use Decree.Commands.Router

  alias Decree.RuleFixtures.{Noted, ScriptedRule, Unrouted}

  defmodule Target do
    @moduledoc false
    defstruct [:ignored]
    def execute(%__MODULE__{}, %Unrouted{id: id}), do: %Noted{id: id}
    def apply(%__MODULE__{} = state, _event), do: state
  end

  dispatch Unrouted, to: Target, identity: :id, identity_prefix: "outside-", rule: ScriptedRule
end

defmodule Decree.RuleFixtures.DecisionPrefixRouter do
  @moduledoc false

  # A router no application includes, whose rule route's streams land under a decision's
  # prefix, `"decision-"`.
  use Decree.Commands.Router

  alias Decree.RuleFixtures.{EmailRule, RegisterUser, User}

  dispatch RegisterUser,
    to: User,
    identity: :user_id,
    identity_prefix: "decision-",
    rule: EmailRule
end

defmodule Decree.RuleFixtures.PlainRouter do
  @moduledoc false

  # The same aggregate with no rule route at all.
  use Decree.Commands.Router

  alias Decree.RuleFixtures.{EraseUser, User}

  identify User, by: :user_id, prefix: "user-"

  dispatch EraseUser, to: User
end

defmodule Decree.RuleFixtures.InnerComposite do
  @moduledoc false
  use Decree.Commands.CompositeRouter

  router Decree.RuleFixtures.OutsideRouter
end

defmodule Decree.RuleFixtures.ComposedRouter do
  @moduledoc false
  use Decree.Commands.CompositeRouter

  router Decree.RuleFixtures.InnerComposite
  router Decree.RuleFixtures.PlainRouter
end

defmodule Decree.RuleFixtures.RecordingAdapter do
  @moduledoc false

  # The Postgres event store adapter, telling the process registered as `:rule_recorder` what
  # reached it, because a failed attempt leaves no row and a plain one looks the same as any.

  @behaviour Decree.EventStore.Adapter

  alias Decree.EventStore.Adapters.EventStore, as: Adapter

  @impl Decree.EventStore.Adapter
  def append_to_stream(adapter_meta, stream_uuid, expected_version, events, opts \\ []) do
    record({:append_to_stream, stream_uuid, expected_version, length(events), opts})

    Adapter.append_to_stream(adapter_meta, stream_uuid, expected_version, events, opts)
  end

  @impl Decree.EventStore.Adapter
  def read_query(adapter_meta, items, opts) do
    record({:read_query, items, opts})

    Adapter.read_query(adapter_meta, items, opts)
  end

  @impl Decree.EventStore.Adapter
  def read_tags(adapter_meta, positions, opts) do
    record({:read_tags, positions})

    Adapter.read_tags(adapter_meta, positions, opts)
  end

  defp record(message) do
    with pid when is_pid(pid) <- Process.whereis(:rule_recorder), do: send(pid, message)
  end

  @impl Decree.EventStore.Adapter
  defdelegate append_to_streams(adapter_meta, appends, opts), to: Adapter

  @impl Decree.EventStore.Adapter
  defdelegate subscribe(adapter_meta, stream_uuid), to: Adapter

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
end

defmodule Decree.RuleFixtures.Stores do
  @moduledoc false

  # A store config for a rule test's application, and the name an instance's store runs under.

  alias Decree.RuleFixtures.{AdoptedAggregate, PickyAggregate, RecordingAdapter}
  alias Decree.RuleFixtures.{RuledAggregate, User}

  @targets [User, RuledAggregate, AdoptedAggregate, PickyAggregate]

  def targets, do: @targets

  def store(opts \\ []) do
    Decree.FrameworkStorage.event_store_config(
      [adapter: RecordingAdapter, conditional_appends: [targets: @targets]] ++ opts
    )
  end

  def named_store(config) do
    instance = Keyword.get(config, :name) || Keyword.fetch!(config, :application)
    name = Module.concat([instance, "FrameworkEventStore"])

    Keyword.update!(config, :event_store, &Keyword.put_new(&1, :name, name))
  end
end

defmodule Decree.RuleFixtures.App do
  @moduledoc false

  use Decree.Application,
    otp_app: :decree,
    key_source: Decree.PersonalFields.Fixtures.Keys,
    event_store: Decree.RuleFixtures.Stores.store(),
    pubsub: :local,
    registry: :local

  router Decree.RuleFixtures.Router
  router Decree.TagIndexFixtures.Router

  def init(config), do: {:ok, Decree.RuleFixtures.Stores.named_store(config)}
end

defmodule Decree.RuleFixtures.PlainApp do
  @moduledoc false

  # Nothing declared and no rule route.
  use Decree.Application,
    otp_app: :decree,
    event_store:
      Decree.FrameworkStorage.event_store_config(adapter: Decree.RuleFixtures.RecordingAdapter),
    pubsub: :local,
    registry: :local

  router Decree.RuleFixtures.PlainRouter

  def init(config), do: {:ok, Decree.RuleFixtures.Stores.named_store(config)}
end

defmodule Decree.RuleFixtures.UndeclaredApp do
  @moduledoc false

  # A rule route, and a store declaring nothing.
  use Decree.Application,
    otp_app: :decree,
    event_store: Decree.FrameworkStorage.event_store_config(),
    pubsub: :local,
    registry: :local

  router Decree.RuleFixtures.Router

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.RuleFixtures.StrayTargetApp do
  @moduledoc false

  # Its rule routes' targets, and a store target no route here names.
  use Decree.Application,
    otp_app: :decree,
    event_store:
      Decree.FrameworkStorage.event_store_config(
        conditional_appends: [
          targets:
            Decree.RuleFixtures.Stores.targets() ++ [Decree.RuleFixtures.OutsideRouter.Target]
        ]
      ),
    pubsub: :local,
    registry: :local

  router Decree.RuleFixtures.Router

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.RuleFixtures.PlainDeclaringApp do
  @moduledoc false

  # A store target, and no rule route at all.
  use Decree.Application,
    otp_app: :decree,
    event_store:
      Decree.FrameworkStorage.event_store_config(
        conditional_appends: [targets: [Decree.RuleFixtures.User]]
      ),
    pubsub: :local,
    registry: :local

  router Decree.RuleFixtures.PlainRouter

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.RuleFixtures.DeclaringStore do
  @moduledoc false

  # Declares on its `use` line. Its connection is the framework store's.
  use Decree.Store,
    otp_app: :decree,
    conditional_appends: [targets: [Decree.RuleFixtures.User]]

  def init(config) do
    {:ok, Keyword.merge(Application.fetch_env!(:decree, Decree.FrameworkEventStore), config)}
  end
end

defmodule Decree.RuleFixtures.CountingStore do
  @moduledoc false

  # Counts its `init/1` calls in the `:counters` its config carries as `init_calls`, and
  # declares `first_targets` on the first call only, so a second call disagrees with the first.
  # Its connection is the framework store's.
  use Decree.Store, otp_app: :decree

  def init(config) do
    {counter, config} = Keyword.pop!(config, :init_calls)
    {first_targets, config} = Keyword.pop(config, :first_targets, [])

    :ok = :counters.add(counter, 1, 1)
    targets = if :counters.get(counter, 1) == 1, do: first_targets, else: []

    {:ok,
     :decree
     |> Application.fetch_env!(Decree.FrameworkEventStore)
     |> Keyword.merge(config)
     |> Keyword.put(:conditional_appends, targets: targets)}
  end
end

defmodule Decree.RuleCase do
  @moduledoc """
  The test case for a rule route dispatched against the store.
  """

  use ExUnit.CaseTemplate

  alias Decree.RuleFixtures.{App, EraseUser, RegisterUser, Ruled}
  alias Decree.TagIndexFixtures
  alias Decree.UUID

  using do
    quote do
      import Decree.DecisionStoreCase,
        only: [
          attach_telemetry: 1,
          corrupt_data: 2,
          hold: 2,
          scope: 0,
          seed: 3,
          seed: 4,
          stored: 2
        ]

      import Decree.RuleCase

      alias Decree.Decision.Rule.Result
      alias Decree.EventStore
      alias Decree.RuleFixtures.{App, EraseUser, Erased, Noted, Read, Registered, RegisterUser}
      alias Decree.RuleFixtures.{RemoveUser, Removed, Ruled, RuledAggregate, Stores, User}
      alias Decree.TagIndexFixtures
      alias Decree.UUID
    end
  end

  # Every family is enabled unless a test is tagged `:no_index_facts`, as a fresh store's
  # release step would leave it.
  setup context do
    Decree.FrameworkStorage.reset!()

    start_supervised!(App)

    if not Map.get(context, :no_index_facts, false), do: TagIndexFixtures.enable!(App)

    :ok
  end

  @doc """
  Disables the index of a family and key on the fixture ledger, and answers its id.
  """
  def disable_index!(family, key \\ nil, reason \\ "repair") do
    TagIndexFixtures.disable!(App, family, key, reason)
  end

  @doc """
  Enables a family and key again, under a new index id.
  """
  def enable_index!(family, key \\ nil), do: TagIndexFixtures.reenable!(App, family, key)

  @doc """
  How many facts the case's setup appends, at positions 1 and up.
  """
  def fact_count, do: length(TagIndexFixtures.families())

  def digest, do: "digest-#{System.unique_integer([:positive])}"

  @doc """
  A `RegisterUser` for a fresh user unless a test names one.
  """
  def register(digest, opts \\ []) do
    {script, fields} = Keyword.split(opts, [:between_reads, :deciding])

    struct!(
      RegisterUser,
      [command_id: UUID.uuid4(), user_id: UUID.uuid4(), digest: digest, script: Map.new(script)]
      |> Keyword.merge(fields)
    )
  end

  @doc """
  Registers a fresh user on an address, and answers the user.
  """
  def registered!(digest) do
    %RegisterUser{user_id: user_id} = command = register(digest)

    :ok = App.dispatch(command)

    user_id
  end

  def erase!(user_id, erased_by \\ nil) do
    :ok =
      App.dispatch(%EraseUser{command_id: UUID.uuid4(), user_id: user_id, erased_by: erased_by})
  end

  @doc """
  A `Ruled` command, its script made of the options named after its callbacks.
  """
  def ruled(opts \\ [], module \\ Ruled) do
    {script, fields} = Keyword.split(opts, [:query, :followup, :execute])

    struct!(
      module,
      [
        command_id: UUID.uuid4(),
        id: UUID.uuid4(),
        scope: Decree.DecisionStoreCase.scope(),
        script: Map.new(script)
      ]
      |> Keyword.merge(fields)
    )
  end

  def user_stream(%RegisterUser{user_id: user_id}), do: "user-" <> user_id
  def user_stream(user_id) when is_binary(user_id), do: "user-" <> user_id

  def ruled_stream(%{id: id}), do: "ruled-" <> id

  def dispatch_async(command, opts \\ []) do
    Task.async(fn -> App.dispatch(command, [timeout: 60_000] ++ opts) end)
  end

  @doc """
  The telemetry events a test has been sent, for the event `name` ends in.
  """
  def received(event, acc \\ []) do
    receive do
      {:telemetry, ^event, measurements, metadata} ->
        received(event, [{measurements, metadata} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
