defmodule Decree.PersonalFields.Fixtures do
  @moduledoc false

  # Fixtures for the personal field tests. A command here carries the events it should
  # produce, so a test states its events and reads what the store kept.

  def email(name \\ "alice"), do: name <> "@secret.example"
end

defmodule Decree.PersonalFields.Fixtures.Keys do
  @moduledoc false

  # The test key source: an ETS table a test flips per subject between a key, `:erased`,
  # `:unknown`, a raise, an exit, a throw and garbage. It counts lookups, and holds the pid
  # handlers report to, because they run in their own processes.

  @behaviour Decree.PersonalFields.KeySource

  use GenServer

  @table __MODULE__

  def start_link(_opts), do: GenServer.start_link(__MODULE__, :ok, name: __MODULE__)

  @impl GenServer
  def init(:ok) do
    :ets.new(@table, [:named_table, :public, :set])
    :ets.insert(@table, {:sink, nil})

    {:ok, :ok}
  end

  def sink(pid \\ self()), do: :ets.insert(@table, {:sink, pid})

  def notify(message) do
    case :ets.lookup(@table, :sink) do
      [{:sink, pid}] when is_pid(pid) -> send(pid, message)
      _none -> :ok
    end

    :ok
  end

  @doc "Gives a subject a fresh key, in the `:key` mode."
  def mint(subject_id) do
    :ets.insert(@table, {{:key, subject_id}, "key-" <> subject_id, :crypto.strong_rand_bytes(32)})
    set(subject_id, :key)

    subject_id
  end

  def set(subject_id, mode), do: :ets.insert(@table, {{:mode, subject_id}, mode})

  @doc "The same key under another id, as a store that lost track of it would answer."
  def relabel(subject_id, key_id) do
    {_key_id, key} = key(subject_id)

    :ets.insert(@table, {{:key, subject_id}, key_id, key})
  end

  def lookups(subject_id) do
    case :ets.lookup(@table, {:lookups, subject_id}) do
      [{_key, count}] -> count
      [] -> 0
    end
  end

  def key(subject_id) do
    [{_key, key_id, key}] = :ets.lookup(@table, {:key, subject_id})

    {key_id, key}
  end

  @impl Decree.PersonalFields.KeySource
  def lookup(subject_id) do
    :ets.update_counter(@table, {:lookups, subject_id}, 1, {{:lookups, subject_id}, 0})

    case :ets.lookup(@table, {:mode, subject_id}) do
      [{_key, :key}] ->
        {key_id, key} = key(subject_id)

        {:ok, key_id, key}

      [{_key, :unknown_once}] ->
        set(subject_id, :key)

        :unknown

      [{_key, :erased}] ->
        :erased

      [{_key, :raise}] ->
        raise "key store down for #{subject_id}"

      [{_key, :exit}] ->
        exit(:key_store_down)

      [{_key, :throw}] ->
        throw(:key_store_down)

      [{_key, :garbage}] ->
        {:ok, "key", "too short"}

      [{_key, :empty_id}] ->
        {:ok, "", :binary.copy(<<0>>, 32)}

      [{_key, :other}] ->
        :something_else

      _unknown ->
        :unknown
    end
  end
end

defmodule Decree.PersonalFields.Fixtures.Events do
  @moduledoc false

  alias Decree.Event.Tags
  alias Decree.Event.Upcaster

  defmodule Registered do
    @derive [
      Jason.Encoder,
      {Decree.Event.PersonalFields,
       personal: [email: :user_id], personal_metadata: [email: :user_id, password_hash: :user_id]}
    ]
    defstruct [:user_id, :email, :note]

    # A decision's tests read it by a `scope:` item, and only a seeded one carries that tag.
    def __conditional_tag_kinds__, do: ["scope"]
  end

  defmodule Invited do
    @derive [
      Jason.Encoder,
      {Decree.Event.PersonalFields,
       personal: [email: {:first_present, [:target_user_id, :invitation_id]}]}
    ]
    defstruct [:invitation_id, :target_user_id, :email]
  end

  defmodule PreAuthFailed do
    @derive [
      Jason.Encoder,
      {Decree.Event.PersonalFields,
       personal: [email: {:when_present, :user_id}, ip_address: {:when_present, :user_id}]}
    ]
    defstruct [:user_id, :email, :ip_address]
  end

  defmodule TwoSubjects do
    @derive [
      Jason.Encoder,
      {Decree.Event.PersonalFields, personal: [owner_email: :owner_id, member_email: :member_id]}
    ]
    defstruct [:owner_id, :member_id, :owner_email, :member_email]
  end

  defmodule Scored do
    @derive [Jason.Encoder, {Decree.Event.PersonalFields, personal: [score: :user_id]}]
    defstruct [:user_id, :score]
  end

  defmodule MetadataOnly do
    @derive [
      Jason.Encoder,
      {Decree.Event.PersonalFields, personal_metadata: [password_hash: :user_id]}
    ]
    defstruct [:user_id]
  end

  defmodule Plain do
    @derive Jason.Encoder
    defstruct [:user_id, :note]

    # As `Registered`.
    def __conditional_tag_kinds__, do: ["scope"]
  end

  defmodule Linked do
    @derive [Jason.Encoder, {Decree.Event.PersonalFields, personal: [provider_uid: :user_id]}]
    defstruct [:user_id, :provider, :provider_uid]
  end

  # Declared, and off the aggregate's fold list.
  defmodule Unfolded do
    @derive [Jason.Encoder, {Decree.Event.PersonalFields, personal: [email: :user_id]}]
    defstruct [:user_id, :email]
  end

  # `apply/2` raises on it with the value in hand, as its `mode` says.
  defmodule Poison do
    @derive [Jason.Encoder, {Decree.Event.PersonalFields, personal: [email: :user_id]}]
    defstruct [:user_id, :email, mode: "match"]
  end

  # `Decree.Event.Tags` reads the plaintext, and can raise with it.
  defmodule Tagged do
    @derive [Jason.Encoder, {Decree.Event.PersonalFields, personal: [email: :user_id]}]
    defstruct [:user_id, :email, :mode]

    # As `Registered`.
    def __conditional_tag_kinds__, do: ["scope"]

    defimpl Tags do
      def tags(%{mode: "raise", email: email}, _context) do
        "never" = email
        {:ok, []}
      end

      def tags(%{mode: "invalid", email: email}, _context), do: {:ok, email}
      def tags(%{mode: "exit", email: email}, _context), do: exit({:gave_up, email})
      def tags(%{mode: "throw", email: email}, _context), do: throw({:thrown, email})
      def tags(%{mode: "error", email: email}, _context), do: {:error, {:refused, email}}
      def tags(%{mode: "bare_error", email: email}, _context), do: {:error, email}
      def tags(%{mode: "atom_error"}, _context), do: {:error, :refused}
      def tags(%{email: nil}, _context), do: {:ok, []}
      def tags(%{email: email}, _context), do: {:ok, ["digest:" <> digest(email)]}

      defp digest(email), do: Base.encode16(:crypto.hash(:sha256, email))
    end
  end

  defmodule TaggedByMetadata do
    @derive [
      Jason.Encoder,
      {Decree.Event.PersonalFields, personal_metadata: [email: :user_id]}
    ]
    defstruct [:user_id]

    defimpl Tags do
      def tags(_event, %{metadata: metadata}) do
        case Map.get(metadata, "email") do
          nil -> {:ok, ["no_email:true"]}
          email -> {:ok, ["seen:" <> email]}
        end
      end
    end
  end

  # A plain event whose tag implementation fails as its `mode` says, and is not redacted.
  defmodule RaisingPlain do
    @derive Jason.Encoder
    defstruct [:user_id, :mode, :note]

    defimpl Tags do
      def tags(%{mode: "exit", note: note}, _context), do: exit({:gave_up, note})
      def tags(%{mode: "throw", note: note}, _context), do: throw({:thrown, note})
      def tags(%{mode: "error", note: note}, _context), do: {:error, {:refused, note}}
      def tags(_event, _context), do: raise(ArgumentError, "plain raise")
    end
  end

  # Declares nothing, and is off every fold list. `apply/2` fails on it as its `mode` says,
  # with the note it carries: redacted on `Account`, and not on `PlainLedger`.
  defmodule PlainPoison do
    @derive Jason.Encoder
    defstruct [:mode, :note]
  end

  # Declares nothing, and is off every fold list. `apply/2` fails on it as its `mode` says,
  # with the state's decrypted email in hand.
  defmodule StateFails do
    @derive Jason.Encoder
    defstruct [:mode]
  end

  defmodule PassedThrough do
    @derive [Jason.Encoder, {Decree.Event.PersonalFields, personal: [email: :user_id]}]
    defstruct [:user_id, :email, version: 1]

    defimpl Upcaster do
      def upcast(%PassedThrough{} = event, _metadata), do: %PassedThrough{event | version: 2}
    end
  end

  defmodule Renamed do
    @derive [Jason.Encoder, {Decree.Event.PersonalFields, personal: [email: :user_id]}]
    defstruct [:user_id, :email]
  end

  defmodule BeforeRename do
    @derive [Jason.Encoder, {Decree.Event.PersonalFields, personal: [email: :user_id]}]
    defstruct [:user_id, :email]

    defimpl Upcaster do
      def upcast(%BeforeRename{user_id: user_id, email: email}, _metadata) do
        %Renamed{user_id: user_id, email: email}
      end
    end
  end

  # A handler does something with the value: see `Decree.PersonalFields.Fixtures.Leaky`.
  defmodule Leak do
    @derive [Jason.Encoder, {Decree.Event.PersonalFields, personal: [email: :user_id]}]
    defstruct [:user_id, :email, :mode]
  end
end

defmodule Decree.PersonalFields.Fixtures.Commands do
  @moduledoc false

  defmodule Emit do
    defstruct [:account_id, :command_id, events: []]
  end

  # Tells the test it is executing, and waits for `:release` before answering its events.
  defmodule Hold do
    defstruct [:account_id, events: []]
  end

  defmodule ExitNow do
    defstruct [:account_id, :reason]
  end

  defmodule RaiseNow do
    defstruct [:account_id]
  end

  # `execute/2` matches the state's email against a value it can't equal.
  defmodule MatchState do
    defstruct [:account_id]
  end

  # No clause of `check/1` takes the state, so the frame's arguments hold it.
  defmodule ClauseOnState do
    defstruct [:account_id]
  end

  # A database-shaped exception whose message holds the state's email.
  defmodule DetailOnState do
    defstruct [:account_id]
  end

  # `execute/2` fails as `mode` says, with the state's email in hand.
  defmodule FailExecute do
    defstruct [:account_id, :mode]
  end

  # `before_execute` fails as `mode` says, with the state's email in hand.
  defmodule FailBefore do
    defstruct [:account_id, :mode]
  end

  # For `PlainLedger`, which uses no personal fields.
  defmodule PlainFail do
    defstruct [:account_id, :mode, :note]
  end
end

defmodule Decree.PersonalFields.Fixtures.Account do
  @moduledoc false

  use Decree.PersonalFields.Aggregate

  alias Decree.PersonalFields.Fixtures.Commands.{
    ClauseOnState,
    DetailOnState,
    Emit,
    ExitNow,
    FailBefore,
    FailExecute,
    Hold,
    MatchState,
    RaiseNow
  }

  alias Decree.PersonalFields.Fixtures.Events.{
    Linked,
    PassedThrough,
    PlainPoison,
    Poison,
    Registered,
    Renamed,
    StateFails,
    TwoSubjects,
    Unfolded
  }

  alias Decree.PersonalFields.Fixtures.{Keys, LeakyFailures}

  @personal_fold [Registered, Linked, Poison, TwoSubjects, PassedThrough, Renamed]
  @personal_state [links: :provider_uid, email: :self]
  @personal_state_subject :user_id

  @derive Jason.Encoder
  defstruct [:user_id, :email, :other_email, :unfolded, links: [], seen: 0]

  def execute(%__MODULE__{}, %Emit{events: events}), do: events

  def execute(%__MODULE__{}, %Hold{events: events}) do
    Keys.notify({:executing, self()})

    receive do
      :release -> events
    end
  end

  def execute(%__MODULE__{}, %ExitNow{reason: reason}), do: exit(reason)
  def execute(%__MODULE__{}, %RaiseNow{}), do: raise("raised on request")

  def execute(%__MODULE__{email: email}, %MatchState{}) do
    "never" = email
    []
  end

  def execute(%__MODULE__{} = state, %ClauseOnState{}), do: check(state)

  def execute(%__MODULE__{email: email}, %DetailOnState{}) do
    LeakyFailures.fail("database", email)
  end

  def execute(%__MODULE__{email: email}, %FailExecute{mode: mode}) do
    LeakyFailures.fail(mode, email)
    []
  end

  def execute(%__MODULE__{}, %FailBefore{}), do: []

  def before_failing(%__MODULE__{email: email}, %{command: %FailBefore{mode: mode}}) do
    LeakyFailures.fail(mode, email)
    :ok
  end

  defp check(%__MODULE__{email: nil}), do: []

  def apply(%__MODULE__{} = state, %Registered{user_id: user_id, email: email}) do
    seen(%__MODULE__{state | user_id: user_id, email: email})
  end

  def apply(%__MODULE__{links: links} = state, %Linked{} = event) do
    %Linked{user_id: user_id, provider: provider, provider_uid: provider_uid} = event
    link = %{provider: provider, provider_uid: provider_uid}

    seen(%__MODULE__{state | user_id: user_id, links: links ++ [link]})
  end

  def apply(%__MODULE__{} = state, %Poison{mode: mode, email: email}) do
    LeakyFailures.fail(mode, email)

    state
  end

  def apply(%__MODULE__{} = state, %PlainPoison{mode: mode, note: note}) do
    LeakyFailures.fail(mode, note)

    state
  end

  def apply(%__MODULE__{email: email} = state, %StateFails{mode: mode}) do
    LeakyFailures.fail(mode, email)

    state
  end

  def apply(%__MODULE__{} = state, %TwoSubjects{} = event) do
    %TwoSubjects{owner_email: owner_email, member_email: member_email} = event

    seen(%__MODULE__{state | email: owner_email, other_email: member_email})
  end

  def apply(%__MODULE__{} = state, %PassedThrough{email: email}) do
    seen(%__MODULE__{state | email: email})
  end

  def apply(%__MODULE__{} = state, %Renamed{email: email}) do
    seen(%__MODULE__{state | email: email})
  end

  def apply(%__MODULE__{} = state, %Unfolded{email: email}) do
    seen(%__MODULE__{state | unfolded: email})
  end

  def apply(%__MODULE__{} = state, _event), do: seen(state)

  defp seen(%__MODULE__{seen: seen} = state), do: %__MODULE__{state | seen: seen + 1}
end

defmodule Decree.PersonalFields.Fixtures.StateOnly do
  @moduledoc false

  # Lists state and no event: its value can only come from a snapshot.

  use Decree.PersonalFields.Aggregate

  alias Decree.PersonalFields.Fixtures.Events.StateFails
  alias Decree.PersonalFields.Fixtures.LeakyFailures

  @personal_state [email: :self]
  @personal_state_subject :id

  @derive Jason.Encoder
  defstruct [:id, :email]

  def apply(%__MODULE__{email: email} = state, %StateFails{mode: mode}) do
    LeakyFailures.fail(mode, email)

    state
  end

  def apply(%__MODULE__{} = state, _event), do: state
end

defmodule Decree.PersonalFields.Fixtures.PlainLedger do
  @moduledoc false

  # Uses no personal fields, so nothing here is redacted.

  alias Decree.PersonalFields.Fixtures.Commands.PlainFail
  alias Decree.PersonalFields.Fixtures.Events.PlainPoison
  alias Decree.PersonalFields.Fixtures.LeakyFailures

  defstruct [:note]

  def execute(%__MODULE__{}, %PlainFail{mode: mode, note: note}) do
    LeakyFailures.fail(mode, note)
    []
  end

  def apply(%__MODULE__{} = state, %PlainPoison{mode: mode, note: note}) do
    LeakyFailures.fail(mode, note)

    state
  end

  def apply(%__MODULE__{} = state, _event), do: state
end

defmodule Decree.PersonalFields.Fixtures.Router do
  @moduledoc false

  use Decree.Commands.Router

  alias Decree.PersonalFields.Fixtures.{Account, PlainLedger}

  alias Decree.PersonalFields.Fixtures.Commands.{
    ClauseOnState,
    DetailOnState,
    Emit,
    ExitNow,
    FailBefore,
    FailExecute,
    Hold,
    MatchState,
    PlainFail,
    RaiseNow
  }

  identify Account, by: :account_id
  identify PlainLedger, by: :account_id

  dispatch [ClauseOnState, DetailOnState, Emit, ExitNow, FailExecute, Hold, MatchState, RaiseNow],
    to: Account

  dispatch FailBefore, to: Account, before_execute: :before_failing
  dispatch PlainFail, to: PlainLedger
end

defmodule Decree.PersonalFields.Fixtures.App do
  @moduledoc false

  alias Decree.PersonalFields.Fixtures.{Keys, Router}

  use Decree.Application,
    otp_app: :decree,
    key_source: Keys,
    event_store: Decree.FrameworkStorage.event_store_config(),
    pubsub: :local,
    registry: :local

  router(Router)

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.PersonalFields.Fixtures.FencedApp do
  @moduledoc false

  alias Decree.PersonalFields.Fixtures.{Keys, Router}

  use Decree.Application,
    otp_app: :decree,
    key_source: Keys,
    personal_metadata_keys: [:email, :password_hash],
    event_store: Decree.FrameworkStorage.event_store_config(),
    pubsub: :local,
    registry: :local

  router(Router)

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

# Declares neither option, and its environment names both: see `config/test.exs`.
defmodule Decree.PersonalFields.Fixtures.EnvApp do
  @moduledoc false

  use Decree.Application,
    otp_app: :decree,
    event_store: Decree.FrameworkStorage.event_store_config(),
    pubsub: :local,
    registry: :local

  def init(config) do
    config = Keyword.merge(config, key_source: Decree.PersonalFields.Fixtures.Keys)
    config = Keyword.put(config, :personal_metadata_keys, [:email])

    {:ok, Decree.FrameworkStorage.name_event_store(config)}
  end
end

defmodule Decree.PersonalFields.Fixtures.Handler do
  @moduledoc false

  # Names its events, and reports what it receives. Its own `error/3` answers `:skip`, and
  # must never be called for a key that can't be read.

  use Decree.Event.Handler,
    application: Decree.PersonalFields.Fixtures.App,
    name: __MODULE__

  alias Decree.PersonalFields.Fixtures.Events.{
    MetadataOnly,
    Registered,
    Renamed,
    PassedThrough,
    PreAuthFailed,
    Scored,
    TwoSubjects
  }

  alias Decree.PersonalFields.Fixtures.Keys

  def handle(%Registered{} = event, metadata), do: report(event, metadata)
  def handle(%MetadataOnly{} = event, metadata), do: report(event, metadata)
  def handle(%TwoSubjects{} = event, metadata), do: report(event, metadata)
  def handle(%PreAuthFailed{} = event, metadata), do: report(event, metadata)
  def handle(%Scored{} = event, metadata), do: report(event, metadata)
  def handle(%PassedThrough{} = event, metadata), do: report(event, metadata)
  def handle(%Renamed{} = event, metadata), do: report(event, metadata)
  def handle(_event, _metadata), do: :ok

  def error(error, _event, _failure_context) do
    Keys.notify({:module_error, error})

    :skip
  end

  defp report(event, metadata) do
    Keys.notify({:handled, event, metadata})

    :ok
  end
end

defmodule Decree.PersonalFields.Fixtures.StatefulHandler do
  @moduledoc false

  # Keeps a decrypted value as its state, as a handler must not, and reports the state it is
  # given. A later event fails as its note says, with that state in hand.

  use Decree.Event.Handler,
    application: Decree.PersonalFields.Fixtures.App,
    name: __MODULE__

  alias Decree.PersonalFields.Fixtures.Events.{Plain, Registered}
  alias Decree.PersonalFields.Fixtures.{Keys, LeakyFailures}

  def handle(%Registered{email: email}, _metadata), do: {:ok, email}

  def handle(%Plain{note: "ok"}, %{state: state}) do
    Keys.notify({:state_given, state})

    :ok
  end

  def handle(%Plain{note: "keep"}, %{state: state}), do: {:ok, state}
  def handle(%Plain{note: mode}, %{state: state}), do: LeakyFailures.fail(mode, state)
end

defmodule Decree.PersonalFields.Fixtures.StatefulReporting do
  @moduledoc false

  # The same, with an `error/3` that reports the state decree hands it, and stops.

  use Decree.Event.Handler,
    application: Decree.PersonalFields.Fixtures.App,
    name: __MODULE__

  alias Decree.Event.FailureContext
  alias Decree.PersonalFields.Fixtures.Events.{Plain, Registered}
  alias Decree.PersonalFields.Fixtures.{Keys, LeakyFailures}

  def handle(%Registered{email: email}, _metadata), do: {:ok, email}
  def handle(%Plain{note: mode}, %{state: state}), do: LeakyFailures.fail(mode, state)

  def error(_error, _event, %FailureContext{handler_state: handler_state}) do
    Keys.notify({:error_state, handler_state})

    {:stop, :handed}
  end
end

defmodule Decree.PersonalFields.Fixtures.PlainStateHandler do
  @moduledoc false

  # Keeps state from an event it was not decrypted for: nothing changes for it.

  use Decree.Event.Handler,
    application: Decree.PersonalFields.Fixtures.App,
    name: __MODULE__

  alias Decree.PersonalFields.Fixtures.Events.Plain
  alias Decree.PersonalFields.Fixtures.LeakyFailures

  def handle(%Plain{note: "ok"}, _metadata), do: {:ok, "kept-state"}
  def handle(%Plain{note: mode}, %{state: state}), do: LeakyFailures.fail(mode, state)
end

defmodule Decree.PersonalFields.Fixtures.OpaqueHandler do
  @moduledoc false

  use Decree.Event.Handler,
    application: Decree.PersonalFields.Fixtures.App,
    name: __MODULE__,
    personal_fields: :opaque

  alias Decree.PersonalFields.Fixtures.Keys

  def handle(event, metadata) do
    Keys.notify({:opaque, event, metadata})

    :ok
  end
end

defmodule Decree.PersonalFields.Fixtures.Leaky do
  @moduledoc false

  # Fails with the value in hand, in each way a handler can. It has no `error/3`.

  use Decree.Event.Handler,
    application: Decree.PersonalFields.Fixtures.App,
    name: __MODULE__

  alias Decree.PersonalFields.Fixtures.Events.Leak
  alias Decree.PersonalFields.Fixtures.LeakyFailures

  def handle(%Leak{mode: mode, email: email}, _metadata), do: LeakyFailures.fail(mode, email)
end

defmodule Decree.PersonalFields.Fixtures.LeakyReporting do
  @moduledoc false

  # The same failures, with an `error/3` that reports what it was handed and stops.

  use Decree.Event.Handler,
    application: Decree.PersonalFields.Fixtures.App,
    name: __MODULE__

  alias Decree.Event.FailureContext
  alias Decree.PersonalFields.Fixtures.Events.Leak
  alias Decree.PersonalFields.Fixtures.{Keys, LeakyFailures}

  def handle(%Leak{mode: mode, email: email}, _metadata), do: LeakyFailures.fail(mode, email)

  def error(error, event, %FailureContext{} = failure_context) do
    Keys.notify({:error_handed, error, event, failure_context})

    {:stop, :handed}
  end
end

defmodule Decree.PersonalFields.Fixtures.UniqueViolation do
  @moduledoc false

  # Shaped as `Postgrex.Error` is: its message is the database's detail.
  defexception [:detail]

  @impl true
  def message(%__MODULE__{detail: detail}), do: detail
end

defmodule Decree.PersonalFields.Fixtures.LeakyFailures do
  @moduledoc false

  alias Decree.PersonalFields.Fixtures.UniqueViolation

  def fail("match", email) do
    "never" = email
    :ok
  end

  def fail("clause", email), do: helper(email)

  def fail("database", email) do
    raise UniqueViolation, detail: "Key (email)=(#{email}) already exists."
  end

  def fail("error", email), do: {:error, {:bad, email}}
  def fail("invalid", email), do: {:weird, email}
  def fail("exit", email), do: exit({:gave_up, email})
  def fail("throw", email), do: throw({:error, {:thrown, email}})

  # Throws shaped as a `handle_call/3` return, which `gen_server` would take as the answer.
  def fail("noreply", email), do: throw({:noreply, email})
  def fail("reply", email), do: throw({:reply, :ok, email})

  defp helper(:never), do: :ok
end

defmodule Decree.PersonalFields.Fixtures.RetryingErrors do
  @moduledoc false

  # An `on_event_handler_error` module that retries, as a host's hold does. It counts.

  alias Decree.Event.FailureContext
  alias Decree.PersonalFields.Fixtures.Keys

  def error(error, _event, %FailureContext{context: context} = failure_context) do
    failures = Map.get(context, :failures, 0) + 1
    Keys.notify({:held, failures, error})

    {:retry, 10,
     %FailureContext{failure_context | context: Map.put(context, :failures, failures)}}
  end
end

defmodule Decree.PersonalFields.Fixtures.SkippingErrors do
  @moduledoc false

  # An `on_event_handler_error` module that tries to skip.

  alias Decree.PersonalFields.Fixtures.Keys

  def error(error, _event, _failure_context) do
    Keys.notify({:asked_to_skip, error})

    :skip
  end
end
