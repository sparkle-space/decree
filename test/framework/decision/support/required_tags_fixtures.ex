defmodule Decree.RequiredTagsFixtures do
  @moduledoc false

  # Fixtures for the required tag tests. `App` is a plain aggregate route on a store that
  # declares nothing, `DecisionApp` the decisions, scripted as `Decree.DecisionFixtures`' are.

  def digest(value), do: Base.encode16(:crypto.hash(:sha256, value))
end

defmodule Decree.RequiredTagsFixtures.Addressed do
  @moduledoc false

  # Opted in: an `address:` tag if and only if there is an address. `forget_tag` is the buggy
  # implementation that loses it.
  @derive Jason.Encoder
  defstruct [:address, :scope, :forget_tag]

  def __required_tag_kinds__(%__MODULE__{address: nil}, _tag_context), do: []
  def __required_tag_kinds__(%__MODULE__{}, _tag_context), do: ["address"]

  def __conditional_tag_kinds__, do: ["address"]

  def __tag_kinds__, do: ["address", "scope"]

  defimpl Decree.Event.Tags do
    def tags(%{scope: nil} = event, _context), do: {:ok, address(event)}
    def tags(%{scope: scope} = event, _context), do: {:ok, ["scope:" <> scope | address(event)]}

    defp address(%{address: nil}), do: []
    defp address(%{forget_tag: true}), do: []
    defp address(%{address: address}), do: ["address:" <> address]
  end
end

defmodule Decree.RequiredTagsFixtures.Contact do
  @moduledoc false

  # A declared personal field, and a tag and a requirement derived from it. `mode` fails
  # `__required_tag_kinds__/2`, with the value in hand.
  @derive [Jason.Encoder, {Decree.Event.PersonalFields, personal: [email: :user_id]}]
  defstruct [:user_id, :email, :mode]

  def __required_tag_kinds__(%__MODULE__{mode: "raise", email: email}, _tag_context) do
    "never" = email
    []
  end

  def __required_tag_kinds__(%__MODULE__{mode: "exit", email: email}, _tag_context) do
    exit({:gave_up, email})
  end

  def __required_tag_kinds__(%__MODULE__{mode: "throw", email: email}, _tag_context) do
    throw({:thrown, email})
  end

  def __required_tag_kinds__(%__MODULE__{mode: "error", email: email}, _tag_context) do
    {:error, {:refused, email}}
  end

  def __required_tag_kinds__(%__MODULE__{email: nil}, _tag_context), do: []
  def __required_tag_kinds__(%__MODULE__{}, _tag_context), do: ["email"]

  def __conditional_tag_kinds__, do: ["email"]

  def __tag_kinds__, do: ["email"]

  defimpl Decree.Event.Tags do
    def tags(%{email: nil}, _context), do: {:ok, []}

    def tags(%{email: email}, _context) do
      {:ok, ["email:" <> Decree.RequiredTagsFixtures.digest(email)]}
    end
  end
end

defmodule Decree.RequiredTagsFixtures.PlainRaising do
  @moduledoc false

  # The same raise, on an event declaring nothing: nothing to redact.
  @derive Jason.Encoder
  defstruct [:note]

  def __required_tag_kinds__(%__MODULE__{note: note}, _tag_context) do
    raise ArgumentError, "plain raise: " <> note
  end
end

defmodule Decree.RequiredTagsFixtures.Malformed do
  @moduledoc false

  # Answers what is not a list of kinds.
  @derive Jason.Encoder
  defstruct [:note]

  def __required_tag_kinds__(%__MODULE__{}, _tag_context), do: [:address | :improper]
end

defmodule Decree.RequiredTagsFixtures.Unlisted do
  @moduledoc false

  # Answers what it is told to, for an answer that is no list at all.
  @derive Jason.Encoder
  defstruct [:answer]

  def __required_tag_kinds__(%__MODULE__{answer: answer}, _tag_context), do: answer
end

defmodule Decree.RequiredTagsFixtures.Unopted do
  @moduledoc false

  # No export at all. Its implementation answers the tags it carries.
  @derive Jason.Encoder
  defstruct tags: []

  defimpl Decree.Event.Tags do
    def tags(%{tags: tags}, _context), do: {:ok, tags}
  end
end

defmodule Decree.RequiredTagsFixtures.Forged do
  @moduledoc false

  # Opted in, requiring only `always/0`, and answering whatever tags it is given: a `command`
  # one among them.
  @derive Jason.Encoder
  defstruct tags: []

  def __required_tag_kinds__(%__MODULE__{}, _tag_context), do: []

  defimpl Decree.Event.Tags do
    def tags(%{tags: tags}, _context), do: {:ok, tags}
  end
end

defmodule Decree.RequiredTagsFixtures.Digested do
  @moduledoc false

  # Answers the tags it carries, and may lack an `email:` or a `label:`.
  @derive Jason.Encoder
  defstruct tags: []

  def __conditional_tag_kinds__, do: ["email", "label"]

  defimpl Decree.Event.Tags do
    def tags(%{tags: tags}, _context), do: {:ok, tags}
  end
end

defmodule Decree.RequiredTagsFixtures.TwoDigested do
  @moduledoc false

  # Answers the tags it carries, and may lack either family of `TwoFamiliesDecision`, so a
  # kind it lacks is F7's to answer and not the required list's.
  @derive Jason.Encoder
  defstruct tags: []

  def __conditional_tag_kinds__, do: ["email", "oauth_uid"]

  defimpl Decree.Event.Tags do
    def tags(%{tags: tags}, _context), do: {:ok, tags}
  end
end

defmodule Decree.RequiredTagsFixtures.Emit do
  @moduledoc false
  defstruct [:ledger_id, :command_id, events: []]
end

defmodule Decree.RequiredTagsFixtures.Ledger do
  @moduledoc false

  # Declares nothing: an aggregate that has heard of none of this.
  alias Decree.RequiredTagsFixtures.Emit

  defstruct seen: 0

  def execute(%__MODULE__{}, %Emit{events: events}), do: events

  def apply(%__MODULE__{seen: seen} = ledger, _event) do
    %__MODULE__{ledger | seen: seen + 1}
  end
end

defmodule Decree.RequiredTagsFixtures.Indexed do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, script: %{}]
end

defmodule Decree.RequiredTagsFixtures.TwoFamilies do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, script: %{}]
end

defmodule Decree.RequiredTagsFixtures.Unindexed do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, script: %{}]
end

defmodule Decree.RequiredTagsFixtures.IndexedDecision do
  @moduledoc false
  use Decree.DecisionFixtures.Scripting

  def index_families, do: %{"email" => :digest, "label" => :plain}
end

defmodule Decree.RequiredTagsFixtures.TwoFamiliesDecision do
  @moduledoc false
  use Decree.DecisionFixtures.Scripting

  def index_families, do: %{"email" => :digest, "oauth_uid" => :digest}
end

defmodule Decree.RequiredTagsFixtures.UnindexedDecision do
  @moduledoc false
  use Decree.DecisionFixtures.Scripting
end

defmodule Decree.RequiredTagsFixtures.Injected do
  @moduledoc false
  defstruct [:command_id, :claim_id, :scope, script: %{}]
end

defmodule Decree.RequiredTagsFixtures.InjectingFamilies do
  @moduledoc false

  # A `use` after `use Decree.Decision`, so this hook runs after that one's.
  defmacro __using__(_opts) do
    quote do
      @before_compile Decree.RequiredTagsFixtures.InjectingFamilies
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      def index_families, do: %{"email" => :digest}
    end
  end
end

defmodule Decree.RequiredTagsFixtures.InjectedDecision do
  @moduledoc false
  use Decree.DecisionFixtures.Scripting
  use Decree.RequiredTagsFixtures.InjectingFamilies
end

defmodule Decree.RequiredTagsFixtures.Router do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.RequiredTagsFixtures.{Emit, Ledger}

  dispatch Emit, to: Ledger, identity: :ledger_id
end

defmodule Decree.RequiredTagsFixtures.DecisionRouter do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.RequiredTagsFixtures.{
    Indexed,
    IndexedDecision,
    Injected,
    InjectedDecision,
    TwoFamilies,
    TwoFamiliesDecision,
    Unindexed,
    UnindexedDecision
  }

  decide(Indexed, to: IndexedDecision, identity: :claim_id, identity_prefix: "indexed-")
  decide(Injected, to: InjectedDecision, identity: :claim_id, identity_prefix: "injected-")
  decide(TwoFamilies, to: TwoFamiliesDecision, identity: :claim_id, identity_prefix: "two-")
  decide(Unindexed, to: UnindexedDecision, identity: :claim_id, identity_prefix: "unindexed-")
end

defmodule Decree.RequiredTagsFixtures.App do
  @moduledoc false

  use Decree.Application,
    otp_app: :decree,
    key_source: Decree.PersonalFields.Fixtures.Keys,
    event_store: Decree.FrameworkStorage.event_store_config(),
    pubsub: :local,
    registry: :local

  router Decree.RequiredTagsFixtures.Router

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.RequiredTagsFixtures.DecisionApp do
  @moduledoc false

  use Decree.Application,
    otp_app: :decree,
    key_source: Decree.PersonalFields.Fixtures.Keys,
    event_store:
      Decree.FrameworkStorage.event_store_config(
        conditional_appends: [stream_prefixes: ["indexed-", "injected-", "two-", "unindexed-"]]
      ),
    pubsub: :local,
    registry: :local

  router Decree.RequiredTagsFixtures.DecisionRouter

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.RequiredTagsFixtures.MockedDecisionApp do
  @moduledoc false

  # The same decisions on a mocked store, to see what a store is asked for.
  use Decree.Application,
    otp_app: :decree,
    event_store: [adapter: Decree.EventStore.Adapters.Mock],
    pubsub: :local,
    registry: :local

  router Decree.RequiredTagsFixtures.DecisionRouter
end
