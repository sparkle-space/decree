defmodule Decree.CommandWindowFixtures do
  @moduledoc false

  # Fixtures for the aggregate command window. `Account` declares a window and an `authorize/2`
  # that refuses an actor who is not the owner; `PlainAccount` declares none. A command carries a
  # script of the functions its aggregate runs, so a test states the behaviour it needs.

  @window_ms 300_000

  def window_ms, do: @window_ms

  # A function identity, for the router that routes `Account` by one.
  def account_id(%{account_id: account_id}), do: account_id
end

defmodule Decree.CommandWindowFixtures.Noted do
  @moduledoc false
  @derive Jason.Encoder
  defstruct [:account_id, :owner, :note]
end

defmodule Decree.CommandWindowFixtures.Open do
  @moduledoc false
  defstruct [:command_id, :account_id, :owner, script: %{}]
end

defmodule Decree.CommandWindowFixtures.Note do
  @moduledoc false
  defstruct [:command_id, :account_id, :actor, script: %{}]
end

defmodule Decree.CommandWindowFixtures.RuledNote do
  @moduledoc false

  # On a route with `Decree.RuleFixtures.ScriptedRule`, which reads the scope's `Read` events.
  defstruct [:command_id, :account_id, :actor, :scope, :rule_result, script: %{}]
end

defmodule Decree.CommandWindowFixtures.PlainNote do
  @moduledoc false
  defstruct [:command_id, :account_id, :actor, script: %{}]
end

defmodule Decree.CommandWindowFixtures.PlainRuledNote do
  @moduledoc false
  defstruct [:command_id, :account_id, :actor, :scope, :rule_result, script: %{}]
end

defmodule Decree.CommandWindowFixtures.Scripted do
  @moduledoc false

  # What both fixture aggregates do.
  defmacro __using__(_opts) do
    quote do
      alias Decree.CommandWindowFixtures.{Noted, Open}

      @derive Jason.Encoder
      defstruct [:account_id, :owner, notes: []]

      def authorize(%__MODULE__{} = state, %{script: %{authorize: authorize}} = command) do
        authorize.(state, command)
      end

      def authorize(%__MODULE__{owner: owner}, %{actor: actor})
          when not is_nil(actor) and actor != owner do
        {:error, :unauthorized}
      end

      def authorize(%__MODULE__{}, _command), do: :ok

      def execute(%__MODULE__{} = state, %{script: %{execute: execute}} = command) do
        execute.(state, command)
      end

      def execute(%__MODULE__{}, %Open{account_id: account_id, owner: owner}) do
        %Noted{account_id: account_id, owner: owner, note: "opened"}
      end

      def execute(%__MODULE__{}, %{account_id: account_id}) do
        %Noted{account_id: account_id, note: "noted"}
      end

      def apply(%__MODULE__{} = state, %Noted{} = event) do
        %__MODULE__{
          state
          | account_id: event.account_id,
            owner: state.owner || event.owner,
            notes: state.notes ++ [event.note]
        }
      end
    end
  end
end

defmodule Decree.CommandWindowFixtures.Account do
  @moduledoc false
  use Decree.Aggregate, command_window: [ms: 300_000]
  use Decree.CommandWindowFixtures.Scripted
end

defmodule Decree.CommandWindowFixtures.PlainAccount do
  @moduledoc false

  # No `command_window:`, so no window: its `authorize/2` is never called.
  use Decree.CommandWindowFixtures.Scripted
end

defmodule Decree.CommandWindowFixtures.Router do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.CommandWindowFixtures.{Account, Note, Open, PlainAccount, PlainNote}
  alias Decree.CommandWindowFixtures.{PlainRuledNote, RuledNote}
  alias Decree.RuleFixtures.ScriptedRule

  identify Account, by: :account_id, prefix: "account-"
  identify PlainAccount, by: :account_id, prefix: "plain-account-"

  dispatch [Open, Note], to: Account
  dispatch RuledNote, to: Account, rule: ScriptedRule

  dispatch PlainNote, to: PlainAccount
  dispatch PlainRuledNote, to: PlainAccount, rule: ScriptedRule
end

defmodule Decree.CommandWindowFixtures.WindowOnlyRouter do
  @moduledoc false

  # `Account` with no rule route.
  use Decree.Commands.Router

  alias Decree.CommandWindowFixtures.{Account, Note, Open}

  identify Account, by: :account_id, prefix: "account-"

  dispatch [Open, Note], to: Account
end

defmodule Decree.CommandWindowFixtures.PlainRuleRouter do
  @moduledoc false

  # A rule route on the aggregate with no window.
  use Decree.Commands.Router

  alias Decree.CommandWindowFixtures.{PlainAccount, PlainRuledNote}
  alias Decree.RuleFixtures.ScriptedRule

  identify PlainAccount, by: :account_id, prefix: "plain-account-"

  dispatch PlainRuledNote, to: PlainAccount, rule: ScriptedRule
end

defmodule Decree.CommandWindowFixtures.FunctionIdentityRouter do
  @moduledoc false

  # `Account` by a function identity with no `stream_tag:`: no kind for its stream tag.
  use Decree.Commands.Router

  alias Decree.CommandWindowFixtures.{Account, Note}

  identify Account, by: &Decree.CommandWindowFixtures.account_id/1, prefix: "account-"

  dispatch Note, to: Account
end

defmodule Decree.CommandWindowFixtures.TaggedFunctionRouter do
  @moduledoc false

  # The same function identity, with the kind named.
  use Decree.Commands.Router

  alias Decree.CommandWindowFixtures.{Account, Note}

  identify Account, by: &Decree.CommandWindowFixtures.account_id/1, prefix: "account-"

  dispatch Note, to: Account, stream_tag: :account
end

defmodule Decree.CommandWindowFixtures.OpaqueRouter do
  @moduledoc false

  # `Account` by an opaque identity, which gives no stream tag.
  use Decree.Commands.Router

  alias Decree.CommandWindowFixtures.{Account, Note}

  identify Account, by: :account_id, prefix: "account-", opaque: true

  dispatch Note, to: Account
end

defmodule Decree.CommandWindowFixtures.Stores do
  @moduledoc false

  alias Decree.CommandWindowFixtures.{Account, PlainAccount}

  def store(targets \\ [Account, PlainAccount]) do
    Decree.FrameworkStorage.event_store_config(
      event_store: Decree.RuleFixtures.RecordingStore,
      conditional_appends: [targets: targets]
    )
  end
end

defmodule Decree.CommandWindowFixtures.App do
  @moduledoc false

  use Decree.Application,
    otp_app: :decree,
    event_store: Decree.CommandWindowFixtures.Stores.store(),
    registry: :local

  router Decree.CommandWindowFixtures.Router
  router Decree.TagIndexFixtures.Router

  def init(config), do: {:ok, Decree.RuleFixtures.Stores.named_store(config)}
end

defmodule Decree.CommandWindowFixtures.UndeclaredWindowApp do
  @moduledoc false

  # A windowed aggregate, and a store declaring no target.
  use Decree.Application,
    otp_app: :decree,
    event_store: Decree.FrameworkStorage.event_store_config(),
    registry: :local

  router Decree.CommandWindowFixtures.WindowOnlyRouter

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.CommandWindowFixtures.DeclaredWindowApp do
  @moduledoc false

  # A windowed aggregate no rule route names, beside a rule route, both declared.
  use Decree.Application,
    otp_app: :decree,
    event_store:
      Decree.FrameworkStorage.event_store_config(
        conditional_appends: [
          targets: [
            Decree.CommandWindowFixtures.Account,
            Decree.CommandWindowFixtures.PlainAccount
          ]
        ]
      ),
    registry: :local

  router Decree.CommandWindowFixtures.WindowOnlyRouter
  router Decree.CommandWindowFixtures.PlainRuleRouter

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.CommandWindowFixtures.FunctionIdentityApp do
  @moduledoc false

  use Decree.Application,
    otp_app: :decree,
    event_store:
      Decree.FrameworkStorage.event_store_config(
        conditional_appends: [targets: [Decree.CommandWindowFixtures.Account]]
      ),
    registry: :local

  router Decree.CommandWindowFixtures.FunctionIdentityRouter

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.CommandWindowFixtures.TaggedFunctionApp do
  @moduledoc false

  use Decree.Application,
    otp_app: :decree,
    event_store:
      Decree.FrameworkStorage.event_store_config(
        conditional_appends: [targets: [Decree.CommandWindowFixtures.Account]]
      ),
    registry: :local

  router Decree.CommandWindowFixtures.TaggedFunctionRouter

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end

defmodule Decree.CommandWindowFixtures.OpaqueApp do
  @moduledoc false

  use Decree.Application,
    otp_app: :decree,
    event_store:
      Decree.FrameworkStorage.event_store_config(
        conditional_appends: [targets: [Decree.CommandWindowFixtures.Account]]
      ),
    registry: :local

  router Decree.CommandWindowFixtures.OpaqueRouter

  def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
end
