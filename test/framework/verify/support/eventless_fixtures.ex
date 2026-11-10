defmodule Decree.VerifyFixtures.Eventless do
  @moduledoc false
  # Compiled routers and handlers for `mix decree.verify.eventless`
  # (test/framework/verify/eventless_task_test.exs). The line numbers of the two eventless
  # arms below are asserted there.

  defmodule Opened, do: defstruct([:id])

  defmodule Open, do: defstruct([:id, :command_id])
  defmodule Close, do: defstruct([:id, :command_id])
  defmodule Rename, do: defstruct([:id, :command_id])

  defmodule Account do
    @moduledoc false
    alias Decree.VerifyFixtures.Eventless.{Close, Open, Opened}

    defstruct [:id, :open?]

    def execute(%Account{open?: true}, %Open{}), do: :ok
    def execute(%Account{}, %Open{id: id}), do: %Opened{id: id}
    def execute(%Account{}, %Close{id: id}), do: [%Opened{id: id}]

    def apply(account, _event), do: account
  end

  defmodule Profile do
    @moduledoc false
    defstruct [:id]

    def rename(%Profile{}, _command), do: []

    def apply(profile, _event), do: profile
  end

  defmodule Ledger do
    @moduledoc false
    alias Decree.VerifyFixtures.Eventless.{Open, Opened, Rename}

    defstruct [:id]

    def execute(%Ledger{}, %Open{id: id}), do: %Opened{id: id}
    def rename(%Ledger{}, %Rename{id: id}), do: {:ok, [%Opened{id: id}]}

    def apply(ledger, _event), do: ledger
  end

  defmodule EventlessRouter do
    @moduledoc false
    use Decree.Commands.Router

    alias Decree.VerifyFixtures.Eventless.{Account, Close, Open, Profile, Rename}

    dispatch [Open, Close], to: Account, identity: :id
    dispatch Rename, to: Profile, function: :rename, identity: :id
  end

  defmodule GreenRouter do
    @moduledoc false
    use Decree.Commands.Router

    alias Decree.VerifyFixtures.Eventless.{Ledger, Open, Rename}

    dispatch Open, to: Ledger, identity: :id
    dispatch Rename, to: Ledger, function: :rename, identity: :id
  end
end
