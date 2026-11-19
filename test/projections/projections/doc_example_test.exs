defmodule Decree.Projections.DocExampleTest do
  # `Decree.Projections.Ecto`'s examples, read from its compiled docs and run as written: the
  # migration on a database of its own, the OTP application started, an event projected. The
  # test supplies what the examples take from the README's minimal application and from Ecto:
  # `MyApp.App`, `MyApp.AccountRegistered` and `MyApp.Repo`. `MyApp` is renamed, so the README
  # test's `MyApp` modules in the same VM are left alone.
  use ExUnit.Case, async: false

  alias Decree.DocBlocks
  alias Decree.Event.Mapper
  alias Decree.EventStore

  @database "decree_projections_doc_example_test" <> System.get_env("MIX_TEST_PARTITION", "")

  defmodule Example.AccountRegistered do
    @derive Jason.Encoder
    defstruct [:account_id, :handle]
  end

  defmodule Example.App do
    use Decree.Application,
      otp_app: :decree,
      event_store: [event_store: Decree.FrameworkEventStore]

    def init(config), do: {:ok, Decree.FrameworkStorage.name_event_store(config)}
  end

  defmodule Example.Repo do
    use Ecto.Repo, otp_app: :decree, adapter: Ecto.Adapters.Postgres
  end

  @namespace inspect(Example)
  @account Module.concat([Example, Accounts, Account])
  @projector Module.concat([Example, Accounts, Projector])

  defp compile!(doc, first_line) do
    doc
    |> DocBlocks.block!(first_line)
    |> String.replace("MyApp", @namespace)
    |> Code.compile_string()
    |> Enum.map(fn {module, _bytecode} -> module end)
  end

  setup do
    # Both tests compile the moduledoc's read model and projector.
    ignore = Code.get_compiler_option(:ignore_module_conflict)
    Code.put_compiler_option(:ignore_module_conflict, true)
    on_exit(fn -> Code.put_compiler_option(:ignore_module_conflict, ignore) end)

    :ok
  end

  test "the moduledoc's projector migrates, starts and projects as written" do
    config =
      :decree
      |> Application.fetch_env!(Decree.Projections.Repo)
      |> Keyword.take([:username, :password, :hostname, :port])
      |> Keyword.put(:database, @database)

    Application.put_env(:decree, Example.Repo, config)
    _ = Ecto.Adapters.Postgres.storage_down(config)
    :ok = Ecto.Adapters.Postgres.storage_up(config)
    on_exit(fn -> Ecto.Adapters.Postgres.storage_down(config) end)

    Decree.FrameworkStorage.reset!()

    moduledoc = DocBlocks.doc!(Decree.Projections.Ecto)

    assert [@account, @projector] -- compile!(moduledoc, "# lib/my_app/accounts.ex") == []

    [migration] =
      compile!(moduledoc, "# priv/repo/migrations/20261120000000_create_accounts.exs")

    {:ok, :ok, _apps} =
      Ecto.Migrator.with_repo(Example.Repo, fn repo ->
        Ecto.Migrator.up(repo, 20_261_120_000_000, migration, log: false)
      end)

    [application] = compile!(moduledoc, "# lib/my_app/application.ex")

    start_supervised!(%{
      id: application,
      start: {application, :start, [:normal, []]},
      type: :supervisor
    })

    account_id = Decree.UUID.uuid4()

    events =
      Mapper.map_to_event_data([
        %Example.AccountRegistered{account_id: account_id, handle: "ada"}
      ])

    :ok = EventStore.append_to_stream(Example.App, "account-" <> account_id, :any_version, events)

    assert %{handle: "ada"} = eventually(fn -> Example.Repo.get(@account, account_id) end)

    assert %{rows: [["accounts", 1]]} =
             Example.Repo.query!(
               "SELECT projection_name, last_seen_event_number FROM projection_versions"
             )
  end

  test "the after_update/3 example compiles as written, on the moduledoc's read model" do
    moduledoc = DocBlocks.doc!(Decree.Projections.Ecto)
    doc = DocBlocks.doc!(Decree.Projections.Ecto, {:callback, :after_update, 3})

    assert @account in compile!(moduledoc, "# lib/my_app/accounts.ex")
    assert @projector in compile!(doc, "# lib/my_app/accounts.ex")

    event = %Example.AccountRegistered{account_id: Decree.UUID.uuid4(), handle: "ada"}
    row = struct(@account, account_id: event.account_id, handle: event.handle)

    # Compiled from the doc at run time, so called through apply/3.
    assert :ok = apply(@projector, :after_update, [event, %{}, %{account: row}])
  end

  defp eventually(fun, attempts \\ 100) do
    case fun.() do
      nil when attempts > 1 ->
        Process.sleep(20)
        eventually(fun, attempts - 1)

      result ->
        result
    end
  end
end
