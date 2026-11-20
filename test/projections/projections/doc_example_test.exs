defmodule Decree.Projections.DocExampleTest do
  # `Decree.Projections.Ecto`'s examples, read from its compiled docs and run as written: the
  # repo and its config, the migration on a database of its own, the OTP application started,
  # an event projected. The test supplies what the examples take from the README's minimal
  # application, `MyApp.App` and `MyApp.AccountRegistered`, and the suite's own connection
  # settings. Every code block in a checked doc opens with a line the test runs, so a new
  # example cannot go unchecked. `MyApp` is renamed, so the README test's `MyApp` modules in
  # the same VM are left alone.
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

  @namespace inspect(Example)
  @repo Module.concat([Example, Repo])
  @account Module.concat([Example, Accounts, Account])
  @projector Module.concat([Example, Accounts, Projector])

  defp example(doc, first_line) do
    doc |> DocBlocks.block!(first_line) |> String.replace("MyApp", @namespace)
  end

  # `doc` has `count` code blocks, and each opens with one of `selectors`, so a new example
  # cannot go unchecked.
  defp assert_covered(doc, selectors, count) do
    first_lines = DocBlocks.first_lines(doc)

    assert length(first_lines) == count
    assert Enum.reject(first_lines, &(&1 in selectors)) == []
  end

  defp compile!(doc, first_line) do
    doc
    |> example(first_line)
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

  # The moduledoc's repo, read model and projector compiled, and its migration run.
  defp migrate!(moduledoc) do
    # The doc's config, with the suite's connection and a database of the test's own.
    [my_app: config] =
      Config.Reader.eval!("config/config.exs", example(moduledoc, "# config/config.exs"))

    assert config[:ecto_repos] == [@repo]

    connection =
      :decree
      |> Application.fetch_env!(Decree.Projections.Repo)
      |> Keyword.take([:username, :password, :hostname, :port])
      |> Keyword.put(:database, @database)

    repo_config = Keyword.merge(Keyword.fetch!(config, @repo), connection)
    Application.put_env(:my_app, @repo, repo_config)
    on_exit(fn -> Application.delete_env(:my_app, @repo) end)

    _ = Ecto.Adapters.Postgres.storage_down(repo_config)
    :ok = Ecto.Adapters.Postgres.storage_up(repo_config)
    on_exit(fn -> Ecto.Adapters.Postgres.storage_down(repo_config) end)

    Decree.FrameworkStorage.reset!()

    assert [@repo] == compile!(moduledoc, "# lib/my_app/repo.ex")
    assert [@account, @projector] -- compile!(moduledoc, "# lib/my_app/accounts.ex") == []

    [migration] =
      compile!(moduledoc, "# priv/repo/migrations/20261120000000_create_accounts.exs")

    {:ok, :ok, _apps} =
      Ecto.Migrator.with_repo(@repo, fn repo ->
        Ecto.Migrator.up(repo, 20_261_120_000_000, migration, log: false)
      end)
  end

  test "the moduledoc's projector migrates, starts and projects as written" do
    moduledoc = DocBlocks.doc!(Decree.Projections.Ecto)
    migrate!(moduledoc)

    [application] = compile!(moduledoc, "# lib/my_app/application.ex")

    start_supervised!(%{
      id: application,
      start: {application, :start, [:normal, []]},
      type: :supervisor
    })

    # The projector's handle span stops once its transaction has committed.
    test_pid = self()
    handler_id = {__MODULE__, make_ref()}

    :telemetry.attach(
      handler_id,
      [:decree, :event, :handle, :stop],
      fn
        _event, _measurements, %{handler_module: @projector}, _config ->
          send(test_pid, :projected)

        _event, _measurements, _metadata, _config ->
          :ok
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    account_id = Decree.UUID.uuid4()

    events =
      Mapper.map_to_event_data([
        %Example.AccountRegistered{account_id: account_id, handle: "ada"}
      ])

    :ok = EventStore.append_to_stream(Example.App, "account-" <> account_id, :any_version, events)

    assert_receive :projected, 5_000

    # Compiled from the doc at run time, so called through apply/3.
    assert %{handle: "ada"} = apply(@repo, :get, [@account, account_id])

    assert %{rows: [["accounts", 1]]} =
             Ecto.Adapters.SQL.query!(
               @repo,
               "SELECT projection_name, last_seen_event_number FROM projection_versions"
             )

    # Each of the five blocks above, once.
    assert_covered(
      moduledoc,
      [
        "# config/config.exs",
        "# lib/my_app/repo.ex",
        "# lib/my_app/accounts.ex",
        "# priv/repo/migrations/20261120000000_create_accounts.exs",
        "# lib/my_app/application.ex"
      ],
      5
    )
  end

  test "the after_update/3 example compiles as written, on the moduledoc's read model" do
    moduledoc = DocBlocks.doc!(Decree.Projections.Ecto)
    doc = DocBlocks.doc!(Decree.Projections.Ecto, {:callback, :after_update, 3})

    # The projector calls the repo, so the repo compiles first.
    assert [@repo] == compile!(moduledoc, "# lib/my_app/repo.ex")
    assert @account in compile!(moduledoc, "# lib/my_app/accounts.ex")
    assert @projector in compile!(doc, "# lib/my_app/accounts.ex")

    event = %Example.AccountRegistered{account_id: Decree.UUID.uuid4(), handle: "ada"}
    row = struct(@account, account_id: event.account_id, handle: event.handle)

    # Compiled from the doc at run time, so called through apply/3.
    assert :ok = apply(@projector, :after_update, [event, %{}, %{account: row}])

    assert_covered(doc, ["# lib/my_app/accounts.ex"], 1)
  end

  test "the project/2 and project/3 examples project as written" do
    moduledoc = DocBlocks.doc!(Decree.Projections.Ecto)
    migrate!(moduledoc)
    start_supervised!(@repo)

    for {arity, event_number} <- [{2, 1}, {3, 2}] do
      doc = DocBlocks.doc!(Decree.Projections.Ecto, {:macro, :project, arity})
      assert @projector in compile!(doc, "# lib/my_app/accounts.ex")
      assert_covered(doc, ["# lib/my_app/accounts.ex"], 1)

      account_id = Decree.UUID.uuid4()
      handle = "ada-#{arity}"
      event = %Example.AccountRegistered{account_id: account_id, handle: handle}
      metadata = %{handler_name: "accounts", event_number: event_number}

      # Compiled from the doc at run time, so called through apply/3.
      assert :ok = apply(@projector, :handle, [event, metadata])
      assert %{handle: ^handle} = apply(@repo, :get, [@account, account_id])
    end

    assert %{rows: [["accounts", 2]]} =
             Ecto.Adapters.SQL.query!(
               @repo,
               "SELECT projection_name, last_seen_event_number FROM projection_versions"
             )
  end
end
