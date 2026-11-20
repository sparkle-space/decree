defmodule Decree.ReadmeExampleTest do
  # The README's installation snippet and its minimal application, read from README.md and run
  # as written, so the README cannot drift from what decree does. Only the connection settings
  # are the suite's: the example's store runs over the framework test database.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Decree.DocBlocks

  @readme Path.expand("../../README.md", __DIR__)

  @connection [:username, :password, :hostname, :port, :database, :pool_size]

  defp blocks do
    for [code] <-
          Regex.scan(~r/^```elixir\n(.*?)^```$/ms, File.read!(@readme), capture: :all_but_first),
        into: %{} do
      [header | _lines] = String.split(code, "\n", parts: 2)
      {header, code}
    end
  end

  defp block!(header) do
    case Map.fetch(blocks(), header) do
      {:ok, code} -> code
      :error -> flunk("README.md has no elixir block opening with #{inspect(header)}")
    end
  end

  test "the installation snippet needs no Postgrex line: decree takes Postgrex from Hex" do
    module = Module.concat([__MODULE__, "Deps#{System.unique_integer([:positive])}"])

    Code.compile_string("""
    defmodule #{inspect(module)} do
      #{block!("# mix.exs")}
      def readme_deps, do: deps()
    end
    """)

    readme_deps = module.readme_deps()

    assert {:postgrex, requirement} = List.keyfind(Mix.Project.config()[:deps], :postgrex, 0)
    assert is_binary(requirement)
    refute List.keymember?(readme_deps, :postgrex, 0)

    assert {:decree, requirement()} in readme_deps

    assert List.keymember?(readme_deps, :jason, 0)
  end

  # The requirement and series of mix.exs's version: a minor or major bump moves both
  # (scripts/release.exs bump), so the release PR passes as generated.
  defp requirement do
    %Version{major: major, minor: minor} = Version.parse!(Mix.Project.config()[:version])
    "~> #{major}.#{minor}.0"
  end

  test "the README's Status and the CHANGELOG's preamble name mix.exs's version" do
    %Version{major: major, minor: minor} = Version.parse!(Mix.Project.config()[:version])

    assert File.read!(@readme) =~ "decree is pre-1.0 (version #{major}.#{minor}.x)."
    assert File.read!(@readme) =~ "`#{requirement()}` takes patch versions only"

    changelog = Path.expand("../../CHANGELOG.md", __DIR__)
    [preamble, _rest] = String.split(File.read!(changelog), "\n## Unreleased\n", parts: 2)
    assert preamble =~ "Depend on `#{requirement()}` to take patches only."
  end

  test "the minimal application compiles and runs as the README says" do
    [my_app: config] = Config.Reader.eval!("config/config.exs", block!("# config/config.exs"))

    connection =
      :decree |> Application.fetch_env!(Decree.FrameworkEventStore) |> Keyword.take(@connection)

    for {key, value} <- config do
      value = if key == MyApp.EventStore, do: Keyword.merge(value, connection), else: value
      Application.put_env(:my_app, key, value)
    end

    Code.compile_string(block!("# lib/my_app.ex"), @readme)

    readme_app_test!()

    Decree.FrameworkStorage.reset!()

    start_supervised!(MyApp.App)
    start_supervised!(MyApp.Welcome)

    test_pid = self()
    handler_id = {__MODULE__, make_ref()}

    :telemetry.attach(
      handler_id,
      [:decree, :event, :handle, :stop],
      fn
        _event, _measurements, %{handler_module: MyApp.Welcome, recorded_event: event}, _ ->
          send(test_pid, {:handled, event.data})

        # Another handler's stop: a raise here would detach this handler.
        _event, _measurements, _metadata, _ ->
          :ok
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    # What the README says a rule dispatch answers before the index is enabled.
    command =
      struct(MyApp.RegisterAccount,
        command_id: Decree.UUID.uuid4(),
        account_id: Decree.UUID.uuid4(),
        handle: "ada"
      )

    assert {{:error, :tag_index_not_enabled}, _binding} =
             Code.eval_string("MyApp.App.dispatch(command)", command: command)

    # `Decree.Store.TagIndex`'s empty-store check, on the README's store: empty until the
    # enabling fact, which the README's next block appends after the same check.
    empty_store_check =
      Decree.Store.TagIndex
      |> DocBlocks.doc!()
      |> DocBlocks.block!("case MyApp.EventStore.stream_info(:all) do")

    assert {:empty, _binding} = Code.eval_string(empty_store_check)

    # The README's release step enables the index on the empty store, registers, then runs
    # again on the store with history, where it only checks.
    log =
      capture_log(fn ->
        Code.eval_string(block!("# Before any other writer starts"))

        for handle <- ["ada", "grace"] do
          assert_receive {:handled, %{__struct__: MyApp.AccountRegistered, handle: ^handle}}
        end
      end)

    assert log =~ "welcome, @ada"
    assert log =~ "welcome, @grace"

    assert {:history, _binding} = Code.eval_string(empty_store_check)

    # One enable, one fact, every family in it, though the step ran twice.
    assert fact_count() == 1
    assert Decree.Store.TagIndex.states(MyApp.EventStore) == {:ok, %{{"entity", nil} => :enabled}}

    # `bootstrap/3`'s own example, on the README's store: it has history, so it only checks.
    bootstrap =
      DocBlocks.doc!(Decree.Store.TagIndex, {:function, :bootstrap, 4})
      |> DocBlocks.block!("enable = fn families ->")

    assert {:ok, _binding} = Code.eval_string(bootstrap)
    assert fact_count() == 1

    # The ledger's duplicate clause: a resend of an enable appends nothing.
    enable =
      struct(MyApp.EnableTagIndexes,
        command_id: Decree.UUID.uuid4(),
        ledger_id: Decree.UUID.uuid4(),
        families: ["entity"]
      )

    assert {:ok, _binding} = Code.eval_string("MyApp.App.dispatch(enable)", enable: enable)
    assert fact_count() == 2

    assert {{:error, :duplicate_command}, _binding} =
             Code.eval_string("MyApp.App.dispatch(enable)", enable: enable)

    assert fact_count() == 2
  end

  # The README's application test, run as written on the README's application: an ExUnit
  # module cannot be compiled once the suite runs, so its one test's body runs here. It
  # passes while the release step's families cover the rule's, and fails with the family
  # named when the list leaves it out.
  defp readme_app_test! do
    assert {:defmodule, _meta, [_name, [do: {:__block__, _block, exprs}]]} =
             Code.string_to_quoted!(block!("# test/my_app/tag_index_test.exs"))

    assert [
             {:use, _use, [{:__aliases__, _case, [:ExUnit, :Case]} | _opts]},
             {:test, _test, [_description, [do: body]]}
           ] = exprs

    run = fn body ->
      Code.eval_quoted(
        quote(
          do:
            (
              import ExUnit.Assertions
              unquote(body)
            )
        )
      )
    end

    assert {true, _binding} = run.(body)

    left_out =
      Macro.prewalk(body, fn
        {{:., _, [{:__aliases__, _, [:MyApp, :TagIndexLedger]}, :families]}, _, []} -> []
        node -> node
      end)

    refute left_out == body
    error = assert_raise ExUnit.AssertionError, fn -> run.(left_out) end
    assert error.left == ["entity"]

    # The list the test checks is the one the release step enables.
    release_step = block!("# Before any other writer starts")

    bootstrap =
      "Decree.Store.TagIndex.bootstrap(MyApp.EventStore, MyApp.TagIndexLedger.families(), enable)"

    assert length(String.split(release_step, bootstrap)) == 3
  end

  test "every elixir block in the README is one a test here runs" do
    headers =
      for [code] <-
            Regex.scan(~r/^```elixir\n(.*?)^```$/ms, File.read!(@readme), capture: :all_but_first) do
        code |> String.split("\n", parts: 2) |> hd()
      end

    assert headers == [
             "# mix.exs",
             "# config/config.exs",
             "# lib/my_app.ex",
             "# Before any other writer starts",
             "# test/my_app/tag_index_test.exs"
           ]
  end

  test "Decree.Store.TagIndex.bootstrap/3's doc has one example, the one the README test runs" do
    doc = DocBlocks.doc!(Decree.Store.TagIndex, {:function, :bootstrap, 4})

    assert DocBlocks.first_lines(doc) == ["enable = fn families ->"]
    assert DocBlocks.block!(doc, "enable = fn families ->") =~ "MyApp.EnableTagIndexes"
  end

  defp fact_count do
    {:ok, %Decree.Store.ReadResult{events: facts}} =
      Code.eval_string("MyApp.EventStore.read_query([[tags: [\"tag_index:fact\"]]])")
      |> elem(0)

    length(facts)
  end

  # `Decree.Store.TagIndex`'s moduledoc, read from its compiled docs. Its empty-store check
  # runs in the test above, on the README's store; its other two blocks run here. Every block
  # opens with one of the three, so a new example cannot go unchecked.
  test "Decree.Store.TagIndex's state tag and named families read as written" do
    doc = DocBlocks.doc!(Decree.Store.TagIndex)

    state_tag = "tag_index_state:<family>:<key or ->:<enabled|disabled>"
    assert String.trim(DocBlocks.block!(doc, state_tag)) == state_tag

    assert Decree.Store.TagIndex.state_tag("tenant_slug", nil, :enabled) ==
             "tag_index_state:tenant_slug:-:enabled"

    assert Decree.Store.TagIndex.state_tag("tenant_slug", "a1", :disabled) ==
             "tag_index_state:tenant_slug:a1:disabled"

    named_families = ~s(config :decree, :tag_index_named_families, ["tenant_slug", "plan_slug"])

    assert [decree: [tag_index_named_families: families]] =
             Config.Reader.eval!(
               "config/config.exs",
               "import Config\n" <> DocBlocks.block!(doc, named_families)
             )

    previous = Application.fetch_env(:decree, :tag_index_named_families)
    Application.put_env(:decree, :tag_index_named_families, families)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:decree, :tag_index_named_families, value)
        :error -> Application.delete_env(:decree, :tag_index_named_families)
      end
    end)

    assert Enum.take(Decree.Store.TagIndex.named_families(), -2) == families

    selectors = [state_tag, named_families, "case MyApp.EventStore.stream_info(:all) do"]
    first_lines = DocBlocks.first_lines(doc)

    assert length(first_lines) == 3
    assert Enum.reject(first_lines, &(&1 in selectors)) == []
  end
end
