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

    assert {:decree, "~> 0.1.0"} in readme_deps
    assert Version.match?(Mix.Project.config()[:version], "~> 0.1.0")

    assert List.keymember?(readme_deps, :jason, 0)
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

    log =
      capture_log(fn ->
        Code.eval_string(block!("# On a fresh store, before anything else is appended"))

        for handle <- ["ada", "grace"] do
          assert_receive {:handled, %{__struct__: MyApp.AccountRegistered, handle: ^handle}}
        end
      end)

    assert log =~ "welcome, @ada"
    assert log =~ "welcome, @grace"

    assert {:history, _binding} = Code.eval_string(empty_store_check)
  end
end
