defmodule Decree.Store.ListEventStoreMigrationsTest do
  use ExUnit.Case

  import ExUnit.CaptureIO

  alias Decree.Store.StorageInitializer

  @moduletag :migration

  setup_all do
    config = TestEventStore.config()

    on_exit(&StorageInitializer.recreate_test_event_store_database!/0)

    [config: config]
  end

  describe "event store with pending migrations" do
    test "should list completed and pending migrations", %{config: config} do
      StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")

      expected = """

      Decree.Store: TestEventStore

        migration     state           migrated_at
      -------------------------------------------------------------
        0.17.0    \tcompleted\t2020-04-28 22:15:48.922963Z
        1.1.0     \tpending
        1.2.0     \tpending
        1.3.0     \tpending
        1.3.2     \tpending
        1.4.0     \tpending
        1.5.0     \tpending
        1.6.0     \tpending
        1.7.0     \tpending
        1.8.0     \tpending
        1.9.0     \tpending
        1.10.0    \tpending
        1.11.0    \tpending

      """

      assert expected == list_migrations(config, eventstore: TestEventStore)
    end

    test "exits with status 1 when running via mix", %{config: config} do
      StorageInitializer.restore_dump!(config, "test/store/fixture/eventstore.dump")

      assert {:shutdown, 1} ==
               catch_exit(
                 Mix.Task.run("decree.store.migrations", ["-e", "TestEventStore", "--quiet"])
               )
    end
  end

  describe "recently created event store" do
    test "lists migrations as completed", %{config: config} do
      create_new_database(config)

      expected_header = """

      Decree.Store: TestEventStore

        migration     state           migrated_at
      -------------------------------------------------------------
      """

      output = list_migrations(config, eventstore: TestEventStore)

      assert String.starts_with?(output, expected_header)

      migrations =
        output |> String.split("\n") |> Enum.drop(5) |> Enum.filter(&(String.trim(&1) != ""))

      assert Enum.all?(migrations, &String.contains?(&1, "completed"))
    end

    test "exits normally when running via mix", %{config: config} do
      create_new_database(config)

      # Task should exit normally
      Mix.Task.run("decree.store.migrations", ["-e", "TestEventStore", "--quiet"])
    end
  end

  defp create_new_database(config) do
    StorageInitializer.recreate_database!(config)

    {:ok, conn} = Postgrex.start_link(config)

    Decree.Store.Storage.Initializer.run!(conn, config)

    :ok = GenServer.stop(conn)
  end

  defp list_migrations(config, opts) do
    capture_io(fn -> Decree.Store.Tasks.Migrations.exec(config, opts) end)
  end
end
