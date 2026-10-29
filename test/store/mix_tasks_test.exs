defmodule Decree.Store.MixTasksTest do
  use Decree.Store.StorageCase

  import ExUnit.CaptureIO

  # The `-e` / `--eventstore` option names the event store a task acts on. Its
  # OptionParser key is `:eventstore`, a CLI option name, not the OTP application.

  describe "`-e` option" do
    test "`decree.store.create` accepts `-e`" do
      assert :ok =
               Mix.Tasks.Decree.Store.Create.run(["-e", "TestEventStore", "--quiet"])
    end

    test "`decree.store.migrations` lists the event store named by `-e`" do
      output =
        capture_io(fn ->
          Mix.Tasks.Decree.Store.Migrations.run(["-e", "TestEventStore"])
        end)

      assert output =~ "Decree.Store: TestEventStore\n"
    end

    test "`decree.store.migrations` lists the event store named by `--eventstore`" do
      output =
        capture_io(fn ->
          Mix.Tasks.Decree.Store.Migrations.run(["--eventstore", "TestEventStore"])
        end)

      assert output =~ "Decree.Store: TestEventStore\n"
    end

    test "`Decree.Store.Tasks.Migrations.exec/2` names the event store given in its `eventstore` option",
         %{config: config} do
      output =
        capture_io(fn ->
          Decree.Store.Tasks.Migrations.exec(config, eventstore: TestEventStore)
        end)

      assert output =~ "Decree.Store: TestEventStore\n"
    end
  end
end
