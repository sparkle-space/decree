defmodule Decree.Store.TasksQuietTest do
  use ExUnit.Case

  alias Decree.Store.Tasks.Drop

  # `quiet: true` silences a task's output, never its failure: a drop that fails
  # must still raise, so `mix decree.store.drop --quiet` exits non-zero.
  #
  # The config points at a local port nothing listens on, so the connection is
  # refused at once and `Database.drop/1` / `Schema.drop/1` return `{:error, message}`.

  describe "a failed drop with `quiet: true`" do
    test "raises `Mix.Error` from a Mix task (the database arm)" do
      config = unreachable_config(schema: "public")

      assert_raise Mix.Error, ~r/The Decree.Store database couldn't be dropped/, fn ->
        Drop.exec(config, is_mix: true, quiet: true)
      end
    end

    test "raises `RuntimeError` outside a Mix task (the database arm)" do
      config = unreachable_config(schema: "public")

      assert_raise RuntimeError, ~r/The Decree.Store database couldn't be dropped/, fn ->
        Drop.exec(config, is_mix: false, quiet: true)
      end
    end

    test "raises `RuntimeError` outside a Mix task (the schema arm)" do
      config = unreachable_config(schema: "some_schema")

      assert_raise RuntimeError, ~r/The Decree.Store schema couldn't be dropped/, fn ->
        Drop.exec(config, quiet: true)
      end
    end

    test "`mix decree.store.drop --quiet` raises `Mix.Error`" do
      Application.put_env(:decree, __MODULE__.UnreachableStore, unreachable_config([]))
      on_exit(fn -> Application.delete_env(:decree, __MODULE__.UnreachableStore) end)

      assert_raise Mix.Error, ~r/couldn't be dropped/, fn ->
        Mix.Tasks.Decree.Store.Drop.run(["-e", inspect(__MODULE__.UnreachableStore), "--quiet"])
      end
    end
  end

  defp unreachable_config(overrides) do
    TestEventStore.config()
    |> Keyword.merge(hostname: "127.0.0.1", port: closed_port())
    |> Keyword.merge(overrides)
  end

  # A port that was free a moment ago: listen on an ephemeral port, then close it.
  defp closed_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end
end

defmodule Decree.Store.TasksQuietTest.UnreachableStore do
  use Decree.Store, otp_app: :decree
end
