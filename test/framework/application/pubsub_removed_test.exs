defmodule Decree.Application.PubSubRemovedTest do
  # The application's `pubsub:` option and `Decree.PubSub` are removed
  # (sparkle-space/sparklespace#8566): nothing in decree published on it once `consistency:` went.
  # Every door that took the option refuses it, whatever its value: the build fails for a `use`
  # option or the application's config, and the start fails for a `start_link/1` option or one
  # `init/1` returns. The store's own `Decree.Store.PubSub` is not this, and stays.
  use Decree.PostgresCase

  alias Decree.Application.Config
  alias Decree.ExampleApplication

  @removed ~r/`pubsub:`.*removed/s

  defmodule InitAddsApp do
    use Decree.Application,
      otp_app: :decree,
      event_store: [event_store: Decree.FrameworkEventStore]

    def init(config) do
      {:ok, config |> Decree.FrameworkStorage.name_event_store() |> Keyword.put(:pubsub, :local)}
    end
  end

  defmodule InitDropsApp do
    use Decree.Application,
      otp_app: :decree,
      event_store: [event_store: Decree.FrameworkEventStore]

    def init(config) do
      {:ok, config |> Decree.FrameworkStorage.name_event_store() |> Keyword.delete(:pubsub)}
    end
  end

  describe "the application pub-sub is gone" do
    test "no `Decree.PubSub`, no adapters and no `pubsub_adapter/1`" do
      refute Code.ensure_loaded?(Decree.PubSub)
      refute Code.ensure_loaded?(Decree.PubSub.Adapter)
      refute Code.ensure_loaded?(Decree.PubSub.LocalPubSub)
      refute Code.ensure_loaded?(Decree.PubSub.PhoenixPubSub)

      Code.ensure_loaded!(Decree.Application)
      refute function_exported?(Decree.Application, :pubsub_adapter, 1)
    end

    test "an application starts with no pub-sub child and no `pubsub` config" do
      pid = start_supervised!(ExampleApplication)

      assert Process.whereis(ExampleApplication) == pid
      assert Process.whereis(Decree.ExampleApplication.LocalRegistry)
      refute Process.whereis(Decree.ExampleApplication.LocalPubSub)
      refute Process.whereis(Decree.ExampleApplication.LocalPubSub.Tracker)
      refute Config.get(ExampleApplication, :pubsub)
    end
  end

  describe "a `use` option or config fails the build" do
    for {value, index} <-
          Enum.with_index([:local, [phoenix_pubsub: [adapter: Phoenix.PubSub.PG2]], nil]) do
      test "on `use Decree.Application, pubsub: #{inspect(value)}`" do
        value = unquote(Macro.escape(value))
        module = Module.concat(__MODULE__, "UseOption#{unquote(index)}")

        assert_raise ArgumentError, @removed, fn ->
          compile(
            quote do
              defmodule unquote(module) do
                use Decree.Application,
                  otp_app: :decree,
                  event_store: [event_store: Decree.FrameworkEventStore],
                  pubsub: unquote(Macro.escape(value))
              end
            end
          )
        end
      end
    end

    test "on `config :otp_app, App, pubsub: ...`" do
      module = Module.concat(__MODULE__, ConfiguredApp)

      Application.put_env(:decree, module,
        event_store: [event_store: Decree.FrameworkEventStore],
        pubsub: :local
      )

      on_exit(fn -> Application.delete_env(:decree, module) end)

      assert_raise ArgumentError, @removed, fn ->
        compile(
          quote do
            defmodule unquote(module) do
              use Decree.Application, otp_app: :decree
            end
          end
        )
      end
    end
  end

  describe "the start is refused" do
    for value <- [:local, nil] do
      test "for `pubsub: #{inspect(value)}` given to start_link/1" do
        message =
          start_refused(fn ->
            ExampleApplication.start_link(name: :pubsub_removed_opts, pubsub: unquote(value))
          end)

        assert message =~ @removed
      end
    end

    test "for `pubsub:` returned from init/1" do
      assert start_refused(fn -> InitAddsApp.start_link() end) =~ @removed
    end

    test "for `pubsub:` given to start_link/1 when init/1 drops it" do
      assert start_refused(fn -> InitDropsApp.start_link(pubsub: :local) end) =~ @removed
    end

    test "and `config/0` refuses an init/1 that returns it" do
      assert_raise ArgumentError, @removed, fn -> InitAddsApp.config() end
    end
  end

  defp compile(quoted), do: Code.compile_quoted(quoted)

  # The refusal is an `ArgumentError`, raised in the caller or answered by the supervisor.
  defp start_refused(start) do
    Process.flag(:trap_exit, true)

    try do
      start.()
    rescue
      error in ArgumentError -> error.message
    else
      {:error, {%ArgumentError{message: message}, _stacktrace}} -> message
      other -> flunk("expected the start to be refused, got: " <> inspect(other))
    end
  end
end
