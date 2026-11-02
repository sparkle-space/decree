defmodule Decree.Commands.CompositeRouter do
  @moduledoc """
  Composite router allows you to combine multiple router modules into a single
  router able to dispatch any registered command from an included child router.

  One example usage is to define a router per context and then combine each
  context's router into a single top-level composite app router used for all
  command dispatching.

  ### Example

  Define a composite router module which imports the commands from each included
  router:

      defmodule Bank.AppRouter do
        use Decree.Commands.CompositeRouter

        router(Bank.Accounts.Router)
        router(Bank.MoneyTransfer.Router)
      end

    One or more routers or composite routers can be included in a
    `Decree.Application`  since it is also a composite router:

      defmodule BankApp do
        use Decree.Application

        router(Bank.AppRouter)
      end

  You can dispatch a command via the application which will then be routed to
  the associated child router:

      command = %OpenAccount{account_number: "ACC123", initial_balance: 1_000}

      :ok = BankApp.dispatch(command)

  Or via the composite router itself, specifying the application:

      :ok = Bank.AppRouter.dispatch(command, application: BankApp)

  A composite router can include composite routers.

  A decision's stream prefix may not overlap an aggregate's across the routers it includes:
  see "Prefixes" in `Decree.Commands.Router.decide/2`.
  """

  alias Decree.Commands.Router

  defmacro __using__(opts) do
    quote do
      require Logger

      import unquote(__MODULE__)

      @before_compile unquote(__MODULE__)

      Module.register_attribute(__MODULE__, :registered_commands, accumulate: false)

      application = Keyword.get(unquote(opts), :application)

      default_dispatch_opts =
        unquote(opts)
        |> Keyword.get(:default_dispatch_opts, [])
        |> Keyword.put(:application, application)

      @default_dispatch_opts default_dispatch_opts
      @registered_commands %{}
      @stream_prefixes %{decision: [], other: []}
      @conditional_targets []
    end
  end

  @doc """
  Register a `Decree.Commands.Router` module within this composite router.

  Will allow the composite router to dispatch any commands registered by the
  included router module. Multiple routers can be registered.
  """
  defmacro router(router_module) do
    quote location: :keep do
      for command <- unquote(router_module).__registered_commands__() do
        case Map.get(@registered_commands, command) do
          nil ->
            @registered_commands Map.put(@registered_commands, command, unquote(router_module))

          existing_router ->
            raise "duplicate registration for #{inspect(command)} command, registered in both #{inspect(existing_router)} and #{inspect(unquote(router_module))}"
        end
      end

      @stream_prefixes Router.merge_stream_prefixes!(
                         @stream_prefixes,
                         unquote(router_module).__stream_prefixes__(),
                         __MODULE__
                       )

      @conditional_targets Enum.uniq(
                             @conditional_targets ++
                               unquote(router_module).__conditional_targets__()
                           )
    end
  end

  defmacro __before_compile__(_env) do
    quote generated: true do
      @doc false
      def __registered_commands__ do
        Enum.map(@registered_commands, fn {command_module, _router} -> command_module end)
      end

      @doc false
      def __stream_prefixes__, do: @stream_prefixes

      @doc false
      def __conditional_targets__, do: @conditional_targets

      @doc """
      Dispatch a registered command.
      """
      @spec dispatch(
              command :: struct(),
              timeout_or_opts :: non_neg_integer() | :infinity | Keyword.t()
            ) :: Router.dispatch_resp()
      def dispatch(command, opts \\ [])

      def dispatch(command, :infinity),
        do: do_dispatch(command, timeout: :infinity)

      def dispatch(command, timeout) when is_integer(timeout),
        do: do_dispatch(command, timeout: timeout)

      def dispatch(command, opts),
        do: do_dispatch(command, opts)

      for {command_module, router} <- @registered_commands do
        @command_module command_module
        @router router

        defp do_dispatch(%@command_module{} = command, opts) do
          opts = Keyword.merge(@default_dispatch_opts, opts)

          @router.dispatch(command, opts)
        end
      end

      # Catch unregistered commands, log and return an error.
      defp do_dispatch(command, _opts) do
        Logger.error(fn ->
          "attempted to dispatch an unregistered command: " <> inspect(command)
        end)

        {:error, :unregistered_command}
      end
    end
  end
end
