defmodule Decree.Commands.Router do
  @moduledoc """
  Command routing macro to allow configuration of each command to its command handler.

  ## Example

  Define a router module which uses `Decree.Commands.Router` and configures
  available commands to dispatch:

      defmodule BankRouter do
        use Decree.Commands.Router

        dispatch OpenAccount,
          to: OpenAccountHandler,
          aggregate: BankAccount,
          identity: :account_number
      end

  The `to` option determines which module receives the command being dispatched.
  This command handler module must implement a `handle/2` function. It receives
  the aggregate's state and the command to execute. Usually the command handler
  module will forward the command to the aggregate.

  Once configured, you can either dispatch a command by using the module and
  specifying the application:

      command = %OpenAccount{
        command_id: command_id,
        account_number: account_number,
        initial_balance: 1_000
      }

      :ok = BankRouter.dispatch(command, application: BankApp)

  Or, more simply, you should include the router module in your application:

      defmodule BankApp do
        use Decree.Application, otp_app: :my_app

        router MyApp.Router
      end

  Then dispatch commands using the app:

      command = %OpenAccount{
        command_id: command_id,
        account_number: account_number,
        initial_balance: 1_000
      }

      :ok = BankApp.dispatch(command)

  ## Dispatch command directly to an aggregate

  You can route a command directly to an aggregate, without requiring an
  intermediate command handler.

  ### Example

      defmodule BankRouter do
        use Decree.Commands.Router

        # Will route to `BankAccount.open_account/2`
        dispatch OpenAccount, to: BankAccount, identity: :account_number
      end

  By default, you must define an `execute/2` function on the aggregate module, which will be
  called with the aggregate's state and the command to execute. Using this approach, you must
  create an `execute/2` clause that pattern-matches on each command that the aggregate should
  handle.

  Alternatively, you may specify the name of a function (also receiving both the aggregate state
  and the command) on your aggregate module to which the command will be dispatched:

  ### Example

      defmodule BankRouter do
        use Decree.Commands.Router

        # Will route to `BankAccount.open_account/2`
        dispatch OpenAccount, to: BankAccount, function: :open_account, identity: :account_number
      end

  ## Define aggregate identity

  You can define the identity field for an aggregate once using the `identify` macro.
  The configured identity will be used for all commands registered to the aggregate,
  unless overridden by a command registration.

  ### Example

      defmodule BankRouter do
        use Decree.Commands.Router

        identify BankAccount,
          by: :account_number,
          prefix: "bank-account-"

        dispatch OpenAccount, to: BankAccount
      end

  An optional identity prefix can be used to distinguish between different
  aggregates that  would otherwise share the same identity. As an example you
  might have a `User` and a `UserPreferences` aggregate that you wish
  to share the same identity. In this scenario you should specify a `prefix`
  for each aggregate (e.g. `"user-"` and `"user-preference-"`).

  The prefix is used as the stream identity when appending and reading the
  aggregate's events: `"<identity_prefix><aggregate_uuid>"`. It can be a string or
  a zero arity function returning a string.

  ### A canonical identity

  The identity a route resolves, the command's field or what the `by:` function answered,
  must be a lowercase canonical UUID (`Decree.UUID.canonical?/1`), which the requester mints.
  It is checked as it was given, before the prefix, and nothing converts, trims or downcases
  it: anything else, a missing field and nil included, answers
  `{:error, :invalid_aggregate_identity}` before the aggregate is opened.

  An aggregate whose identity is someone else's, such as a Stripe id, declares it opaque:

      identify StripeCustomer, by: :customer_id, prefix: "stripe-customer-", opaque: true

  An opaque identity is any non-empty binary, taken as it is. `opaque:` is declared once, on
  the `identify`, or on a `dispatch` route for an aggregate with no `identify`; a route's
  `opaque:` for an aggregate that has an `identify` is refused when the router compiles,
  because the `identify` owns the identity. A decision's identity is never opaque: it is its
  stream's entity tag, so `decide/2` takes no `opaque:`, and an `identify` of a decision
  module with `opaque: true` is refused when the router compiles.

  ## Command id

  Every dispatched command carries a `command_id`, a lowercase canonical UUID its requester
  mints (see `Decree.Command`). The default middleware `Decree.Middleware.CommandId` refuses
  any other with `{:error, :command_id_required}` or `{:error, :invalid_command_id}`. It runs
  after every middleware the router registers, which see the command first and whose own
  refusal is the answer, and before `Decree.Middleware.ExtractAggregateIdentity`, so a bad
  `command_id` is refused before the identity is looked at. The `command_uuid` a dispatch
  mints never stands in for it.

  A router refuses, when it compiles, a command module that does not
  `use Decree.Command` only when it asks to:

      use Decree.Commands.Router, require_decree_command: true

  It is off by default for now, and the dispatch refusal does not depend on it.

  ## Check a command against a rule

  A route can name a `Decree.Decision.Rule`, which an aggregate's append for that command has
  to hold to, beyond its own stream. See "Rule" in `dispatch/2`.

  ## Decide a command with a decision

  A command can be routed to a `Decree.Decision`, which has no aggregate process: it reads
  what it names, folds it and decides, and its append is conditional on what it read. See
  `decide/2` for the route, and for what is refused when the router compiles.

  ## Consistency

  You can choose the consistency guarantee when dispatching a command. The
  available options are:

    - `:eventual` (default) - don't block command dispatch while waiting for
      event handlers

          :ok = BankApp.dispatch(command)
          :ok = BankApp.dispatch(command, consistency: :eventual)

    - `:strong` - block command dispatch until all strongly
      consistent event handlers have successfully processed all events created
      by the command.

      Use this when you have event handlers that update read models you need to
      query immediately after dispatching the command.

          :ok = BankApp.dispatch(command, consistency: :strong)

    - Provide an explicit list of event handler modules (or their configured
      names), containing only those handlers you'd like to wait for. No other
      handlers will be awaited on, regardless of their own configured
      consistency setting.

      ```elixir
      :ok = BankApp.dispatch(command, consistency: [ExampleHandler, AnotherHandler])
      :ok = BankApp.dispatch(command, consistency: ["ExampleHandler", "AnotherHandler"])
      ```

      Note you cannot opt-in to strong consistency for a handler that has been
      configured as eventually consistent.

  ## Dispatch return

  By default a successful command dispatch will return `:ok`. You can change
  this behaviour by specifying a `returning` option.

  The supported options are:

    - `:aggregate_state` - to return the updated aggregate state.

    - `:aggregate_version` - to return only the aggregate version.

    - `:events` - to return the resultant domain events. An empty list will be
      returned if no events were produced.

    - `:execution_result` - to return a `Decree.Commands.ExecutionResult`
      struct containing the aggregate's identity, state, version, and any events
      produced from the command along with their associated metadata.

    - `false` - don't return anything except an `:ok`.

  ### Aggregate state

  Return the updated aggregate state as part of the dispatch result:

      {:ok, %BankAccount{}} = BankApp.dispatch(command, returning: :aggregate_state)

  This is useful when you want to immediately return fields from the aggregate's
  state without requiring a read model projection and waiting for the event(s)
  to be projected. It may also be appropriate to use this feature for unit
  tests.

  However, be warned that tightly coupling an aggregate's state with read
  requests may be harmful. It's why CQRS enforces the separation of reads from
  writes by defining two separate and specialised models.

  ### Aggregate version

  You can optionally choose to return the aggregate's version as part of the
  dispatch result:

      {:ok, aggregate_version} = BankApp.dispatch(command, returning: :aggregate_version)

  This is useful when you need to wait for an event handler, such as a read model
  projection, to be up-to-date before querying its data.

  ### Execution results

  You can also choose to return the execution result as part of the dispatch
  result:

      alias Decree.Commands.ExecutionResult

      {:ok, %ExecutionResult{} = result} = BankApp.dispatch(command, returning: :execution_result)

  Or by setting the `default_dispatch_return` in your application config file:

      # config/config.exs
      config :decree, default_dispatch_return: :execution_result

  Use the execution result struct to get information from the events produced
  from the command.

  ## Metadata

  You can associate metadata with all events created by the command.

  Supply a map containing key/value pairs comprising the metadata:

      :ok = BankApp.dispatch(command, metadata: %{"ip_address" => "127.0.0.1"})

  ## Causation id

  You can associate a `:causation_id` with the dispatched command, which will
  also be persisted as the `causation_id` of every event the command produces.
  This lets you reconstruct a chain-of-causation across multiple events
  without persisting commands separately.

      :ok = BankApp.dispatch(command, causation_id: some_event_id)

  When `:causation_id` is not provided, the command's own `command_uuid` is
  used as the resulting events' `causation_id`.

  An event handler that dispatches a follow-up command can pass the handled
  event's id (`metadata.event_id`) as the `:causation_id`, so the full
  causation chain is observable on the events alone.

  """

  alias Decree.Aggregates.DefaultLifespan
  alias Decree.Commands.{ExecutionResult, Router}
  alias Decree.Telemetry
  alias Decree.UUID

  defmacro __using__(opts) do
    otp_app = Keyword.get(opts, :otp_app, :decree)
    app_module = Keyword.get(opts, :application)
    require_decree_command = Keyword.get(opts, :require_decree_command, false)

    quote do
      require Logger

      import unquote(__MODULE__)

      @before_compile unquote(__MODULE__)
      @behaviour Router

      Module.register_attribute(__MODULE__, :registered_commands, accumulate: true)
      Module.register_attribute(__MODULE__, :registered_middleware, accumulate: true)
      Module.register_attribute(__MODULE__, :registered_identities, accumulate: false)

      @require_decree_command Router.require_decree_command!(unquote(require_decree_command))

      @default_dispatch_opts [
        application: unquote(app_module),
        consistency: Router.get_opt(unquote(opts), :default_consistency, :eventual),
        returning: Router.get_default_dispatch_return(unquote(opts)),
        timeout: 5_000,
        lifespan:
          if unquote(app_module) do
            Application.compile_env(
              unquote(otp_app),
              [unquote(app_module), :aggregate_lifespan],
              DefaultLifespan
            )
          else
            DefaultLifespan
          end,
        metadata: %{},
        retry_attempts: 10
      ]

      @default_middleware [
        Decree.Middleware.CommandId,
        Decree.Middleware.ExtractAggregateIdentity,
        Decree.Middleware.ConsistencyGuarantee
      ]

      @registered_identities %{}
    end
  end

  @doc """
  Include the given middleware module to be called before and after
  success or failure of each command dispatch

  The middleware module must implement the `Decree.Middleware` behaviour.

  Middleware modules are executed in the order they are defined, and all of them before
  the router's default middleware: `Decree.Middleware.CommandId`, which refuses a command
  whose `command_id` is not a canonical UUID, then
  `Decree.Middleware.ExtractAggregateIdentity` and `Decree.Middleware.ConsistencyGuarantee`.
  A registered middleware so sees every command, and its own halt is the answer.

  ## Example

      defmodule BankRouter do
        use Decree.Commands.Router

        middleware CommandLogger
        middleware MyCommandValidator
        middleware AuthorizeCommand

        dispatch [OpenAccount, DepositMoney], to: BankAccount, identity: :account_number
      end

  """
  defmacro middleware(middleware_module) do
    quote do
      @registered_middleware unquote(middleware_module)
    end
  end

  @doc """
  Define an aggregate's identity

  You can define the identity field for an aggregate using the `identify` macro.
  The configured identity will be used for all commands registered to the
  aggregate, unless overridden by a command registration.

  ## Example

      defmodule BankRouter do
        use Decree.Commands.Router

        identify BankAccount,
          by: :account_number,
          prefix: "bank-account-"
      end

  The options are `by:`, a field or a one-arity function, `prefix:`, a string or a zero-arity
  function, and `opaque:`, `true` for an identity that is any non-empty binary rather than a
  canonical UUID, `false` by default. See "Define aggregate identity" in the module doc.
  """
  defmacro identify(aggregate_module, opts) do
    quote location: :keep, bind_quoted: [aggregate_module: aggregate_module, opts: opts] do
      case Map.get(@registered_identities, aggregate_module) do
        nil ->
          by =
            case Keyword.get(opts, :by) do
              nil ->
                raise "#{inspect(aggregate_module)} aggregate identity is missing the `by` option"

              by when is_atom(by) ->
                by

              by when is_function(by, 1) ->
                by

              invalid ->
                raise "#{inspect(aggregate_module)} aggregate identity has an invalid `by` option: #{inspect(invalid)}"
            end

          prefix =
            case Keyword.get(opts, :prefix) do
              nil ->
                nil

              prefix when is_function(prefix, 0) ->
                prefix

              prefix when is_binary(prefix) ->
                prefix

              invalid ->
                raise "#{inspect(aggregate_module)} aggregate has an invalid identity prefix: #{inspect(invalid)}"
            end

          opaque =
            case Keyword.fetch(opts, :opaque) do
              :error ->
                false

              {:ok, opaque} when is_boolean(opaque) ->
                opaque

              {:ok, invalid} ->
                raise "#{inspect(aggregate_module)} aggregate has an invalid `opaque` option: #{inspect(invalid)}, expected true or false"
            end

          @registered_identities Map.put(@registered_identities, aggregate_module,
                                   by: by,
                                   prefix: prefix,
                                   opaque: opaque
                                 )

        config ->
          raise "#{inspect(aggregate_module)} aggregate has already been identified by: `#{inspect(Keyword.get(config, :by))}`"
      end
    end
  end

  @doc """
  Configure the command, or list of commands, to be dispatched to the
  corresponding handler and aggregate.

  ## Example

      defmodule BankRouter do
        use Decree.Commands.Router

        dispatch [OpenAccount, DepositMoney], to: BankAccount, identity: :account_number
      end

  ## Stream tag

  Every event a route appends carries an entity tag for its stream, `<kind>:<id>`, as long
  as the identity the stream was routed by is a lowercase canonical UUID. Anything else,
  such as a Stripe id, gives no tag. The id is that identity as a string, never the stream
  name with its `identify` prefix.

  The kind is the identity field's name without a trailing `_id`, so `user_id` gives
  `user:` and `content_ref` gives `content_ref:`. A route names it with `stream_tag:`,
  an atom other than `nil`, `true` or `false` whose name matches `^[a-z][a-z0-9_]*$`,
  so that it fits the tag grammar. `dispatch` refuses anything else when the router
  compiles:

      dispatch [RequestExport], to: Export, identity: :requester_user_id, stream_tag: :user

  A function identity (`by: &Module.function/1`) has no field name, so it has a stream
  tag only when the route declares `stream_tag:`. The id is then what the function
  answered when the dispatch resolved the identity, once, before the aggregate is called.

  ## Rule

  `rule:` names a `Decree.Decision.Rule` for this route alone, so the aggregate's other commands
  stay plain:

      dispatch RegisterUser, to: User, identity: :user_id, rule: EmailRule

  The aggregate reads what the rule names, `execute/2` finds the events in the command's
  `:rule_result` field, and the append goes in under a condition on what was read. The event
  store's `conditional_appends` must name the route's aggregate in `targets:`, which a declared
  prefix does not stand in for, and an application that includes the route fails to start
  without it: a check started right after its store raises an `ArgumentError`. See "Two
  declarations" in `Decree.Decision.Rule`.

  `dispatch` refuses, with an `ArgumentError` when the router compiles, a `rule:` that is not a
  compiled `Decree.Decision.Rule`, and a command with no `:rule_result` field. `decide/2` takes
  no `rule:`: a decision is its own rule.

  ## Opaque identity

  `opaque: true` takes any non-empty binary as the route's identity, rather than only a
  canonical UUID, for an aggregate with no `identify`:

      dispatch RecordStripeEvent, to: StripeEvent, identity: :event_id, opaque: true

  For an aggregate with an `identify`, `opaque:` belongs there, and a route's is refused when
  the router compiles. See "Define aggregate identity" in the module doc.

  """
  defmacro dispatch(command_module_or_modules, opts) do
    register(command_module_or_modules, parse_opts(opts))
  end

  @doc """
  Configure the command, or list of commands, to be decided by a `Decree.Decision`.

  ## Example

      defmodule AccountRouter do
        use Decree.Commands.Router

        decide ClaimEmail, to: EmailClaims, identity: :user_id, identity_prefix: "user-"
      end

  The params are `to`, `identity`, `identity_prefix`, `stream_tag`, `timeout` and
  `consistency`, as on `dispatch/2`, and `identify/2` works on the decision module as it does
  on an aggregate. The events land on `<identity_prefix><identity>`, or on the members
  `c:Decree.Decision.decide/2` names, under that prefix. Middleware, `returning:` and
  `consistency:` work as on an aggregate route. See `Decree.Decision`.

  ## Prefixes

  A decision's stream prefix must be declared in the event store's `conditional_appends`:
  the store then refuses anything but a decision's conditional append on those streams. So
  that it can, a route is refused when the router compiles, with an `ArgumentError`, for:

    - no identity, neither `identity:` nor an `identify` with `by:`: no dispatch could name
      a stream.
    - no prefix, or `""`: the store could not tell these streams from any other.
    - a function prefix: nothing can compare it at build time.
    - a function identity with no `stream_tag:`: it has no kind for the stream's entity tag.
    - an aggregate route's or an `identify`'s binary prefix overlapping the decision's, one
      being a prefix of the other: the aggregate's appends would be refused at run time.

  The same overlap is refused across the routers of a `Decree.Commands.CompositeRouter`, and
  of a `Decree.Application`, which is one.

  An aggregate's function prefix and an aggregate's bare identity, with no prefix, cannot be
  checked at build time. The store's refusal holds them:
  `{:error, :plain_append_to_conditional_prefix}` when such a stream lands in a declared
  prefix.
  """
  defmacro decide(command_module_or_modules, opts) do
    register(command_module_or_modules, parse_decide_opts(opts))
  end

  defp register(command_module_or_modules, opts) do
    for command_module <- List.wrap(command_module_or_modules) do
      quote do
        if Enum.any?(@registered_commands, fn {command_module, _command_opts} ->
             command_module == unquote(command_module)
           end) do
          raise ArgumentError,
            message:
              "Command `#{inspect(unquote(command_module))}` has already been registered in router `#{inspect(__MODULE__)}`"
        end

        command_opts = Keyword.merge(@default_dispatch_opts, unquote(opts))

        Router.validate_decree_command!(unquote(command_module), @require_decree_command)
        Router.validate_stream_tag!(unquote(command_module), command_opts)
        Router.validate_rule!(unquote(command_module), command_opts)
        Router.validate_opaque!(unquote(command_module), command_opts)

        @registered_commands {unquote(command_module), command_opts}
      end
    end
  end

  @type dispatch_resp ::
          :ok
          | {:ok, aggregate_state :: struct()}
          | {:ok, aggregate_version :: non_neg_integer()}
          | {:ok, execution_result :: ExecutionResult.t()}
          | {:error, :unregistered_command}
          | {:error, :consistency_timeout}
          | {:error, reason :: term()}

  @doc """
  Dispatch the given command to the registered handler.

  Returns `:ok` on success, or `{:error, reason}` on failure.

  ## Example

      command = %OpenAccount{
        command_id: command_id,
        account_number: account_number,
        initial_balance: 1_000
      }

      :ok = BankRouter.dispatch(command)

  """
  @callback dispatch(command :: struct()) :: dispatch_resp()

  @doc """
  Dispatch the given command to the registered handler providing a timeout.

    - `command` is a command struct which must be registered with the router.

    - `timeout_or_opts` is either an integer timeout, `:infinity`, or a keyword
      list of options.

      The timeout must be an integer greater than zero which specifies how many
      milliseconds to allow the command to be handled, or the atom `:infinity`
      to wait indefinitely. The default timeout value is five seconds.

      Alternatively, an options keyword list can be provided with the following
      options.

      Options:

        - `causation_id` - an optional UUID used to identify the cause of the
          command being dispatched.

        - `command_uuid` - an optional UUID used to identify the command being
          dispatched.

        - `correlation_id` - an optional UUID used to correlate related
          commands/events together.

        - `consistency` - one of `:eventual` (default) or `:strong`. By
          setting the consistency to `:strong` a successful command dispatch
          will block until all strongly consistent event handlers and process
          managers have handled all events created by the command.

        - `metadata` - an optional map containing key/value pairs comprising
          the metadata to be associated with all events created by the
          command.

        - `returning` - to choose what response is returned from a successful
            command dispatch. The default is to return an `:ok`.

            The available options are:

            - `:aggregate_state` - to return the updated aggregate state in the
              successful response: `{:ok, aggregate_state}`.

            - `:aggregate_version` - to include the aggregate stream version
              in the successful response: `{:ok, aggregate_version}`.

            - `:events` - to return the resultant domain events. An empty list
              will be returned if no events were produced.

            - `:execution_result` - to return a `Decree.Commands.ExecutionResult`
              struct containing the aggregate's identity, version, and any
              events produced from the command along with their associated
              metadata.

            - `false` - don't return anything except an `:ok`.

        - `timeout` - as described above.

  Returns `:ok` on success unless the `:returning` option is specified where
  it returns one of `{:ok, aggregate_state}`, `{:ok, aggregate_version}`, or
  `{:ok, %Decree.Commands.ExecutionResult{}}`.

  Returns `{:error, reason}` on failure.

  ## Example

      command = %OpenAccount{
        command_id: command_id,
        account_number: account_number,
        initial_balance: 1_000
      }

      :ok = BankRouter.dispatch(command, consistency: :strong, timeout: 30_000)

  """
  @callback dispatch(
              command :: struct(),
              timeout_or_opts :: non_neg_integer() | :infinity | Keyword.t()
            ) :: dispatch_resp()

  defmacro __before_compile__(_env) do
    quote generated: true do
      @doc false
      def __registered_commands__ do
        Enum.map(@registered_commands, fn {command_module, _command_opts} -> command_module end)
      end

      @stream_prefixes Router.stream_prefixes!(
                         __MODULE__,
                         @registered_commands,
                         @registered_identities
                       )

      @doc false
      def __stream_prefixes__, do: @stream_prefixes

      @conditional_targets Router.conditional_targets(@registered_commands)

      Router.validate_opaque_routes!(__MODULE__, @registered_commands, @registered_identities)

      @doc false
      def __conditional_targets__, do: @conditional_targets

      @doc false
      def dispatch(command, opts \\ [])

      @doc false
      def dispatch(command, :infinity),
        do: do_dispatch(command, timeout: :infinity)

      @doc false
      def dispatch(command, timeout) when is_integer(timeout),
        do: do_dispatch(command, timeout: timeout)

      @doc false
      def dispatch(command, opts),
        do: do_dispatch(command, opts)

      @middleware Enum.reduce(@registered_middleware, @default_middleware, fn middleware, acc ->
                    [middleware | acc]
                  end)

      for {command_module, command_opts} <- @registered_commands do
        @aggregate Keyword.fetch!(command_opts, :aggregate)
        @decision Keyword.get(command_opts, :decision)
        @identified_by @decision || @aggregate
        @handler Keyword.fetch!(command_opts, :to)
        @function Keyword.fetch!(command_opts, :function)
        @before_execute Keyword.get(command_opts, :before_execute)
        @lifespan Keyword.get(command_opts, :lifespan)
        @identity Keyword.get(command_opts, :identity)
        @identity_prefix Keyword.get(command_opts, :identity_prefix)
        @identity_opaque Keyword.get(command_opts, :opaque, false)
        @stream_tag Keyword.get(command_opts, :stream_tag)
        @rule Keyword.get(command_opts, :rule)

        @command_module command_module
        @command_opts command_opts

        defp do_dispatch(%@command_module{} = command, opts) do
          alias Decree.Commands.Dispatcher
          alias Decree.Commands.Dispatcher.Payload

          opts = Keyword.merge(@command_opts, opts)

          application = Keyword.fetch!(opts, :application)
          causation_id = Keyword.get(opts, :causation_id)
          command_uuid = Keyword.get_lazy(opts, :command_uuid, &UUID.uuid4/0)
          consistency = Keyword.fetch!(opts, :consistency)
          correlation_id = Keyword.get_lazy(opts, :correlation_id, &UUID.uuid4/0)
          metadata = Keyword.fetch!(opts, :metadata) |> validate_metadata()

          retry_attempts = Keyword.get(opts, :retry_attempts)
          timeout = Keyword.fetch!(opts, :timeout)

          returning =
            cond do
              Keyword.get(opts, :include_execution_result) == true ->
                :execution_result

              Keyword.get(opts, :include_aggregate_version) == true ->
                :aggregate_version

              (returning = Keyword.get(opts, :returning)) in [
                :aggregate_state,
                :aggregate_version,
                :events,
                :execution_result,
                false
              ] ->
                returning

              true ->
                false
            end

          {identity, identity_prefix, identity_opaque} =
            case Map.get(@registered_identities, @identified_by) do
              nil ->
                {@identity, @identity_prefix, @identity_opaque}

              config ->
                identity = Keyword.get(config, :by, @identity)
                prefix = Keyword.get(config, :prefix, @identity_prefix)
                opaque = Keyword.get(config, :opaque, @identity_opaque)

                {identity, prefix, opaque}
            end

          payload = %Payload{
            application: application,
            command: command,
            command_uuid: command_uuid,
            causation_id: causation_id,
            correlation_id: correlation_id,
            consistency: consistency,
            handler_module: @handler,
            handler_function: @function,
            handler_before_execute: @before_execute,
            aggregate_module: @aggregate,
            decision_module: @decision,
            identity: identity,
            identity_prefix: identity_prefix,
            identity_opaque: identity_opaque,
            stream_tag: @stream_tag,
            rule: @rule,
            returning: returning,
            timeout: timeout,
            lifespan: @lifespan,
            metadata: metadata,
            middleware: @middleware,
            retry_attempts: retry_attempts
          }

          Dispatcher.dispatch(payload)
        end
      end

      @doc false
      def __route__(command_module)

      for {command_module, command_opts} <- @registered_commands do
        @route_command command_module
        @route Router.route(command_opts, @registered_identities)

        def __route__(@route_command), do: @route
      end

      def __route__(_command_module), do: nil

      # Catch unregistered commands, log and return an error.
      defp do_dispatch(command, opts) do
        event_prefix = [:decree, :application, :dispatch]
        application = Keyword.fetch!(opts, :application)

        context = %Decree.Aggregates.ExecutionContext{
          command: command
        }

        telemetry_metadata = %{
          application: application,
          error: nil,
          execution_context: context
        }

        start_time = Telemetry.start(event_prefix, telemetry_metadata)

        Logger.error(fn ->
          "attempted to dispatch an unregistered command: " <> inspect(command)
        end)

        Telemetry.stop(
          event_prefix,
          start_time,
          Map.put(telemetry_metadata, :error, :unregistered_command)
        )

        {:error, :unregistered_command}
      end

      # Make sure the metadata must be Map.t()
      defp validate_metadata(value) when is_map(value), do: value
      defp validate_metadata(_), do: raise(ArgumentError, message: "metadata must be an map")
    end
  end

  # Checked on the evaluated value, here and not in `parse_opts/1`, which sees the option's
  # AST. A boolean is an atom, and `stream_tag: false` must not store a `false:` kind. A
  # kind has no colon, and a module's name would store `Elixir.Some.Module:`. Not
  # naming a kind is the way to take the default: there is no opt-out value.
  @doc false
  def validate_stream_tag!(command_module, command_opts) do
    case Keyword.fetch(command_opts, :stream_tag) do
      :error ->
        :ok

      {:ok, stream_tag} ->
        unless stream_tag_kind?(stream_tag) do
          raise ArgumentError,
                "#{inspect(command_module)} has an invalid `stream_tag` option: " <>
                  "#{inspect(stream_tag)}, expected an atom other than nil, true or false " <>
                  "whose name matches ^[a-z][a-z0-9_]*$"
        end

        :ok
    end
  end

  # Here and not at the route's first dispatch: the field is what carries the read to
  # `execute/2`, and a missing one would raise on every attempt.
  @doc false
  def validate_rule!(command_module, command_opts) do
    case Keyword.get(command_opts, :rule) do
      nil ->
        :ok

      rule ->
        unless is_atom(rule) and match?({:module, _}, Code.ensure_compiled(rule)) and
                 function_exported?(rule, :__rule__, 0) do
          raise ArgumentError,
                "#{inspect(command_module)} has an invalid `rule` option: #{inspect(rule)}, " <>
                  "expected a compiled module that uses Decree.Decision.Rule"
        end

        unless Map.has_key?(struct(command_module), :rule_result) do
          raise ArgumentError,
                "#{inspect(command_module)} is routed to the rule #{inspect(rule)} and has no " <>
                  "`:rule_result` field, which is where what the rule read reaches `execute/2`"
        end

        :ok
    end
  end

  @doc false
  def require_decree_command!(value) when is_boolean(value), do: value

  def require_decree_command!(value) do
    raise ArgumentError,
          "invalid `require_decree_command` option: #{inspect(value)}, expected true or false"
  end

  # Off until every command adopts `Decree.Command`. `Code.ensure_compiled/1` and not a load
  # test, as for a rule: under the compiler a command module may not be compiled yet.
  @doc false
  def validate_decree_command!(_command_module, false), do: :ok

  def validate_decree_command!(command_module, true) do
    unless match?({:module, _}, Code.ensure_compiled(command_module)) and
             Decree.Command.command?(command_module) do
      raise ArgumentError,
            "#{inspect(command_module)} is not a Decree.Command, which this router requires " <>
              "(`require_decree_command: true`): `use Decree.Command` in it"
    end

    :ok
  end

  # Checked on the evaluated value, as `stream_tag:` is.
  @doc false
  def validate_opaque!(command_module, command_opts) do
    case Keyword.fetch(command_opts, :opaque) do
      :error ->
        :ok

      {:ok, opaque} when is_boolean(opaque) ->
        :ok

      {:ok, invalid} ->
        raise ArgumentError,
              "#{inspect(command_module)} has an invalid `opaque` option: #{inspect(invalid)}, " <>
                "expected true or false"
    end
  end

  # Here and not in the macro, because an `identify` may follow its `dispatch`: the
  # `identify` owns the identity, so a route cannot say it is opaque, nor that it is not.
  @doc false
  def validate_opaque_routes!(router, registered_commands, registered_identities) do
    for {command, opts} <- registered_commands,
        Keyword.has_key?(opts, :opaque),
        aggregate = opts[:aggregate],
        Map.has_key?(registered_identities, aggregate) do
      raise ArgumentError,
            "#{inspect(command)} in #{inspect(router)} declares `opaque:` on its route to " <>
              "#{inspect(aggregate)}, which has an `identify`: declare `opaque:` on the " <>
              "`identify`, which owns the identity"
    end

    :ok
  end

  # A command's route as its dispatch resolves it, `identify` included, for what reads a route
  # outside a dispatch: the application's start check, `Decree.Aggregate.check_command_window/2`
  # and `mix decree.verify.eventless`, which reads `handler:` and `function:`, the module and
  # function the dispatch calls (`decide/2` on a decision). Atoms and the route's own identity
  # function only, so no aggregate module is called here.
  @doc false
  def route(command_opts, registered_identities) do
    identified_by = command_opts[:decision] || command_opts[:aggregate]
    opaque = Keyword.get(command_opts, :opaque, false)

    {identity, identity_prefix, identity_opaque} =
      case Map.get(registered_identities, identified_by) do
        nil ->
          {command_opts[:identity], command_opts[:identity_prefix], opaque}

        config ->
          {Keyword.get(config, :by, command_opts[:identity]),
           Keyword.get(config, :prefix, command_opts[:identity_prefix]),
           Keyword.get(config, :opaque, opaque)}
      end

    [
      aggregate: command_opts[:aggregate],
      decision: command_opts[:decision],
      identity: identity,
      identity_prefix: identity_prefix,
      identity_opaque: identity_opaque,
      stream_tag: command_opts[:stream_tag],
      rule: command_opts[:rule],
      handler: command_opts[:to],
      function: command_opts[:function]
    ]
  end

  # What a condition on this router's rule routes names as its target.
  @doc false
  def conditional_targets(registered_commands) do
    registered_commands
    |> Enum.filter(fn {_command, opts} -> opts[:rule] end)
    |> Enum.map(fn {_command, opts} -> opts[:aggregate] end)
    |> Enum.uniq()
  end

  @doc false
  def stream_tag_kind?(kind) when kind in [nil, true, false], do: false

  # `\A` and `\z`, because `$` also matches before a trailing newline.
  def stream_tag_kind?(kind) when is_atom(kind) do
    Regex.match?(~r/\A[a-z][a-z0-9_]*\z/, Atom.to_string(kind))
  end

  def stream_tag_kind?(_kind), do: false

  # The router's prefixes, each route's as its dispatch will resolve it, once `identify` has
  # been seen. Refuses what `decide/2` lists, here and not in the macro because an `identify`
  # may follow a `decide`.
  @doc false
  def stream_prefixes!(router, registered_commands, registered_identities) do
    decisions =
      for {command, opts} <- registered_commands, decision = opts[:decision] do
        {identity, prefix} = route_identity(registered_identities, decision, opts)
        opaque = registered_identities |> Map.get(decision, []) |> Keyword.get(:opaque, false)

        validate_decision_route!(router, command, decision, identity, prefix, opaque, opts)

        {decision, prefix}
      end

    decision_modules = Enum.map(decisions, &elem(&1, 0))

    routes =
      for {_command, opts} <- registered_commands, not Keyword.has_key?(opts, :decision) do
        registered_identities |> route_identity(opts[:aggregate], opts) |> elem(1)
      end

    identified =
      for {module, config} <- registered_identities, module not in decision_modules do
        Keyword.get(config, :prefix)
      end

    prefixes = %{
      decision: decisions |> Enum.map(&elem(&1, 1)) |> Enum.uniq(),
      other: (routes ++ identified) |> Enum.filter(&binary_prefix?/1) |> Enum.uniq()
    }

    merge_stream_prefixes!(%{decision: [], other: []}, prefixes, router)
  end

  # Merges one router's prefixes into a composite's. Raises when a decision's overlaps
  # another's prefix, one being a prefix of the other, whichever router holds which.
  @doc false
  def merge_stream_prefixes!(known, added, router) do
    merged = %{
      decision: Enum.uniq(known.decision ++ added.decision),
      other: Enum.uniq(known.other ++ added.other)
    }

    with {decision, other} <- overlap(merged) do
      raise ArgumentError,
            "#{inspect(router)} has an aggregate prefix #{inspect(other)} overlapping the " <>
              "decision prefix #{inspect(decision)}: the store refuses a plain append under " <>
              "a decision's prefix"
    end

    merged
  end

  defp overlap(%{decision: decisions, other: others}) do
    Enum.find_value(decisions, fn decision ->
      other =
        Enum.find(others, fn other ->
          String.starts_with?(decision, other) or String.starts_with?(other, decision)
        end)

      if other, do: {decision, other}
    end)
  end

  defp route_identity(registered_identities, module, opts) do
    case Map.get(registered_identities, module) do
      nil -> {opts[:identity], opts[:identity_prefix]}
      config -> {Keyword.get(config, :by, opts[:identity]), Keyword.get(config, :prefix)}
    end
  end

  defp validate_decision_route!(router, command, decision, identity, prefix, opaque, opts) do
    named = "#{inspect(command)} decided by #{inspect(decision)} in #{inspect(router)}"

    cond do
      is_nil(identity) ->
        raise ArgumentError, "#{named} has no identity: name `identity:` or `identify ... by:`"

      opaque ->
        raise ArgumentError,
              "#{named} has an `identify` with `opaque: true`: a decision's identity is its " <>
                "stream's entity tag, so it must be a canonical UUID"

      is_function(prefix) ->
        raise ArgumentError, "#{named} has a function prefix, which cannot be checked at build"

      not binary_prefix?(prefix) ->
        raise ArgumentError, "#{named} has no stream prefix"

      is_function(identity) and is_nil(opts[:stream_tag]) ->
        raise ArgumentError, "#{named} has a function identity with no `stream_tag:`"

      true ->
        :ok
    end
  end

  defp binary_prefix?(prefix), do: is_binary(prefix) and prefix != ""

  @doc false
  def get_opt(opts, name, default \\ nil) do
    Keyword.get(opts, name) || Application.get_env(:decree, name) || default
  end

  @doc false
  def get_default_dispatch_return(opts) do
    cond do
      (default_dispatch_return = get_opt(opts, :default_dispatch_return)) in [
        :aggregate_state,
        :aggregate_version,
        :events,
        :execution_result,
        false
      ] ->
        default_dispatch_return

      get_opt(opts, :include_execution_result) == true ->
        :execution_result

      get_opt(opts, :include_aggregate_version) == true ->
        :aggregate_version

      true ->
        false
    end
  end

  @register_params [
    :to,
    :function,
    :before_execute,
    :aggregate,
    :identity,
    :identity_prefix,
    :stream_tag,
    :rule,
    :opaque,
    :timeout,
    :lifespan,
    :consistency
  ]

  @decide_params [:to, :identity, :identity_prefix, :stream_tag, :timeout, :consistency]

  defp parse_decide_opts(opts) do
    with {unknown_param, _} <- Enum.find(opts, fn {param, _} -> param not in @decide_params end) do
      raise """
      unexpected decide parameter "#{unknown_param}"
      available params are: #{Enum.map_join(@decide_params, ", ", &to_string/1)}
      """
    end

    case Keyword.fetch(opts, :to) do
      {:ok, to} -> [decision: to, aggregate: nil, function: :decide] ++ opts
      :error -> raise "decide missing required parameter: :to"
    end
  end

  defp parse_opts(opts) do
    {to, opts} = Keyword.pop(opts, :to)
    {aggregate, opts} = Keyword.pop(opts, :aggregate)
    {function, opts} = Keyword.pop(opts, :function)

    with {unknown_param, _} <- Enum.find(opts, fn {param, _} -> param not in @register_params end) do
      raise """
      unexpected dispatch parameter "#{unknown_param}"
      available params are: #{Enum.map_join(@register_params, ", ", &to_string/1)}
      """
    end

    case {to, aggregate} do
      {nil, _aggregate} ->
        raise "dispatch missing required parameter: :to"

      {to, nil} ->
        function = function || :execute
        [function: function, to: to, aggregate: to] ++ opts

      {to, aggregate} ->
        function = function || :handle
        [function: function, to: to, aggregate: aggregate] ++ opts
    end
  end
end
