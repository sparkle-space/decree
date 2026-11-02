defmodule Decree.Event.Handler do
  use GenServer
  use Decree.Registration
  use TelemetryRegistry

  telemetry_event(%{
    event: [:decree, :event, :handle, :start],
    description: "Emitted when an event handler starts handling an event",
    measurements: "%{system_time: integer()}",
    metadata: """
    %{application: Decree.Application.t(),
      context: map(),
      handler_name: String.t(),
      handler_module: atom(),
      handler_state: map(),
      recorded_event: RecordedEvent.t()}
    """
  })

  telemetry_event(%{
    event: [:decree, :event, :handle, :stop],
    description: "Emitted when an event handler stops handling an event",
    measurements: "%{duration: non_neg_integer()}",
    metadata: """
    %{:application => Decree.Application.t(),
      :context => map(),
      :handler_name => String.t(),
      :handler_module => atom(),
      :handler_state => map(),
      :recorded_event => RecordedEvent.t(),
      optional(:error) => any()}
    """
  })

  telemetry_event(%{
    event: [:decree, :event, :handle, :exception],
    description: "Emitted when an event handler raises an exception",
    measurements: "%{duration: non_neg_integer()}",
    metadata: """
    %{application: Decree.Application.t(),
      context: map(),
      handler_name: String.t(),
      handler_module: atom(),
      handler_state: map(),
      recorded_event: RecordedEvent.t(),
      kind: :throw | :error | :exit,
      reason: any(),
      stacktrace: list()}
    """
  })

  telemetry_event(%{
    event: [:decree, :event, :batch, :start],
    description: "Emitted when an event handler starts handling a batch of events",
    measurements: "%{system_time: integer()}",
    metadata: """
    %{application: Decree.Application.t(),
      context: map(),
      handler_name: String.t(),
      handler_module: atom(),
      handler_state: map(),
      first_event_id: binary(),
      last_event_id: binary(),
      event_count: integer(),
      flush_reason: :size | :timeout | :immediate}
    """
  })

  telemetry_event(%{
    event: [:decree, :event, :batch, :stop],
    description: "Emitted when an event handler stops handling a batch of events",
    measurements: "%{duration: non_neg_integer()}",
    metadata: """
    %{application: Decree.Application.t(),
      context: map(),
      handler_name: String.t(),
      handler_module: atom(),
      handler_state: map(),
      first_event_id: binary(),
      last_event_id: binary(),
      event_count: integer(),
      recorded_event: RecordedEvent.t() | nil,
      flush_reason: :size | :timeout | :immediate,
      optional(:error) => any()}
    """
  })

  telemetry_event(%{
    event: [:decree, :event, :batch, :exception],
    description: "Emitted when an event batch handler raises an exception",
    measurements: "%{duration: non_neg_integer()}",
    metadata: """
    %{application: Decree.Application.t(),
      context: map(),
      handler_name: String.t(),
      handler_module: atom(),
      handler_state: map(),
      recorded_event: => RecordedEvent.t() | nil,
      first_event_id: binary(),
      last_event_id: binary(),
      event_count: integer(),
      flush_reason: :size | :timeout | :immediate,
      kind: :throw | :error | :exit,
      reason: any(),
      optional(:stacktrace) => list()}
    """
  })

  @moduledoc """
  Defines the behaviour an event handler must implement and
  provides a convenience macro that implements the behaviour, allowing you to
  handle only the events you are interested in processing.

  You should start your event handlers using a `Supervisor` to
  ensure they are restarted on error.

  ### Example

      defmodule ExampleHandler do
        use Decree.Event.Handler,
          application: ExampleApp,
          name: "ExampleHandler"

        def handle(%AnEvent{..}, _metadata) do
          # ... process the event
          :ok
        end
      end

  Start your event handler process (or use a `Supervisor`):

      {:ok, _handler} = ExampleHandler.start_link()

  ## Event handler name

  The name you specify is used when subscribing to the event store. You must use
  a unique name for each event handler you start. Also, you
  *should not* change the name once the handler has been deployed. A new
  subscription will be created if you change the name and the event handler will
  receive already handled events.

  You can use the module name of your event handler using the `__MODULE__`
  special form:

      defmodule ExampleHandler do
        use Decree.Event.Handler,
          application: ExampleApp,
          name: __MODULE__
      end

  ## Subscription options

  You can choose to start the event handler's event store subscription from
  `:origin`, `:current` position, or an exact event number using the
  `start_from` option. The default is to use the origin so your handler will
  receive *all* events.

  Use the `:current` position when you don't want newly created event handlers
  to go through all previous events. An example would be adding an event handler
  to send transactional emails to an already deployed system containing many
  historical events.

  The `start_from` option *only applies* when the subscription is initially
  created, the first time the handler starts. Whenever the handler restarts the
  subscription will resume from the next event after the last successfully
  processed event. Restarting an event handler does not restart its
  subscription.

  ### Example

  Set the `start_from` option (`:origin`, `:current`, or an explicit event
  number) when using `Decree.Event.Handler`:

      defmodule ExampleHandler do
        use Decree.Event.Handler,
          application: ExampleApp,
          name: "ExampleHandler",
          start_from: :origin
      end

  You can optionally override `:start_from` by passing it as option when
  starting your handler:

      {:ok, _handler} = ExampleHandler.start_link(start_from: :current)

  ### Subscribing to an individual stream

  By default event handlers will subscribe to all events appended to any stream.
  Provide a `subscribe_to` option to subscribe to a single stream.

      defmodule ExampleHandler do
        use Decree.Event.Handler,
          application: ExampleApp,
          name: __MODULE__,
          subscribe_to: "stream1234"
      end

  This will ensure the handler only receives events appended to that stream.

  ## Runtime event handler configuration

  Runtime options can be provided to the event handler's `start_link/1` function
  or its child spec. The `c:init/1` callback function can also be used to define
  runtime configuration.

  ### Example

  Provide runtime configuration to `start_link/1`:

      {:ok, _pid} =
        ExampleHandler.start_link(
          application: ExampleApp,
          name: "ExampleHandler"
        )

  Or when supervised:

      Supervisor.start_link([
        {ExampleHandler, application: ExampleApp, name: "ExampleHandler"}
      ], strategy: :one_for_one)

  ## Event handler state

  An event handler can define and update state which is held in the `GenServer`
  process memory. It is passed to the `handle/2` (or handle_batch/1) function as part of the
  metadata using the `:state` key. The state is transient and will be lost
  whenever the process restarts.

  Initial state can be set in the `init/1` callback function by adding a
  `:state` key to the config. It can also be provided when starting the handler
  process:

      ExampleHandler.start_link(state: initial_state)

  Or when supervised:

      Supervisor.start_link([
        {ExampleHandler, state: initial_state}
      ], strategy: :one_for_one)

  State can be updated by returning `{:ok, new_state}` from any `handle/2`
  (or `handle_batch/2`) function. Returning an `:ok` reply will keep the state unchanged.

  Handler state is also included in the `Decree.Event.FailureContext` struct
  passed to the `error/3` callback function.

  ### Example

      defmodule StatefulEventHandler do
        use Decree.Event.Handler,
          application: ExampleApp,
          name: __MODULE__

        def init(config) do
          config = Keyword.put_new(config, :state, %{})

          {:ok, config}
        end

        def handle(event, metadata) do
          %{state: state} = metadata

          new_state = mutate_state(state)

          {:ok, new_state}
        end
      end

  ## Concurrency

  An event handler may be configured to start multiple processes to handle the
  events concurrently. By default one process will be started, processing events
  one at a time in order. The `:concurrency` option determines how many event
  handler processes are started. It must be a positive integer.

  Note with concurrent processing events will likely be processed out of order.
  If you need to enforce an order, such as per stream or by using a field from
  an event, you can define a `c:partition_by/2` callback function in the event
  handler module. The function will receive each event and its metadata and must
  return a consistent term indicating the event's partition. Events which return
  the same term are guaranteed to be processed in order by the same event
  handler instance. While events with different partitions may be processed
  concurrently by another instance. An attempt will be made to distribute
  events as evenly as possible to all running event handler instances.

  Only `:eventual` consistency is supported when multiple handler processes are
  configured with a `:concurrency` of greater than one.

  ### Example

      defmodule ConcurrentProcessingEventHandler do
        alias Decree.EventStore.RecordedEvent

        use Decree.Event.Handler,
          application: ExampleApp,
          name: __MODULE__,
          concurrency: 10

        def init(config) do
          # Fetch the index of this event handler instance (0..9 in this example)
          index = Keyword.fetch!(config, :index)

          {:ok, config}
        end

        def handle(event, metadata) do
          :ok
        end

        # Partition events by their stream
        def partition_by(event, metadata) do
          %{stream_id: stream_id} = metadata

          stream_id
        end
      end

  ## Consistency

  For each event handler you can define its consistency, as one of either
  `:strong` or `:eventual`.

  This setting is used when dispatching commands and specifying the
  `consistency` option.

  When you dispatch a command using `:strong` consistency, after successful
  command dispatch the process will block until all event handlers configured to
  use `:strong` consistency have processed the domain events created by the
  command. This is useful when you have a read model updated by an event handler
  that you wish to query for data affected by the command dispatch. With
  `:strong` consistency you are guaranteed that the read model will be
  up-to-date after the command has successfully dispatched. It can be safely
  queried for data updated by any of the events created by the command.

  The default setting is `:eventual` consistency. Command dispatch will return
  immediately upon confirmation of event persistence, not waiting for any event
  handlers.

  Note strong consistency does not imply a transaction covers the command
  dispatch and event handling. It only guarantees that the event handler will
  have processed all events produced by the command: if event handling fails
  the events will have still been persisted.

  ### Example

  Define an event handler with `:strong` consistency:

      defmodule ExampleHandler do
        use Decree.Event.Handler,
          application: ExampleApp,
          name: "ExampleHandler",
          consistency: :strong
      end

  ## Batching

  Event handlers can process events in batches rather than one at a time. This
  allows writing to a target system in a single transaction and acknowledging
  all events at once, reducing overhead.

  To enable batching, set the `batch_size` option and implement `handle_batch/1`
  instead of `handle/2`. The callback receives a list of `{event, metadata}`
  tuples. On returning `:ok`, all events in the batch are acknowledged. There
  is no partial acknowledgement.

  Batching and concurrency are not supported together; setting both will raise
  a compilation error.

  ### `batch_size`

  The `batch_size` option controls the Decree.Store subscription's in-flight
  buffer — how many events the subscription can deliver without waiting for
  acknowledgement. It is not an accumulation mechanism at the handler level.
  The handler processes each delivery from the subscription immediately.

  During live operation, when events arrive one at a time, `handle_batch/1`
  is called with a single event — regardless of the configured `batch_size`.
  Larger batches form naturally only when the subscription has multiple events
  queued, such as during catch-up replay, back-pressure recovery, or bulk
  appends.

  When `batch_timeout` is configured, `batch_size` also serves as the maximum
  size of the handler's internal buffer — once the buffer reaches `batch_size`
  events, it is flushed immediately regardless of the timeout.

  ### `batch_timeout`

  The `batch_timeout` option adds time-based buffering at the handler level.
  When set, events are accumulated in the handler process and flushed when
  either condition is met first:

  * `batch_size` events have accumulated, OR
  * `batch_timeout` milliseconds have elapsed since the first buffered event

  This enables use cases such as accumulating changes before writing them in
  one go to a projection, batching bulk database inserts, or reducing per-event
  overhead for side effects — even when events arrive individually during
  steady-state operation.

  `batch_timeout` accepts a positive integer (milliseconds) or `:infinity`
  (the default — no buffering, backwards compatible with previous behaviour).
  Requires `batch_size` to be configured.

  ### Example

      defmodule OrderProjector do
        use Decree.Event.Handler,
          application: ExampleApp,
          name: "OrderProjector",
          batch_size: 50,
          batch_timeout: 100

        def handle_batch(events) do
          # Receives up to 50 events, or whatever accumulated within 100ms
          :ok
        end
      end

  ### Telemetry

  Batch telemetry events include a `flush_reason` field:

  * `:size` — batch reached `batch_size`
  * `:timeout` — `batch_timeout` elapsed before the batch filled
  * `:immediate` — no `batch_timeout` configured; events processed as delivered

  ### Example

  Define an event handler with `:strong` consistency:

      defmodule ExampleHandler do
        use Decree.Event.Handler,
          application: ExampleApp,
          name: "ExampleHandler",
          consistency: :strong
      end

  ## Dynamic application

  An event handler's application can be provided as an option to `start_link/1`.
  This can be used to start the same handler multiple times, each using a
  separate Decree application and event store.

  ### Example

  Start an event handler process for each tenant in a multi-tenanted app,
  guaranteeing that the data and processing remains isolated between tenants.

      for tenant <- [:tenant1, :tenant2, :tenant3] do
        {:ok, _app} = MyApp.Application.start_link(name: tenant)
        {:ok, _handler} = ExampleHandler.start_link(application: tenant)
      end

  Typically you would start the event handlers using a supervisor:

      children =
        for tenant <- [:tenant1, :tenant2, :tenant3] do
          {ExampleHandler, application: tenant}
        end

      Supervisor.start_link(children, strategy: :one_for_one)

  The above example requires three named Decree applications to have already
  been started.

  ## Personal fields

  An event can declare personal fields (`Decree.PersonalFields`), which are stored encrypted. A
  handler receives them in plaintext for each event it names in a head of `handle/2`, and
  `Decree.Projections.Ecto`'s `project` names one. The set is derived from the heads at compile
  time and there is no list to keep: `__personal_fields__/0` answers it. Every other event
  reaches the handler's catch-all as it is stored, so one lost key does not stall a handler
  over an event it ignores.

  A head is read when its first argument is a struct, descending through `=`. A trailing
  clause ignoring both arguments, with `_`-prefixed variables and no guard, is the one
  catch-all. Any other head can't be read: a bound first argument, `%module{}`, a map, an ignored
  event beside a bound or matched metadata argument, a guarded or non-trailing catch-all. So
  can't `handle_batch/1`, which gets no decrypt.

  A module with such a head names nothing and sees stored values. If it reads no personal
  field it says so, and is then never decrypted for:

      use Decree.Event.Handler,
        application: ExampleApp,
        name: __MODULE__,
        personal_fields: :opaque

  `:opaque` is the only value `:personal_fields` takes, and anything else is a `CompileError`.

  With `config :decree, enforce_personal_fields: true` a head that can't be read and isn't
  `:opaque` is a `CompileError` naming the module and the head's line. This flag is temporary:
  it lets an application compile at this version before it has adopted, and once sparklespace
  has, strict becomes the default and the flag is deleted.

  ### When a key can't be read

  `{:error, :key_unknown}`, from the decrypt or from `handle/2`, never reaches the handler's
  own `c:error/3`, whose answer could skip the event. It goes to the application's
  `on_event_handler_error`. `nil`, `:stop` and `:backoff` stop the handler, and a stop never
  acks. A module's `error/3` decides, and a `:skip` from it is turned into a stop, so nothing
  can skip an event at this seam. The event stays unacked until the key can be read.

  ### What stays out of logs

  A failure on an event decrypted for this handler is treated as carrying the value. A raise
  from `handle/2` becomes a `Decree.PersonalFields.RedactedError` with a scrubbed stacktrace
  before anything sees it, `c:error/3` included. Decree's own log lines, telemetry and stop
  reasons show an `{:error, reason}` return reduced by
  `Decree.PersonalFields.RedactedError.reduce/1`. An exit leaves reduced the same way, and a
  throw leaves as the exit `{:nocatch, reduced}`, so the handler stops: thrown on, a reduced
  `{:noreply, _}` would be read as the callback's return and keep the handler running on a state
  of `:redacted`. A handler must not put a declared value in
  an error reason, because decree reduces it in its own log and telemetry but hands it to
  `c:error/3` as it stands. A handler must not keep a declared value in its state either,
  because decree hides it in its own output and hands it to the handler as it stands: once a
  decrypted event's `handle/2` answers `{:ok, state}`, telemetry and `format_status/1` show
  `:redacted`, and every later failure of that handler is treated as carrying the value.

  The `RecordedEvent` is never rewritten, so a failed event logged, passed to `c:error/3` or
  carried by telemetry holds ciphertext, as does `partition_by/2`'s.

  ## Telemetry

  #{telemetry_docs()}

  """

  require Logger

  alias Decree.Event.ErrorHandler
  alias Decree.Event.FailureContext
  alias Decree.Event.Handler
  alias Decree.Event.PersonalFields, as: PersonalFieldsProtocol
  alias Decree.Event.Upcast
  alias Decree.EventStore.RecordedEvent
  alias Decree.EventStore.Subscription
  alias Decree.PersonalFields
  alias Decree.PersonalFields.RedactedError
  alias Decree.Subscriptions
  alias Decree.Telemetry

  @type domain_event :: struct()
  @type metadata :: map()
  @type subscribe_from :: :origin | :current | non_neg_integer()
  @type consistency :: :eventual | :strong

  @doc deprecated: "Use the after_start/1 callback instead."
  @callback init() :: :ok | {:stop, reason :: any()}

  @doc """
  Optional initialisation callback function called when the handler starts.

  Can be used to start any related processes when the event handler is started.

  This callback function must return `:ok`, `{:ok, state}` to return new state,
  or `{:stop, reason}` to stop the handler process. Any other return value
  will terminate the event handler with an error.

  ### Example

      defmodule ExampleHandler do
        use Decree.Event.Handler,
          application: ExampleApp,
          name: "ExampleHandler"

        # Optional initialisation
        def after_start(handler_state) do
          new_handler_state = Map.put(handler_state, :foo, "bar")
          {:ok, new_handler_state}
        end

        def handle(%AnEvent{..}, _metadata) do
          # Process the event ...
          :ok
        end
      end

  """
  @callback after_start(handler_state :: term()) ::
              :ok | {:ok, state :: map()} | {:stop, reason :: any()}

  @doc """
  Optional callback function called to configure the handler before it starts.

  It is passed the merged compile-time and runtime config, and must return the
  updated config as `{:ok, config}`.

  Note this function is called before the event handler process is started and
  *is not* run from the handler's process. You cannot use `self()` to access the
  handler's PID.

  ### Example

  The `c:init/1` function is used to define the handler's application and name
  based upon a value provided at runtime:

      defmodule ExampleHandler do
        use Decree.Event.Handler

        def init(config) do
          {tenant, config} = Keyword.pop!(config, :tenant)

          config =
            config
            |> Keyword.put(:application, Module.concat([ExampleApp, tenant]))
            |> Keyword.put(:name, Module.concat([__MODULE__, tenant]))

          {:ok, config}
        end
      end

  Usage:

      {:ok, _pid} = ExampleHandler.start_link(tenant: :tenant1)

  """
  @callback init(config :: Keyword.t()) :: {:ok, Keyword.t()}

  @doc """
  Handle a domain event and its metadata.

  Return `:ok` on success, `{:error, :already_seen_event}` to ack and skip the
  event, or `{:error, reason}` on failure.
  """
  @callback handle(domain_event, metadata) ::
              :ok
              | {:ok, new_state :: any()}
              | {:error, :already_seen_event}
              | {:error, reason :: any()}

  @doc """
  Handle a batch of domain events and their metadata.

  Return `:ok` on success. All events in the batch will be acknowledged.

  On error, you can either return `{:error, reason}` to indicate something
  with the whole batch went wrong

  Note that this interface may change as more experience with use cases
  for batching is gained.
  """
  @callback handle_batch([{domain_event, metadata}]) ::
              :ok
              | {:ok, new_state :: any()}
              | {:error, reason :: any()}

  @doc """
  Called when an event `handle/2` callback returns an error.

  The `c:error/3` function allows you to control how event handling failures
  are handled. The function is passed the error returned by the event handler
  (e.g. `{:error, :failure}`), the event causing the error, and a context map
  containing state passed between retries.

  Use pattern matching on the error and/or failed event to explicitly handle
  certain errors or events. Use the context map to track any transient state you
  need to access between retried failures.

  You can return one of the following responses depending upon the
  error severity:

  - `{:retry, context}` - retry the failed event, provide a context
    map, or updated `Decree.Event.FailureContext` struct, containing any
    state to be passed to subsequent failures. This could be used to count the
    number of failures, stopping after too many.

  - `{:retry, delay, context}` - retry the failed event, after sleeping for
    the requested delay (in milliseconds). Context is a map or
    `Decree.Event.FailureContext` struct as described in `{:retry, context}`
    above.

  - `:skip` - skip the failed event by acknowledging receipt. In batching mode,
    this will acknowledge and skip _all_ events up to and including the failed
    event

  - `{:stop, reason}` - stop the event handler with the given reason.

  The default behaviour if you don't provide an `c:error/3` callback is to stop
  the event handler using the exact error reason returned from the `handle/2`
  function. If the event handler is supervised using restart `permanent` or
  `transient` stopping on error will cause the handler to be restarted. It will
  likely crash again as it will reprocess the problematic event. This can lead
  to cascading failures going up the supervision tree.

  ### Example error handling

      defmodule ExampleHandler do
        use Decree.Event.Handler,
          application: ExampleApp,
          name: __MODULE__

        require Logger

        alias Decree.Event.FailureContext

        def handle(%AnEvent{}, _metadata) do
          # simulate event handling failure
          {:error, :failed}
        end

        def error({:error, :failed}, %AnEvent{} = event, %FailureContext{context: context}) do
          context = record_failure(context)

          case Map.get(context, :failures) do
            too_many when too_many >= 3 ->
              # skip bad event after third failure
              Logger.warning("Skipping bad event, too many failures: " <> inspect(event))

              :skip

            _ ->
              # retry event, failure count is included in context map
              {:retry, context}
          end
        end

        defp record_failure(context) do
          Map.update(context, :failures, 1, fn failures -> failures + 1 end)
        end
      end

  """
  @callback error(
              error :: term(),
              failed_event :: domain_event | [domain_event] | nil,
              failure_context :: FailureContext.t()
            ) ::
              {:retry, context :: map() | FailureContext.t()}
              | {:retry, delay :: non_neg_integer(), context :: map() | FailureContext.t()}
              | :skip
              | {:stop, reason :: term()}

  @doc """
  Determine which partition an event belongs to.

  Only applicable when an event handler has been configured with more than one
  instance via the `:concurrency` option.
  """
  @callback partition_by(domain_event, metadata) :: any()

  @doc """
  Called before an event handler gets reset
  """
  @callback before_reset() :: :ok

  @optional_callbacks init: 0,
                      init: 1,
                      error: 3,
                      partition_by: 2,
                      before_reset: 0,
                      handle: 2,
                      handle_batch: 1

  defmacro __using__(using_opts) do
    quote location: :keep do
      @before_compile unquote(__MODULE__)
      @behaviour Handler

      # Evaluated here, not in the macro: `Decree.Projections.Ecto` passes its options as an
      # attribute of this module.
      @personal_fields_option Keyword.get(unquote(using_opts), :personal_fields)

      @doc """
      Start an event handler `GenServer` process linked to the current process.

      ## Options

        - `:application` - the Decree application.

        - `:name` - name of the event handler used to determine its unique event
          store subscription.

        - `:concurrency` - determines how many processes are started to
          concurrently process events. The default is one process.

        - `:consistency` - one of either `:eventual` (default) or `:strong`.

        - `:start_from` - where to start the event store subscription from when
          first created (default: `:origin`).

        - :subscribe_to - which stream to subscribe to can be either `:all` to
          subscribe to all events or a named stream (default: `:all`).

        - :batch_size - controls the Decree.Store subscription's in-flight buffer
          size. When `batch_timeout` is also set, this is the maximum number of
          events the handler buffers before flushing. Enables `handle_batch/1`.

        - :batch_timeout - maximum milliseconds to wait for events to accumulate
          in the handler buffer before flushing. Defaults to `:infinity` (no
          buffering; events processed immediately as delivered). Requires
          `:batch_size`.

        - :personal_fields - `:opaque` declares that this handler reads no personal
          field, so nothing is decrypted for it. Read at compile time, from `use` alone.

      The default options supported by `GenServer.start_link/3` are supported,
      including the `:hibernate_after` option which allows the process to go
      into hibernation after a period of inactivity.
      """
      def start_link(opts \\ []) do
        opts = Keyword.merge(unquote(using_opts), opts)

        {application, name, config} = Handler.parse_config!(__MODULE__, opts)

        Handler.start_link(application, name, __MODULE__, config)
      end

      @doc """
      Provides a child specification to allow the event handler to be easily
      supervised.

      Supports the same options as `start_link/3`.

      The default options supported by `GenServer.start_link/3` are also
      supported, including the `:hibernate_after` option which allows the
      process to go into hibernation after a period of inactivity.

      ### Example

          Supervisor.start_link([
            {ExampleHandler, []}
          ], strategy: :one_for_one)

      """
      def child_spec(opts) do
        opts = Keyword.merge(unquote(using_opts), opts)

        spec =
          case Keyword.get(opts, :concurrency, 1) do
            1 ->
              %{
                id: {__MODULE__, opts},
                start: {__MODULE__, :start_link, [opts]},
                restart: :permanent,
                type: :worker
              }

            concurrency when is_integer(concurrency) and concurrency > 1 ->
              opts = Keyword.put(opts, :module, __MODULE__)

              Handler.Supervisor.child_spec(opts)

            invalid ->
              raise ArgumentError,
                    "invalid `:concurrency` for event handler, expected a positive integer but got: " <>
                      inspect(invalid)
          end

        Supervisor.child_spec(spec, [])
      end

      @doc false
      def after_start(_state) do
        # TODO: remove this when we remove init/0
        if function_exported?(__MODULE__, :init, 0) do
          apply(__MODULE__, :init, [])
        else
          :ok
        end
      end

      @doc false
      def init(config), do: {:ok, config}

      @doc false
      def before_reset, do: :ok

      defoverridable init: 1, after_start: 1, before_reset: 0
    end
  end

  # GenServer start options
  @start_opts [:debug, :name, :timeout, :spawn_opt, :hibernate_after]

  # Event handler configuration options
  @handler_opts [
    :application,
    :name,
    :concurrency,
    :consistency,
    :index,
    :start_from,
    :subscribe_to,
    :subscription_opts,
    :state,
    :batch_size,
    :batch_timeout,
    :personal_fields
  ]

  @doc false
  def parse_config!(module, config) do
    {:ok, config} = module.init(config)

    {_valid, invalid} = Keyword.split(config, @start_opts ++ @handler_opts)

    if Enum.any?(invalid) do
      raise ArgumentError,
            inspect(module) <> " specifies invalid options: " <> inspect(Keyword.keys(invalid))
    end

    if Keyword.has_key?(config, :concurrency) and Keyword.has_key?(config, :batch_size) do
      raise ArgumentError,
            "both `:concurrency` and `:batch_size` are specified, this is not yet supported. Please choose one or the other."
    end

    # batch_timeout requires batch_size
    if Keyword.has_key?(config, :batch_timeout) and not Keyword.has_key?(config, :batch_size) do
      raise ArgumentError,
            inspect(module) <>
              " :batch_timeout requires :batch_size. Remove the timeout or configure batching."
    end

    # Compile time only: `__personal_fields__/0` holds what it decided.
    config = Keyword.delete(config, :personal_fields)

    {application, config} = Keyword.pop(config, :application)

    unless application do
      raise ArgumentError, inspect(module) <> " expects :application option"
    end

    {name, config} = Keyword.pop(config, :name)

    unless name = parse_name(name) do
      raise ArgumentError, inspect(module) <> " expects :name option"
    end

    {batch_size, config} = Keyword.pop(config, :batch_size)
    {batch_timeout, config} = Keyword.pop(config, :batch_timeout, :infinity)

    # Validate batch_size
    unless is_nil(batch_size) or (is_integer(batch_size) and batch_size > 0) do
      raise ArgumentError,
            inspect(module) <>
              " :batch_size must be nil or positive integer, got: " <>
              inspect(batch_size)
    end

    # Validate batch_timeout
    unless batch_timeout == :infinity or (is_integer(batch_timeout) and batch_timeout > 0) do
      raise ArgumentError,
            inspect(module) <>
              " :batch_timeout must be :infinity or positive integer, got: " <>
              inspect(batch_timeout)
    end

    config =
      case batch_size do
        nil ->
          # Delegate to `handle_event/2` when `batch_size` is not specified
          config
          |> Keyword.put(:handler_callback, :event)
          |> Keyword.put(:batch_size, nil)
          |> Keyword.put(:batch_timeout, :infinity)

        size when is_integer(size) and size > 0 ->
          config
          |> Keyword.update(:subscription_opts, [buffer_size: size], fn opts ->
            Keyword.put(opts, :buffer_size, size)
          end)
          # Delegate to `handle_batch/2` when `batch_size` is specified
          |> Keyword.put(:handler_callback, :batch)
          |> Keyword.put(:batch_size, size)
          |> Keyword.put(:batch_timeout, batch_timeout)
      end

    {application, name, config}
  end

  @doc false
  def parse_name(name) when name in [nil, ""], do: nil
  def parse_name(name) when is_binary(name), do: name
  def parse_name(name), do: inspect(name)

  @doc false
  defmacro __before_compile__(env) do
    defs = Module.definitions_in(env.module)
    has_one = Keyword.get(defs, :handle) == 2
    has_batch = Keyword.get(defs, :handle_batch) == 1

    if has_one and has_batch do
      raise CompileError,
        file: nil,
        line: nil,
        description: "#{env.module} has both `handle/2` and `handle_batch/1` callbacks."
    else
      # Read before the default handlers below are defined.
      personal_fields = personal_fields!(env, has_batch)

      # Generate default handlers
      quote generated: true do
        @doc false
        def __personal_fields__, do: unquote(Macro.escape(personal_fields))

        @doc false
        def handle(_event, _metadata), do: :ok

        @doc false
        def handle_batch(_events), do: :ok
      end
    end
  end

  defp personal_fields!(env, has_batch) do
    case Module.get_attribute(env.module, :personal_fields_option) do
      :opaque ->
        :opaque

      nil ->
        derive_personal_fields!(env, has_batch)

      invalid ->
        raise CompileError,
          file: env.file,
          line: env.line,
          description:
            "#{inspect(env.module)} sets `:personal_fields` to #{inspect(invalid)}, " <>
              "the only value it takes is `:opaque`"
    end
  end

  defp derive_personal_fields!(env, has_batch) do
    {modules, unreadable} = read_heads(definition_clauses(env.module, {:handle, 2}))

    unreadable =
      if has_batch do
        [{_meta, _args, _guards, _body} = clause | _rest] =
          definition_clauses(env.module, {:handle_batch, 1})

        [elem(clause, 0)[:line] | unreadable]
      else
        unreadable
      end

    if unreadable != [] and Application.compile_env(env, :decree, :enforce_personal_fields, false) do
      line = Enum.min(unreadable)

      raise CompileError,
        file: env.file,
        line: line,
        description:
          "#{inspect(env.module)} has a head at line #{line} that does not name its event, " <>
            "so it can't be known whether a personal field reaches it. Name the event " <>
            "in the head, or declare `personal_fields: :opaque` if it reads none"
    end

    Map.new(modules, &{&1, true})
  end

  defp definition_clauses(module, definition) do
    case Module.get_definition(module, definition) do
      nil -> []
      {:v1, _kind, _meta, clauses} -> clauses
    end
  end

  # Answers the struct modules the heads name, and the lines of the heads that can't be read.
  defp read_heads(clauses) do
    last = length(clauses) - 1

    clauses
    |> Enum.with_index()
    |> Enum.reduce({[], []}, fn {{meta, [event, metadata], guards, _body}, index},
                                {modules, unreadable} ->
      case struct_modules(event) do
        [_ | _] = named ->
          {named ++ modules, unreadable}

        [] ->
          if index == last and guards == [] and ignored?(event) and ignored?(metadata) do
            {modules, unreadable}
          else
            {modules, [meta[:line] | unreadable]}
          end
      end
    end)
  end

  defp struct_modules({:=, _meta, [left, right]}) do
    struct_modules(left) ++ struct_modules(right)
  end

  defp struct_modules({:%, _meta, [module, _map]}) when is_atom(module), do: [module]
  defp struct_modules(_pattern), do: []

  defp ignored?({name, _meta, context}) when is_atom(name) and is_atom(context) do
    name |> Atom.to_string() |> String.starts_with?("_")
  end

  defp ignored?(_pattern), do: false

  @doc false
  defstruct [
    :application,
    :consistency,
    :handler_callback,
    :handler_name,
    :handler_module,
    :handler_state,
    :last_seen_event,
    :subscription,
    :subscribe_timer,
    # The subscription process `after_start/1` last ran for: a subscription sends
    # `{:subscribed, subscription}` again when it takes back a lost advisory lock,
    # and `after_start/1` runs once per subscription (again after a reset).
    :started_subscription,
    # Batch timeout support
    :batch_size,
    :batch_timeout,
    :batch_timer_ref,
    :batch_buffer,
    # Set once a decrypted event's `handle/2` answers a state: from then on the state can hold
    # a decrypted value, and `handler_state` stays out of decree's own output.
    sensitive_state: false
  ]

  @doc false
  def start_link(application, handler_name, handler_module, opts \\ []) do
    {start_opts, handler_opts} = Keyword.split(opts, @start_opts)

    index = Keyword.get(handler_opts, :index)
    name = name(application, handler_name, index)
    consistency = consistency(handler_opts)
    subscription = new_subscription(application, handler_name, handler_module, handler_opts)

    handler = %Handler{
      application: application,
      handler_name: handler_name,
      handler_module: handler_module,
      handler_callback: Keyword.fetch!(handler_opts, :handler_callback),
      handler_state: Keyword.get(handler_opts, :state),
      consistency: consistency,
      subscription: subscription,
      batch_size: Keyword.get(handler_opts, :batch_size),
      batch_timeout: Keyword.get(handler_opts, :batch_timeout, :infinity),
      batch_timer_ref: nil,
      batch_buffer: []
    }

    with {:ok, pid} <- Registration.start_link(application, name, __MODULE__, handler, start_opts) do
      # Register the started event handler as a subscription with the given consistency
      :ok = Subscriptions.register(application, handler_name, handler_module, pid, consistency)

      {:ok, pid}
    end
  end

  @doc false
  def name(application, handler_name, index \\ nil)
  def name(application, handler_name, nil), do: {application, __MODULE__, handler_name}
  def name(application, handler_name, index), do: {application, __MODULE__, handler_name, index}

  @doc false
  @impl GenServer
  def init(%Handler{} = state) do
    Process.flag(:trap_exit, true)

    {:ok, state, {:continue, :subscribe_to_events}}
  end

  @impl GenServer
  def terminate(reason, state) do
    Logger.debug(describe(state) <> " is shutting down due to #{inspect(reason)}")
  end

  @doc false
  @impl GenServer
  def handle_continue(:subscribe_to_events, %Handler{} = state) do
    {:noreply, subscribe_to_events(state)}
  end

  @doc false
  @impl GenServer
  def handle_info(:reset, %Handler{} = state) do
    %Handler{handler_module: handler_module} = state

    case handler_module.before_reset() do
      :ok ->
        try do
          state = state |> reset_subscription() |> subscribe_to_events()

          {:noreply, state}
        catch
          {:error, reason} ->
            {:stop, reason, state}
        end

      {:stop, reason} ->
        Logger.debug(
          describe(state) <>
            " `before_reset/0` callback has requested to stop. (reason: #{inspect(reason)})"
        )

        {:stop, reason, state}
    end
  end

  @doc false
  @impl GenServer
  def handle_info(:subscribe_to_events, %Handler{} = state) do
    {:noreply, subscribe_to_events(state)}
  end

  @doc false
  # The subscription has taken back its lost advisory lock and resumes delivering
  # events; `after_start/1` already ran for it.
  @impl GenServer
  def handle_info(
        {:subscribed, subscription},
        %Handler{
          subscription: %Subscription{subscription_pid: subscription},
          started_subscription: subscription
        } = state
      ) do
    Logger.info(describe(state) <> " has resubscribed to event store after losing its lock")

    {:noreply, state}
  end

  @doc false
  # Subscription to event store has successfully subscribed, init event handler
  @impl GenServer
  def handle_info(
        {:subscribed, subscription},
        %Handler{subscription: %Subscription{subscription_pid: subscription}} = state
      ) do
    Logger.debug(describe(state) <> " has successfully subscribed to event store")

    state = %Handler{state | started_subscription: subscription}

    %Handler{handler_module: handler_module} = state

    if function_exported?(handler_module, :init, 0) do
      Logger.warning("#{inspect(handler_module)}.init/0 is deprecated, use after_start/1 instead")
    end

    case handler_module.after_start(state.handler_state) do
      :ok ->
        {:noreply, state}

      {:ok, %{} = new_handler_state} ->
        new_state = %{state | handler_state: new_handler_state}
        {:noreply, new_state}

      {:stop, reason} ->
        Logger.debug(describe(state) <> " `after_start/1` callback has requested to stop")

        {:stop, reason, state}
    end
  end

  @doc false
  # The subscription lost its advisory lock: another instance may now deliver the
  # same events. Events buffered for a batch are dropped unhandled; they were not
  # acknowledged, so they are delivered again from the last acknowledged event to
  # whichever instance holds the lock. The subscription sends `{:subscribed,
  # subscription}` once it takes the lock back.
  @impl GenServer
  def handle_info(
        {:subscription_lock_lost, subscription},
        %Handler{subscription: %Subscription{subscription_pid: subscription}} = state
      ) do
    Logger.warning(describe(state) <> " subscription lost its advisory lock, waiting to resume")

    state = cancel_batch_timer(state)

    {:noreply, %Handler{state | batch_buffer: []}}
  end

  # From a subscription process the handler no longer uses (one replaced by a reset).
  @impl GenServer
  def handle_info({:subscription_lock_lost, _subscription}, state) do
    {:noreply, state}
  end

  @doc false
  @impl GenServer
  def handle_info({:events, events}, state) do
    %Handler{
      application: application,
      handler_callback: callback,
      batch_timeout: batch_timeout
    } = state

    Logger.debug(describe(state) <> " received #{length(events)} event(s)")

    # Upcast events once before any processing
    events = Upcast.upcast_event_stream(events, additional_metadata: %{application: application})

    try do
      state =
        case {callback, batch_timeout} do
          {:event, _} ->
            # Non-batched: process immediately
            Enum.reduce(events, state, &handle_event/2)

          {:batch, timeout} when timeout in [:infinity, nil] ->
            # Batched without timeout: process immediately (Decree.Store handles batching)
            handle_batch(events, %{flush_reason: :immediate}, state)

          {:batch, _timeout} ->
            # Batched with timeout: buffer events and flush on size or timeout
            buffer_and_maybe_flush(events, state)
        end

      {:noreply, state}
    catch
      {:error, reason} ->
        # Stop after event handling returned an error
        {:stop, reason, state}
    end
  end

  @doc false
  @impl GenServer
  def handle_info(:flush_batch_timeout, state) do
    %Handler{batch_buffer: buffer} = state

    Logger.debug(describe(state) <> " flushing batch due to timeout: #{length(buffer)} event(s)")

    try do
      state = flush_batch_buffer(state, :timeout)
      {:noreply, state}
    catch
      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  @doc false
  @impl GenServer
  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %Handler{subscription: %Subscription{subscription_ref: ref}} = state
      ) do
    Logger.debug(describe(state) <> " subscription DOWN due to: #{inspect(reason)}")

    # Stop event handler when event store subscription process terminates.
    {:stop, reason, state}
  end

  @doc false
  @impl GenServer
  def handle_info({:EXIT, _pid, :normal}, state) do
    # linked process exited normally, don't shutdown
    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:EXIT, _pid, reason}, state) do
    {:stop, reason, state}
  end

  @doc false
  @impl GenServer
  def handle_info(message, state) do
    Logger.error(
      describe(state) <> " received unexpected message: " <> inspect(message, pretty: true)
    )

    {:noreply, state}
  end

  # Events buffered for a batch belong to the subscription being reset: they are
  # dropped unhandled, and the new subscription delivers them again.
  defp reset_subscription(%Handler{} = state) do
    %Handler{subscription: subscription} = state

    subscription = Subscription.reset(subscription)

    state = cancel_batch_timer(state)

    %Handler{
      state
      | last_seen_event: nil,
        subscription: subscription,
        subscribe_timer: nil,
        started_subscription: nil,
        batch_buffer: []
    }
  end

  defp subscribe_to_events(%Handler{} = state) do
    %Handler{subscription: subscription} = state

    case Subscription.subscribe(subscription, self()) do
      {:ok, subscription} ->
        %Handler{state | subscription: subscription, subscribe_timer: nil}

      {:error, error} ->
        {backoff, subscription} = Subscription.backoff(subscription)

        Logger.info(
          describe(state) <>
            " failed to subscribe to event store due to: " <>
            inspect(error) <> ", retrying in " <> inspect(backoff) <> "ms"
        )

        subscribe_timer = Process.send_after(self(), :subscribe_to_events, backoff)

        %Handler{state | subscription: subscription, subscribe_timer: subscribe_timer}
    end
  end

  # Buffer events and check flush conditions for batched handlers with timeout.
  # Called only when batch_timeout is configured (not :infinity).
  # Events are already upcasted before this function is called.
  defp buffer_and_maybe_flush(events, %Handler{} = state) do
    %Handler{
      batch_buffer: buffer,
      batch_size: batch_size
    } = state

    current_buffer = buffer || []
    new_buffer = current_buffer ++ events
    state = %Handler{state | batch_buffer: new_buffer}

    # Start timer if this is first event in batch
    state = maybe_start_batch_timer(state)

    # Check if we should flush based on size
    if length(new_buffer) >= batch_size do
      state
      |> cancel_batch_timer()
      |> flush_batch_buffer(:size)
    else
      state
    end
  end

  # Flush accumulated batch buffer (empty buffer guard)
  defp flush_batch_buffer(%Handler{batch_buffer: []} = state, _flush_reason), do: state
  defp flush_batch_buffer(%Handler{batch_buffer: nil} = state, _flush_reason), do: state

  defp flush_batch_buffer(%Handler{batch_buffer: buffer} = state, flush_reason) do
    Logger.debug(
      describe(state) <> " flushing batch of #{length(buffer)} event(s) due to #{flush_reason}"
    )

    # Clear buffer and timer BEFORE processing to prevent race condition
    # If timer fires during batch processing, it will see empty buffer
    state = %Handler{state | batch_buffer: [], batch_timer_ref: nil}

    # Process the batch with cleared state and flush reason in context
    handle_batch(buffer, %{flush_reason: flush_reason}, state)
  end

  # Start batch timer only if not already running, if we have events and timeout is configured
  defp maybe_start_batch_timer(%Handler{batch_buffer: []} = state), do: state
  defp maybe_start_batch_timer(%Handler{batch_timeout: :infinity} = state), do: state

  defp maybe_start_batch_timer(
         %Handler{
           batch_timer_ref: nil,
           batch_timeout: batch_timeout
         } = state
       )
       when is_integer(batch_timeout) do
    timer_ref = Process.send_after(self(), :flush_batch_timeout, batch_timeout)
    %Handler{state | batch_timer_ref: timer_ref}
  end

  defp maybe_start_batch_timer(state), do: state

  # Cancel batch timer if running
  defp cancel_batch_timer(%Handler{batch_timer_ref: nil} = state), do: state

  defp cancel_batch_timer(%Handler{batch_timer_ref: ref} = state) do
    case Process.cancel_timer(ref) do
      false ->
        drain_flush_batch_timeout_message()
        %Handler{state | batch_timer_ref: nil}

      _remaining ->
        %Handler{state | batch_timer_ref: nil}
    end
  end

  defp drain_flush_batch_timeout_message do
    receive do
      :flush_batch_timeout -> :ok
    after
      0 -> :ok
    end
  end

  # `:sys.get_status/1` and a crash report print this.
  @doc false
  @impl GenServer
  def format_status(%{state: %Handler{sensitive_state: true} = state} = status) do
    status
    |> Map.put(:state, %Handler{state | handler_state: :redacted})
    |> Map.replace(:log, [])
  end

  def format_status(status), do: status

  defp handle_event(event, context \\ %{}, handler)

  # Ignore already seen event, at `concurrency: 1` only.
  #
  # With one instance, `last_seen_event` is the instance's position in the stream, so
  # an event at or below it was handled already. With `concurrency: n > 1`, each
  # instance sees only some of the events, and `last_seen_event` is the number of the
  # event this instance last acked (`confirm_receipt/2` sets it), not a stream
  # position, and not monotonic: acking a redelivered earlier event lowers it. When a
  # sibling instance exits mid-event, the store redelivers its in-flight event to
  # another instance, which may already have handled a later one; skipping it there
  # would ack the event without handling it, and lose it. So at concurrency > 1 a
  # redelivered event always goes to `handle/2`, within the at-least-once contract
  # (commanded/commanded#671, sparkle-space/sparklespace#7429).
  #
  # `handle_batch/3` keeps its already-seen check: `:batch_size` and `:concurrency`
  # cannot be combined, so a batching handler always runs at `concurrency: 1`.
  defp handle_event(
         %RecordedEvent{event_number: event_number} = event,
         _context,
         %Handler{
           last_seen_event: last_seen_event,
           subscription: %Subscription{concurrency: 1}
         } = state
       )
       when not is_nil(last_seen_event) and event_number <= last_seen_event do
    Logger.debug(describe(state) <> " has already seen event ##{inspect(event_number)}")

    confirm_receipt(event, state)
  end

  # Delegate event to handler module.
  defp handle_event(%RecordedEvent{} = event, context, %Handler{} = state) do
    telemetry_metadata = telemetry_metadata(event, context, state)
    start_time = telemetry_start(telemetry_metadata, :handle)
    decrypts? = decrypts?(event, state)
    # `handle/2` gets the state in its metadata, so a failure on any event can hold it.
    redacted? = decrypts? or state.sensitive_state

    case delegate_event_to_handler(event, decrypts?, redacted?, state) do
      :ok ->
        telemetry_stop(start_time, telemetry_metadata, :handle)

        confirm_receipt(event, state)

      {:ok, handler_state} ->
        state = %Handler{state | handler_state: handler_state, sensitive_state: redacted?}

        telemetry_stop(
          start_time,
          Map.put(telemetry_metadata, :handler_state, shown_handler_state(state)),
          :handle
        )

        confirm_receipt(event, state)

      {:error, :already_seen_event} ->
        telemetry_stop(
          start_time,
          Map.put(telemetry_metadata, :error, :already_seen_event),
          :handle
        )

        confirm_receipt(event, state)

      {:error, reason} = error ->
        shown_reason = shown_reason(reason, redacted?)

        log_event_error({:error, shown_reason}, event, state)
        telemetry_stop(start_time, Map.put(telemetry_metadata, :error, shown_reason), :handle)

        failure_context = build_failure_context(event, context, state)
        retry_fun = fn context, state -> handle_event(event, context, state) end
        handle_event_error(error, event, failure_context, state, retry_fun, redacted?)

      {:error, reason, stacktrace} ->
        log_event_error({:error, reason, stacktrace}, event, state)
        telemetry_exception(start_time, :error, reason, stacktrace, telemetry_metadata, :handle)

        failure_context = build_failure_context(event, context, stacktrace, state)
        retry_fun = fn context, state -> handle_event(event, context, state) end
        handle_event_error({:error, reason}, event, failure_context, state, retry_fun, redacted?)

      invalid ->
        shown_value = if redacted?, do: ":redacted", else: inspect(invalid, pretty: true)

        Logger.error(
          describe(state) <>
            " failed to handle event " <>
            inspect(event, pretty: true) <>
            ", `handle/2` function returned an invalid value: " <>
            shown_value <>
            ", expected `:ok` or `{:error, term}`"
        )

        telemetry_stop(
          start_time,
          Map.put(telemetry_metadata, :error, :invalid_return_value),
          :handle
        )

        error = {:error, :invalid_return_value}
        failure_context = build_failure_context(event, context, state)

        next = fn context, state -> handle_event(event, context, state) end
        handle_event_error(error, event, failure_context, state, next, redacted?)
    end
  end

  # An event this handler is decrypted for: it declares something, and a head names it.
  defp decrypts?(%RecordedEvent{data: data}, %Handler{handler_module: handler_module}) do
    case PersonalFieldsProtocol.declaration(data) do
      nil ->
        false

      _declaration ->
        case handler_module.__personal_fields__() do
          :opaque -> false
          %{} = personal_fields -> is_map_key(personal_fields, data.__struct__)
        end
    end
  end

  # What decree's own log lines, telemetry and stop reasons may show. A handler's `error/3`
  # still gets the reason as it stands: it classifies on it.
  defp shown_reason(reason, true), do: RedactedError.reduce(reason)
  defp shown_reason(reason, false), do: reason

  defp handle_batch(events, context, handler)

  defp handle_batch(events, context, %Handler{last_seen_event: last_seen_event} = state)
       when is_number(last_seen_event) do
    %{event_number: last_event_number} = last_event = List.last(events)

    if last_event_number <= last_seen_event do
      Logger.debug(describe(state) <> " has already seen event ##{inspect(last_event_number)}")

      confirm_receipt(last_event, state)
    else
      events
      |> Enum.reject(&(&1.event_number <= last_seen_event))
      |> do_handle_batch(context, state)
    end
  end

  defp handle_batch(events, context, %Handler{} = state) do
    do_handle_batch(events, context, state)
  end

  defp do_handle_batch([], _context, state), do: state

  defp do_handle_batch(events, context, %Handler{} = state) do
    telemetry_metadata = batch_telemetry_metadata(events, context, state)
    start_time = telemetry_start(telemetry_metadata, :batch)

    case delegate_event_to_handler(events, state) do
      :ok ->
        telemetry_stop(start_time, telemetry_metadata, :batch)
        confirm_receipt(events, state)

      {:ok, handler_state} ->
        telemetry_stop(start_time, %{telemetry_metadata | handler_state: state}, :batch)
        confirm_receipt(events, %Handler{state | handler_state: handler_state})

      {:error, reason} = error ->
        log_batch_error(error, events, state)
        telemetry_stop(start_time, Map.put(telemetry_metadata, :error, reason), :batch)

        failure_context = build_failure_context(nil, context, state)
        retry_fun = fn context, state -> handle_batch(events, context, state) end
        handle_event_error(error, events, failure_context, state, retry_fun)

      {:error, reason, stacktrace} ->
        log_batch_error({:error, reason, stacktrace}, events, state)
        telemetry_exception(start_time, :error, reason, stacktrace, telemetry_metadata, :batch)

        failure_context = build_failure_context(nil, context, stacktrace, state)
        retry_fun = fn context, state -> handle_batch(events, context, state) end
        handle_event_error({:error, reason}, events, failure_context, state, retry_fun)

      invalid ->
        error =
          {:error,
           "invalid value: #{inspect(invalid, pretty: true)}, expected `:ok` or `{:error, term}`"}

        log_batch_error(error, events, state)

        telemetry_stop(
          start_time,
          Map.put(telemetry_metadata, :error, :invalid_return_value),
          :batch
        )

        failure_context = build_failure_context(nil, context, state)
        retry_fun = fn context, state -> handle_batch(events, context, state) end
        handle_event_error(error, nil, failure_context, state, retry_fun)
    end
  end

  defp delegate_event_to_handler(events, %Handler{} = state) when is_list(events) do
    enriched_events =
      Enum.map(events, fn e = %RecordedEvent{data: data} ->
        {data, enrich_metadata(e, state)}
      end)

    %Handler{handler_module: handler_module} = state

    try do
      handler_module.handle_batch(enriched_events)
    rescue
      error ->
        stacktrace = __STACKTRACE__
        {:error, error, stacktrace}
    end
  end

  defp delegate_event_to_handler(%RecordedEvent{} = event, false, false, %Handler{} = state) do
    %RecordedEvent{data: data} = event
    %Handler{handler_module: handler_module} = state

    metadata = enrich_metadata(event, state)

    try do
      handler_module.handle(data, metadata)
    rescue
      error ->
        stacktrace = __STACKTRACE__
        {:error, error, stacktrace}
    end
  end

  # The decrypted copy goes to `handle/2` and nowhere else: from here on, anything that leaves
  # this function can hold what it failed on. A decrypt error is the delegate's result, and
  # `handle/2` is not called.
  defp delegate_event_to_handler(%RecordedEvent{} = event, decrypts?, true, %Handler{} = state) do
    %Handler{application: application, handler_module: handler_module} = state

    try do
      with {:ok, %RecordedEvent{data: data} = readable} <-
             readable_event(event, decrypts?, application) do
        handler_module.handle(data, enrich_metadata(readable, state))
      end
    rescue
      error ->
        {:error, RedactedError.redact(error), RedactedError.scrub_stacktrace(__STACKTRACE__)}
    catch
      :exit, reason -> exit(RedactedError.reduce(reason))
      :throw, value -> RedactedError.leave_throw(value, __STACKTRACE__)
    end
  end

  defp readable_event(event, true, application) do
    PersonalFields.decrypt_recorded_event(application, event)
  end

  defp readable_event(event, false, _application), do: {:ok, event}

  defp build_failure_context(
         maybe_failed_event,
         context,
         stacktrace \\ nil,
         %Handler{} = state
       ) do
    %Handler{application: application, handler_name: handler_name, handler_state: handler_state} =
      state

    metadata = enrich_metadata(maybe_failed_event, state)

    %FailureContext{
      application: application,
      handler_name: handler_name,
      handler_state: handler_state,
      context: context,
      metadata: metadata,
      stacktrace: stacktrace
    }
  end

  # Enrich the metadata with additional fields from the recorded event, plus the
  # associated Decree application and the event handler's name.
  defp enrich_metadata(nil, _), do: %{}

  defp enrich_metadata(%RecordedEvent{} = event, %Handler{} = state) do
    %Handler{application: application, handler_name: handler_name, handler_state: handler_state} =
      state

    RecordedEvent.enrich_metadata(event,
      additional_metadata: %{
        application: application,
        handler_name: handler_name,
        state: handler_state
      }
    )
  end

  defp handle_event_error(
         error,
         maybe_failed_event,
         %FailureContext{} = failure_context,
         %Handler{} = state,
         retry_fun,
         redacted? \\ false
       ) do
    data =
      case maybe_failed_event do
        events when is_list(events) -> Enum.map(events, fn %RecordedEvent{data: data} -> data end)
        %RecordedEvent{data: data} -> data
        nil -> nil
      end

    case on_error(state, error, data, failure_context) do
      {:retry, %FailureContext{context: context}} when is_map(context) ->
        # Retry the failed event
        Logger.info(describe(state) <> " is retrying failed event")

        retry_fun.(context, state)

      {:retry, context} when is_map(context) ->
        # Retry the failed event
        Logger.info(describe(state) <> " is retrying failed event")

        retry_fun.(context, state)

      {:retry, delay, %FailureContext{context: context}}
      when is_map(context) and is_integer(delay) and delay >= 0 ->
        # Retry the failed event after waiting for the given delay, in milliseconds
        Logger.info(describe(state) <> " is retrying failed event after #{inspect(delay)}ms")

        :timer.sleep(delay)

        retry_fun.(context, state)

      {:retry, delay, context} when is_map(context) and is_integer(delay) and delay >= 0 ->
        # Retry the failed event after waiting for the given delay, in milliseconds
        Logger.info(describe(state) <> " is retrying failed event after #{inspect(delay)}ms")

        :timer.sleep(delay)

        retry_fun.(context, state)

      :skip ->
        # Skip the failed event by confirming receipt
        Logger.info(describe(state) <> " is skipping event")
        confirm_receipt(maybe_failed_event, state)

      {:stop, reason} ->
        reason = shown_reason(reason, redacted?)

        Logger.warning(describe(state) <> " has requested to stop: #{inspect(reason)}")

        # Stop event handler with given reason
        throw({:error, reason})

      invalid ->
        shown_response = shown_reason(invalid, redacted?)

        Logger.warning(
          describe(state) <> " returned an invalid error response: #{inspect(shown_response)}"
        )

        # Stop event handler with original error
        throw(if redacted?, do: {:error, shown_reason(elem(error, 1), true)}, else: error)
    end
  end

  # Never the module's own `error/3`, which could skip the event and lose the value. The
  # application's seam decides, and at this seam nothing skips: a stop never acks.
  defp on_error(%Handler{} = state, {:error, :key_unknown} = error, data, failure_context) do
    %Handler{application: application} = state

    case Decree.Application.on_event_handler_error(application) do
      default when default in [nil, :stop, :backoff] ->
        ErrorHandler.stop_on_error(error, data, failure_context)

      module when is_atom(module) ->
        case module.error(error, data, failure_context) do
          :skip -> {:stop, :key_unknown}
          response -> response
        end
    end
  end

  defp on_error(%Handler{} = state, error, data, failure_context) do
    %Handler{application: application, handler_module: handler_module} = state

    if function_exported?(handler_module, :error, 3) do
      handler_module.error(error, data, failure_context)
    else
      case Decree.Application.on_event_handler_error(application) do
        default when default in [nil, :stop] ->
          ErrorHandler.stop_on_error(error, data, failure_context)

        :backoff ->
          ErrorHandler.backoff(error, data, failure_context)

        module when is_atom(module) ->
          module.error(error, data, failure_context)
      end
    end
  end

  defp log_event_error({:error, :key_unknown}, %RecordedEvent{} = failed_event, state) do
    %RecordedEvent{event_number: event_number} = failed_event

    Logger.warning(
      describe(state) <> " can't read a personal field of event ##{inspect(event_number)}"
    )
  end

  defp log_event_error(error, %RecordedEvent{} = failed_event, %Handler{} = state) do
    reason =
      case error do
        {:error, reason} -> inspect(reason, pretty: true)
        {:error, reason, stacktrace} -> Exception.format(:error, reason, stacktrace)
      end

    Logger.error(
      describe(state) <>
        " failed to handle event:\n" <>
        inspect(failed_event, pretty: true) <> ", due to:\n" <> reason
    )
  end

  defp log_batch_error(error, events, state) do
    reason =
      case error do
        {:error, reason} -> inspect(reason, pretty: true)
        {:error, reason, stacktrace} -> Exception.format(:error, reason, stacktrace)
      end

    Logger.error(
      describe(state) <>
        " failed to handle batch from " <>
        inspect(List.first(events), pretty: true) <>
        " to " <>
        inspect(List.last(events), pretty: true) <>
        " due to: " <>
        inspect(reason, pretty: true)
    )
  end

  # Confirm receipt of one or more events.
  defp confirm_receipt(recorded_events, %Handler{} = state) when is_list(recorded_events) do
    %Handler{
      application: application,
      consistency: consistency,
      handler_name: handler_name,
      subscription: subscription
    } = state

    last_event = List.last(recorded_events)
    %RecordedEvent{event_number: event_number} = last_event

    Logger.debug(fn ->
      describe(state) <>
        " confirming receipt of event ##{inspect(event_number)}" <>
        " of events: #{inspect(recorded_events)}"
    end)

    # If we have a batch, we only confirm the last event received to the
    # subscription.
    :ok = Subscription.ack_event(subscription, last_event)
    :ok = Subscriptions.ack_event(application, handler_name, consistency, last_event)

    %Handler{state | last_seen_event: event_number}
  end

  defp confirm_receipt(recorded_event, %Handler{} = state),
    do: confirm_receipt([recorded_event], state)

  # Determine the partition key for an event to ensure ordered processing when
  # necessary.
  defp partition_event(
         %RecordedEvent{} = event,
         application,
         handler_name,
         handler_module
       ) do
    %RecordedEvent{data: data} =
      event = Upcast.upcast_event(event, additional_metadata: %{application: application})

    metadata =
      RecordedEvent.enrich_metadata(event,
        additional_metadata: %{
          application: application,
          handler_name: handler_name,
          state: nil
        }
      )

    try do
      handler_module.partition_by(data, metadata)
    rescue
      error ->
        stacktrace = __STACKTRACE__
        Logger.error(Exception.format(:error, error, stacktrace))

        1
    end
  end

  defp telemetry_start(telemetry_metadata, telemetry_type) do
    Telemetry.start([:decree, :event, telemetry_type], telemetry_metadata)
  end

  defp telemetry_stop(start_time, telemetry_metadata, telemetry_type) do
    Telemetry.stop([:decree, :event, telemetry_type], start_time, telemetry_metadata)
  end

  defp telemetry_exception(
         start_time,
         kind,
         reason,
         stacktrace,
         telemetry_metadata,
         telemetry_type
       ) do
    Telemetry.exception(
      [:decree, :event, telemetry_type],
      start_time,
      kind,
      reason,
      stacktrace,
      telemetry_metadata
    )
  end

  defp batch_telemetry_metadata(recorded_events, context, %Handler{} = state)
       when is_list(recorded_events) do
    first_event = List.first(recorded_events)
    last_event = List.last(recorded_events)

    %Handler{
      application: application,
      handler_name: handler_name,
      handler_module: handler_module,
      handler_state: handler_state
    } = state

    %{
      application: application,
      handler_name: handler_name,
      handler_module: handler_module,
      handler_state: handler_state,
      context: context,
      recorded_event: nil,
      first_event_id: first_event.event_id,
      last_event_id: last_event.event_id,
      event_count: length(recorded_events),
      flush_reason: Map.get(context, :flush_reason, :immediate)
    }
  end

  defp telemetry_metadata(recorded_event, context, %Handler{} = state) do
    %Handler{
      application: application,
      handler_name: handler_name,
      handler_module: handler_module
    } = state

    %{
      application: application,
      handler_name: handler_name,
      handler_module: handler_module,
      handler_state: shown_handler_state(state),
      context: context,
      recorded_event: recorded_event
    }
  end

  defp shown_handler_state(%Handler{sensitive_state: true}), do: :redacted
  defp shown_handler_state(%Handler{handler_state: handler_state}), do: handler_state

  defp consistency(opts) do
    case opts[:consistency] || Application.get_env(:decree, :default_consistency, :eventual) do
      :eventual ->
        :eventual

      :strong ->
        if Keyword.get(opts, :concurrency, 1) > 1 do
          # Strong consistency is not supported for event handlers with concurrency
          raise ArgumentError, message: "cannot use `:strong` consistency with concurrency"
        else
          :strong
        end

      invalid ->
        raise ArgumentError, message: "invalid `consistency` option: #{inspect(invalid)}"
    end
  end

  defp new_subscription(application, handler_name, handler_module, handler_opts) do
    partition_by =
      if function_exported?(handler_module, :partition_by, 2) do
        fn event -> partition_event(event, application, handler_name, handler_module) end
      end

    Subscription.new(
      application: application,
      concurrency: Keyword.get(handler_opts, :concurrency, 1),
      partition_by: partition_by,
      subscription_name: handler_name,
      subscribe_from: Keyword.get(handler_opts, :start_from, :origin),
      subscribe_to: Keyword.get(handler_opts, :subscribe_to, :all),
      subscription_opts: Keyword.get(handler_opts, :subscription_opts, [])
    )
  end

  defp describe(%Handler{handler_module: handler_module}),
    do: inspect(handler_module)
end
