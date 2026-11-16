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

  telemetry_event(%{
    event: [:decree, :event, :retry, :scheduled],
    description:
      "Emitted when an event handler schedules the retry of a failed event or batch " <>
        "for after a delay, once per scheduled retry",
    measurements: "%{delay: non_neg_integer(), system_time: integer()}",
    metadata: """
    %{application: Decree.Application.t(),
      context: map(),
      handler_name: String.t(),
      handler_module: atom(),
      handler_state: map() | :redacted,
      recorded_event: RecordedEvent.t() | nil,
      event_count: non_neg_integer()}
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
  clause ignoring both arguments, with `_`-prefixed variables its body does not read and no
  guard, is the one catch-all: an underscored argument the body reads does not count as
  ignored. Any other head can't be read: a bound first argument, `%module{}`, a map, an
  ignored event beside a bound or matched metadata argument, a guarded or non-trailing
  catch-all. So can't `handle_batch/1`, which gets no decrypt.

  A module with such a head names nothing and sees stored values. If it reads no personal
  field it says so, and is then never decrypted for:

      use Decree.Event.Handler,
        application: ExampleApp,
        name: __MODULE__,
        personal_fields: :opaque

  `:opaque` is the only value `:personal_fields` takes, and anything else is a `CompileError`.

  Every handler, `:opaque` or not, also answers `__handle_heads__/0`: each `handle/2` clause it
  defines, in order and a `use`-injected one included, as `{line, event_pattern, guards}`, the
  expanded first argument and its list of guards (`[]` without one). Decree's own catch-all
  is not in it, and a `handle_batch/1` handler answers `[]`. It is for a tool that checks what
  a handler's heads match, so it reads the compiled heads rather than the source.

  Every module that uses the handler, and so every `Decree.Projections.Ecto` projector, also
  persists the attribute `decree_event_handler: [true]`, read back as
  `module.module_info(:attributes)[:decree_event_handler]`. It lets such a tool tell decree's
  expansion from a module that only declares `@behaviour Decree.Event.Handler` and writes its
  own `__handle_heads__/0`. It marks the `use`, and proves nothing more: a module that
  registers and sets the persisted attribute by hand forges it, which is deliberate
  tampering, as below.

  The export is checked after compile: the module's final `handle/2` clauses must be the heads
  `__handle_heads__/0` answers, followed by decree's catch-all. A `CompileError` names the
  module when they are not, as when a module defines `__handle_heads__/0` itself, or when
  another `use` defines or overrides `handle/2` in a `@before_compile` registered after
  decree's. Put such a `use` before `use Decree.Event.Handler`. The check covers what macros
  and compile hooks do, not deliberate tampering such as removing decree's `@after_compile`.

  A head that can't be read and isn't `:opaque` is a `CompileError` naming the module and the
  head's line.

  ### When a key can't be read

  `{:error, :key_unknown}`, from the decrypt or from `handle/2`, never reaches the handler's
  own `c:error/3`, whose answer could skip the event. It goes to the application's
  `on_event_handler_error`. `nil`, `:stop` and `:backoff` stop the handler, and a stop never
  acks. A module's `error/3` decides, and a `:skip` from it is turned into a stop, so nothing
  can skip an event at this seam. The event stays unacked until the key can be read.

  ### When a command meets an unknown event type

  During a rolling deploy a handler on an older release can be part-way through an event it
  knows when a newer release appends an event of a type the older one does not know to the
  stream its follow-up command targets. The aggregate refuses that command with
  `{:error, :unknown_event_type}` rather than fold past the event, and `handle/2` or
  `handle_batch/1` returns it.

  `{:error, :unknown_event_type}` takes the same route as `{:error, :key_unknown}`, from either
  callback: it never reaches the handler's own `c:error/3` and goes to the application's
  `on_event_handler_error`. `nil`, `:stop` and `:backoff` stop the handler, and a stop never
  acks. A module's `error/3` decides, and a `:skip` from it is turned into a stop, so nothing
  can skip an event at this seam. The event, or the batch, stays unacked until a pod that knows
  the type serves the command.

  From `handle/2` it logs one warning naming the event's number, without the event. From
  `handle_batch/1` it logs through the generic batch error line, which shows the batch's first
  and last events as stored, as `{:error, :key_unknown}` does.

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

  `{:error, :key_unknown}` and `{:error, :unknown_event_type}` never reach it: the
  application's `on_event_handler_error` decides them, and nothing can skip them. See
  "When a key can't be read" and "When a command meets an unknown event type" in the
  module documentation.

  You can return one of the following responses depending upon the
  error severity:

  - `{:retry, context}` - retry the failed event, provide a context
    map, or updated `Decree.Event.FailureContext` struct, containing any
    state to be passed to subsequent failures. This could be used to count the
    number of failures, stopping after too many.

  - `{:retry, delay, context}` - retry the failed event after the requested
    delay (in milliseconds). Context is a map or
    `Decree.Event.FailureContext` struct as described in `{:retry, context}`
    above.

    The handler does not sleep through the delay: it sets a timer and goes
    back to its mailbox, a delay of `0` included. While it waits it answers
    system messages (such as `:sys.get_state/1`), and it stops on a `DOWN`
    from its subscription or a non-normal exit signal, a supervisor's
    shutdown included, without waiting for the timer. Events delivered
    meanwhile are held. When the timer fires it retries the failed event (or
    batch), then handles the rest of that delivery, then the held deliveries,
    in the order they arrived. A lock loss or a reset during the wait drops
    the pending retry and every held event unhandled: none was acknowledged,
    so the subscription delivers them again from the last acknowledged event,
    and the failed event's next failure starts from a fresh context.

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
              Logger.warning("Skipping bad event, too many failures: " <> inspect(event.__struct__))

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
      @after_compile unquote(__MODULE__)
      @behaviour Handler

      # Persisted, so a tool reading the compiled module tells decree's expansion from a
      # module that only declares the behaviour (see the moduledoc).
      Module.register_attribute(__MODULE__, :decree_event_handler, persist: true)
      @decree_event_handler true

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
      handle_heads = handle_heads(env.module)

      # Generate default handlers
      quote generated: true do
        @doc false
        def __personal_fields__, do: unquote(Macro.escape(personal_fields))

        @doc false
        def __handle_heads__, do: unquote(Macro.escape(handle_heads))

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

    if unreadable != [] do
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

  # Every `handle/2` clause the module defines, `use`-injected ones included, as
  # `{line, event_pattern, guards}`: the expanded first argument and the list of guard
  # expressions (`[]` without a guard). Read before the default `handle/2` below is defined,
  # so decree's own catch-all is never one. A `handle_batch/1` module has no `handle/2`.
  defp handle_heads(module) do
    module |> definition_clauses({:handle, 2}) |> Enum.map(&clause_head/1)
  end

  defp clause_head({meta, [event, _metadata], guards, _body}), do: {meta[:line], event, guards}

  # Checks the final state against what `__before_compile__` exported: the module's `handle/2`
  # clauses must be exactly the heads `__handle_heads__/0` answers, followed by decree's own
  # catch-all. A later `@before_compile` that adds, replaces or overrides a `handle/2` clause,
  # or a module that defines `__handle_heads__/0` itself, fails it. Called with no export
  # test, because under the compiler the module is not loaded here; a direct call loads it.
  @doc false
  def __after_compile__(%Macro.Env{module: module} = env, _bytecode) do
    clauses = definition_clauses(module, {:handle, 2})

    if not exported_heads?(clauses, module.__handle_heads__()) do
      raise CompileError,
        file: env.file,
        line: env.line,
        description:
          "#{inspect(module)}: `handle/2` or `__handle_heads__/0` changed after decree read " <>
            "the heads of `handle/2`, so `__handle_heads__/0` would not answer them. Define " <>
            "`handle/2` in the module, or in a `use` whose own `@before_compile` is " <>
            "registered before `use Decree.Event.Handler`, and do not define " <>
            "`__handle_heads__/0`"
    end
  end

  defp exported_heads?([_ | _] = clauses, heads) do
    {user_clauses, [last]} = Enum.split(clauses, -1)

    catch_all?(last) and Enum.map(user_clauses, &clause_head/1) == heads
  end

  defp exported_heads?([], _heads), do: false

  # Decree's own `handle/2` from `__before_compile__`: generated here, both arguments ignored
  # variables of this module's context, no guard, `:ok`.
  defp catch_all?({meta, [{:_event, _, __MODULE__}, {:_metadata, _, __MODULE__}], [], :ok}) do
    meta[:generated] == true and meta[:context] == __MODULE__
  end

  defp catch_all?(_clause), do: false

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
    |> Enum.reduce({[], []}, fn {{meta, [event, metadata], guards, body}, index},
                                {modules, unreadable} ->
      case struct_modules(event) do
        [_ | _] = named ->
          {named ++ modules, unreadable}

        [] ->
          if index == last and guards == [] and ignored?(event, body) and ignored?(metadata, body) do
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

  # A bare `_` is always ignored. An `_`-prefixed variable is ignored only while the clause body
  # does not read it: Elixir lets a body read one (with a warning), and that head sees stored
  # values. A read is the compiler's own variable identity: the same name, context and
  # `:version` that the expanded head argument carries in its meta. A nested binding of the same
  # name (`fn _event -> ... end`, `case x do _event -> ... end`) has its own version and is not
  # a read. On a compiler whose expanded variables carry no `:version` (Elixir 1.20 does), it
  # falls back to name and context, which counts such a nested binding as a read: stricter, so
  # safe.
  defp ignored?({:_, _meta, context}, _body) when is_atom(context), do: true

  defp ignored?({name, meta, context}, body) when is_atom(name) and is_atom(context) do
    String.starts_with?(Atom.to_string(name), "_") and
      not reads_variable?(body, name, context, Keyword.fetch(meta, :version))
  end

  defp ignored?(_pattern, _body), do: false

  defp reads_variable?(body, name, context, version) do
    {_ast, found} =
      Macro.prewalk(body, false, fn
        {^name, meta, ^context} = node, acc ->
          {node, acc or same_version?(version, Keyword.fetch(meta, :version))}

        node, acc ->
          {node, acc}
      end)

    found
  end

  defp same_version?({:ok, version}, {:ok, version}), do: true
  defp same_version?({:ok, _version}, _other), do: false
  defp same_version?(:error, _other), do: true

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
    # A retry `{:retry, delay, context}` scheduled, while its timer runs: `%{token, timer_ref,
    # retry, rest, deferred}`, where `retry` runs the failed event or batch again on a handler
    # state, `rest` is the rest of its delivery (already upcast) and `deferred` the `{:events, _}`
    # and `:flush_batch_timeout` messages that arrived meanwhile, in arrival order.
    retry_wait: nil,
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
    Logger.debug(
      describe(state) <> " is shutting down due to " <> inspect(RedactedError.reduce(reason))
    )
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
        stopping_on_error(state, fn ->
          state |> reset_subscription() |> subscribe_to_events()
        end)

      {:stop, reason} ->
        Logger.debug(
          describe(state) <>
            " `before_reset/0` callback has requested to stop. (reason: " <>
            inspect(RedactedError.reduce(reason)) <> ")"
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
  # same events. Events buffered for a batch are dropped unhandled, and so are a
  # pending retry wait's retry, the rest of its delivery and the deliveries held
  # behind it; none was acknowledged, so they are delivered again from the last
  # acknowledged event to whichever instance holds the lock. The subscription
  # sends `{:subscribed, subscription}` once it takes the lock back.
  @impl GenServer
  def handle_info(
        {:subscription_lock_lost, subscription},
        %Handler{subscription: %Subscription{subscription_pid: subscription}} = state
      ) do
    Logger.warning(describe(state) <> " subscription lost its advisory lock, waiting to resume")

    state = state |> cancel_batch_timer() |> cancel_retry_wait()

    {:noreply, %Handler{state | batch_buffer: []}}
  end

  # From a subscription process the handler no longer uses (one replaced by a reset).
  @impl GenServer
  def handle_info({:subscription_lock_lost, _subscription}, state) do
    {:noreply, state}
  end

  @doc false
  # While a retry waits, a delivery is held behind it: it is handled after the retry and the
  # rest of the retried delivery.
  @impl GenServer
  def handle_info({:events, _events} = message, %Handler{retry_wait: %{}} = state) do
    {:noreply, defer(message, state)}
  end

  @impl GenServer
  def handle_info({:events, events}, state) do
    stopping_on_error(state, fn -> dispatch_events(events, state, []) end)
  end

  @doc false
  # A batch is never flushed around a pending retry: the flush is held behind it too.
  @impl GenServer
  def handle_info(:flush_batch_timeout = message, %Handler{retry_wait: %{}} = state) do
    {:noreply, defer(message, state)}
  end

  @impl GenServer
  def handle_info(:flush_batch_timeout, state) do
    stopping_on_error(state, fn -> flush_on_timeout(state, []) end)
  end

  @doc false
  # The pending retry's timer.
  @impl GenServer
  def handle_info({:retry_wait, token}, %Handler{retry_wait: %{token: token}} = state) do
    stopping_on_error(state, fn -> resume_after_wait(state) end)
  end

  # The timer of a wait that a reset or a lock loss dropped, when its cancel came too late.
  @impl GenServer
  def handle_info({:retry_wait, _token}, state) do
    {:noreply, state}
  end

  @doc false
  @impl GenServer
  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %Handler{subscription: %Subscription{subscription_ref: ref}} = state
      ) do
    Logger.debug(
      describe(state) <> " subscription DOWN due to: " <> inspect(RedactedError.reduce(reason))
    )

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
      describe(state) <>
        " received unexpected message: " <> inspect(RedactedError.reduce(message))
    )

    {:noreply, state}
  end

  # Events buffered for a batch belong to the subscription being reset: they are
  # dropped unhandled, and the new subscription delivers them again. So are a
  # pending retry wait's retry, the rest of its delivery and the deliveries held
  # behind it.
  defp reset_subscription(%Handler{} = state) do
    %Handler{subscription: subscription} = state

    subscription = Subscription.reset(subscription)

    state = state |> cancel_batch_timer() |> cancel_retry_wait()

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
            inspect(RedactedError.reduce(error)) <> ", retrying in " <> inspect(backoff) <> "ms"
        )

        subscribe_timer = Process.send_after(self(), :subscribe_to_events, backoff)

        %Handler{state | subscription: subscription, subscribe_timer: subscribe_timer}
    end
  end

  # A `{:stop, reason}` from `error/3`, or an invalid answer from it, is thrown as
  # `{:error, reason}` (`handle_event_error/7`): the handler stops with the state it had when
  # the message arrived. `fun` answers the new state.
  defp stopping_on_error(%Handler{} = state, fun) do
    {:noreply, fun.()}
  catch
    {:error, reason} -> {:stop, reason, state}
  end

  # Upcast a delivery once, then hand it to the configured callback. `deferred` is what is held
  # behind it, carried into a retry wait that this delivery arms.
  defp dispatch_events(events, %Handler{} = state, deferred) do
    %Handler{
      application: application,
      handler_callback: callback,
      batch_timeout: batch_timeout
    } = state

    Logger.debug(describe(state) <> " received #{length(events)} event(s)")

    events = Upcast.upcast_event_stream(events, additional_metadata: %{application: application})

    case {callback, batch_timeout} do
      {:event, _} ->
        # Non-batched: process immediately
        handle_events(events, state, deferred)

      {:batch, timeout} when timeout in [:infinity, nil] ->
        # Batched without timeout: process immediately (Decree.Store handles batching)
        catching_retry_wait(state, [], deferred, fn state ->
          handle_batch(events, %{flush_reason: :immediate}, state)
        end)

      {:batch, _timeout} ->
        # Batched with timeout: buffer events and flush on size or timeout
        catching_retry_wait(state, [], deferred, &buffer_and_maybe_flush(events, &1))
    end
  end

  defp flush_on_timeout(%Handler{} = state, deferred) do
    %Handler{batch_buffer: buffer} = state

    Logger.debug(describe(state) <> " flushing batch due to timeout: #{length(buffer)} event(s)")

    catching_retry_wait(state, [], deferred, &flush_batch_buffer(&1, :timeout))
  end

  # Handle a non-batched delivery one event at a time. An event whose retry is scheduled for
  # after a delay stops the loop: the wait is armed with the events after it as `rest`.
  defp handle_events([], %Handler{} = state, _deferred), do: state

  defp handle_events([event | rest], %Handler{} = state, deferred) do
    case catching_retry_wait(state, rest, deferred, &handle_event(event, &1)) do
      %Handler{retry_wait: nil} = state -> handle_events(rest, state, deferred)
      %Handler{} = waiting -> waiting
    end
  end

  # Runs `fun` on `state`. A retry scheduled inside it (`schedule_retry/5`) unwinds to here as a
  # `{:decree_retry_wait, delay, retry, state}` throw, its state the one the failed event or
  # batch was handled on, and is armed on that state with `rest` and `deferred` behind it. Only
  # that tag is caught: an `{:error, reason}` throw reaches `stopping_on_error/2`.
  defp catching_retry_wait(%Handler{} = state, rest, deferred, fun) do
    fun.(state)
  catch
    {:decree_retry_wait, delay, retry, %Handler{} = retry_state} ->
      arm_retry_wait(retry_state, delay, retry, rest, deferred)
  end

  # The handler does not sleep: it sets a timer and goes back to its mailbox, so a `DOWN` from
  # its subscription, an exit signal and a system message are served while it waits.
  defp arm_retry_wait(%Handler{} = state, delay, retry, rest, deferred) do
    token = make_ref()
    timer_ref = Process.send_after(self(), {:retry_wait, token}, delay)

    retry_wait = %{
      token: token,
      timer_ref: timer_ref,
      retry: retry,
      rest: rest,
      deferred: deferred
    }

    %Handler{state | retry_wait: retry_wait}
  end

  defp defer(message, %Handler{retry_wait: %{deferred: deferred} = retry_wait} = state) do
    %Handler{state | retry_wait: %{retry_wait | deferred: deferred ++ [message]}}
  end

  # The retry runs on the current state, then the rest of its delivery, then the held messages,
  # each as `handle_info/2` would have handled it. A step that arms a new wait carries what is
  # not yet handled into it, in order.
  defp resume_after_wait(%Handler{retry_wait: retry_wait} = state) do
    %{retry: retry, rest: rest, deferred: deferred} = retry_wait

    state = %Handler{state | retry_wait: nil}

    with %Handler{retry_wait: nil} = state <- catching_retry_wait(state, rest, deferred, retry),
         %Handler{retry_wait: nil} = state <- handle_events(rest, state, deferred) do
      handle_deferred(deferred, state)
    end
  end

  defp handle_deferred([], %Handler{} = state), do: state

  defp handle_deferred([message | deferred], %Handler{} = state) do
    case handle_deferred_message(message, state, deferred) do
      %Handler{retry_wait: nil} = state -> handle_deferred(deferred, state)
      %Handler{} = waiting -> waiting
    end
  end

  defp handle_deferred_message({:events, events}, state, deferred),
    do: dispatch_events(events, state, deferred)

  defp handle_deferred_message(:flush_batch_timeout, state, deferred),
    do: flush_on_timeout(state, deferred)

  # A timer that already fired leaves its `{:retry_wait, token}` in the mailbox, which
  # `handle_info/2` ignores once `retry_wait` no longer holds its token.
  defp cancel_retry_wait(%Handler{retry_wait: nil} = state), do: state

  defp cancel_retry_wait(%Handler{retry_wait: %{timer_ref: timer_ref}} = state) do
    _remaining = Process.cancel_timer(timer_ref)

    %Handler{state | retry_wait: nil}
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

  # `:sys.get_status/1` and a crash report print this. A pending `retry_wait` is shown as it
  # stands: it holds no handler state, only refs, closures (printed without what they capture),
  # and `RecordedEvent`s and `{:events, _}` messages that are never decrypted, so a personal
  # field in them is ciphertext.
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
        handle_event_error(error, event, failure_context, state, event, retry_fun, redacted?)

      {:error, reason, stacktrace} ->
        log_event_error({:error, reason, stacktrace}, event, state)
        telemetry_exception(start_time, :error, reason, stacktrace, telemetry_metadata, :handle)

        failure_context = build_failure_context(event, context, stacktrace, state)
        retry_fun = fn context, state -> handle_event(event, context, state) end

        handle_event_error(
          {:error, reason},
          event,
          failure_context,
          state,
          event,
          retry_fun,
          redacted?
        )

      invalid ->
        Logger.error(
          describe(state) <>
            " failed to handle " <>
            describe_event(event) <>
            ", `handle/2` function returned an invalid value: " <>
            inspect(RedactedError.reduce(invalid)) <>
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
        handle_event_error(error, event, failure_context, state, event, next, redacted?)
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
        handle_event_error(error, events, failure_context, state, events, retry_fun)

      {:error, reason, stacktrace} ->
        log_batch_error({:error, reason, stacktrace}, events, state)
        telemetry_exception(start_time, :error, reason, stacktrace, telemetry_metadata, :batch)

        failure_context = build_failure_context(nil, context, stacktrace, state)
        retry_fun = fn context, state -> handle_batch(events, context, state) end
        handle_event_error({:error, reason}, events, failure_context, state, events, retry_fun)

      invalid ->
        # `error/3` is still given the value in the error, as it was; the log line names its
        # class only.
        error =
          {:error,
           "invalid value: #{inspect(invalid, pretty: true)}, expected `:ok` or `{:error, term}`"}

        Logger.error(
          describe(state) <>
            " failed to handle batch from " <>
            describe_event(List.first(events)) <>
            " to " <>
            describe_event(List.last(events)) <>
            ", `handle_batch/1` returned an invalid value: " <>
            inspect(RedactedError.reduce(invalid)) <>
            ", expected `:ok` or `{:error, term}`"
        )

        telemetry_stop(
          start_time,
          Map.put(telemetry_metadata, :error, :invalid_return_value),
          :batch
        )

        failure_context = build_failure_context(nil, context, state)
        retry_fun = fn context, state -> handle_batch(events, context, state) end
        handle_event_error(error, nil, failure_context, state, events, retry_fun)
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

  # `retried` is what `retry_fun` handles again: the failed event, or the batch (also when
  # `maybe_failed_event` is `nil`, after a batch's invalid return value).
  defp handle_event_error(
         error,
         maybe_failed_event,
         %FailureContext{} = failure_context,
         %Handler{} = state,
         retried,
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
        schedule_retry(delay, context, retried, state, retry_fun)

      {:retry, delay, context} when is_map(context) and is_integer(delay) and delay >= 0 ->
        schedule_retry(delay, context, retried, state, retry_fun)

      :skip ->
        # Skip the failed event by confirming receipt
        Logger.info(describe(state) <> " is skipping event")
        confirm_receipt(maybe_failed_event, state)

      {:stop, reason} ->
        reason = shown_reason(reason, redacted?)

        Logger.warning(
          describe(state) <> " has requested to stop: " <> inspect(RedactedError.reduce(reason))
        )

        # Stop event handler with given reason
        throw({:error, reason})

      invalid ->
        Logger.warning(
          describe(state) <>
            " returned an invalid error response: " <> inspect(RedactedError.reduce(invalid))
        )

        # Stop event handler with original error
        throw(if redacted?, do: {:error, shown_reason(elem(error, 1), true)}, else: error)
    end
  end

  # Retry the failed event or batch after `delay` milliseconds, a delay of 0 included, without
  # sleeping: the throw unwinds the handling of the delivery to `catching_retry_wait/4`, outside
  # `delegate_event_to_handler/4`'s `try`, which arms a timer and returns the handler to its
  # mailbox. `state` is the state the failure was handled on (for a batch flush, with its buffer
  # already cleared); the retry itself runs on the state current when the timer fires.
  defp schedule_retry(delay, context, retried, %Handler{} = state, retry_fun) do
    Logger.info(describe(state) <> " is retrying failed event after #{inspect(delay)}ms")

    telemetry_retry_scheduled(delay, context, retried, state)

    retry = fn state -> retry_fun.(context, state) end

    throw({:decree_retry_wait, delay, retry, state})
  end

  # Never the module's own `error/3`, which could skip the event and lose its effect: a key
  # that can't be read, or a follow-up command refused over an event type this release does
  # not know. The application's seam decides, and at this seam nothing skips: a stop never
  # acks.
  defp on_error(%Handler{} = state, {:error, reason} = error, data, failure_context)
       when reason in [:key_unknown, :unknown_event_type] do
    %Handler{application: application} = state

    case Decree.Application.on_event_handler_error(application) do
      default when default in [nil, :stop, :backoff] ->
        ErrorHandler.stop_on_error(error, data, failure_context)

      module when is_atom(module) ->
        case module.error(error, data, failure_context) do
          :skip -> {:stop, reason}
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

  defp log_event_error({:error, :unknown_event_type}, %RecordedEvent{} = failed_event, state) do
    %RecordedEvent{event_number: event_number} = failed_event

    Logger.warning(
      describe(state) <>
        " can't handle event ##{inspect(event_number)}: its follow-up met an event type" <>
        " this release does not know"
    )
  end

  defp log_event_error(error, %RecordedEvent{} = failed_event, %Handler{} = state) do
    Logger.error(
      describe(state) <>
        " failed to handle " <> describe_event(failed_event) <> ", due to: " <> shown_error(error)
    )
  end

  defp log_batch_error(error, events, state) do
    Logger.error(
      describe(state) <>
        " failed to handle batch from " <>
        describe_event(List.first(events)) <>
        " to " <>
        describe_event(List.last(events)) <>
        " due to: " <>
        shown_error(error)
    )
  end

  # What a log line shows of a failure (sparkle-space/sparklespace#8358): an error reply by its
  # class, and a raise by the exception's module and a scrubbed stacktrace, never its message.
  defp shown_error({:error, reason}), do: inspect(RedactedError.reduce(reason))

  defp shown_error({:error, exception, stacktrace}),
    do: RedactedError.format_reduced(exception, stacktrace)

  # An event as a log line names it: its number, its type and the last eight characters of its
  # stream's id, never its data or metadata (sparkle-space/sparklespace#8358).
  defp describe_event(%RecordedEvent{} = event) do
    %RecordedEvent{event_number: event_number, event_type: event_type, stream_id: stream_id} =
      event

    "event ##{inspect(event_number)} (#{event_type}) of " <>
      inspect(Decree.Store.LogTerms.suffix(stream_id))
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

    # The events' numbers, never the events: their data, metadata and stream id stay out of
    # the log (sparkle-space/sparklespace#8357, #8358).
    Logger.debug(fn ->
      describe(state) <>
        " confirming receipt of event ##{inspect(event_number)}" <>
        " of events: #{inspect(Enum.map(recorded_events, & &1.event_number))}"
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

        Logger.error(
          inspect(handler_module) <>
            ".partition_by/2 raised, partitioning by 1: " <>
            RedactedError.format_reduced(error, stacktrace)
        )

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

  defp telemetry_retry_scheduled(delay, context, retried, %Handler{} = state) do
    %Handler{
      application: application,
      handler_name: handler_name,
      handler_module: handler_module
    } = state

    {recorded_event, event_count} =
      case retried do
        %RecordedEvent{} = event -> {event, 1}
        events when is_list(events) -> {nil, length(events)}
      end

    :telemetry.execute(
      [:decree, :event, :retry, :scheduled],
      %{delay: delay, system_time: System.system_time()},
      %{
        application: application,
        handler_name: handler_name,
        handler_module: handler_module,
        handler_state: shown_handler_state(state),
        context: context,
        recorded_event: recorded_event,
        event_count: event_count
      }
    )
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
