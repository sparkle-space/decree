defprotocol Decree.Event.Tags do
  @moduledoc """
  The tags an event carries in the store, beyond the ones the framework adds.

  `tags/2` receives the event and a `Decree.Event.TagContext` and answers
  `{:ok, tags}` or `{:error, reason}`. It runs once per event per append attempt, inside
  the aggregate, so it must be deterministic: the same event and context give the same
  tags. It must read no database, clock or process state a retry could see differently.

  `Decree.Event.Append.prepare/2` calls it for every event a command appends, then adds
  the framework's tags: `command:`, `command_kind:`, `correlation:` and the stream's
  entity tag. Those need nothing from this protocol.

  ## An event with no implementation

  The protocol falls back to `Any`, which answers `{:ok, []}`. So an event with no
  implementation still appends, with the framework's tags. A struct with no implementation
  is not refused at runtime: an application that has not opted in keeps working, and so
  does every event a library or test defines.

  "A struct with no implementation fails the build" is met at build time instead, by
  `Decree.Event.TagCoverage`, which an application calls from its own build check.

  ## An event with no domain tags

  `@derive Decree.Event.Tags` is the explicit way to say an event has none. It answers
  `{:ok, []}` as the fallback does, and it counts as implemented for
  `Decree.Event.TagCoverage`:

      defmodule AccountClosed do
        @derive [Jason.Encoder, Decree.Event.Tags]
        defstruct [:account_id]
      end

  ## Failing an append

  `{:error, reason}` fails the append and is the command's answer. Nothing is appended and
  the aggregate's state is unchanged. A raise is answered the same way, as a raise from
  `execute/2` is. `reason` reaches the aggregate as an event store error would, so it must
  not be one the aggregate acts on: `:wrong_expected_version` retries the command, and an
  `:unavailable` `Decree.Store.AppendError` stops the aggregate.

  ## What an application must do to adopt it

    1. Implement the protocol, or `@derive` it, for every event a command can append.
    2. Call `Decree.Event.TagCoverage.verify!/1` with those event modules from a build
       check of its own. Nothing in decree calls it.
    3. Give each command a `command_id` field, and a `correlation_id` field where it
       carries a key. The framework reads those two names from the command, and from
       nowhere else. A command without one gets no tag of that kind.
    4. Route each stream by a field holding a lowercase canonical UUID, or name its
       kind with `stream_tag:` on `dispatch`. See `Decree.Commands.Router.dispatch/2`.

  A tag must be a string of valid UTF-8 with no NUL byte, 512 bytes at most. The store
  answers `{:error, :invalid_tag}` or `{:error, :tag_too_long}` otherwise. See
  `Decree.Store.EventData`.
  """

  # `Any` has no `__deriving__`, so `@derive` builds an implementation that delegates to it.
  @fallback_to_any true

  @doc """
  Answers the event's own tags, or an error that fails the append.
  """
  @spec tags(t, Decree.Event.TagContext.t()) :: {:ok, [String.t()]} | {:error, term}
  def tags(event, context)
end

defimpl Decree.Event.Tags, for: Any do
  def tags(_event, _context), do: {:ok, []}
end
