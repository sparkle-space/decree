defmodule Decree.Event.TagCoverage do
  @moduledoc """
  Finds the event modules with no `Decree.Event.Tags` implementation.

  This is the build-time half of the protocol. At runtime an event with no implementation
  falls back to `Any` and appends with framework tags alone, so only a check like this one
  can say that an event was forgotten.

  It is opt-in. Nothing in decree calls it, and no compile hook, router macro or `use` runs
  it, so a build that has not opted in cannot fail. An application calls it from a build
  check of its own, with its list of event modules:

      Decree.Event.TagCoverage.verify!(MyApp.Events.all())

  It asks whether the implementation module exists, and never calls
  `Decree.Event.Tags.impl_for/1`: that answers the `Any` fallback, and building a struct to
  ask can trip `@enforce_keys`. The implementation module exists whether or not protocols
  are consolidated. `@derive Decree.Event.Tags` builds one.
  """

  alias Decree.Event.Tags

  @doc """
  Answers the modules in `event_modules` with no implementation, in the order given.
  """
  @spec missing([module()]) :: [module()]
  def missing(event_modules) when is_list(event_modules) do
    Enum.reject(event_modules, &implemented?/1)
  end

  @doc """
  Answers `:ok` when every module has an implementation, and raises `ArgumentError`
  naming those that do not.
  """
  @spec verify!([module()]) :: :ok
  def verify!(event_modules) when is_list(event_modules) do
    case missing(event_modules) do
      [] ->
        :ok

      missing ->
        raise ArgumentError,
              "no Decree.Event.Tags implementation for: " <>
                Enum.map_join(missing, ", ", &inspect/1) <>
                ". Implement the protocol, or `@derive Decree.Event.Tags` for an event " <>
                "with no domain tags"
    end
  end

  defp implemented?(event_module), do: Code.ensure_loaded?(Module.concat(Tags, event_module))
end
