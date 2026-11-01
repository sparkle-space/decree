defmodule Decree.Decision.Assertions do
  @moduledoc """
  Test helpers for a `Decree.Decision`, for an application to import.

      use ExUnit.Case

      import Decree.Decision.Assertions

      test "a claimed email refuses a second claim" do
        assert_refuses(ClaimEmail, [%EmailClaimed{digest: "d"}], %ClaimEmailCommand{digest: "d"},
          :email_taken
        )
      end

      test "a free email is claimed" do
        assert_decides(ClaimEmail, [], command, %EmailClaimed{digest: "d"})
      end

  They run `c:Decree.Decision.evolve/2`, `c:Decree.Decision.admit/2` and
  `c:Decree.Decision.decide/2` with no store: the given events stand for what the reads
  would answer, so they never read time or a database. What only a store can answer, such
  as a retry or a command match, needs a dispatch.

  A failure is an `ExUnit.AssertionError`, which is why ExUnit has to be running, as it is
  under `mix test`.
  """

  alias Decree.Decision.Dispatch
  alias Decree.Event.TagContext

  @doc """
  Folds `given_events`, then admits and decides `command`, answering as a dispatch would.

    - `{:ok, events}`, every new event in append order, as a dispatch with
      `returning: :events` answers, or `{:ok, state}`, the fold with the new events applied,
      with `returning: :aggregate_state`.
    - `{:error, reason}`, the refusal a dispatch answers. `:duplicate` from
      `c:Decree.Decision.decide/2` is `{:error, :duplicate_command}`.

  ## Options

    - `:returning` - `:events`, the default, or `:aggregate_state`.
    - `:state` - the state to start from, instead of `c:Decree.Decision.initial_state/0`.

  """
  @spec decision_result(module(), [struct()], struct(), keyword()) ::
          {:ok, [struct()]} | {:ok, term()} | {:error, term()}
  def decision_result(decision, given_events, command, opts \\ []) do
    with {:ok, shape, state} <- run(decision, given_events, command, opts) do
      case Keyword.get(opts, :returning, :events) do
        :events -> {:ok, Dispatch.events_of(shape)}
        :aggregate_state -> {:ok, state}
      end
    end
  end

  @doc """
  Asserts that the decision decides `expected`: an event, a list of events, or
  `{:streams, members}`, as `c:Decree.Decision.decide/2` would answer it. Answers the state
  the fold reached with the new events applied.

  Takes the options of `decision_result/4`.
  """
  @spec assert_decides(module(), [struct()], struct(), term(), keyword()) :: term()
  def assert_decides(decision, given_events, command, expected, opts \\ []) do
    {:ok, expected_shape} = Dispatch.shape(expected)

    case run(decision, given_events, command, opts) do
      {:ok, ^expected_shape, state} ->
        state

      {:ok, shape, _state} ->
        flunk(
          "expected #{inspect(decision)} to decide:\n\n  #{inspect(expected_shape)}\n\n" <>
            "but it decided:\n\n  #{inspect(shape)}"
        )

      {:error, reason} ->
        flunk(
          "expected #{inspect(decision)} to decide:\n\n  #{inspect(expected_shape)}\n\n" <>
            "but it refused with:\n\n  #{inspect(reason)}"
        )
    end
  end

  @doc """
  Asserts that the decision refuses with `reason`.

  Takes the options of `decision_result/4`.
  """
  @spec assert_refuses(module(), [struct()], struct(), term(), keyword()) :: :ok
  def assert_refuses(decision, given_events, command, reason, opts \\ []) do
    case run(decision, given_events, command, opts) do
      {:error, ^reason} ->
        :ok

      {:error, other} ->
        flunk(
          "expected #{inspect(decision)} to refuse with:\n\n  #{inspect(reason)}\n\n" <>
            "but it refused with:\n\n  #{inspect(other)}"
        )

      {:ok, shape, _state} ->
        flunk(
          "expected #{inspect(decision)} to refuse with:\n\n  #{inspect(reason)}\n\n" <>
            "but it decided:\n\n  #{inspect(shape)}"
        )
    end
  end

  @doc """
  Asserts that `c:Decree.Decision.query/2` answers `expected` for the command.

  ## Options

    - `:metadata` - the dispatch metadata the tag context holds. Defaults to `%{}`.
    - `:business_events` - check `c:Decree.Decision.followup_query/3` instead, given these
      events.

  """
  @spec assert_query_items(module(), struct(), term(), keyword()) :: :ok
  def assert_query_items(decision, command, expected, opts \\ []) do
    tag_context = %TagContext{metadata: Keyword.get(opts, :metadata, %{}), command: command}

    items =
      case Keyword.fetch(opts, :business_events) do
        {:ok, events} -> decision.followup_query(command, tag_context, events)
        :error -> decision.query(command, tag_context)
      end

    if items == expected do
      :ok
    else
      flunk(
        "expected #{inspect(decision)} to query:\n\n  #{inspect(expected)}\n\n" <>
          "but it queried:\n\n  #{inspect(items)}"
      )
    end
  end

  defp run(decision, given_events, command, opts) do
    state = Keyword.get_lazy(opts, :state, &decision.initial_state/0)
    state = given_events |> List.wrap() |> Enum.reduce(state, &decision.evolve(&2, &1))

    with :ok <- decision.admit(state, command),
         {:ok, shape} <- Dispatch.shape(decision.decide(state, command)),
         events = Dispatch.events_of(shape),
         :ok <- within_batch(decision, events) do
      {:ok, shape, Enum.reduce(events, state, &decision.evolve(&2, &1))}
    end
  end

  defp within_batch(decision, events) do
    if length(events) > decision.__decision__().max_batch_events do
      {:error, :decision_batch_too_large}
    else
      :ok
    end
  end

  # ExUnit is not a dependency of an application's release, so nothing here may name it at
  # compile time.
  defp flunk(message), do: apply(ExUnit.Assertions, :flunk, [message])
end
