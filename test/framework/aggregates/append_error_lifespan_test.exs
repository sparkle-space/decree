defmodule Decree.Aggregates.AppendErrorLifespanTest do
  use ExUnit.Case, async: true

  alias Decree.Aggregates.DefaultLifespan
  alias Decree.Store.AppendError

  # A typed append error is a reply, not an exception, so the default lifespan
  # answers `:infinity` for every class. That is the lifespan policy only: the
  # aggregate itself still stops after an `:unavailable` one, whatever its
  # lifespan says (see aggregate_append_outcome_unknown_test.exs).
  test "the default lifespan keeps the aggregate running after a typed append error" do
    for class <- [:unavailable, :retryable, :rejected] do
      error = %AppendError{class: class, code: nil, reason: :test}

      assert DefaultLifespan.after_error(error) == :infinity
    end
  end
end
