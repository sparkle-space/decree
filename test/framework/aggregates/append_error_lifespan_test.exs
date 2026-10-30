defmodule Decree.Aggregates.AppendErrorLifespanTest do
  use ExUnit.Case, async: true

  alias Decree.Aggregates.DefaultLifespan
  alias Decree.Store.AppendError

  # A typed append error is a reply, not an exception: the default lifespan keeps
  # the aggregate running after it, as it did for the plain atoms it replaced.
  test "the default lifespan keeps the aggregate running after a typed append error" do
    for class <- [:unavailable, :retryable, :rejected] do
      error = %AppendError{class: class, code: nil, reason: :test}

      assert DefaultLifespan.after_error(error) == :infinity
    end
  end
end
