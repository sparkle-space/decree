defmodule Decree.PostgresCase do
  @moduledoc false

  # A test case whose applications run on the Postgres event store. Each test
  # starts from an empty store, as the in-memory adapter upstream used gave it.

  use ExUnit.CaseTemplate

  setup do
    Decree.FrameworkStorage.reset!()
  end
end
