defmodule Decree.MockChildSpec do
  @moduledoc false

  # What a supervisor asks of a child given as `{module, arg}`. The store mock answers it too,
  # so an application on the mock starts no store process (see `Decree.MockEventStoreCase`).
  @callback child_spec(arg :: term()) :: Supervisor.child_spec()
end

Mox.defmock(Decree.EventStore.Mock, for: [Decree.Store, Decree.MockChildSpec])
Mox.defmock(Decree.Commands.MockRouter, for: Decree.Commands.Router)
Mox.defmock(Decree.Application.Mock, for: Decree.Application)
Mox.defmock(Decree.Serialization.TypeProvider.Mock, for: Decree.EventStore.TypeProvider)
