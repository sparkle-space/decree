if Code.ensure_loaded?(Postgrex) && Code.ensure_loaded?(Jason) do
  Postgrex.Types.define(Decree.Store.PostgresTypes, [], json: Jason)
end
