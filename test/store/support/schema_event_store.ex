defmodule SchemaEventStore do
  use Decree.Store, otp_app: :decree, schema: "example"
end
