defmodule Decree.Event.ConcurrencyEvent do
  @derive Jason.Encoder
  defstruct [:stream_uuid, :index, :partition]
end
