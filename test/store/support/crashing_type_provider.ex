defmodule Decree.Store.CrashingTypeProvider do
  @moduledoc false

  # A custom `Decree.EventStore.TypeProvider` whose `to_struct/1` crashes with an exception
  # `Decree.Serialization.JsonSerializer.known_type?/1` does not read as an unknown type, so
  # the crash raises (decree 772c21f). Configure it with `FiniteTypeProvider.configure!/1`;
  # the provider is global, so only a test that is not async may set it.

  @behaviour Decree.EventStore.TypeProvider

  alias Decree.Serialization.ModuleNameTypeProvider

  @impl Decree.EventStore.TypeProvider
  def to_string(struct), do: ModuleNameTypeProvider.to_string(struct)

  @impl Decree.EventStore.TypeProvider
  def to_struct(_type), do: raise("type provider crashed")
end
