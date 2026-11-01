defmodule Decree.Projections.PersonalFieldsProjector do
  @moduledoc false

  # A real projector over a declared event. Its own `error/3` answers `:skip`, and must never
  # be called for a key that can't be read.

  use Decree.Projections.Ecto,
    application: Decree.PersonalFields.Fixtures.App,
    name: "PersonalFieldsProjector"

  alias Decree.PersonalFields.Fixtures.Events.Registered
  alias Decree.PersonalFields.Fixtures.Keys
  alias Decree.Projections.Projection

  project %Registered{}, metadata, fn multi ->
    Keys.notify(:projected)

    Ecto.Multi.insert(multi, :projection, %Projection{name: metadata["password_hash"]})
  end

  @impl Decree.Event.Handler
  def error(error, _event, _failure_context) do
    Keys.notify({:module_error, error})

    :skip
  end
end
