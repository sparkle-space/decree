defmodule Decree.Projections.RuntimeConfigProjector do
  use Decree.Projections.Ecto

  alias Decree.Projections.Events.AnEvent
  alias Decree.Projections.Projection

  project %AnEvent{} = event, fn multi ->
    %AnEvent{name: name, pid: pid} = event

    send(pid, {:project, name})

    Ecto.Multi.insert(multi, :my_projection, %Projection{name: name})
  end
end
