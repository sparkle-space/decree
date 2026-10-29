defmodule Decree.Commands.Composite.CompositeCompositeRouter do
  @moduledoc false

  use Decree.Commands.CompositeRouter

  alias Decree.Commands.Composite.ExampleCompositeRouter

  router(ExampleCompositeRouter)
end
