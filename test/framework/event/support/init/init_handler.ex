defmodule Decree.Event.InitHandler do
  use Decree.Event.Handler,
    application: Decree.MockedApp,
    name: __MODULE__

  def init do
    Process.send(:test, {:init, self()}, [])
  end
end
