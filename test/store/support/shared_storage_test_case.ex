defmodule Decree.Store.SharedStorageTestCase do
  defmacro define_tests(do: block) do
    quote do
      defmacro __using__(options) do
        block = unquote(Macro.escape(block))

        quote do
          use Decree.Store.StorageCase

          @moduletag unquote(options)

          unquote(block)
        end
      end
    end
  end
end
