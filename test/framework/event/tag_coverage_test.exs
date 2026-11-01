defmodule Decree.Event.TagCoverageTest do
  use ExUnit.Case

  alias Decree.Event.{TagCoverage, Tags}

  defmodule Implemented do
    defstruct [:id]
  end

  defimpl Decree.Event.Tags, for: Implemented do
    def tags(_event, _context), do: {:ok, ["id:1"]}
  end

  defmodule Derived do
    @derive Decree.Event.Tags
    defstruct [:id]
  end

  defmodule Missing do
    defstruct [:id]
  end

  defmodule EnforcedMissing do
    @enforce_keys [:id]
    defstruct [:id]
  end

  defmodule EnforcedImplemented do
    @enforce_keys [:id]
    defstruct [:id]
  end

  defimpl Decree.Event.Tags, for: EnforcedImplemented do
    def tags(_event, _context), do: {:ok, []}
  end

  defmodule NoStruct do
  end

  @modules [Implemented, Derived, Missing, EnforcedMissing, EnforcedImplemented, NoStruct]

  describe "`missing/1`" do
    test "answers the modules with no implementation, in the order given" do
      assert TagCoverage.missing(@modules) == [Missing, EnforcedMissing, NoStruct]
    end

    test "answers nothing for no modules" do
      assert TagCoverage.missing([]) == []
    end

    test "answers a module that does not exist" do
      assert TagCoverage.missing([Decree.Event.TagCoverageTest.NoSuchEvent]) ==
               [Decree.Event.TagCoverageTest.NoSuchEvent]
    end

    test "is not fooled by the fallback an event with no implementation takes" do
      assert Tags.impl_for(%Missing{}) == Tags.Any
      assert Tags.tags(%Missing{}, nil) == {:ok, []}

      assert TagCoverage.missing([Missing]) == [Missing]
    end
  end

  describe "a derived struct" do
    test "answers no domain tags" do
      assert Tags.tags(%Derived{}, nil) == {:ok, []}
    end

    test "has an implementation of its own" do
      refute Tags.impl_for(%Derived{}) == Tags.Any
    end
  end

  describe "`verify!/1`" do
    test "answers `:ok` when every module is implemented" do
      assert TagCoverage.verify!([Implemented, Derived, EnforcedImplemented]) == :ok
    end

    test "raises `ArgumentError` naming the modules with none" do
      error =
        assert_raise ArgumentError, fn ->
          TagCoverage.verify!([Implemented, Missing, EnforcedMissing])
        end

      assert error.message =~ inspect(Missing)
      assert error.message =~ inspect(EnforcedMissing)
      refute error.message =~ inspect(Implemented)
    end
  end

  describe "with the protocol consolidated" do
    setup do
      refute Protocol.consolidated?(Tags)

      on_exit(fn ->
        :code.purge(Tags)
        {:module, Tags} = :code.load_file(Tags)
      end)

      types = [Any, Implemented, Derived, EnforcedImplemented]
      {:ok, binary} = Protocol.consolidate(Tags, types)
      {:module, Tags} = :code.load_binary(Tags, ~c"consolidated in a test", binary)

      assert Protocol.consolidated?(Tags)

      :ok
    end

    test "answers the same" do
      assert TagCoverage.missing(@modules) == [Missing, EnforcedMissing, NoStruct]
      assert TagCoverage.verify!([Implemented, Derived, EnforcedImplemented]) == :ok
    end
  end
end
