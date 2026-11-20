defmodule Decree.Decision.TagIndexFamiliesTest do
  # The named families come from the application environment, which these tests change.
  use ExUnit.Case, async: false

  alias Decree.Decision.Index
  alias Decree.Store.TagIndex

  @key :tag_index_named_families
  @framework ~w(command command_kind correlation)

  setup do
    previous = Application.fetch_env(:decree, @key)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:decree, @key, value)
        :error -> Application.delete_env(:decree, @key)
      end
    end)
  end

  describe "the named families" do
    test "are decree's own kinds when none is configured" do
      Application.delete_env(:decree, @key)

      assert TagIndex.named_families() == @framework
    end

    test "are decree's own kinds, then the configured ones, in order" do
      Application.put_env(:decree, @key, ["tenant_slug", "plan_slug"])

      assert TagIndex.named_families() == @framework ++ ["tenant_slug", "plan_slug"]
    end

    for invalid <- [
          "tenant_slug",
          [:tenant_slug],
          [""],
          ["a:b"],
          ["tag_index"],
          ["tag_index_state"]
        ] do
      test "refuse a configured #{inspect(invalid)}" do
        Application.put_env(:decree, @key, unquote(invalid))

        assert_raise ArgumentError, ~r/tag_index_named_families/, fn ->
          TagIndex.named_families()
        end
      end
    end
  end

  describe "a decision's requirements" do
    test "put an unconfigured kind in the entity family" do
      Application.delete_env(:decree, @key)

      assert Index.requirements(false, [[tags: ["tenant_slug:acme"]]], %{}) ==
               {:ok, ["entity"]}
    end

    test "put a configured kind in its own family" do
      Application.put_env(:decree, @key, ["tenant_slug"])

      assert Index.requirements(false, [[tags: ["tenant_slug:acme"]]], %{}) ==
               {:ok, ["tenant_slug"]}
    end

    test "keep decree's own kinds in their own families with none configured" do
      Application.delete_env(:decree, @key)

      assert Index.requirements(true, [[tags: ["correlation:x", "command_kind:Y"]]], %{}) ==
               {:ok, ["command", "correlation", "command_kind"]}
    end
  end
end
