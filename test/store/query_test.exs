defmodule Decree.Store.QueryTest do
  use ExUnit.Case, async: true

  alias Decree.Store.Query

  describe "compile/1" do
    test "gives a tag and type item one probe per type" do
      assert {:ok, query} = Query.compile([[types: ["a", "b"], tags: ["x"]]])

      assert query == %Query{
               probe_items: [0, 0],
               probe_tags: ["x", "x"],
               probe_types: ["a", "b"],
               rest_items: [],
               rest_tags: []
             }
    end

    test "gives a tag item one probe with no type, and its other tags to the rest" do
      assert {:ok, query} = Query.compile([[tags: ["x", "y", "z"]]])

      assert query == %Query{
               probe_items: [0],
               probe_tags: ["x"],
               probe_types: [nil],
               rest_items: [0, 0],
               rest_tags: ["y", "z"]
             }
    end

    test "gives a type item one probe per type with no tag" do
      assert {:ok, query} = Query.compile([%{types: ["a", "b"]}])

      assert query == %Query{
               probe_items: [0, 0],
               probe_tags: [nil, nil],
               probe_types: ["a", "b"],
               rest_items: [],
               rest_tags: []
             }
    end

    test "keeps each item's index" do
      assert {:ok, query} =
               Query.compile([
                 [types: ["a"]],
                 %{tags: ["x", "y"]},
                 [types: ["b", "c"], tags: ["z", "w"]]
               ])

      assert query == %Query{
               probe_items: [0, 1, 2, 2],
               probe_tags: [nil, "x", "z", "z"],
               probe_types: ["a", nil, "b", "c"],
               rest_items: [1, 2],
               rest_tags: ["y", "w"]
             }
    end

    test "removes repeats inside an item and keeps its first tag first" do
      assert {:ok, query} =
               Query.compile([[types: ["a", "a"], tags: ["x", "y", "x", "y"]]])

      assert query == %Query{
               probe_items: [0],
               probe_tags: ["x"],
               probe_types: ["a"],
               rest_items: [0],
               rest_tags: ["y"]
             }
    end

    test "treats an empty list of types or tags as absent" do
      assert {:ok, %Query{probe_types: [nil]}} = Query.compile([[types: [], tags: ["x"]]])
      assert {:ok, %Query{probe_tags: [nil]}} = Query.compile([[types: ["a"], tags: []]])
    end

    test "accepts a tag at the limit, counted in bytes" do
      assert {:ok, _query} = Query.compile([[tags: [String.duplicate("a", 512)]]])
    end

    test "refuses a tag over the limit, counted in bytes" do
      assert {:error, :tag_too_long} = Query.compile([[tags: [String.duplicate("a", 513)]]])
      assert {:error, :tag_too_long} = Query.compile([[tags: [String.duplicate("é", 300)]]])

      long_second = ["x", String.duplicate("a", 513)]
      assert {:error, :tag_too_long} = Query.compile([[tags: long_second]])
    end

    test "does not apply the tag limit to a type" do
      assert {:ok, _query} = Query.compile([[types: [String.duplicate("a", 513)]]])
    end

    for {name, items} <- [
          {"nil", nil},
          {"a map of items", quote(do: %{types: ["a"]})},
          {"a bare string", "tag"},
          {"an empty list", []},
          {"an improper list", quote(do: [[tags: ["x"]] | :rest])},
          {"an item that is a string", ["tag"]},
          {"an item that is a list of strings", [["tag"]]},
          {"an item that is a struct", quote(do: [%URI{}])},
          {"an item with neither", [[]]},
          {"a map item with neither", quote(do: [%{}])},
          {"an item with both empty", [[types: [], tags: []]]},
          {"a type that is an atom", [[types: [:type]]]},
          {"a nil type", [[types: [nil]]]},
          {"tags that are not a list", [[tags: "x"]]},
          {"types that are nil", [[types: nil, tags: ["x"]]]},
          {"an improper list of tags", quote(do: [[tags: ["x" | "y"]]])},
          {"a misspelt key", [[tag: ["x"], types: ["a"]]]},
          {"a repeated key", [[tags: ["x"], tags: ["y"]]]},
          {"a string key", quote(do: [%{"tags" => ["x"]}])},
          {"a type with a NUL byte", quote(do: [[types: [<<"a", 0>>]]])}
        ] do
      test "refuses #{name}" do
        assert {:error, :invalid_query} = Query.compile(unquote(items))
      end
    end

    for {name, tag} <- [
          {"an atom", :tag},
          {"nil", nil},
          {"a number", 1},
          {"not UTF-8", quote(do: <<0xFF>>)},
          {"a lone NUL byte", quote(do: <<0>>)},
          {"holding a NUL byte", quote(do: <<"a", 0>>)}
        ] do
      test "answers `:invalid_tag` for a tag that is #{name}" do
        assert {:error, :invalid_tag} = Query.compile([[tags: [unquote(tag)]]])
        assert {:error, :invalid_tag} = Query.compile([[tags: ["x", unquote(tag)]]])
        assert {:error, :invalid_tag} = Query.compile([[tags: ["x"]], [tags: [unquote(tag)]]])
      end
    end

    test "answers `:invalid_tag` for a tag both invalid and over the limit" do
      assert {:error, :invalid_tag} = Query.compile([[tags: [String.duplicate(<<0>>, 513)]]])
    end

    test "answers `:invalid_query` before `:invalid_tag` for a query that is both" do
      assert {:error, :invalid_query} = Query.compile([[tags: [<<0>>]], []])
    end

    test "refuses an invalid item beside a valid one" do
      assert {:error, :invalid_query} = Query.compile([[tags: ["x"]], []])
    end
  end
end
