defmodule Decree.Decision.IndexRequirementsTest do
  # `Decree.Decision.Index.requirements/3`, public so an application can check the families
  # its rules read against the list it gives `Decree.Store.TagIndex.bootstrap/3`. decree's
  # test config names `tenant_slug`, `plan_slug`, `email` and `oauth_uid` as families.
  use ExUnit.Case, async: true

  alias Decree.Decision.Index
  alias Decree.DocBlocks
  alias Decree.Store.TagIndex

  @fingerprint "0123456789abcdef"

  test "Decree.Decision.Index is documented, and requirements/3 is its one public function" do
    {:docs_v1, _anno, _language, _format, moduledoc, _metadata, entries} =
      Code.fetch_docs(Index)

    assert %{"en" => _text} = moduledoc

    documented =
      for {{:function, name, arity}, _anno, _signature, doc, _metadata} <- entries,
          doc != :hidden,
          do: {name, arity}

    assert documented == [requirements: 3]
  end

  test "a rule's items: each family once, in the order its tags name them" do
    items = [
      [types: ["Elixir.MyApp.AccountRegistered"], tags: ["handle:ada", "plan_slug:pro"]],
      %{tags: ["handle:bob", "tenant_slug:acme"]}
    ]

    assert Index.requirements(false, items, %{}) ==
             {:ok, ["entity", "plan_slug", "tenant_slug"]}
  end

  test "a command item puts command first" do
    assert Index.requirements(true, [[tags: ["handle:ada"]]], %{}) == {:ok, ["command", "entity"]}
    assert Index.requirements(true, [], %{}) == {:ok, ["command"]}
    assert Index.requirements(false, [], %{}) == {:ok, []}
  end

  test "a digest tag requires its family and the key its fingerprint names" do
    items = [[tags: ["email:#{@fingerprint}:d1", "email:#{@fingerprint}:d2"]]]

    assert Index.requirements(false, items, %{"email" => :digest}) ==
             {:ok, [{"email", @fingerprint}]}
  end

  test "a digest tag whose fingerprint does not read is :error" do
    assert Index.requirements(false, [[tags: ["email:no-fingerprint"]]], %{"email" => :digest}) ==
             :error
  end

  test "the facts' own kinds require nothing" do
    assert Index.requirements(false, [[tags: ["tag_index:fact"]]], %{}) == {:ok, []}
  end

  test "the answer is in bootstrap/3's grammar" do
    items = [[tags: ["handle:ada", "email:#{@fingerprint}:d1"]]]
    {:ok, required} = Index.requirements(true, items, %{"email" => :digest})

    enabled = %{
      {"command", nil} => :enabled,
      {"entity", nil} => :enabled,
      {"email", @fingerprint} => :enabled
    }

    assert TagIndex.missing(enabled, required) == []
    assert TagIndex.missing(Map.delete(enabled, {"entity", nil}), required) == ["entity"]
    assert required -- ["command", "entity", {"email", @fingerprint}] == []
  end

  test "requirements/3's doc has one example, and it runs as written" do
    doc = DocBlocks.doc!(Index, {:function, :requirements, 3})
    first_line = ~s(items = [[tags: ["handle:ada"]]])

    assert DocBlocks.first_lines(doc) == [first_line]
    assert {{:ok, ["entity"]}, _binding} = Code.eval_string(DocBlocks.block!(doc, first_line))
  end
end
