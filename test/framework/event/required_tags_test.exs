defmodule Decree.Event.RequiredTagsTest do
  use ExUnit.Case

  alias Decree.Aggregates.ExecutionContext
  alias Decree.Application.Config
  alias Decree.Event.{Append, RequiredTags, TagContext}
  alias Decree.Event.Tagging.Commands.Emit
  alias Decree.Event.Tagging.Events.{Reloaded, Unimplemented}
  alias Decree.EventStore.Adapters.EventStore.Mapper
  alias Decree.EventStore.EventData
  alias Decree.RequiredTagsFixtures.{Addressed, Digested, Forged, Malformed, Unlisted, Unopted}

  defmodule App do
    use Decree.Application, otp_app: :decree
  end

  setup_all do
    Config.associate(self(), App, application: App)
  end

  defp prepare(event, command_id \\ "command-1") do
    context = %ExecutionContext{command: %Emit{command_id: command_id}, metadata: %{}}

    Append.prepare(App, "stream-1", [%EventData{data: event, metadata: %{}}], context)
  end

  describe "`required/2`" do
    test "is `always/0` and the event's answer" do
      assert RequiredTags.required(%Addressed{address: "a"}, %TagContext{}) ==
               {:ok, ["command", "command_kind", "address"]}

      assert RequiredTags.required(%Addressed{}, %TagContext{}) ==
               {:ok, ["command", "command_kind"]}
    end

    test "is nothing for an event without the export, whatever else it exports" do
      assert RequiredTags.required(%Unopted{}, %TagContext{}) == {:ok, []}
      assert RequiredTags.required(%Digested{}, %TagContext{}) == {:ok, []}
    end

    test "refuses an answer that is not a list" do
      for answer <- [nil, :address, %{}] do
        assert RequiredTags.required(%Unlisted{answer: answer}, %TagContext{}) ==
                 {:error, :invalid_required_tag_kinds},
               inspect(answer)
      end
    end

    test "passes on an improper list, which is the store's to refuse" do
      assert {:ok, ["command", "command_kind", :address | :improper]} =
               RequiredTags.required(%Malformed{}, %TagContext{})
    end

    test "holds no stream kind and no correlation" do
      assert RequiredTags.always() == ["command", "command_kind"]
    end
  end

  describe "the other two exports" do
    test "answer nothing, or the framework's, for a module without them" do
      assert RequiredTags.conditional_kinds(Unopted) == []
      assert RequiredTags.producible_kinds(Unopted) == ["command", "command_kind", "correlation"]
    end

    test "answer what a module lists" do
      assert RequiredTags.conditional_kinds(Addressed) == ["address"]

      assert RequiredTags.producible_kinds(Addressed) ==
               ["command", "command_kind", "correlation", "address", "scope"]
    end
  end

  describe "`Append.prepare/4`" do
    test "puts the list on the event's copy" do
      assert {:ok, [%EventData{required_tag_kinds: ["command", "command_kind", "address"]}]} =
               prepare(%Addressed{address: "a"})
    end

    test "gives an event without the export an empty list" do
      assert {:ok, [%EventData{required_tag_kinds: []}]} = prepare(%Unimplemented{})
    end

    # This `App` has no store, so the answer can only be `prepare/4`'s.
    test "refuses an opted-in event with no usable `command_id`, a forged tag or none" do
      for tags <- [[], ["command:forged"], ["command"]], command_id <- [nil, ""] do
        assert prepare(%Forged{tags: tags}, command_id) == {:error, :missing_required_tag},
               inspect({tags, command_id})
      end
    end

    test "passes an event without the export with no `command_id`" do
      assert {:ok, [%EventData{tags: tags}]} = prepare(%Unopted{tags: ["command:forged"]}, nil)
      assert "command:forged" in tags
    end

    test "the list reaches the store's event data" do
      assert %Decree.Store.EventData{required_tag_kinds: ["a"]} =
               Mapper.to_event_data(%EventData{data: %Unopted{}, required_tag_kinds: ["a"]})
    end
  end

  describe "an export of a module not loaded yet" do
    setup do
      :code.purge(Reloaded)
      :code.delete(Reloaded)
      :code.purge(Reloaded)

      refute :erlang.module_loaded(Reloaded)

      :ok
    end

    test "still opts the event in" do
      assert {:ok, [%EventData{required_tag_kinds: ["command", "command_kind", "reloaded"]}]} =
               prepare(%Reloaded{})
    end

    test "still reads as conditional" do
      assert RequiredTags.conditional_kinds(Reloaded) == ["reloaded"]
    end

    test "still reads as producible" do
      assert "reloaded" in RequiredTags.producible_kinds(Reloaded)
    end
  end
end
