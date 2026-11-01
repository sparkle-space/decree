defmodule Decree.Event.AppendTest do
  use ExUnit.Case, async: true

  alias Decree.Aggregates.ExecutionContext
  alias Decree.Application.Config
  alias Decree.Event.Append
  alias Decree.Event.TagContext
  alias Decree.Event.Tagging.Commands.Emit
  alias Decree.Event.Tagging.Events.{Refused, Tagged, Unimplemented}
  alias Decree.EventStore.Adapters.EventStore.Mapper
  alias Decree.EventStore.EventData
  alias Decree.UUID

  defmodule Bare do
    defstruct [:_id]
  end

  defmodule App do
    use Decree.Application, otp_app: :decree
  end

  @kind "command_kind:Decree.Event.Tagging.Commands.Emit"

  setup_all do
    Config.associate(self(), App, application: App)
  end

  defp prepare(event_data, context), do: Append.prepare(App, "stream-1", event_data, context)

  defp data(event, metadata \\ %{}), do: %EventData{data: event, metadata: metadata}

  defp context(command, fields \\ []) do
    struct!(%ExecutionContext{command: command}, fields)
  end

  test "the tag context holds the metadata and the command, and nothing else" do
    assert %TagContext{} |> Map.from_struct() |> Map.keys() |> Enum.sort() == [
             :command,
             :metadata
           ]
  end

  test "the event's own tags come first, the framework's after" do
    command_id = UUID.uuid4()
    command = %Emit{user_id: UUID.uuid4(), command_id: command_id}

    assert {:ok, [%EventData{tags: tags}]} =
             prepare(
               [data(%Tagged{tags: ["a:1"]})],
               context(command, identity: :user_id, aggregate_identity: command.user_id)
             )

    assert tags == ["a:1", "command:" <> command_id, @kind, "user:" <> command.user_id]
  end

  test "keeps every other field of the event data, and the order of the events" do
    first = %EventData{
      causation_id: "cause",
      correlation_id: "correlation",
      event_type: "type",
      data: %Tagged{note: "first"},
      metadata: %{"a" => 1}
    }

    second = data(%Unimplemented{note: "second"})

    assert {:ok, [prepared_first, prepared_second]} =
             prepare([first, second], context(%Emit{}))

    assert prepared_first == %EventData{first | tags: [@kind]}
    assert prepared_second == %EventData{second | tags: [@kind]}
  end

  test "is pure: the same input gives the same answer, and the input is untouched" do
    metadata = %{"who" => "alice"}
    user_id = UUID.uuid4()

    context =
      context(%Emit{user_id: user_id},
        identity: :user_id,
        aggregate_identity: user_id,
        metadata: metadata
      )

    event_data = [data(%Tagged{tags: ["a:1"]}, metadata)]

    assert {:ok, prepared} = prepare(event_data, context)
    assert {:ok, ^prepared} = prepare(event_data, context)

    assert [%EventData{tags: []}] = event_data
    assert context.metadata == metadata
    assert [%EventData{metadata: ^metadata}] = prepared
  end

  test "the stored tags reach the store's event data" do
    assert %Decree.Store.EventData{tags: ["a:1"]} =
             Mapper.to_event_data(%EventData{data: %Tagged{}, tags: ["a:1"]})
  end

  test "the first error stops and is returned as it stands" do
    event_data = [
      data(%Tagged{tags: ["a:1"]}),
      data(%Refused{reason: "first"}),
      data(%Refused{reason: "second"})
    ]

    assert prepare(event_data, context(%Emit{})) == {:error, {:refused, "first"}}
  end

  test "an implementation answering neither shape raises" do
    event_data = [data(%Tagged{tags: "not a list"})]

    assert_raise CaseClauseError, fn -> prepare(event_data, context(%Emit{})) end
  end

  describe "the key tags" do
    for {name, value} <- [{"nil", nil}, {"empty", ""}, {"a number", 1}, {"an atom", :id}] do
      test "are absent for a value that is #{name}" do
        command = %Emit{command_id: unquote(value), correlation_id: unquote(value)}

        assert {:ok, [%EventData{tags: [@kind]}]} =
                 prepare([data(%Unimplemented{})], context(command))
      end
    end

    test "are absent for a command with no such field" do
      command = %Bare{}

      assert {:ok, [%EventData{tags: ["command_kind:Decree.Event.AppendTest.Bare"]}]} =
               prepare([data(%Unimplemented{})], context(command))
    end

    test "never read the context's correlation or causation ids" do
      context = context(%Emit{}, correlation_id: UUID.uuid4(), causation_id: UUID.uuid4())

      assert {:ok, [%EventData{tags: [@kind]}]} =
               prepare([data(%Unimplemented{})], context)
    end
  end

  describe "the stream tag" do
    test "is absent from a context built with no identity" do
      command = %Emit{user_id: UUID.uuid4()}

      assert {:ok, [%EventData{tags: [@kind]}]} =
               prepare([data(%Unimplemented{})], context(command))
    end

    test "is absent with no resolved identity, whatever the command's field holds" do
      context = context(%Emit{user_id: UUID.uuid4()}, identity: :user_id)

      assert {:ok, [%EventData{tags: [@kind]}]} =
               prepare([data(%Unimplemented{})], context)
    end

    test "is the resolved identity, not the command's field" do
      resolved = UUID.uuid4()

      context =
        context(%Emit{user_id: UUID.uuid4()}, identity: :user_id, aggregate_identity: resolved)

      assert {:ok, [%EventData{tags: [@kind, tag]}]} =
               prepare([data(%Unimplemented{})], context)

      assert tag == "user:" <> resolved
    end

    test "never calls an identity function" do
      resolved = UUID.uuid4()

      context =
        context(%Emit{},
          identity: fn _command -> flunk("the identity function was called") end,
          aggregate_identity: resolved,
          stream_tag: :user
        )

      assert {:ok, [%EventData{tags: [@kind, tag]}]} =
               prepare([data(%Unimplemented{})], context)

      assert tag == "user:" <> resolved
    end

    test "is absent for an identity function with no `stream_tag`" do
      context =
        context(%Emit{}, identity: fn _command -> nil end, aggregate_identity: UUID.uuid4())

      assert {:ok, [%EventData{tags: [@kind]}]} =
               prepare([data(%Unimplemented{})], context)
    end

    test "is absent for a context with a resolved identity and no way to name its kind" do
      context = context(%Emit{}, aggregate_identity: UUID.uuid4())

      assert {:ok, [%EventData{tags: [@kind]}]} =
               prepare([data(%Unimplemented{})], context)
    end

    test "is absent for a field whose stem is empty" do
      id = UUID.uuid4()

      assert {:ok, [%EventData{tags: ["command_kind:Decree.Event.AppendTest.Bare"]}]} =
               prepare(
                 [data(%Unimplemented{})],
                 context(%Bare{_id: id}, identity: :_id, aggregate_identity: id)
               )
    end

    test "takes its kind from `stream_tag`, over the field's stem" do
      user_id = UUID.uuid4()

      context =
        context(%Emit{user_id: user_id},
          identity: :user_id,
          aggregate_identity: user_id,
          stream_tag: :member
        )

      assert {:ok, [%EventData{tags: [@kind, tag]}]} =
               prepare([data(%Unimplemented{})], context)

      assert tag == "member:" <> user_id
    end
  end
end
