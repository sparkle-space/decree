defmodule Decree.Middleware.ExtractAggregateIdentityTest do
  use Decree.PostgresCase

  # A routed identity is a lowercase canonical UUID, checked on the value the command (or the
  # route's identity function) gave, before the prefix and before any conversion. Nothing is
  # trimmed, downcased or stringified. Each router registers no middleware.

  alias Decree.Aggregates.Aggregate
  alias Decree.{CommandIdCases, DefaultApp, EventStore, Registration, UUID}

  defmodule Marked do
    @moduledoc false
    @derive Jason.Encoder
    defstruct [:ref]
  end

  defmodule Mark do
    @moduledoc false
    defstruct [:ref, :command_id, :test]
  end

  defmodule MarkWithNoRef do
    @moduledoc false
    defstruct [:command_id, :test]
  end

  defmodule Marker do
    @moduledoc false
    defstruct [:ref]

    def execute(%__MODULE__{}, %{test: test} = command) do
      send(test, {:executed, command})

      %Marked{ref: Map.get(command, :ref)}
    end

    def apply(%__MODULE__{} = state, %Marked{ref: ref}), do: %__MODULE__{state | ref: ref}
  end

  defmodule FieldRouter do
    @moduledoc false
    use Decree.Commands.Router

    identify Marker, by: :ref, prefix: "marker-"

    dispatch [Mark, MarkWithNoRef], to: Marker
  end

  defmodule FunctionRouter do
    @moduledoc false
    use Decree.Commands.Router

    identify Marker, by: &__MODULE__.identity/1, prefix: "marker-"

    dispatch Mark, to: Marker

    def identity(%Mark{ref: ref}), do: ref
  end

  setup do
    start_supervised!(DefaultApp)

    :ok
  end

  def mark(fields) do
    struct!(Mark, [ref: UUID.uuid4(), command_id: UUID.uuid4(), test: self()] ++ fields)
  end

  def dispatch(router, command), do: router.dispatch(command, application: DefaultApp)

  # Nothing ran: no aggregate process, no stream named after the identity, no command seen.
  def refute_marked(identity) do
    if is_binary(identity) do
      stream_uuid = "marker-" <> identity

      assert Registration.whereis_name(
               DefaultApp,
               Aggregate.name(DefaultApp, Marker, stream_uuid)
             ) == :undefined

      assert {:error, :stream_not_found} = EventStore.stream_forward(DefaultApp, stream_uuid)
    end

    refute_received {:executed, _command}
  end

  def assert_marked(command, stream_uuid) do
    assert_received {:executed, ^command}

    ref = command.ref

    assert [%{data: %Marked{ref: ^ref}}] =
             DefaultApp |> EventStore.stream_forward(stream_uuid) |> Enum.to_list()
  end

  @refused [
    {"not a UUID", "abc"},
    {"an uppercase UUID", "6F9619FF-8B86-D011-B42D-00C04FC964FF"},
    {"a UUID with a leading space", " 6f9619ff-8b86-d011-b42d-00c04fc964ff"},
    {"a UUID with a trailing space", "6f9619ff-8b86-d011-b42d-00c04fc964ff "},
    {"32 hex digits with no dashes", "6f9619ff8b86d011b42d00c04fc964ff"},
    {"an integer", 123},
    {"an atom", :ref},
    {"an empty string", ""},
    {"nil", nil}
  ]

  describe "a field identity" do
    for {name, identity} <- @refused do
      test "refuses #{name} with `:invalid_aggregate_identity`" do
        command = mark(ref: unquote(identity))

        assert {:error, :invalid_aggregate_identity} = dispatch(FieldRouter, command)

        refute_marked(unquote(identity))
      end
    end

    test "refuses a command with no identity field" do
      command = %MarkWithNoRef{command_id: UUID.uuid4(), test: self()}

      assert {:error, :invalid_aggregate_identity} = dispatch(FieldRouter, command)

      refute_marked(nil)
    end

    test "passes a canonical UUID, and the prefix applies after the check" do
      command = mark([])

      assert :ok = dispatch(FieldRouter, command)

      assert_marked(command, "marker-" <> command.ref)
    end

    test "checks the identity, not the prefixed stream name" do
      command = mark(ref: "marker-" <> UUID.uuid4())

      assert {:error, :invalid_aggregate_identity} = dispatch(FieldRouter, command)

      refute_marked(command.ref)
    end
  end

  describe "a function identity" do
    test "passes what it answered when that is a canonical UUID" do
      command = mark([])

      assert :ok = dispatch(FunctionRouter, command)

      assert_marked(command, "marker-" <> command.ref)
    end

    for {name, identity} <- @refused do
      test "refuses #{name} it answered with `:invalid_aggregate_identity`" do
        command = mark(ref: unquote(identity))

        assert {:error, :invalid_aggregate_identity} = dispatch(FunctionRouter, command)

        refute_marked(unquote(identity))
      end
    end
  end

  describe "the `command_id` refusal" do
    test "comes before the identity's" do
      command = mark(ref: "abc", command_id: nil)

      assert {:error, :command_id_required} = dispatch(FieldRouter, command)
    end

    test "is not answered for a canonical one next to a bad identity" do
      command = mark(ref: "abc", command_id: CommandIdCases.canonical())

      assert {:error, :invalid_aggregate_identity} = dispatch(FieldRouter, command)
    end
  end
end

defmodule Decree.Middleware.ExtractAggregateIdentityOpaqueTest do
  use Decree.PostgresCase

  # An `opaque: true` route takes any non-empty binary identity, as it was sent. The routers
  # compile at run time: `opaque:` on a `dispatch` route is refused where it is not known.

  alias Decree.{DefaultApp, EventStore, UUID}
  alias Decree.Middleware.ExtractAggregateIdentityTest, as: Fence

  @identify_router Decree.Middleware.ExtractAggregateIdentityOpaqueTest.IdentifyRouter
  @route_router Decree.Middleware.ExtractAggregateIdentityOpaqueTest.RouteRouter

  setup_all do
    Code.compile_string("""
    defmodule #{inspect(@identify_router)} do
      use Decree.Commands.Router

      alias Decree.Middleware.ExtractAggregateIdentityTest.{Mark, Marker}

      identify Marker, by: :ref, prefix: "marker-", opaque: true

      dispatch Mark, to: Marker
    end

    defmodule #{inspect(@route_router)} do
      use Decree.Commands.Router

      alias Decree.Middleware.ExtractAggregateIdentityTest.{Mark, Marker}

      dispatch Mark, to: Marker, identity: :ref, identity_prefix: "marker-", opaque: true
    end
    """)

    :ok
  end

  setup do
    start_supervised!(DefaultApp)

    :ok
  end

  for {where, router} <- [
        {"`identify`", @identify_router},
        {"the `dispatch` route", @route_router}
      ] do
    describe "opaque on #{where}" do
      test "passes a non-UUID binary as it was sent" do
        command = Fence.mark(ref: "evt_123")

        assert :ok = Fence.dispatch(unquote(router), command)

        Fence.assert_marked(command, "marker-evt_123")
      end

      test "passes a canonical UUID" do
        command = Fence.mark([])

        assert :ok = Fence.dispatch(unquote(router), command)

        Fence.assert_marked(command, "marker-" <> command.ref)
      end

      for {name, identity} <- [
            {"an empty string", ""},
            {"nil", nil},
            {"an integer", 123},
            {"an atom", :evt_123},
            {"a map", %{}}
          ] do
        test "refuses #{name} with `:invalid_aggregate_identity`" do
          command = Fence.mark(ref: unquote(Macro.escape(identity)))

          assert {:error, :invalid_aggregate_identity} = Fence.dispatch(unquote(router), command)

          Fence.refute_marked(unquote(Macro.escape(identity)))
        end
      end

      test "still refuses a bad `command_id` first" do
        command = Fence.mark(ref: "evt_123", command_id: "evt_123")

        assert {:error, :invalid_command_id} = Fence.dispatch(unquote(router), command)

        assert {:error, :stream_not_found} =
                 EventStore.stream_forward(DefaultApp, "marker-evt_123")
      end
    end
  end

  test "a canonical UUID on an opaque route still carries no repair" do
    ref = String.upcase(UUID.uuid4())
    command = Fence.mark(ref: ref)

    assert :ok = Fence.dispatch(@identify_router, command)

    Fence.assert_marked(command, "marker-" <> ref)
  end
end
