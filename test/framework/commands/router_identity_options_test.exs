defmodule Decree.Commands.RouterIdentityOptionsTest do
  use ExUnit.Case, async: true

  # What a router refuses when it compiles: a command that is not a `Decree.Command` when the
  # router asks for one, and an `opaque:` declaration a route cannot carry. Every router here
  # compiles at run time, under a name of its own.

  defp unique(name),
    do: "Decree.Commands.RouterIdentityOptionsTest.#{name}#{System.unique_integer([:positive])}"

  defp compile(source), do: Code.compile_string(source)

  # A command module that adopts `Decree.Command`, compiled before the router that names it.
  defp adopting_command do
    name = unique("Adopting")

    compile("""
    defmodule #{name} do
      use Decree.Command

      defstruct [:id, :command_id]
    end
    """)

    name
  end

  defp plain_command do
    name = unique("Plain")

    compile("""
    defmodule #{name} do
      defstruct [:id, :command_id]
    end
    """)

    name
  end

  defp aggregate do
    name = unique("Aggregate")

    compile("""
    defmodule #{name} do
      defstruct [:id]

      def execute(_state, _command), do: []
      def apply(state, _event), do: state
    end
    """)

    name
  end

  defp router(use_opts, body) do
    compile("""
    defmodule #{unique("Router")} do
      use Decree.Commands.Router#{use_opts}

      #{body}
    end
    """)
  end

  describe "`require_decree_command: true`" do
    test "refuses a plain struct when the router compiles" do
      command = plain_command()
      aggregate = aggregate()

      assert_raise ArgumentError, ~r/#{command}.*Decree\.Command/s, fn ->
        router(
          ", require_decree_command: true",
          "dispatch #{command}, to: #{aggregate}, identity: :id"
        )
      end
    end

    test "accepts a `Decree.Command`" do
      command = adopting_command()
      aggregate = aggregate()

      assert [{_router, _binary}] =
               router(
                 ", require_decree_command: true",
                 "dispatch #{command}, to: #{aggregate}, identity: :id"
               )
    end

    test "refuses a plain struct on a decision route too" do
      command = plain_command()

      assert_raise ArgumentError, ~r/#{command}.*Decree\.Command/s, fn ->
        router(
          ", require_decree_command: true",
          """
          decide #{command},
            to: Decree.DecisionFixtures.ScriptedDecision,
            identity: :id,
            identity_prefix: "scripted-"
          """
        )
      end
    end
  end

  describe "the default" do
    test "accepts a plain struct" do
      command = plain_command()
      aggregate = aggregate()

      assert [{_router, _binary}] =
               router("", "dispatch #{command}, to: #{aggregate}, identity: :id")
    end

    test "is the same as `require_decree_command: false`" do
      command = plain_command()
      aggregate = aggregate()

      assert [{_router, _binary}] =
               router(
                 ", require_decree_command: false",
                 "dispatch #{command}, to: #{aggregate}, identity: :id"
               )
    end
  end

  describe "`opaque:`" do
    test "a route's `opaque:` on an identified aggregate is refused" do
      command = plain_command()
      aggregate = aggregate()

      for value <- ["true", "false"] do
        assert_raise ArgumentError, ~r/opaque/, fn ->
          router("", """
          identify #{aggregate}, by: :id

          dispatch #{command}, to: #{aggregate}, opaque: #{value}
          """)
        end
      end
    end

    test "is refused when the `identify` follows the route" do
      command = plain_command()
      aggregate = aggregate()

      assert_raise ArgumentError, ~r/opaque/, fn ->
        router("", """
        dispatch #{command}, to: #{aggregate}, identity: :id, opaque: true

        identify #{aggregate}, by: :id
        """)
      end
    end

    test "a route with no `identify` takes it" do
      command = plain_command()
      aggregate = aggregate()

      assert [{_router, _binary}] =
               router("", "dispatch #{command}, to: #{aggregate}, identity: :id, opaque: true")
    end

    test "an `identify` takes it" do
      command = plain_command()
      aggregate = aggregate()

      assert [{_router, _binary}] =
               router("", """
               identify #{aggregate}, by: :id, opaque: true

               dispatch #{command}, to: #{aggregate}
               """)
    end

    for value <- [":yes", "nil", ~s("true"), "1"] do
      test "an `identify` refuses `opaque: #{value}`" do
        command = plain_command()
        aggregate = aggregate()

        assert_raise RuntimeError, ~r/invalid `opaque` option/, fn ->
          router("", """
          identify #{aggregate}, by: :id, opaque: #{unquote(value)}

          dispatch #{command}, to: #{aggregate}
          """)
        end
      end

      test "a route refuses `opaque: #{value}`" do
        command = plain_command()
        aggregate = aggregate()

        assert_raise ArgumentError, ~r/invalid `opaque` option/, fn ->
          router(
            "",
            "dispatch #{command}, to: #{aggregate}, identity: :id, opaque: #{unquote(value)}"
          )
        end
      end
    end

    test "an `identify` of a decision refuses `opaque: true`" do
      command = plain_command()

      assert_raise ArgumentError, ~r/opaque/, fn ->
        router("", """
        identify Decree.DecisionFixtures.ScriptedDecision,
          by: :id,
          prefix: "scripted-",
          opaque: true

        decide #{command}, to: Decree.DecisionFixtures.ScriptedDecision
        """)
      end
    end

    test "an `identify` of a decision takes `opaque: false`" do
      command = plain_command()

      assert [{_router, _binary}] =
               router("", """
               identify Decree.DecisionFixtures.ScriptedDecision,
                 by: :id,
                 prefix: "scripted-",
                 opaque: false

               decide #{command}, to: Decree.DecisionFixtures.ScriptedDecision
               """)
    end

    test "`decide` takes no `opaque:`" do
      command = plain_command()

      assert_raise RuntimeError, ~r/unexpected decide parameter "opaque"/, fn ->
        router("", """
        decide #{command},
          to: Decree.DecisionFixtures.ScriptedDecision,
          identity: :id,
          identity_prefix: "scripted-",
          opaque: true
        """)
      end
    end
  end
end
