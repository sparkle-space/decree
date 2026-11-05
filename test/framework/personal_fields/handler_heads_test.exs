defmodule Decree.PersonalFields.HandlerHeadsTest do
  use ExUnit.Case

  import ExUnit.CaptureIO

  @registered "Decree.PersonalFields.Fixtures.Events.Registered"
  @plain "Decree.PersonalFields.Fixtures.Events.Plain"

  defp module_name,
    do: "Decree.PersonalFields.HandlerHeadsTest.H#{System.unique_integer([:positive])}"

  defp compile(body, options \\ "") do
    name = module_name()

    compile_quietly(name, """
    defmodule #{name} do
      use Decree.Event.Handler, application: Decree.DefaultApp, name: "#{name}"#{options}
      #{body}
    end
    """)
  end

  # Some heads here are unreachable on purpose, and the compiler says so.
  defp compile_quietly(name, source) do
    {_modules, _warnings} = with_io(:stderr, fn -> Code.compile_string(source) end)

    Module.concat([name])
  end

  describe "the heads it reads" do
    for {name, body, expected} <- [
          {"a bare struct", "def handle(%#{@registered}{}, _metadata), do: :ok", [@registered]},
          {"a struct on the left of `=`",
           "def handle(%#{@registered}{} = event, _metadata), do: {:ok, event}", [@registered]},
          {"a struct on the right of `=`",
           "def handle(event = %#{@registered}{}, _metadata), do: {:ok, event}", [@registered]},
          {"a struct under two `=`",
           "def handle(%#{@registered}{} = a = b, _metadata), do: {:ok, a, b}", [@registered]},
          {"a struct with a pattern in it",
           ~s(def handle(%#{@registered}{note: "x"}, _metadata\), do: :ok), [@registered]},
          {"a guarded struct",
           "def handle(%#{@registered}{note: note}, _m) when is_binary(note), do: :ok",
           [@registered]},
          {"a struct beside bound metadata",
           "def handle(%#{@registered}{}, metadata), do: {:ok, metadata}", [@registered]},
          {"two structs and a trailing catch-all",
           """
           def handle(%#{@registered}{}, _metadata), do: :ok
           def handle(%#{@plain}{}, _metadata), do: :ok
           def handle(_event, _metadata), do: :ok
           """, [@registered, @plain]},
          {"a catch-all alone", "def handle(_, _), do: :ok", []},
          {"a catch-all whose body rebinds its event name in an fn",
           "def handle(_event, _metadata), do: (fn _event -> :ok end).(:inner)", []},
          {"a catch-all whose body rebinds its metadata name in a case",
           "def handle(_event, _metadata), do: (case :x do _metadata -> :ok end)", []},
          {"no handle/2", "", []}
        ] do
      test "reads #{name}" do
        expected = Map.new(unquote(expected), &{Module.concat([&1]), true})

        assert compile(unquote(body)).__personal_fields__() == expected
      end
    end

    test "reads a projector's `project`" do
      name = module_name()

      module =
        compile_quietly(name, """
        defmodule #{name} do
          use Decree.Projections.Ecto,
            application: Decree.DefaultApp,
            name: "#{name}",
            repo: Decree.Projections.Repo

          project %#{@registered}{}, fn multi -> multi end
          project %#{@plain}{}, metadata, fn multi -> {multi, metadata} end
        end
        """)

      assert module.__personal_fields__() == %{
               Module.concat([@registered]) => true,
               Module.concat([@plain]) => true
             }
    end

    test "a projector passes `personal_fields: :opaque` through its options" do
      name = module_name()

      module =
        compile_quietly(name, """
        defmodule #{name} do
          use Decree.Projections.Ecto,
            application: Decree.DefaultApp,
            name: "#{name}",
            repo: Decree.Projections.Repo,
            personal_fields: :opaque

          def handle(event, metadata), do: update_projection(event, metadata, & &1)
        end
        """)

      assert module.__personal_fields__() == :opaque
    end
  end

  describe "a head it can't read" do
    for {name, body} <- [
          {"a generic head", "def handle(event, _metadata), do: {:ok, event}"},
          {"a bound first argument beside a struct head",
           """
           def handle(%#{@registered}{}, _metadata), do: :ok
           def handle(event, _metadata), do: {:ok, event}
           """},
          {"an ignored event with bound metadata",
           "def handle(_event, metadata), do: {:ok, metadata}"},
          {"an ignored event with matched metadata",
           ~s(def handle(_event, %{"email" => email}\), do: {:ok, email})},
          {"a variable struct",
           "def handle(%module{} = event, _m) when module in [#{@registered}], do: {:ok, event}"},
          {"an ignored struct", "def handle(%_{} = event, _m), do: {:ok, event}"},
          {"a map", "def handle(%{note: note}, _m), do: {:ok, note}"},
          {"a catch-all that reads its event",
           "def handle(_event, _metadata), do: {:ok, _event}"},
          {"a catch-all that reads its metadata",
           "def handle(_event, _metadata), do: {:ok, _metadata}"},
          {"a guarded catch-all", "def handle(_event, _m) when 1 + 1 == 3, do: :ok"},
          {"a catch-all that is not trailing",
           """
           def handle(_event, %{a: 1}), do: :ok
           def handle(_event, _metadata), do: :ok
           """},
          {"a catch-all ahead of a struct",
           """
           def handle(_event, _metadata), do: :ok
           def handle(%#{@registered}{}, _metadata), do: :ok
           """},
          {"a `handle_batch/1`", "def handle_batch(events), do: {:ok, events}"}
        ] do
      test "#{name} fails to compile, naming the module and line" do
        error = assert_raise CompileError, fn -> compile(unquote(body)) end

        assert error.description =~ "Decree.PersonalFields.HandlerHeadsTest.H"
        assert error.line >= 3
        assert error.description =~ "line #{error.line}"
      end

      test "#{name} compiles with `:opaque`" do
        assert compile(unquote(body), ", personal_fields: :opaque").__personal_fields__() ==
                 :opaque
      end
    end
  end

  describe "the option" do
    test "an invalid value fails to compile" do
      assert_raise CompileError, ~r/the only value it takes is `:opaque`/, fn ->
        compile("def handle(_event, _metadata), do: :ok", ", personal_fields: :nope")
      end

      assert_raise CompileError, ~r/the only value it takes is `:opaque`/, fn ->
        compile("def handle(_event, _metadata), do: :ok", ~s(, personal_fields: "opaque"))
      end
    end

    test "is accepted by `start_link/1` and does not reach the handler's config" do
      module = compile("def handle(_event, _metadata), do: :ok", ", personal_fields: :opaque")

      assert {Decree.DefaultApp, _name, config} =
               Decree.Event.Handler.parse_config!(module,
                 application: Decree.DefaultApp,
                 name: "n",
                 personal_fields: :opaque
               )

      refute Keyword.has_key?(config, :personal_fields)
    end
  end
end
