defmodule Decree.Event.HandleHeadsTest do
  use ExUnit.Case

  import ExUnit.CaptureIO

  @registered Decree.PersonalFields.Fixtures.Events.Registered
  @plain Decree.PersonalFields.Fixtures.Events.Plain

  defp module_name,
    do: "Decree.Event.HandleHeadsTest.H#{System.unique_integer([:positive])}"

  defp compile(body, options \\ "") do
    name = module_name()

    compile_quietly(name, """
    defmodule #{name} do
      use Decree.Event.Handler, application: Decree.DefaultApp, name: "#{name}"#{options}
    #{body}
    end
    """)
  end

  # Some heads here are unreachable on purpose, and the compiler says so. Debug info is kept,
  # which the test run otherwise drops, so the compiled clauses can be read back.
  defp compile_quietly(name, source) do
    module = Module.concat([name])
    debug_info = Code.get_compiler_option(:debug_info)
    Code.put_compiler_option(:debug_info, true)

    try do
      {modules, _warnings} = with_io(:stderr, fn -> Code.compile_string(source) end)
      {^module, binary} = List.keyfind(modules, module, 0)

      {module, binary}
    after
      Code.put_compiler_option(:debug_info, debug_info)
    end
  end

  # The compiled `handle/2` clauses, read back from the module's debug info: what decree's
  # `__before_compile__` saw, plus its own catch-all as the last clause.
  defp compiled_handle_clauses(module, binary) do
    {:ok, {^module, [debug_info: {:debug_info_v1, backend, data}]}} =
      :beam_lib.chunks(binary, [:debug_info])

    {:ok, %{definitions: definitions}} = backend.debug_info(:elixir_v1, module, data, [])

    {{:handle, 2}, :def, _meta, clauses} = List.keyfind(definitions, {:handle, 2}, 0)

    clauses
  end

  defp head({meta, [event, _metadata], guards, _body}), do: {meta[:line], event, guards}

  describe "a handler's `handle/2` clauses" do
    test "come back one per clause, in source order, with their lines and guards" do
      {module, binary} =
        compile("""
          def handle(%#{inspect(@registered)}{note: note}, _metadata) when is_binary(note),
            do: :ok

          def handle(%#{inspect(@registered)}{} = event, _metadata)
              when is_atom(event) or is_nil(event)
              when is_map(event),
              do: :ok

          def handle(%#{inspect(@plain)}{}, _metadata), do: :ok
          def handle(_event, _metadata), do: :ok
        """)

      heads = module.__handle_heads__()

      assert [
               {3, {:%, _, [@registered, {:%{}, _, [note: {:note, _, _}]}]},
                [{{:., _, [:erlang, :is_binary]}, _, [{:note, _, _}]}]},
               {6, {:=, _, [{:%, _, [@registered, {:%{}, _, []}]}, {:event, _, _}]},
                [_or_guard, {{:., _, [:erlang, :is_map]}, _, [{:event, _, _}]}]},
               {11, {:%, _, [@plain, {:%{}, _, []}]}, []},
               {12, {:_event, _, _}, []}
             ] = heads

      # The raw expanded clauses: every user-written clause and nothing else.
      clauses = compiled_handle_clauses(module, binary)
      assert heads == clauses |> Enum.drop(-1) |> Enum.map(&head/1)
    end

    test "include a clause injected by `use`, beside the module's own" do
      injector = module_name()

      compile_quietly(injector, """
      defmodule #{injector} do
        defmacro __using__(_options) do
          quote do
            def handle(%#{inspect(@registered)}{} = event, _metadata), do: {:ok, event}
          end
        end
      end
      """)

      {module, binary} =
        compile("""
          use #{injector}

          def handle(%#{inspect(@plain)}{}, _metadata), do: :ok
        """)

      assert [
               {_injected_line, {:=, _, [{:%, _, [@registered, _]}, {:event, _, _}]}, []},
               {5, {:%, _, [@plain, _]}, []}
             ] = module.__handle_heads__()

      assert module.__handle_heads__() ==
               module |> compiled_handle_clauses(binary) |> Enum.drop(-1) |> Enum.map(&head/1)
    end

    test "include a clause injected by `use` in a module with no `handle/2` of its own" do
      injector = module_name()

      compile_quietly(injector, """
      defmodule #{injector} do
        defmacro __using__(_options) do
          quote do
            def handle(event, _metadata), do: {:ok, event}
          end
        end
      end
      """)

      {module, _binary} = compile("use #{injector}", ", personal_fields: :opaque")

      assert [{_line, {:event, _, _}, []}] = module.__handle_heads__()
    end

    test "include a projector's `project` clauses" do
      name = module_name()

      {module, _binary} =
        compile_quietly(name, """
        defmodule #{name} do
          use Decree.Projections.Ecto,
            application: Decree.DefaultApp,
            name: "#{name}",
            repo: Decree.Projections.Repo

          project %#{inspect(@registered)}{}, fn multi -> multi end
          project %#{inspect(@plain)}{}, metadata, fn multi -> {multi, metadata} end
        end
        """)

      assert [
               {7, {:=, _, [{:%, _, [@registered, _]}, {:event, _, _}]}, []},
               {8, {:=, _, [{:%, _, [@plain, _]}, _event]}, []}
             ] = module.__handle_heads__()
    end
  end

  describe "decree's own clauses" do
    test "a handler with no `handle/2` answers no heads" do
      {module, binary} = compile("")

      assert module.__handle_heads__() == []
      assert [_catch_all] = compiled_handle_clauses(module, binary)
    end

    test "decree's catch-all is never a head" do
      {module, binary} = compile("  def handle(%#{inspect(@plain)}{}, _metadata), do: :ok")

      assert [{3, {:%, _, [@plain, _]}, []}] = module.__handle_heads__()
      assert [_user, _catch_all] = compiled_handle_clauses(module, binary)
    end
  end

  describe "a `handle_batch/1` handler" do
    test "answers no heads" do
      {module, _binary} =
        compile(
          "  def handle_batch(events), do: {:ok, events}",
          ", batch_size: 2, personal_fields: :opaque"
        )

      assert module.__handle_heads__() == []
    end
  end

  describe "an `:opaque` handler" do
    test "still answers its heads, a generic one included" do
      {module, binary} =
        compile(
          """
            def handle(%#{inspect(@registered)}{}, _metadata), do: :ok
            def handle(event, metadata) when is_map(metadata), do: {:ok, event}
          """,
          ", personal_fields: :opaque"
        )

      assert module.__personal_fields__() == :opaque

      assert [
               {3, {:%, _, [@registered, _]}, []},
               {4, {:event, _, _}, [{{:., _, [:erlang, :is_map]}, _, [{:metadata, _, _}]}]}
             ] = module.__handle_heads__()

      assert module.__handle_heads__() ==
               module |> compiled_handle_clauses(binary) |> Enum.drop(-1) |> Enum.map(&head/1)
    end
  end
end
