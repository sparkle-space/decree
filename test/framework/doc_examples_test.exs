defmodule Decree.DocExamplesTest do
  # The examples in four moduledocs copied from Commanded, read from their compiled docs. They
  # name the decree application `MyApp.App`, apart from a consumer's OTP application module,
  # `MyApp.Application`, which only `Decree.Projections.Ecto` names, as that module. Every
  # module and config example compiles or evaluates as written. A fragment that needs a running
  # store compiles, without running, as the body of a function that takes the caller's
  # variables. A compile reports no warning. `MyApp` is renamed, so the README test's `MyApp`
  # modules in the same VM are left alone.
  use ExUnit.Case, async: false

  alias Decree.DocBlocks

  defmodule Example.Router do
    use Decree.Commands.Router
  end

  defmodule Example.Accounts.Router do
    use Decree.Commands.Router
  end

  defmodule Example.Billing.Router do
    use Decree.Commands.Router
  end

  defmodule Example.Notifications.Router do
    use Decree.Commands.Router
  end

  @namespace inspect(Example)
  @app Module.concat([Example, App])

  setup do
    # Several examples define the same application module.
    ignore = Code.get_compiler_option(:ignore_module_conflict)
    Code.put_compiler_option(:ignore_module_conflict, true)
    on_exit(fn -> Code.put_compiler_option(:ignore_module_conflict, ignore) end)

    :ok
  end

  defp rename(block), do: String.replace(block, "MyApp", @namespace)

  defp compile!(block) do
    {modules, diagnostics} = Code.with_diagnostics(fn -> Code.compile_string(rename(block)) end)
    assert diagnostics == []
    Enum.map(modules, fn {module, _bytecode} -> module end)
  end

  # Each fragment that opens with `first_line` as the body of `run/n`, compiled and never
  # called; answers the first. `occurrences` is how many lines in the doc open with
  # `first_line`, so a regressed example cannot fall through to a later block that opens the
  # same way.
  defp compile_fragment!(doc, first_line, variables, occurrences) do
    assert [block | _later] = blocks = DocBlocks.blocks(doc, first_line)
    assert length(blocks) == occurrences

    for fragment <- blocks do
      compile!("""
      defmodule MyApp.Fragment do
        def run(#{Enum.join(variables, ", ")}) do
      #{fragment}
        end
      end
      """)
    end

    block
  end

  defp config!(block),
    do: Config.Reader.eval!("config/config.exs", "import Config\n" <> rename(block))

  test "only Decree.Projections.Ecto's docs name MyApp.Application, as the OTP application" do
    {:ok, modules} = :application.get_key(:decree, :modules)

    naming =
      for module <- modules,
          {:docs_v1, _anno, _language, _format, moduledoc, _metadata, entries} <-
            [Code.fetch_docs(module)],
          doc <- [moduledoc | Enum.map(entries, &elem(&1, 3))],
          match?(%{"en" => text} when is_binary(text), doc),
          doc["en"] =~ "MyApp.Application",
          uniq: true,
          do: module

    assert naming == [Decree.Projections.Ecto]
  end

  test "Decree.Application's examples compile, and its config evaluates, as written" do
    doc = DocBlocks.doc!(Decree.Application)

    # One router, the inline config, three routers, the dispatch defaults, the personal fields.
    blocks = DocBlocks.blocks(doc, "defmodule MyApp.App do")
    assert length(blocks) == 5

    for block <- blocks do
      assert [@app] == compile!(block)
      assert function_exported?(@app, :dispatch, 2)
    end

    # Compiled from the doc at run time, so called through apply/3.
    assert apply(@app, :__key_source__, []) == Module.concat([Example, SubjectKeys])
    assert apply(@app, :__personal_metadata_keys__, []) == [:email, :password_hash]

    assert [my_app: [{@app, config}]] =
             config!(DocBlocks.block!(doc, "# config/config.exs"))

    assert config == [
             event_store: [event_store: Module.concat([Example, EventStore])],
             registry: :local
           ]

    # The tenant loop's first line opens the standalone example and recurs inside `children =`.
    for {first_line, variables, occurrences} <- [
          {"{:ok, _pid} = MyApp.App.start_link()", [], 1},
          {"Supervisor.start_link([", [], 1},
          {":ok = MyApp.App.dispatch(command, opts)", ["command", "opts"], 1},
          {"for tenant <- [:tenant1, :tenant2, :tenant3] do", [], 2},
          {"children =", [], 1},
          {":ok = MyApp.App.dispatch(command, application: :tenant1)", ["command"], 1}
        ] do
      assert compile_fragment!(doc, first_line, variables, occurrences) =~ "MyApp.App"
    end
  end

  test "Decree.Aggregates.Aggregate's snapshotting config evaluates as written" do
    doc = DocBlocks.doc!(Decree.Aggregates.Aggregate)

    assert [my_app: [{@app, [snapshotting: snapshotting]}]] =
             config!(DocBlocks.block!(doc, "config :my_app, MyApp.App,"))

    assert snapshotting == %{
             Module.concat([Example, ExampleAggregate]) => [
               snapshot_every: 10,
               snapshot_version: 1
             ]
           }
  end

  test "Decree.Event.Handler's examples compile as written" do
    # The application from `Decree.Application`'s first example, and the README's event.
    application = DocBlocks.doc!(Decree.Application)
    assert [@app] == compile!(DocBlocks.block!(application, "defmodule MyApp.App do"))

    compile!("""
    defmodule MyApp.AccountRegistered do
      defstruct [:account_id, :handle]
    end
    """)

    doc = DocBlocks.doc!(Decree.Event.Handler)

    # The handlers, compiled before the fragments that start them.
    modules = [
      {"defmodule MyApp.ExampleHandler do", 4},
      {"defmodule MyApp.StatefulEventHandler do", 1},
      {"defmodule MyApp.ConcurrentProcessingEventHandler do", 1},
      {"defmodule MyApp.OrderProjector do", 1}
    ]

    for {first_line, occurrences} <- modules do
      blocks = DocBlocks.blocks(doc, first_line)
      assert length(blocks) == occurrences

      for block <- blocks do
        assert [module] = compile!(block)
        assert function_exported?(module, :start_link, 1)
      end
    end

    # The tenant loop's first line opens the standalone example and recurs inside `children =`.
    fragments = [
      {"{:ok, _handler} = MyApp.ExampleHandler.start_link()", [], 1},
      {"{:ok, _handler} = MyApp.ExampleHandler.start_link(start_from: :current)", [], 1},
      {"{:ok, _pid} =", [], 1},
      {"Supervisor.start_link([", [], 1},
      {"MyApp.ExampleHandler.start_link(state: initial_state)", ["initial_state"], 1},
      {"Supervisor.start_link([{MyApp.ExampleHandler, state: initial_state}], strategy: :one_for_one)",
       ["initial_state"], 1},
      {"for tenant <- [:tenant1, :tenant2, :tenant3] do", [], 2},
      {"children =", [], 1}
    ]

    for {first_line, variables, occurrences} <- fragments do
      assert compile_fragment!(doc, first_line, variables, occurrences) =~
               "MyApp.ExampleHandler"
    end

    assert DocBlocks.block!(doc, "for tenant <- [:tenant1, :tenant2, :tenant3] do") =~
             "MyApp.App.start_link(name: tenant)"

    # A `use` alone, compiled as a module's body. Its first line opens the `use` in each of the
    # seven handlers before it too.
    opaque = "use Decree.Event.Handler,"
    blocks = DocBlocks.blocks(doc, opaque)
    assert length(blocks) == 8
    assert [module] = compile!("defmodule MyApp.OpaqueHandler do\n#{List.last(blocks)}end\n")
    assert apply(module, :__personal_fields__, []) == :opaque

    # Every code block in the moduledoc opens with one of these.
    selectors = Enum.map(modules ++ fragments, &elem(&1, 0)) ++ [opaque]
    first_lines = DocBlocks.first_lines(doc)

    assert length(first_lines) == 16
    assert Enum.reject(first_lines, &(&1 in selectors)) == []
  end

  test "Decree.PersonalFields.KeySource's application compiles as written" do
    doc = DocBlocks.doc!(Decree.PersonalFields.KeySource)

    assert [@app] == compile!(DocBlocks.block!(doc, "defmodule MyApp.App do"))
    assert apply(@app, :__key_source__, []) == Module.concat([Example, SubjectKeys])
  end
end
