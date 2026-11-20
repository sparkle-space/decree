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

  # The fragment as the body of `run/n`, compiled and never called. `occurrences` is how many
  # lines in the doc open with `first_line`, so a regressed example cannot fall through to a
  # later block that opens the same way.
  defp compile_fragment!(doc, first_line, variables, occurrences) do
    assert [block | _later] = blocks = DocBlocks.blocks(doc, first_line)
    assert length(blocks) == occurrences

    compile!("""
    defmodule MyApp.Fragment do
      def run(#{Enum.join(variables, ", ")}) do
    #{block}
      end
    end
    """)

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

  test "Decree.Event.Handler's multi-tenant example starts the named decree applications" do
    # The application from `Decree.Application`'s first example, and a handler for the doc's
    # `ExampleHandler`.
    application = DocBlocks.doc!(Decree.Application)
    assert [@app] == compile!(DocBlocks.block!(application, "defmodule MyApp.App do"))

    compile!("""
    defmodule MyApp.ExampleHandler do
      use Decree.Event.Handler, application: MyApp.App, name: "ExampleHandler"
    end
    """)

    doc =
      Decree.Event.Handler
      |> DocBlocks.doc!()
      |> String.replace("ExampleHandler.", "MyApp.ExampleHandler.")

    # Its first line opens the standalone example and recurs inside `children =`.
    block = compile_fragment!(doc, "for tenant <- [:tenant1, :tenant2, :tenant3] do", [], 2)

    assert block =~ "MyApp.App.start_link(name: tenant)"
  end

  test "Decree.PersonalFields.KeySource's application compiles as written" do
    doc = DocBlocks.doc!(Decree.PersonalFields.KeySource)

    assert [@app] == compile!(DocBlocks.block!(doc, "defmodule MyApp.App do"))
    assert apply(@app, :__key_source__, []) == Module.concat([Example, SubjectKeys])
  end
end
