defmodule Decree.DocExamplesTest do
  # The examples in four moduledocs copied from Commanded, read from their compiled docs. They
  # name the decree application `MyApp.App`, apart from a consumer's OTP application module,
  # `MyApp.Application`, which only `Decree.Projections.Ecto` names, as that module. Every module and config example compiles or evaluates as written; a
  # fragment that needs a running store or a caller's variables is parsed. `MyApp` is renamed,
  # so the README test's `MyApp` modules in the same VM are left alone.
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
    block
    |> rename()
    |> Code.compile_string()
    |> Enum.map(fn {module, _bytecode} -> module end)
  end

  defp config!(block),
    do: Config.Reader.eval!("config/config.exs", "import Config\n" <> rename(block))

  defp parse!(doc, first_line) do
    block = DocBlocks.block!(doc, first_line)
    assert {:ok, _quoted} = Code.string_to_quoted(block)
    block
  end

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

    for first_line <- [
          "{:ok, _pid} = MyApp.App.start_link()",
          "Supervisor.start_link([",
          ":ok = MyApp.App.dispatch(command, opts)",
          "for tenant <- [:tenant1, :tenant2, :tenant3] do",
          "children =",
          ":ok = MyApp.App.dispatch(command, application: :tenant1)"
        ] do
      assert parse!(doc, first_line) =~ "MyApp.App"
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
    doc = DocBlocks.doc!(Decree.Event.Handler)
    block = parse!(doc, "for tenant <- [:tenant1, :tenant2, :tenant3] do")

    assert block =~ "MyApp.App.start_link(name: tenant)"
  end

  test "Decree.PersonalFields.KeySource's application compiles as written" do
    doc = DocBlocks.doc!(Decree.PersonalFields.KeySource)

    assert [@app] == compile!(DocBlocks.block!(doc, "defmodule MyApp.App do"))
    assert apply(@app, :__key_source__, []) == Module.concat([Example, SubjectKeys])
  end
end
