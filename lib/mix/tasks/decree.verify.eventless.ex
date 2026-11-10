defmodule Mix.Tasks.Decree.Verify.Eventless do
  @shortdoc "Fails on any routed handler arm that accepts a command without an event"

  @moduledoc """
  Fails on any routed handler arm that can accept a command without appending an event.

      mix decree.verify.eventless --router MyApp.Router [--router MyApp.OtherRouter] [--no-compile]

  An aggregate handler answering `[]`, `nil`, `:ok`, `{:ok, []}` or `{:ok, nil}` is
  accepted with no events; this task reports every routed arm that can answer one, and a
  decision's `decide/2` answering those or `{:streams, []}`. There is no allow-list and no
  waiver. The walk, its shapes, what it follows, what it fails closed on and its stated
  residuals are documented on `Decree.Verify.Eventless`.

  ## Population

  Each `--router` (a `Decree.Commands.Router`, a `Decree.Commands.CompositeRouter` or a
  `Decree.Application`, at least one, repeatable) is read through its registry:
  `__registered_commands__/0` and, for each command, `__route__/1`. A decision route walks
  the decision's `decide/2`; any other route walks the `handler:` module's `function:` (an
  aggregate's `execute/2`, a `to: Handler, aggregate: Agg` route's `handle/2`, or the
  route's `function:`). Routes sharing a module and function are walked once.

  The sources are every `.ex` file under the project's `elixirc_paths`. A receiver the
  walk cannot find there is claimed as internal, and reported unclassified, when it is
  one of the project application's own modules (`Application.spec(app, :modules)`);
  anything else, a dependency included, is external.

  Unless `--no-compile` is given the task runs `mix compile` first. It starts no
  application and reads no database.
  """

  use Mix.Task

  alias Decree.Verify.Eventless

  @usage "usage: mix decree.verify.eventless --router MODULE [--router MODULE ...] [--no-compile]"

  @impl Mix.Task
  def run(args) do
    {opts, rest} = OptionParser.parse!(args, strict: [router: :keep, compile: :boolean])

    if rest != [], do: Mix.raise("unexpected arguments #{inspect(rest)}. " <> @usage)

    routers = Keyword.get_values(opts, :router)
    if routers == [], do: Mix.raise("--router is required. " <> @usage)

    if Keyword.get(opts, :compile, true), do: Mix.Task.run("compile")

    config = Mix.Project.config()
    root = File.cwd!()

    entries = routers |> Enum.flat_map(&entries/1) |> Enum.uniq_by(&{&1.module, &1.fun})

    result =
      Eventless.lint(entries,
        root: root,
        paths: Keyword.get(config, :elixirc_paths, ["lib"]),
        internal?: internal_predicate(config[:app])
      )

    case result do
      {:ok, %{modules: modules, entries: walked}} ->
        Mix.shell().info(
          "decree.verify.eventless: OK - #{walked} handler clause(s) in #{modules} " <>
            "routed module(s), no eventless or unclassified tail"
        )

      {:error, findings} ->
        Mix.shell().error("decree.verify.eventless: #{length(findings)} finding(s):")
        Enum.each(findings, &Mix.shell().error("  " <> &1.message))

        Mix.raise(
          "decree.verify.eventless failed. An accepted command appends an event; a " <>
            "recognised repeat answers {:error, :duplicate_command}. There is no allow-list."
        )
    end
  end

  defp entries(name) do
    router = Module.concat([name])

    unless Code.ensure_loaded?(router) and function_exported?(router, :__registered_commands__, 0) and
             function_exported?(router, :__route__, 1) do
      Mix.raise(
        "#{name} is not a router: it exports no __registered_commands__/0 and __route__/1"
      )
    end

    for command <- router.__registered_commands__() do
      route =
        router.__route__(command) || Mix.raise("#{name} has no route for #{inspect(command)}")

      cond do
        route[:decision] ->
          %{module: route[:decision], fun: :decide, kind: :decision}

        route[:handler] && route[:function] ->
          %{module: route[:handler], fun: route[:function], kind: :aggregate}

        true ->
          Mix.raise(
            "the route of #{inspect(command)} in #{name} names no handler and function: " <>
              "#{inspect(route)}"
          )
      end
    end
  end

  # The project application's own modules, as its `.app` lists them.
  defp internal_predicate(app) do
    case Application.load(app) do
      :ok ->
        :ok

      {:error, {:already_loaded, ^app}} ->
        :ok

      {:error, reason} ->
        Mix.raise("cannot load the #{inspect(app)} application: #{inspect(reason)}")
    end

    modules = app |> Application.spec(:modules) |> List.wrap() |> MapSet.new()
    &MapSet.member?(modules, &1)
  end
end
