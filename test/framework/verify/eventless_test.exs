defmodule Decree.Verify.EventlessTest do
  @moduledoc """
  The walker behind `mix decree.verify.eventless` (sparkle-space/sparklespace#7865). Each
  case writes its handler modules (and the helpers they call) under a temp root and names
  the routed entries explicitly, as the task does from a router's registry.
  """

  # Every case reads only its own temp root.
  use ExUnit.Case, async: true

  alias Decree.Verify.Eventless

  @aggregate_file "lib/fixture/aggregates/thing.ex"
  @thing Fixture.Aggregates.Thing

  defp tmp_root do
    root =
      Path.join(
        System.tmp_dir!(),
        "decree_verify_eventless_test_#{:erlang.unique_integer([:positive])}"
      )

    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    root
  end

  defp write!(root, relative, source) do
    path = Path.join(root, relative)
    path |> Path.dirname() |> File.mkdir_p!()
    File.write!(path, source)
  end

  defp internal?(module), do: String.starts_with?(inspect(module), "Fixture.")

  defp lint(root, entries),
    do: Eventless.lint(entries, root: root, paths: ["lib"], internal?: &internal?/1)

  defp entry(module, fun \\ :execute, kind \\ :aggregate),
    do: %{module: module, fun: fun, kind: kind}

  # Lints `Fixture.Aggregates.Thing`, written to `lib/fixture/aggregates/thing.ex`, routed
  # by `entries` (its `execute/2` by default), plus any extra `{relative_path, source}`.
  defp lint_aggregate(source, extra \\ [], entries \\ [entry(@thing)]) do
    root = tmp_root()
    write!(root, @aggregate_file, source)
    Enum.each(extra, fn {relative, extra_source} -> write!(root, relative, extra_source) end)
    lint(root, entries)
  end

  defp findings({:error, findings}),
    do: Enum.map(findings, &{&1.kind, &1.shape, &1.file, &1.line})

  defp findings({:ok, _counts}), do: []

  defp shapes(result),
    do: result |> findings() |> Enum.map(fn {kind, shape, _, _} -> {kind, shape} end)

  describe "lint/2 — RED: the eventless shapes" do
    test "a `[]` on its own line, with its own line number" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(_state, _command) do
            _ignored = 1

            []
          end
        end
        """)

      assert [{:eventless, "[]", @aggregate_file, 5}] = findings(result)
    end

    test "a `do: []` keyword body" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(_state, _command), do: []
        end
        """)

      assert [{:eventless, "[]"}] = shapes(result)
    end

    test "`nil`, `:ok` and `{:ok, []}`, one clause each" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(%{kind: :a}, _command), do: nil
          def execute(%{kind: :b}, _command), do: :ok
          def execute(%{kind: :c}, _command), do: {:ok, []}
        end
        """)

      assert Enum.sort(shapes(result)) ==
               Enum.sort([{:eventless, "nil"}, {:eventless, ":ok"}, {:eventless, "{:ok, []}"}])
    end

    test "a `nil` on the left of `&&` is returned, so it is a finding" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(_state, _command), do: nil && [%{ev: 1}]
        end
        """)

      assert [{:eventless, "nil"}] = shapes(result)
    end

    test "a truthy eventless value on the left of `||` is returned, a `nil` there is not" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(%{kind: :a}, _command), do: [] || [%{ev: 1}]
          def execute(_state, _command), do: nil || [%{ev: 2}]
        end
        """)

      assert [{:eventless, "[]"}] = shapes(result)
    end

    test "an `if` with no `else` is an implicit nil" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(state, _command) do
            if state.active, do: %{event: true}
          end
        end
        """)

      assert [{:eventless, "if without else (implicit nil)"}] = shapes(result)
    end

    test "a `case` branch" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(state, _command) do
            case state.status do
              "active" -> %{event: true}
              _other -> []
            end
          end
        end
        """)

      assert [{:eventless, "[]"}] = shapes(result)
    end

    test "a `with` `else` clause" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(state, _command) do
            with :ok <- check(state) do
              %{event: true}
            else
              {:error, :already_so} -> []
              {:error, _} = error -> error
            end
          end

          defp check(_state), do: :ok
        end
        """)

      assert [{:eventless, "[]"}] = shapes(result)
    end

    test "a `with` with no `else` falls through a literal its pattern cannot match" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(%{kind: :a}, _command), do: with(:ok <- nil, do: [%{ev: 1}])

          def execute(%__MODULE__{}, _cmd),
            do: (with :ok <- nil, do: [%{ev: 1}]) && [%{ev: 2}]
        end
        """)

      assert [{:eventless, "nil"}, {:eventless, "nil"}] = shapes(result)
    end

    test "a `with` with no `else` falls through a followed remote helper's literal `nil`" do
      result =
        lint_aggregate(
          """
          defmodule Fixture.Aggregates.Thing do
            alias Fixture.Shared.Check

            def execute(state, _command) do
              with :ok <- Check.check(state), do: [%{ev: 1}]
            end
          end
          """,
          [
            {"lib/fixture/shared/check.ex",
             """
             defmodule Fixture.Shared.Check do
               def check(_state), do: nil
             end
             """}
          ]
        )

      assert [{:eventless, "nil"}] = shapes(result)
    end

    test "a local helper's tail, reported in the helper and reached from the entry clause" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(state, command), do: decide(state, command)

          defp decide(%{active: true}, _command), do: %{event: true}
          defp decide(_state, _command), do: []
        end
        """)

      assert {:error, [finding]} = result
      assert finding.shape == "[]"
      assert finding.fun == {:decide, 2}
      assert finding.origin.fun == :execute
      assert finding.message =~ "Fixture.Aggregates.Thing.decide/2  []  (reached from"
    end

    test "a `use Decree.Aggregate, command_window:` module is walked through its own `execute/2`" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          use Decree.Aggregate, command_window: [ms: 1000]
          defstruct [:thing_id]
          def authorize(_state, _command), do: :ok
          def execute(_state, _command), do: []
          def apply(state, _event), do: state
        end
        """)

      assert {:error, [%{kind: :eventless, shape: "[]", fun: {:execute, 2}}]} = result
    end

    test "a remote helper's tail, through an alias, in a module that is not routed" do
      result =
        lint_aggregate(
          """
          defmodule Fixture.Aggregates.Thing do
            alias Fixture.Shared.Membership

            def execute(state, _command), do: Membership.register(state.member?)
          end
          """,
          [
            {"lib/fixture/shared/membership.ex",
             """
             defmodule Fixture.Shared.Membership do
               def register(true), do: []
               def register(false), do: %{event: true}
             end
             """}
          ]
        )

      assert [{:eventless, "[]", "lib/fixture/shared/membership.ex", 2}] = findings(result)
    end

    test "a `Keyword.get(opts, :replay_answer, [])` default a caller does not override" do
      helper =
        {"lib/fixture/shared/registration.ex",
         """
         defmodule Fixture.Shared.Registration do
           def register_member(replay?, opts) do
             cond do
               replay? -> Keyword.get(opts, :replay_answer, [])
               true -> Keyword.fetch!(opts, :emit).()
             end
           end
         end
         """}

      result =
        lint_aggregate(
          """
          defmodule Fixture.Aggregates.Thing do
            alias Fixture.Shared.Registration

            def execute(state, _command) do
              Registration.register_member(state.replay?, emit: fn -> %{event: true} end)
            end
          end
          """,
          [helper]
        )

      assert [
               {:eventless, "[] (a Keyword.get/Map.get default)",
                "lib/fixture/shared/registration.ex", 4}
             ] = findings(result)

      # GREEN twin: the caller names its answer, so the default is unreachable.
      green =
        lint_aggregate(
          """
          defmodule Fixture.Aggregates.Thing do
            alias Fixture.Shared.Registration

            def execute(state, _command) do
              Registration.register_member(state.replay?,
                replay_answer: {:error, :duplicate_command},
                emit: fn -> %{event: true} end
              )
            end
          end
          """,
          [helper]
        )

      assert {:ok, _} = green
    end

    test "a `Map.get/3` default and the implicit nil of `Map.get/2` at a tail" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(%{kind: :a} = state, _command), do: Map.get(state, :pending, :ok)
          def execute(state, _command), do: Map.get(state, :pending)
        end
        """)

      assert Enum.sort(shapes(result)) ==
               Enum.sort([
                 {:eventless, ":ok (a Keyword.get/Map.get default)"},
                 {:eventless, "nil (Map.get/2 with no default)"}
               ])
    end

    test "a literal passed as a keyword option to a helper that returns it, reported at the caller" do
      result =
        lint_aggregate(
          """
          defmodule Fixture.Aggregates.Thing do
            alias Fixture.Shared.Answer

            def execute(state, _command) do
              Answer.answer(state.replay?,
                replay_answer: [],
                emit: fn -> %{event: true} end
              )
            end
          end
          """,
          [
            {"lib/fixture/shared/answer.ex",
             """
             defmodule Fixture.Shared.Answer do
               def answer(replay?, opts) do
                 answer = Keyword.fetch!(opts, :replay_answer)
                 if replay?, do: answer, else: Keyword.fetch!(opts, :emit).()
               end
             end
             """}
          ]
        )

      assert [{:eventless, "[]", @aggregate_file, _line}] = findings(result)
    end

    test "an option read through `Keyword.put/3`, `put_new/3`, `merge/2` and `delete/2`" do
      helper =
        {"lib/fixture/shared/answer.ex",
         """
         defmodule Fixture.Shared.Answer do
           def answer(opts) do
             opts =
               opts
               |> Keyword.put(:emit, fn -> %{event: true} end)
               |> Keyword.put_new(:fallback, :ok)
               |> Keyword.merge(other: 1)
               |> Keyword.delete(:other)

             Keyword.fetch!(opts, :fallback)
           end
         end
         """}

      result =
        lint_aggregate(
          """
          defmodule Fixture.Aggregates.Thing do
            alias Fixture.Shared.Answer

            def execute(_state, _command), do: Answer.answer([])
          end
          """,
          [helper]
        )

      assert [{:eventless, ":ok", "lib/fixture/shared/answer.ex", _line}] = findings(result)
    end

    test "an `fn` a followed helper applies at its tail is analysed" do
      result =
        lint_aggregate(
          """
          defmodule Fixture.Aggregates.Thing do
            alias Fixture.Shared.Emit

            def execute(_state, _command), do: Emit.run(fn -> [] end)
          end
          """,
          [
            {"lib/fixture/shared/emit.ex",
             """
             defmodule Fixture.Shared.Emit do
               def run(emit), do: emit.()
             end
             """}
          ]
        )

      assert [{:eventless, "[]", @aggregate_file, 4}] = findings(result)
    end

    test "a `try`, a `receive` and a `|>` tail" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(%{kind: :a}, _command) do
            try do
              [%{ev: 1}]
            rescue
              _error -> nil
            end
          end

          def execute(%{kind: :b}, _command) do
            receive do
              :go -> [%{ev: 1}]
            after
              0 -> :ok
            end
          end

          def execute(state, _command), do: state |> answer()

          defp answer(_state), do: {:ok, nil}
        end
        """)

      assert Enum.sort(shapes(result)) ==
               Enum.sort([
                 {:eventless, "nil"},
                 {:eventless, ":ok"},
                 {:eventless, "{:ok, nil}"}
               ])
    end

    test "a custom `function:` entry is walked, and the module's `execute/2` is not" do
      result =
        lint_aggregate(
          """
          defmodule Fixture.Aggregates.Thing do
            def execute(_state, _command), do: :ok
            def open_account(_state, _command), do: []
          end
          """,
          [],
          [entry(@thing, :open_account)]
        )

      assert {:error, [%{kind: :eventless, shape: "[]", fun: {:open_account, 2}} = finding]} =
               result

      assert finding.origin.fun == :open_account
    end

    test "a `to: Handler` entry walks the handler module's `handle/2`" do
      result =
        lint_aggregate(
          """
          defmodule Fixture.Aggregates.Thing do
            defstruct [:id]
          end
          """,
          [
            {"lib/fixture/handlers/thing_handler.ex",
             """
             defmodule Fixture.Handlers.ThingHandler do
               def handle(_aggregate, _command), do: {:ok, []}
             end
             """}
          ],
          [entry(Fixture.Handlers.ThingHandler, :handle)]
        )

      assert [{:eventless, "{:ok, []}", "lib/fixture/handlers/thing_handler.ex", 2}] =
               findings(result)
    end

    test "`{:streams, []}` is eventless from a decision's `decide/2`, not from an aggregate's" do
      source = """
      defmodule Fixture.Aggregates.Thing do
        def decide(_state, _command), do: {:streams, []}
      end
      """

      assert [{:eventless, "{:streams, []}", @aggregate_file, 2}] =
               findings(lint_aggregate(source, [], [entry(@thing, :decide, :decision)]))

      assert {:ok, %{modules: 1, entries: 1}} =
               lint_aggregate(source, [], [entry(@thing, :decide, :aggregate)])
    end

    test "a decision's `decide/2` answers the aggregate shapes too" do
      result =
        lint_aggregate(
          """
          defmodule Fixture.Aggregates.Thing do
            def decide(%{kind: :a}, _command), do: :ok
            def decide(_state, command), do: [%{ev: command.id}]
          end
          """,
          [],
          [entry(@thing, :decide, :decision)]
        )

      assert [{:eventless, ":ok"}] = shapes(result)
    end
  end

  describe "lint/2 — GREEN" do
    test "a helper returning `[]` in a NON-tail use is not a finding" do
      assert {:ok, %{modules: 1, entries: 1}} =
               lint_aggregate("""
               defmodule Fixture.Aggregates.Thing do
                 def execute(_state, command) do
                   %{event: true, entries: stringify_entries(command.entries)}
                 end

                 defp stringify_entries(nil), do: []
                 defp stringify_entries(entries), do: Enum.map(entries, &to_string/1)
               end
               """)
    end

    test "a duplicate, an event struct and a list of events" do
      assert {:ok, %{entries: 3}} =
               lint_aggregate("""
               defmodule Fixture.Aggregates.Thing do
                 def execute(%{kind: :a}, _command), do: {:error, :duplicate_command}
                 def execute(%{kind: :b}, command), do: %Fixture.Events.Happened{id: command.id}
                 def execute(_state, command), do: [%{one: command.id}, %{two: command.id}]
               end
               """)
    end

    test "a tail `emit.()` inside a helper, handed an event-returning fn" do
      assert {:ok, _} =
               lint_aggregate(
                 """
                 defmodule Fixture.Aggregates.Thing do
                   alias Fixture.Shared.Emit

                   def execute(_state, command), do: Emit.run(fn -> %{event: command.id} end)
                 end
                 """,
                 [
                   {"lib/fixture/shared/emit.ex",
                    """
                    defmodule Fixture.Shared.Emit do
                      def run(emit), do: emit.()
                    end
                    """}
                 ]
               )
    end

    test "a branch guarded by `X != :ok` whose tail is X, and a `{:error, _} = err` clause" do
      assert {:ok, _} =
               lint_aggregate("""
               defmodule Fixture.Aggregates.Thing do
                 def execute(_state, command) do
                   cond do
                     validate(command) != :ok -> validate(command)
                     true -> %{event: true}
                   end
                 end

                 def execute(_state, command) do
                   case validate(command) do
                     {:error, _} = err -> err
                     :ok -> %{event: true}
                   end
                 end

                 defp validate(nil), do: :ok
                 defp validate(_command), do: {:error, :bad}
               end
               """)
    end

    test "a `[]` on the left of `&&` is truthy, so it is never returned" do
      assert {:ok, %{entries: 1}} =
               lint_aggregate("""
               defmodule Fixture.Aggregates.Thing do
                 def execute(_state, _command), do: [] && [%{ev: 1}]
               end
               """)
    end

    test "an `if` with an event-returning `else`" do
      assert {:ok, %{entries: 1}} =
               lint_aggregate("""
               defmodule Fixture.Aggregates.Thing do
                 def execute(state, _command) do
                   if state.active, do: %{event: true}, else: {:error, :inactive}
                 end
               end
               """)
    end

    test "a `with` whose patterns certainly match their literals, or that has an `else`" do
      assert {:ok, %{entries: 4}} =
               lint_aggregate("""
               defmodule Fixture.Aggregates.Thing do
                 def execute(%{kind: :a}, _command),
                   do: with({:ok, x} <- {:ok, []}, do: [%{ev: x}])

                 def execute(%{kind: :b}, _command), do: with(x <- nil, do: [%{ev: x}])
                 def execute(%{kind: :c}, _command), do: with(:ok <- :ok, do: [%{ev: 1}])

                 def execute(_state, _command) do
                   with :ok <- nil do
                     [%{ev: 1}]
                   else
                     _other -> {:error, :refused}
                   end
                 end
               end
               """)
    end

    test "an unrouted module with an eventless `execute/2` is not in the population" do
      assert {:ok, %{modules: 1, entries: 1}} =
               lint_aggregate(
                 """
                 defmodule Fixture.Aggregates.Thing do
                   def execute(_state, command), do: [%{ev: command.id}]
                 end
                 """,
                 [
                   {"lib/fixture/aggregates/other.ex",
                    """
                    defmodule Fixture.Aggregates.Other do
                      def execute(_state, _command), do: []
                    end
                    """}
                 ]
               )
    end

    test "entries sharing one module and function are walked once per clause" do
      assert {:ok, %{modules: 1, entries: 2}} =
               lint_aggregate(
                 """
                 defmodule Fixture.Aggregates.Thing do
                   def execute(%{kind: :a}, command), do: [%{ev: command.id}]
                   def execute(_state, _command), do: {:error, :refused}
                 end
                 """,
                 [],
                 [entry(@thing), entry(@thing)]
               )
    end

    test "a call into a module outside the index that `internal?` calls external passes" do
      assert {:ok, %{entries: 1}} =
               lint_aggregate("""
               defmodule Fixture.Aggregates.Thing do
                 def execute(state, _command), do: Other.Library.events_for(state)
               end
               """)
    end
  end

  describe "lint/2 — fail closed" do
    test "a call through a variable module and `apply/3` are unclassified" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(%{kind: :a} = state, command), do: state.handler.handle(command)
          def execute(%{kind: :b}, command), do: apply(Fixture.Elsewhere, :handle, [command])
        end
        """)

      assert [{:unclassified, first}, {:unclassified, second}] = shapes(result)
      assert first =~ "unclassified:"
      assert second =~ "unclassified: `apply`"
    end

    test "a runtime value on the left of `&&` is a tail, so a field read is unclassified" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(%__MODULE__{} = state, _cmd), do: state.ready && [%{ev: 1}]
        end
        """)

      assert [{:unclassified, "unclassified: a runtime field read `state.ready`"}] =
               shapes(result)
    end

    test "a pattern-bound tail variable is unclassified" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(state, _command) do
            case state.pending do
              {:ok, events} -> events
              _other -> {:error, :nothing_pending}
            end
          end
        end
        """)

      assert [{:unclassified, "unclassified: the pattern-bound variable `events`"}] =
               shapes(result)
    end

    test "a routed module with no source under the scanned paths is unclassified" do
      result =
        lint_aggregate(
          """
          defmodule Fixture.Aggregates.Thing do
            def execute(_state, command), do: [%{ev: command.id}]
          end
          """,
          [],
          [entry(@thing), entry(Fixture.Aggregates.Missing)]
        )

      assert [{:unclassified, shape}] = shapes(result)

      assert shape =~
               "routed module Fixture.Aggregates.Missing has no source under the scanned paths"
    end

    test "a routed module with no public `execute/2` and no `use` is unclassified" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          defstruct [:status]
          def apply(state, _event), do: state
        end
        """)

      assert [{:unclassified, shape, @aggregate_file, 0}] = findings(result)
      assert shape =~ "no public execute/2"
      refute shape =~ "use"
    end

    test "a routed module with no public `execute/2` that `use`s another module is unclassified" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          use Fixture.InjectsExecute
        end
        """)

      assert [{:unclassified, shape}] = shapes(result)
      assert shape =~ "no public execute/2"
      assert shape =~ "use Fixture.InjectsExecute"
    end

    test "a `defp execute/2` alone is no public entry, so it is unclassified" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          defp execute(_state, command), do: [%{ev: command.id}]
        end
        """)

      assert [{:unclassified, shape, @aggregate_file, 0}] = findings(result)
      assert shape =~ "no public execute/2"
    end

    test "an unindexed receiver `internal?` claims is unclassified" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          def execute(state, _command), do: Fixture.Gone.events_for(state)
        end
        """)

      assert [
               {:unclassified,
                "unclassified: Fixture.Gone.events_for/1, a module outside the index"}
             ] = shapes(result)
    end

    test "a local call with no def that an `internal?` unindexed import could supply" do
      result =
        lint_aggregate("""
        defmodule Fixture.Aggregates.Thing do
          import Fixture.Gone
          def execute(state, _command), do: events_for(state)
        end
        """)

      assert [{:unclassified, shape}] = shapes(result)
      assert shape =~ "events_for/1"
    end

    test "lint/2 refuses to run without an `internal?:` predicate" do
      assert_raise ArgumentError, ~r/internal\?/, fn ->
        Eventless.lint([entry(@thing)], root: tmp_root(), paths: ["lib"])
      end
    end
  end
end
