defmodule Decree.Verify.Eventless do
  @moduledoc """
  Finds the routed handler arms that can accept a command without an event.

  `Decree.Aggregates.Aggregate` calls a route's handler (`execute/2` on the aggregate,
  `handle/2` on a `to: Handler, aggregate: Agg` route, or the route's `function:`) with
  `Kernel.apply/3`, and reads an answer of `[]`, `nil` or `:ok` as accepted with no
  events: it replies `{:ok, []}` and appends nothing. `{:ok, []}` and `{:ok, nil}` reach
  the same place through `List.wrap/1`. A recognised repeat answers
  `{:error, :duplicate_command}` instead, and every other accepted arm returns at least
  one event (sparkle-space/sparklespace#5680). `lint/2` reports every routed arm that can
  answer one of those eventless values, with no allow-list and no waiver; no runtime or
  router guard backs it. `mix decree.verify.eventless` runs it over a host's routers.

  A decision's `decide/2` is walked with the same tails plus `{:streams, []}`.
  `Decree.Decision.Dispatch.shape/1` already refuses `[]`, `:ok`, `nil` and
  `{:streams, []}` at runtime with `{:error, :decision_returned_no_events}` (and
  `{:ok, _}` with `:invalid_decision_return`), so for `decide/2` this is an early answer
  at lint time. The gap it fences is the aggregate handler, which the aggregate accepts
  with no events.

  ## Mechanism

  A syntax-only source walk (`Code.string_to_quoted!/2`; nothing is compiled or loaded).
  Every `.ex` file under the given `paths:` is parsed and split into one scope per
  `defmodule`, at any depth, nested names qualified against their enclosing chain and
  nested bodies pruned from their parent's. That gives a module index: each module's
  `def`/`defp`/`defdelegate` clauses by name and arity (`\\\\` defaults counted, a
  bodyless default head applied), its `alias` table (file-wide plus its own body), its
  `import`s (anywhere in its body, function-local ones included, with their `only:` and
  `except:` selectors; one inside a `quote` belongs to the caller and is skipped), its
  `use`s and its literal module attributes.

  ## Population

  The caller names the entries: `%{module: module, fun: atom, kind: :aggregate |
  :decision}`, one per routed handler function, read from a router's registry (`mix
  decree.verify.eventless` reads `__route__/1`). Entries sharing a module and function are
  walked once, as a `:decision` when any of them is one (its tails are a superset). The
  walk starts at every PUBLIC `def` clause of `fun/2` in the module, a `defdelegate`
  included; a `defp` is never called by `Kernel.apply/3`. `use Decree.Aggregate` injects
  no `execute/2` (the command window runs in the aggregate process before the handler is
  applied), so such a module is walked through its own `execute/2` like any other.

  ## Shapes

  A TAIL value is a finding when it is one of:

    1. `[]`;
    2. `nil` (including an `if`/`unless` with no `else`);
    3. `:ok`;
    4. `{:ok, []}` or `{:ok, nil}`;
    5. a `Keyword.get/3` or `Map.get/3` default that is one of the above, or the implicit
       `nil` default of `Keyword.get/2` and `Map.get/2`;
    6. `{:streams, []}`, from a `:decision` entry only: written literally, or a
       `{:streams, value}` whose value resolves to `[]` (a binding, a module attribute, a
       followed call). Any other value passes, an unclassifiable one included: runtime
       members are the normal decision answer.

  ## Tail positions

  The last expression of a clause body or block (`do: …` included); every branch of
  `if`/`unless`, `cond`, `case`, `with` (its `do` body and every `else` clause), `try`
  (`do`, `rescue`, `catch`, `else`) and `receive` (clauses and `after`); both operands of
  `||` and `&&`, the right operand of `and` and `or` (a non-boolean left raises
  `BadBooleanError`, so the left is never returned), both of `++` when both can be empty,
  the first argument of `tap/2`, the value of the function `then/2` applies (`Kernel.tap/2`
  and `Kernel.then/2` alike), and the value of a tail `=` or `|>` (a pipe is rewritten
  into its call first). A falsy left of `||` and a truthy left of `&&` are never returned,
  so an eventless literal there is a finding only when it can be: a truthy one (`[]`,
  `:ok`, `{:ok, …}`) on the left of `||`, a `nil` on the left of `&&`.

  A `with` with no `else` returns the value of a `<-` clause whose pattern it does not
  match. Only a literal eventless fall-through is caught: an eventless literal a clause's
  right side resolves to is a finding unless the pattern certainly matches it (a bare
  variable, `_`, an equal literal, or a tuple or `[]` that matches element-wise). A guard,
  a pin, a map, a struct or a non-empty list pattern may not match, so the value stays a
  finding. A runtime fall-through value is a stated residual below and is not marked
  `unclassified`. A bare-variable clause never falls through.

  A tail VARIABLE is resolved to its binding. One bound by `var = expr` in an enclosing
  block, or by a bare-variable `with` clause or `case` clause, is analysed as `expr` (the
  subject, for a `case` clause), in the scope where it was bound. One bound to a followed
  callee's parameter is analysed as the caller's argument, in the caller's scope. A
  variable bound inside a pattern, an entry clause's own parameter, or an unbound name is
  `unclassified`.

  ## Reachability

  From a tail position the walk follows:

    * a local call to a `def`/`defp` of the same module, or to a function of a module the
      module `import`s, when it is indexed — every clause of that name and arity. An
      import anywhere in the module counts for every call in it: lexical scope is not
      modelled, so the walk cannot tell which import a call sees, and it follows every
      import whose selectors may supply the name and arity. A literal `name: arity` list,
      `:functions`, `:macros` or `:sigils` selects exactly (for a module outside the index,
      whose kinds are unknown, `:functions` and `:macros` may supply anything); any other
      selector (an attribute, a variable) may supply anything;
    * a remote call whose receiver resolves to an indexed module: an alias (through the
      module's `alias` table, chained), `__MODULE__`, `__MODULE__.X`, a nested module's
      short name, or a module atom;
    * an anonymous call `f.(args)` at a tail, resolved through its binding to a `fn` (its
      clause bodies are tails, in the `fn`'s defining scope, with its parameters bound to
      the call's arguments), a capture `&f/n` or `&Mod.f/n` (followed), or a
      `Keyword.fetch!/2`/`Keyword.get/2,3` on a resolvable options list. An `fn` handed to
      a followed helper is therefore analysed where the helper applies it at a tail; an
      `fn` handed to a call the walk does not follow (`Enum.flat_map/2`) is not analysed;
    * a `Keyword.fetch!/2` or `Keyword.get/2,3` whose options resolve to a literal keyword
      list, through bindings, parameters, `Keyword.put/3`, `Keyword.put_new/3`,
      `Keyword.merge/2` and `Keyword.delete/2` — the option's value is a tail. So a
      literal eventless value passed as an option to a followed helper that returns it is
      a finding, at the line where the caller wrote it.

  A followed function already on the current call path is not re-entered (recursion); a
  path deeper than 40 calls is `unclassified`.

  A receiver that resolves to no indexed module is classified by the caller's
  `internal?:` predicate: one it claims is `unclassified` (its source is not under the
  scanned paths); any other is external and passes, as `Enum`, `Map` and a dependency's
  modules do, apart from the shape-5 defaults above. `Kernel` functions pass. The Kernel
  reading is never removed: an `import Kernel` selector that may leave the name out,
  anywhere in the module, only puts the imports that may supply the call beside it (an
  import's position is not modelled). Kernel's own exports are known, so an
  `import Kernel` supplies no other name.

  ## Fail closed

  A tail the walk cannot classify is reported `unclassified` and fails like a finding: a
  routed module with no source under the scanned paths; a routed module with no public
  `fun/2` in source (naming its `use`s, any of which may inject one); a call through a
  variable module, `apply/2,3`, an anonymous call it cannot resolve, `Keyword.fetch!/2` or
  `Keyword.get/2,3` on options it cannot resolve, a local call with no `def` in the module
  and no import that could supply it (or one an `internal?` import outside the index
  could, including a Kernel name an `import Kernel` selector may leave out), a local call
  to a macro the module defines (a local macro) or a public `defmacro` an indexed import's
  selectors may supply (an imported macro, even when another candidate is a def or
  Kernel), a call to an indexed module that defines no such function in source, an
  `internal?` receiver outside the index, a receiver whose first segment is an alias the
  module's table (file-wide plus its own) maps to two or more different modules (the walk
  models no lexical scope, so it cannot tell which one the call sees; the same target
  aliased twice is no conflict), a module attribute with no literal value, a runtime field
  or `Access` read, `quote`/`unquote`/`super`, and the unresolved variables above.

  ## Stated residuals

    * a runtime-empty list from a call the walk does not follow (`Enum.map/2` over an
      empty input, a `for` comprehension, `++` of two runtime lists);
    * the fall-through of a `with` with no `else`, when the failing clause's value is not
      a literal the walk resolves (a runtime value that fails the pattern);
    * `apply/3` with a static module and function — reported `unclassified`, never
      followed;
    * a `Decree.Aggregate.Multi` whose run produces no events;
    * a function in a module `internal?` calls external (a dependency), which is not
      read, a local call an import of such a module may supply included (its macros are
      not read either);
    * a followed function's clauses are all analysed, whichever one the caller's
      arguments select;
    * a Kernel name an `import Kernel` excludes keeps the Kernel reading beside its real
      supplier (a dependency's `then/2`), which can report a false finding, fail-closed;
    * a decision's `{:streams, members}` whose members are empty at runtime, or whose
      value the walk cannot resolve: `Decree.Decision.Dispatch.shape/1` refuses an empty or
      malformed members list at runtime, so it is not reported.

  ## Findings

  `lint/2` answers `{:ok, %{modules: n, entries: m}}` (distinct routed modules, entry
  clauses walked) or `{:error, findings}`, each a `%{kind: :eventless | :unclassified,
  file, line, module, fun, shape, origin, message}` map, sorted by file and line. The
  message reads `path:line  Module.fun/arity  <shape>  (reached from Module.fun clause at
  path:line)`. A literal carries no line in the AST, so its line is recovered from the
  enclosing construct's metadata (the previous expression's end, a `->` arrow, a `do`);
  it can point at that construct.
  """

  alias Decree.Verify.AliasMap
  alias Decree.Verify.ModuleScopes

  @max_depth 40

  @no_value_calls [:raise, :reraise, :throw, :exit]
  @value_operators [
    :not,
    :!,
    :==,
    :!=,
    :===,
    :!==,
    :<,
    :>,
    :<=,
    :>=,
    :in,
    :+,
    :-,
    :*,
    :/,
    :<>,
    :..,
    :..//,
    :"::"
  ]
  @value_literals [:%, :%{}, :{}, :<<>>, :fn, :&, :__MODULE__, :__ENV__, :__DIR__, :__CALLER__]
  @unclassifiable [:quote, :unquote, :unquote_splicing, :super, :var!]

  # The functions and macros every module imports by default; an `import Kernel` selector
  # can leave out Kernel's own, never a special form.
  @kernel_macros MapSet.new(Kernel.__info__(:macros))
  @kernel_own MapSet.union(MapSet.new(Kernel.__info__(:functions)), @kernel_macros)
  @kernel MapSet.union(@kernel_own, MapSet.new(Kernel.SpecialForms.__info__(:macros)))

  @type entry :: %{module: module(), fun: atom(), kind: :aggregate | :decision}
  @type finding :: %{
          kind: :eventless | :unclassified,
          file: String.t(),
          line: non_neg_integer(),
          module: module(),
          fun: {atom(), arity()},
          shape: String.t(),
          origin: map() | nil,
          message: String.t()
        }

  @doc """
  Walks `entries` over the sources under `opts[:paths]` (relative to `opts[:root]`).

  Options:

    * `:root` — the directory `:paths` and the findings' paths are relative to; required;
    * `:paths` — the source directories to index, relative to `:root`; required;
    * `:internal?` — a 1-arity predicate on a module atom, true for a module whose source
      should be under `:paths`; required, so no caller treats every unindexed module as
      external by omission.
  """
  @spec lint([entry()], keyword()) ::
          {:ok, %{modules: non_neg_integer(), entries: non_neg_integer()}}
          | {:error, [finding()]}
  def lint(entries, opts) do
    root = fetch_option!(opts, :root)
    paths = fetch_option!(opts, :paths)
    internal? = fetch_option!(opts, :internal?)

    unless is_function(internal?, 1) do
      raise ArgumentError,
            "Decree.Verify.Eventless.lint/2 expects `internal?:` to be a 1-arity function, " <>
              "got: #{inspect(internal?)}"
    end

    index = build_index(root, paths)
    entries = merge_entries(entries)

    {walked, findings} =
      Enum.reduce(entries, {0, []}, fn entry, {count, acc} ->
        {clauses, found} = walk_routed(entry, index, internal?)
        {count + clauses, found ++ acc}
      end)

    modules = entries |> Enum.map(& &1.module) |> Enum.uniq() |> length()

    case findings |> Enum.uniq_by(&finding_key/1) |> Enum.sort_by(&{&1.file, &1.line}) do
      [] -> {:ok, %{modules: modules, entries: walked}}
      found -> {:error, Enum.map(found, &with_message/1)}
    end
  end

  defp fetch_option!(opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} ->
        value

      :error ->
        raise ArgumentError,
              "Decree.Verify.Eventless.lint/2 requires the `#{key}:` option"
    end
  end

  # One entry per module and function, walked as a decision when any route names it one:
  # a decision's tails are a superset of an aggregate handler's.
  defp merge_entries(entries) do
    entries
    |> Enum.group_by(&{&1.module, &1.fun}, & &1.kind)
    |> Enum.map(fn {{module, fun}, kinds} ->
      %{module: module, fun: fun, kind: if(:decision in kinds, do: :decision, else: :aggregate)}
    end)
  end

  defp finding_key(f), do: {f.file, f.line, f.module, f.fun, f.shape, f.origin}

  # ---------------------------------------------------------------------------
  # Index
  # ---------------------------------------------------------------------------

  defp build_index(root, paths) do
    for path <- paths,
        file <- Path.wildcard(Path.join([root, path, "**", "*.ex"])),
        reduce: %{} do
      acc ->
        Enum.reduce(index_file(file, root), acc, fn rec, inner ->
          Map.put(inner, rec.module, rec)
        end)
    end
  end

  defp index_file(file, root) do
    ast =
      file
      |> File.read!()
      |> Code.string_to_quoted!(file: file, columns: true, token_metadata: true)

    file_aliases = AliasMap.pairs(ast)
    relative = Path.relative_to(file, root)

    for {module, body} <- ModuleScopes.scopes(ast), is_atom(module) and not is_nil(module) do
      body = ModuleScopes.prune_nested_defmodules(body)
      exprs = block_exprs(body)
      aliases = file_aliases ++ AliasMap.pairs(body, module)

      %{
        module: module,
        file: relative,
        defs: collect_defs(exprs),
        aliases: Map.new(aliases),
        alias_conflicts: AliasMap.conflicts(aliases),
        imports: collect_imports(body),
        uses: for({:use, _, [target | _]} <- exprs, do: target),
        attributes: collect_attributes(exprs)
      }
    end
  end

  defp block_exprs({:__block__, _meta, exprs}), do: exprs
  defp block_exprs(nil), do: []
  defp block_exprs(expr), do: [expr]

  # Every `import` in the module body, function-local ones included, with its selectors.
  # A `quote` body is skipped: its imports belong to the caller it is injected into.
  defp collect_imports(body) do
    {_ast, found} =
      Macro.prewalk(body, [], fn
        {:quote, _, args}, acc when is_list(args) ->
          {nil, acc}

        {:import, _, [target | opts]} = node, acc when length(opts) <= 1 ->
          {node, [import_record(target, opts) | acc]}

        node, acc ->
          {node, acc}
      end)

    Enum.reverse(found)
  end

  # A selector is a literal `name: arity` list, `:functions`, `:macros` or `:sigils`;
  # anything else (an attribute, a variable) is `:unknown`, which may select anything.
  defp import_record(target, opts) do
    {only, except} =
      case opts do
        [] ->
          {nil, nil}

        [opts] when is_list(opts) ->
          if keyword_literal?(opts),
            do: {selector(opts, :only), selector(opts, :except)},
            else: {:unknown, :unknown}

        _non_literal ->
          {:unknown, :unknown}
      end

    %{target: target, only: only, except: except}
  end

  defp selector(opts, key) do
    case Keyword.fetch(opts, key) do
      :error ->
        nil

      {:ok, kind} when kind in [:functions, :macros, :sigils] ->
        kind

      {:ok, pairs} when is_list(pairs) ->
        if Enum.all?(pairs, &match?({name, arity} when is_atom(name) and is_integer(arity), &1)),
          do: pairs,
          else: :unknown

      {:ok, _other} ->
        :unknown
    end
  end

  defp collect_attributes(exprs) do
    for {:@, _, [{name, _, [value]}]} <- exprs, is_atom(name), into: %{}, do: {name, value}
  end

  # `%{name => [clause]}`. A clause records its total parameter count `n` and the
  # defaults declared on it or on a bodyless head of the same name and count, so a call
  # with fewer arguments binds them as Elixir does.
  defp collect_defs(exprs) do
    raw =
      for expr <- exprs, clause = def_clause(expr), clause != nil, do: clause

    defaults =
      for %{defaults: d, name: name, n: n} <- raw, map_size(d) > 0, into: %{}, do: {{name, n}, d}

    raw
    |> Enum.filter(&(&1.body != nil))
    |> Enum.map(fn c -> %{c | defaults: Map.get(defaults, {c.name, c.n}, c.defaults)} end)
    |> Enum.group_by(& &1.name)
  end

  defp def_clause({kind, meta, [head | rest]})
       when kind in [:def, :defp, :defmacro, :defmacrop] do
    head = strip_guard(head)

    case head do
      {name, _hmeta, params} when is_atom(name) ->
        params = if is_list(params), do: params, else: []

        %{
          kind: kind,
          name: name,
          n: length(params),
          params: Enum.map(params, &strip_default/1),
          defaults: defaults_of(params),
          body: def_body(rest),
          meta: meta,
          line: Keyword.get(meta, :line, 0)
        }

      _computed ->
        nil
    end
  end

  defp def_clause({:defdelegate, meta, [head, opts]}) when is_list(opts) do
    with {name, _hmeta, params} when is_atom(name) and is_list(params) <- head,
         {:ok, to} <- Keyword.fetch(opts, :to) do
      params = Enum.map(params, &strip_default/1)
      as = Keyword.get(opts, :as, name)

      %{
        kind: :def,
        name: name,
        n: length(params),
        params: params,
        defaults: defaults_of(elem(head, 2)),
        body: [do: {{:., meta, [to, as]}, meta, params}],
        meta: meta,
        line: Keyword.get(meta, :line, 0)
      }
    else
      _other -> nil
    end
  end

  defp def_clause(_expr), do: nil

  defp strip_guard({:when, _, [head | _guards]}), do: head
  defp strip_guard(head), do: head

  defp strip_default({:\\, _, [param, _default]}), do: param
  defp strip_default(param), do: param

  defp defaults_of(params) do
    params
    |> Enum.with_index()
    |> Enum.flat_map(fn
      {{:\\, _, [_param, default]}, idx} -> [{idx, default}]
      _other -> []
    end)
    |> Map.new()
  end

  defp def_body([body]) when is_list(body), do: body
  defp def_body(_rest), do: nil

  # Clauses of `name` callable with `arity` arguments.
  defp clauses_for(rec, name, arity, kinds) do
    rec.defs
    |> Map.get(name, [])
    |> Enum.filter(fn c ->
      c.kind in kinds and arity <= c.n and arity >= c.n - map_size(c.defaults)
    end)
  end

  defp defines_macro?(rec, name, arity),
    do: clauses_for(rec, name, arity, [:defmacro, :defmacrop]) != []

  # ---------------------------------------------------------------------------
  # Entry points
  # ---------------------------------------------------------------------------

  # `{clauses walked, findings}` for one routed entry.
  defp walk_routed(%{module: module, fun: fun} = entry, index, internal?) do
    case Map.fetch(index, module) do
      {:ok, rec} ->
        case clauses_for(rec, fun, 2, [:def]) do
          [] ->
            {0, [no_entry_finding(rec, fun)]}

          clauses ->
            {length(clauses),
             Enum.flat_map(clauses, &walk_entry(&1, rec, entry, index, internal?))}
        end

      :error ->
        {0, [no_source_finding(module, fun)]}
    end
  end

  defp no_source_finding(module, fun) do
    %{
      kind: :unclassified,
      file: "(no source)",
      line: 0,
      module: module,
      fun: {fun, 2},
      shape:
        "unclassified: routed module #{inspect(module)} has no source under the scanned paths",
      origin: nil
    }
  end

  defp no_entry_finding(rec, fun) do
    injected =
      case rec.uses do
        [] ->
          ""

        uses ->
          ", and " <>
            Enum.map_join(uses, ", ", &"`use #{Macro.to_string(&1)}`") <> " may inject it"
      end

    %{
      kind: :unclassified,
      file: rec.file,
      line: 0,
      module: rec.module,
      fun: {fun, 2},
      shape: "unclassified: no public #{fun}/2 in source#{injected}",
      origin: nil
    }
  end

  defp walk_entry(clause, rec, entry, index, internal?) do
    origin = %{module: rec.module, fun: clause.name, file: rec.file, line: clause.line}

    ctx = %{
      index: index,
      internal?: internal?,
      kind: entry.kind,
      rec: rec,
      fun: {clause.name, 2},
      vars: bind_entry_params(clause.params),
      stack: [{rec.module, clause.name, 2}],
      origin: origin,
      line: clause.line,
      depth: 0
    }

    walk_body(clause, ctx)
  end

  defp bind_entry_params(params) do
    params |> pattern_vars() |> Map.new(&{&1, :entry})
  end

  # A clause body: `do` alone, or the implicit `try` of a body carrying
  # `rescue`/`catch`/`else`.
  defp walk_body(%{body: body, meta: meta}, ctx) do
    if Keyword.keys(body) == [:do] do
      tail(Keyword.fetch!(body, :do), do_ctx(ctx, meta))
    else
      try_branches(body, meta, ctx)
    end
  end

  # ---------------------------------------------------------------------------
  # Tail analysis
  # ---------------------------------------------------------------------------

  defp tail(_expr, %{depth: depth} = ctx) when depth > @max_depth,
    do: [unclassified(ctx, "a call path deeper than #{@max_depth} followed calls")]

  defp tail([], ctx), do: [eventless(ctx, "[]", [])]
  defp tail(nil, ctx), do: [eventless(ctx, "nil", nil)]
  defp tail(:ok, ctx), do: [eventless(ctx, ":ok", :ok)]
  defp tail({:ok, []}, ctx), do: [eventless(ctx, "{:ok, []}", {:ok, []})]
  defp tail({:ok, nil}, ctx), do: [eventless(ctx, "{:ok, nil}", {:ok, nil})]
  defp tail({:ok, value}, ctx) when is_tuple(value), do: tail_ok(value, ctx)

  # `Decree.Decision.Dispatch.shape/1` reads it as no events; an aggregate's answer of
  # `{:streams, []}` is appended as a two-element list, so it is not.
  defp tail({:streams, []}, %{kind: :decision} = ctx),
    do: [eventless(ctx, "{:streams, []}", {:streams, []})]

  defp tail({:streams, value}, %{kind: :decision} = ctx) when not is_list(value),
    do: tail_streams(value, ctx)

  defp tail({_left, _right}, _ctx), do: []
  defp tail(list, _ctx) when is_list(list), do: []

  defp tail(literal, _ctx) when is_atom(literal) or is_number(literal) or is_binary(literal),
    do: []

  defp tail({_form, meta, _args} = expr, ctx) when is_list(meta) do
    tail_node(expr, node_ctx(ctx, meta))
  end

  defp tail(_other, ctx), do: [unclassified(ctx, "an unrecognised expression")]

  # `{:ok, value}`: the value is what decree appends, after `List.wrap/1`.
  defp tail_ok(value, ctx) do
    for finding <- tail(value, ctx),
        do:
          if(finding.kind == :eventless,
            do: %{finding | shape: "{:ok, #{finding.shape}}", value: {:ok, finding.value}},
            else: finding
          )
  end

  # `{:streams, value}`: only a value that resolves to `[]` is a finding. Runtime members
  # are the normal decision answer, and `Decree.Decision.Dispatch.shape/1` refuses an
  # empty or malformed one at runtime, so every other result is dropped.
  defp tail_streams(value, ctx) do
    for %{kind: :eventless, value: []} = finding <- tail(value, ctx),
        do: %{finding | shape: "{:streams, #{finding.shape}}", value: {:streams, []}}
  end

  # --- blocks and matches ---

  defp tail_node({:__block__, _meta, [single]}, ctx), do: tail(single, ctx)

  defp tail_node({:__block__, _meta, [_ | _] = exprs}, ctx) do
    {init, [last]} = Enum.split(exprs, -1)
    ctx = Enum.reduce(init, ctx, &bind_statement/2)
    tail(last, last_ctx(ctx, List.last(init), last))
  end

  defp tail_node({:=, _meta, [_pattern, value]}, ctx), do: tail(value, ctx)

  defp tail_node({:|>, _meta, [lhs, rhs]}, ctx), do: tail(pipe(lhs, rhs), ctx)

  # --- branches ---

  defp tail_node({op, meta, [condition, branches]}, ctx)
       when op in [:if, :unless] and is_list(branches) do
    {do_sense, else_sense} = if op == :if, do: {:!=, :==}, else: {:==, :!=}
    do_body = Keyword.get(branches, :do)

    do_findings =
      do_body
      |> tail(do_ctx(ctx, meta))
      |> exclude(excluded_by(condition, do_sense, do_body))

    else_findings =
      case Keyword.fetch(branches, :else) do
        {:ok, else_body} ->
          else_body
          |> tail(else_ctx(ctx, meta))
          |> exclude(excluded_by(condition, else_sense, else_body))

        :error ->
          [eventless(ctx, "#{op} without else (implicit nil)", nil)]
      end

    do_findings ++ else_findings
  end

  defp tail_node({:cond, _meta, [[do: clauses]]}, ctx) when is_list(clauses),
    do: Enum.flat_map(clauses, &clause_tail(&1, ctx, :cond))

  defp tail_node({:case, _meta, [subject, [do: clauses]]}, ctx) when is_list(clauses) do
    # A clause whose whole pattern is one variable receives the subject, minus
    # every literal an EARLIER unguarded clause already matched.
    {findings, _matched} =
      Enum.flat_map_reduce(clauses, [], fn clause, matched ->
        found =
          case clause do
            {:->, _, [[{name, _, context}], _body]} when is_atom(name) and is_atom(context) ->
              exclude(clause_tail(clause, ctx, {:subject, subject, ctx}), matched)

            _other ->
              clause_tail(clause, ctx, {:subject, subject, ctx})
          end

        {found, matched ++ literal_pattern(clause)}
      end)

    findings
  end

  defp tail_node({:with, meta, args}, ctx) when is_list(args) and args != [] do
    {clauses, branches} =
      case List.last(args) do
        kw when is_list(kw) -> {Enum.drop(args, -1), kw}
        _no_body -> {args, []}
      end

    {fall_through_findings, with_ctx} =
      Enum.reduce(clauses, {[], ctx}, fn clause, {found, clause_ctx} ->
        {found ++ with_fall_through(clause, clause_ctx, branches),
         bind_with_clause(clause, clause_ctx)}
      end)

    do_findings =
      case Keyword.fetch(branches, :do) do
        {:ok, body} -> tail(body, do_ctx(with_ctx, meta))
        :error -> []
      end

    else_findings =
      branches
      |> Keyword.get(:else, [])
      |> List.wrap()
      |> Enum.flat_map(&clause_tail(&1, ctx, :none))

    fall_through_findings ++ do_findings ++ else_findings
  end

  defp tail_node({:try, meta, [branches]}, ctx) when is_list(branches),
    do: try_branches(branches, meta, ctx)

  defp tail_node({:receive, meta, [branches]}, ctx) when is_list(branches) do
    clauses = branches |> Keyword.get(:do, []) |> List.wrap()
    after_clauses = branches |> Keyword.get(:after, []) |> List.wrap()

    Enum.flat_map(clauses, &clause_tail(&1, ctx, :none)) ++
      Enum.flat_map(after_clauses, &clause_tail(&1, node_ctx(ctx, meta), :none))
  end

  defp tail_node({:for, _meta, _args}, _ctx), do: []

  # --- operators ---

  defp tail_node({:||, _meta, [left, right]}, ctx) do
    # A falsy left is replaced by the right, so only a truthy eventless left
    # value (`[]`, `:ok`, `{:ok, []}`) can be returned from it.
    left_findings = Enum.reject(tail(left, ctx), &(&1.kind == :eventless and &1.value == nil))
    left_findings ++ tail(right, ctx)
  end

  defp tail_node({:&&, _meta, [left, right]}, ctx) do
    # A truthy left is replaced by the right, so only a falsy eventless left
    # value (`nil`) can be returned from it. Every other left value, a runtime
    # one included, is analysed as any tail is.
    left_findings = Enum.reject(tail(left, ctx), &(&1.kind == :eventless and &1.value != nil))
    left_findings ++ tail(right, ctx)
  end

  # A non-boolean left raises `BadBooleanError`, so the left is never returned: the right
  # operand is, when it is reached.
  defp tail_node({op, _meta, [_left, right]}, ctx) when op in [:and, :or], do: tail(right, ctx)

  defp tail_node({:++, _meta, [left, right]}, ctx) do
    left_findings = tail(left, ctx)
    right_findings = tail(right, ctx)

    # Concatenation is empty only when both sides are.
    if left_findings == [] or right_findings == [],
      do: [],
      else: left_findings ++ right_findings
  end

  defp tail_node({op, _meta, args}, _ctx) when op in @value_operators and is_list(args), do: []
  defp tail_node({op, _meta, _args}, _ctx) when op in @value_literals, do: []
  defp tail_node({op, _meta, args}, _ctx) when op in @no_value_calls and is_list(args), do: []

  defp tail_node({op, _meta, _args}, ctx) when op in @unclassifiable,
    do: [unclassified(ctx, "`#{op}`")]

  # --- module attributes ---

  defp tail_node({:@, _meta, [{name, _, context}]}, ctx)
       when is_atom(name) and is_atom(context) do
    case Map.fetch(ctx.rec.attributes, name) do
      {:ok, value} -> tail(value, %{ctx | vars: %{}})
      :error -> [unclassified(ctx, "module attribute @#{name} with no literal value")]
    end
  end

  # --- variables ---

  defp tail_node({name, _meta, context}, ctx) when is_atom(name) and is_atom(context),
    do: tail_var(name, ctx)

  # --- calls ---

  defp tail_node({{:., _, [fexpr]}, _meta, args}, ctx) when is_list(args),
    do: apply_fun(fexpr, ctx, Enum.map(args, &{:expr, &1, ctx}), ctx)

  defp tail_node({{:., _, [receiver, name]}, meta, []} = expr, ctx) when is_atom(name) do
    if Keyword.get(meta, :no_parens, false) and not module_receiver?(receiver),
      do: [unclassified(ctx, "a runtime field read `#{Macro.to_string(expr)}`")],
      else: remote_call(receiver, name, [], ctx)
  end

  defp tail_node({{:., _, [receiver, name]}, _meta, args}, ctx)
       when is_atom(name) and is_list(args),
       do: remote_call(receiver, name, args, ctx)

  defp tail_node({name, _meta, args}, ctx) when is_atom(name) and is_list(args),
    do: local_call(name, args, ctx)

  defp tail_node(expr, ctx),
    do: [unclassified(ctx, "an unrecognised expression `#{short(expr)}`")]

  defp module_receiver?({:__aliases__, _, _}), do: true
  defp module_receiver?({:__MODULE__, _, ctx}) when is_atom(ctx), do: true
  defp module_receiver?(atom) when is_atom(atom), do: true
  defp module_receiver?(_other), do: false

  # `try` (or a def's implicit one): `do`, the `rescue`/`catch`/`else`
  # clauses. `after` produces no value.
  defp try_branches(branches, meta, ctx) do
    do_findings =
      case Keyword.fetch(branches, :do) do
        {:ok, body} -> tail(body, do_ctx(ctx, meta))
        :error -> []
      end

    clause_findings =
      for key <- [:rescue, :catch, :else],
          clause <- branches |> Keyword.get(key, []) |> List.wrap(),
          finding <- clause_tail(clause, ctx, :none),
          do: finding

    do_findings ++ clause_findings
  end

  # A `->` clause body. Its pattern's variables are pattern-bound, except a
  # `case` clause whose whole pattern is one variable, which binds the subject.
  defp clause_tail({:->, meta, [patterns, body]}, ctx, subject) do
    vars =
      case {patterns, subject} do
        # A `cond` condition is an expression, not a pattern: it binds nothing
        # the walk reads.
        {_condition, :cond} ->
          ctx.vars

        {[pattern], {:subject, value, value_ctx}} ->
          bind_pattern(strip_guard(pattern), {:expr, value, value_ctx}, ctx.vars)

        _other ->
          Enum.reduce(patterns, ctx.vars, &bind_pattern(strip_guard(&1), nil, &2))
      end

    line = Keyword.get(meta, :line, ctx.line) + Keyword.get(meta, :newlines, 0)
    found = tail(body, %{ctx | vars: vars, line: line})

    case {patterns, subject} do
      {[condition], :cond} -> exclude(found, excluded_by(condition, :!=, body))
      _other -> found
    end
  end

  defp clause_tail(_other, ctx, _subject), do: [unclassified(ctx, "an unrecognised clause")]

  # --- variable resolution ---

  defp tail_var(name, ctx) do
    case Map.fetch(ctx.vars, name) do
      {:ok, {:expr, value, value_ctx}} ->
        tail(value, %{value_ctx | depth: ctx.depth, stack: ctx.stack})

      {:ok, {:shape, pattern}} ->
        tail(pattern, %{ctx | vars: bind_pattern(pattern, nil, %{})})

      {:ok, :pattern} ->
        [unclassified(ctx, "the pattern-bound variable `#{name}`")]

      {:ok, :entry} ->
        [unclassified(ctx, "the entry clause's own parameter `#{name}`")]

      :error ->
        [unclassified(ctx, "the unbound variable `#{name}`")]
    end
  end

  defp bind_statement({:=, meta, [pattern, value]}, ctx) do
    bind_match(pattern, value, node_ctx(ctx, meta), ctx)
  end

  defp bind_statement(_expr, ctx), do: ctx

  # `a = b = expr` binds both; a bare variable binds the value; any other
  # pattern's variables are pattern-bound.
  defp bind_match(pattern, value, value_ctx, ctx),
    do: %{ctx | vars: bind_pattern(pattern, {:expr, value, value_ctx}, ctx.vars)}

  # Binds the variables of `pattern` matched against `value` (`{:expr, e,
  # ctx}`, or nil when unknown). A bare variable takes the value. In
  # `left = right`, a variable beside a CONSTRUCTOR pattern (a struct, map,
  # tuple, list or literal) takes that pattern's shape (`{:error, _} = err`
  # is a two-tuple), which is at least as precise as the value. Every other
  # variable is pattern-bound.
  defp bind_pattern({name, _, context}, value, vars) when is_atom(name) and is_atom(context),
    do: Map.put(vars, name, value || :pattern)

  defp bind_pattern({:=, _, [left, right]}, value, vars) do
    vars = bind_pattern(left, shape_or(right, value), vars)
    bind_pattern(right, shape_or(left, value), vars)
  end

  defp bind_pattern(pattern, _value, vars),
    do: pattern |> pattern_vars() |> Enum.reduce(vars, &Map.put(&2, &1, :pattern))

  defp shape_or(pattern, value), do: if(constructor?(pattern), do: {:shape, pattern}, else: value)

  defp constructor?({op, _, _}) when op in [:%, :%{}, :{}, :<<>>], do: true
  defp constructor?({_name, _meta, context}) when is_atom(context), do: false
  defp constructor?({_left, _right}), do: true
  defp constructor?(list) when is_list(list), do: true

  defp constructor?(literal) when is_atom(literal) or is_number(literal) or is_binary(literal),
    do: true

  defp constructor?(_other), do: false

  # Literal values an unguarded `case` clause pattern matches outright.
  defp literal_pattern({:->, _, [[pattern], _body]}) do
    if eventless_value?(pattern), do: [pattern], else: []
  end

  defp literal_pattern(_clause), do: []

  # `{:streams, []}` is a finding only from a decision, so excluding it is a no-op for an
  # aggregate entry.
  defp eventless_value?(value),
    do: value in [[], nil, :ok, {:ok, []}, {:ok, nil}, {:streams, []}]

  # A branch taken only when `X != v` (`X !== v`, or `v != X`), whose tail is
  # `X` itself, cannot answer `v`. `sense` is the comparison that holds in the
  # branch: `:!=` for an `if`'s `do` and a `cond` clause, `:==` negated for an
  # `if`'s `else`.
  defp excluded_by({op, _, [left, right]}, :!=, body) when op in [:!=, :!==],
    do: guarded_value(left, right, body)

  defp excluded_by({op, _, [left, right]}, :==, body) when op in [:==, :===],
    do: guarded_value(left, right, body)

  defp excluded_by(_condition, _sense, _body), do: []

  defp guarded_value(left, right, body) do
    cond do
      eventless_value?(right) and same_expr?(left, body) -> [right]
      eventless_value?(left) and same_expr?(right, body) -> [left]
      true -> []
    end
  end

  defp same_expr?(a, b), do: strip_meta(a) == strip_meta(b)

  defp strip_meta(expr) do
    expr
    |> unwrap_block()
    |> Macro.prewalk(fn
      {form, meta, args} when is_list(meta) -> {form, [], args}
      node -> node
    end)
  end

  defp unwrap_block({:__block__, _, [single]}), do: unwrap_block(single)
  defp unwrap_block(expr), do: expr

  defp exclude(findings, []), do: findings

  defp exclude(findings, values),
    do: Enum.reject(findings, &(&1.kind == :eventless and &1.value in values))

  # A `with` with no `else` returns the value of a `<-` clause whose pattern
  # it does not match. Only an eventless literal the right side resolves to is
  # kept, and only when the pattern may not match it; an unclassified result
  # is dropped (a runtime fall-through is a stated residual). A bare-variable
  # pattern matches anything, so its right side never falls through. With an
  # `else`, the value goes to the `else` clauses instead.
  defp with_fall_through({:<-, meta, [pattern, value]}, ctx, branches) do
    cond do
      Keyword.has_key?(branches, :else) ->
        []

      bare_variable?(pattern) ->
        []

      true ->
        value
        |> tail(node_ctx(ctx, meta))
        |> Enum.filter(&(&1.kind == :eventless and not certainly_matches?(pattern, &1.value)))
    end
  end

  defp with_fall_through(_clause, _ctx, _branches), do: []

  defp bare_variable?({name, _, context}) when is_atom(name) and is_atom(context), do: true
  defp bare_variable?(_pattern), do: false

  # Whether `pattern` certainly matches the literal `value`. Conservative: a
  # guard, a pin, a map, a struct, a binary or anything else it cannot
  # evaluate answers false, so the value stays a finding.
  defp certainly_matches?({name, _, context}, _value) when is_atom(name) and is_atom(context),
    do: true

  defp certainly_matches?({:=, _, [left, right]}, value),
    do: certainly_matches?(left, value) and certainly_matches?(right, value)

  defp certainly_matches?({:{}, _, elements}, value) when is_list(elements),
    do: is_tuple(value) and all_match?(elements, Tuple.to_list(value))

  defp certainly_matches?({_form, meta, _args}, _value) when is_list(meta), do: false

  defp certainly_matches?({left, right}, value),
    do: is_tuple(value) and all_match?([left, right], Tuple.to_list(value))

  defp certainly_matches?([], value), do: value == []
  defp certainly_matches?(list, _value) when is_list(list), do: false

  defp certainly_matches?(literal, value)
       when is_atom(literal) or is_number(literal) or is_binary(literal),
       do: literal === value

  defp certainly_matches?(_pattern, _value), do: false

  defp all_match?(patterns, values),
    do:
      length(patterns) == length(values) and
        Enum.all?(Enum.zip(patterns, values), fn {p, v} -> certainly_matches?(p, v) end)

  defp bind_with_clause({:<-, meta, [pattern, value]}, ctx),
    do: bind_match(strip_guard(pattern), value, node_ctx(ctx, meta), ctx)

  defp bind_with_clause({:=, meta, [pattern, value]}, ctx),
    do: bind_match(pattern, value, node_ctx(ctx, meta), ctx)

  defp bind_with_clause(_expr, ctx), do: ctx

  # Every variable a pattern binds: not a pinned one, not `_`-prefixed.
  defp pattern_vars(pattern) do
    {_ast, vars} =
      Macro.prewalk(pattern, [], fn
        {:^, _, _}, acc ->
          {nil, acc}

        {:@, _, _}, acc ->
          {nil, acc}

        {name, _, context} = node, acc when is_atom(name) and is_atom(context) ->
          if String.starts_with?(Atom.to_string(name), "_"),
            do: {node, acc},
            else: {node, [name | acc]}

        node, acc ->
          {node, acc}
      end)

    Enum.uniq(vars)
  end

  # --- local calls ---

  defp local_call(:apply, _args, ctx), do: [unclassified(ctx, "`apply`")]
  defp local_call(name, _args, _ctx) when name in [:struct, :struct!], do: []

  defp local_call(name, args, ctx) do
    arity = length(args)

    cond do
      clauses_for(ctx.rec, name, arity, [:def, :defp]) != [] ->
        follow(ctx.rec, name, args, ctx, [:def, :defp])

      defines_macro?(ctx.rec, name, arity) ->
        [unclassified(ctx, "the local macro #{name}/#{arity}")]

      true ->
        imported_call(name, args, ctx)
    end
  end

  # No lexical scope is modelled, so a call may see any module that may supply it, and
  # each is analysed: Kernel for a name it exports (never removed), every indexed
  # import's macro (unclassified) or def (followed), and, when Kernel is not the sure
  # supplier, an `internal?` import outside the index (unclassified) or an external one
  # (passes). With none, the call is unclassified.
  defp imported_call(name, args, ctx) do
    arity = length(args)
    {kernel?, kernel_excluded?} = kernel_supplies(ctx, name, arity)
    macros = imported_definers(ctx, name, arity, :defmacro)
    defs = imported_definers(ctx, name, arity, :def)
    others? = not kernel? or kernel_excluded?
    internal = if others?, do: unindexed_imports(ctx, name, arity, :internal), else: []
    external = if others?, do: unindexed_imports(ctx, name, arity, :external), else: []

    found =
      if(kernel?, do: kernel_call(name, args, ctx), else: []) ++
        Enum.map(
          macros,
          &unclassified(ctx, "the imported macro #{inspect(&1.module)}.#{name}/#{arity}")
        ) ++
        Enum.flat_map(defs, &follow(&1, name, args, ctx, [:def])) ++
        Enum.map(
          internal,
          &unclassified(
            ctx,
            "the local call #{name}/#{arity}, which `import #{Macro.to_string(&1.target)}`, " <>
              "a module outside the index, may supply"
          )
        )

    if kernel? or macros != [] or defs != [] or internal != [] or external != [],
      do: found,
      else: [unclassified(ctx, "the local call #{name}/#{arity}, which has no def in the module")]
  end

  defp kernel_call(name, args, ctx) when name in [:then, :tap] and length(args) == 2,
    do: external_call(Kernel, name, args, ctx)

  defp kernel_call(_name, _args, _ctx), do: []

  # `{supplied?, excluded?}`: whether Kernel exports `name/arity`, and whether an
  # `import Kernel` anywhere (top-level or function-local, literal or not) may leave it
  # out. Kernel is never removed: an import's position is not modelled, so a top-level
  # exclusion written after a def that uses Kernel's name must not hide it. An exclusion
  # only puts the imports that may supply the call beside Kernel.
  defp kernel_supplies(ctx, name, arity) do
    cond do
      not MapSet.member?(@kernel, {name, arity}) ->
        {false, false}

      not MapSet.member?(@kernel_own, {name, arity}) ->
        {true, false}

      true ->
        kind = if MapSet.member?(@kernel_macros, {name, arity}), do: :defmacro, else: :def

        excluded? =
          Enum.any?(
            ctx.rec.imports,
            &(resolve_receiver(&1.target, ctx) == {:external, Kernel} and
                supplies(&1, name, arity, kind) != :yes)
          )

        {true, excluded?}
    end
  end

  # Every indexed module an import may supply `name/arity` from as a public `kind` clause.
  defp imported_definers(ctx, name, arity, kind) do
    for imp <- ctx.rec.imports,
        {:indexed, rec} <- [resolve_receiver(imp.target, ctx)],
        clauses_for(rec, name, arity, [kind]) != [],
        supplies(imp, name, arity, kind) != :no,
        uniq: true,
        do: rec
  end

  # The imports of `class` (`:internal` or `:external`) outside the index that may supply
  # `name/arity`. Kernel's exports are known, so its import is never a candidate here.
  defp unindexed_imports(ctx, name, arity, class) do
    for imp <- ctx.rec.imports,
        {^class, module} <- [resolve_receiver(imp.target, ctx)],
        module != Kernel,
        supplies(imp, name, arity, nil) != :no,
        do: imp
  end

  # `:yes`, `:no` or `:maybe`: whether `import` supplies `name/arity`, a clause of `kind`
  # (`:def`, `:defmacro`, or `nil` for a module outside the index, whose kinds are
  # unknown).
  defp supplies(%{only: only, except: except}, name, arity, kind) do
    case {admits(only, name, arity, kind), names(except, name, arity)} do
      {:no, _} -> :no
      {_, :yes} -> :no
      {:yes, :no} -> :yes
      _maybe -> :maybe
    end
  end

  defp admits(nil, _name, _arity, _kind), do: :yes
  defp admits(pairs, name, arity, _kind) when is_list(pairs), do: names(pairs, name, arity)
  defp admits(:sigils, name, _arity, _kind), do: yes_no(sigil?(name))
  defp admits(:functions, _name, _arity, kind), do: kind_is(kind, :def)
  defp admits(:macros, _name, _arity, kind), do: kind_is(kind, :defmacro)
  defp admits(:unknown, _name, _arity, _kind), do: :maybe

  defp names(nil, _name, _arity), do: :no
  defp names(pairs, name, arity) when is_list(pairs), do: yes_no({name, arity} in pairs)
  defp names(_unknown, _name, _arity), do: :maybe

  defp kind_is(nil, _kind), do: :maybe
  defp kind_is(kind, kind), do: :yes
  defp kind_is(_other, _kind), do: :no

  defp sigil?(name), do: String.starts_with?(Atom.to_string(name), "sigil_")

  defp yes_no(true), do: :yes
  defp yes_no(false), do: :no

  # --- remote calls ---

  defp remote_call(receiver, name, args, ctx) do
    case resolve_receiver(receiver, ctx) do
      {:indexed, rec} ->
        follow(rec, name, args, ctx, [:def])

      {:external, module} ->
        external_call(module, name, args, ctx)

      :variable ->
        [unclassified(ctx, "a call through a variable module `#{short(receiver)}.#{name}`")]

      {:conflict, alias_name} ->
        targets = Enum.map_join(ctx.rec.alias_conflicts[alias_name], ", ", &inspect/1)

        [
          unclassified(
            ctx,
            "the alias `#{alias_name}` names more than one module in this file: #{targets}"
          )
        ]

      {:internal, segments} ->
        [
          unclassified(
            ctx,
            "#{Enum.join(segments, ".")}.#{name}/#{length(args)}, a module outside the index"
          )
        ]
    end
  end

  defp external_call(Keyword, name, [opts, key | rest], ctx) when name in [:get, :fetch!] do
    default =
      case {name, rest} do
        {:get, [default]} -> {:default, default}
        {:get, []} -> {:implicit_nil, "Keyword.get/2"}
        {:fetch!, _} -> :raises
      end

    outcomes = keyword_lookup(opts, key, ctx)

    Enum.flat_map(outcomes, fn
      {:value, value, value_ctx} -> tail(value, %{value_ctx | depth: ctx.depth, stack: ctx.stack})
      :absent -> default_tail(default, ctx)
      :unknown -> unresolved_options(name, key, ctx) ++ default_tail(default, ctx)
    end)
    |> Enum.uniq()
  end

  defp external_call(Map, :get, [_map, _key, default], ctx),
    do: default_tail({:default, default}, ctx)

  defp external_call(Map, :get, [_map, _key], ctx),
    do: default_tail({:implicit_nil, "Map.get/2"}, ctx)

  defp external_call(Access, :get, [container | _], ctx),
    do: [unclassified(ctx, "a runtime Access read on `#{short(container)}`")]

  defp external_call(Kernel, :then, [value, fun], ctx),
    do: apply_fun(fun, ctx, [{:expr, value, ctx}], ctx)

  defp external_call(Kernel, :tap, [value, _fun], ctx), do: tail(value, ctx)

  defp external_call(module, :apply, _args, ctx) when module in [Kernel, :erlang],
    do: [unclassified(ctx, "`apply`")]

  defp external_call(_module, _name, _args, _ctx), do: []

  defp default_tail({:default, default}, ctx) do
    for finding <- tail(default, ctx) do
      if finding.kind == :eventless,
        do: %{finding | shape: "#{finding.shape} (a Keyword.get/Map.get default)"},
        else: finding
    end
  end

  defp default_tail({:implicit_nil, call}, ctx),
    do: [eventless(ctx, "nil (#{call} with no default)", nil)]

  defp default_tail(:raises, _ctx), do: []

  defp unresolved_options(name, key, ctx),
    do: [unclassified(ctx, "Keyword.#{name} of #{short(key)} on options the walk cannot resolve")]

  # Resolves a receiver to `{:indexed, rec}`, `{:external, module}`,
  # `{:internal, segments}` (a name the index does not hold and the caller's
  # `internal?` predicate claims), `{:conflict, alias}` (a first segment the module's
  # alias table maps to more than one module) or `:variable`.
  defp resolve_receiver({:__MODULE__, _, context}, ctx) when is_atom(context),
    do: {:indexed, ctx.rec}

  defp resolve_receiver({:__aliases__, _, [{:__MODULE__, _, context} | rest]}, ctx)
       when is_atom(context) do
    if Enum.all?(rest, &is_atom/1),
      do: by_segments([module_segments(ctx.rec.module) ++ rest], ctx),
      else: :variable
  end

  defp resolve_receiver({:__aliases__, _, [first | _] = segments}, ctx) do
    cond do
      not Enum.all?(segments, &is_atom/1) -> :variable
      Map.has_key?(ctx.rec.alias_conflicts, first) -> {:conflict, first}
      true -> by_segments(candidates(segments, ctx), ctx)
    end
  end

  defp resolve_receiver(module, ctx) when is_atom(module) and not is_nil(module) do
    case Atom.to_string(module) do
      "Elixir." <> name ->
        by_segments([Enum.map(String.split(name, "."), &String.to_atom/1)], ctx)

      _erlang ->
        if ctx.internal?.(module), do: {:internal, [module]}, else: {:external, module}
    end
  end

  defp resolve_receiver(_receiver, _ctx), do: :variable

  # The written name, expanded through the alias table (chained, at most a few
  # steps), as written, and qualified against each enclosing module (a nested
  # module's short name).
  defp candidates(segments, ctx) do
    expanded = expand_chain(segments, ctx.rec.aliases, 5)
    module = module_segments(ctx.rec.module)

    enclosing =
      for n <- length(module)..1//-1, do: Enum.take(module, n) ++ segments

    Enum.uniq([expanded, segments | enclosing])
  end

  defp expand_chain(segments, _aliases, 0), do: segments

  defp expand_chain(segments, aliases, steps) do
    case AliasMap.expand(segments, aliases) do
      ^segments -> segments
      expanded -> expand_chain(expanded, Map.delete(aliases, hd(segments)), steps - 1)
    end
  end

  defp by_segments([first | _] = candidates, ctx) do
    found =
      Enum.find_value(candidates, fn segments ->
        Map.get(ctx.index, Module.concat(segments))
      end)

    # The predicate is asked about the name as written, expanded through the alias table:
    # the enclosing-module candidates are guesses the index settles, not names.
    module = Module.concat(first)

    cond do
      found -> {:indexed, found}
      ctx.internal?.(module) -> {:internal, first}
      true -> {:external, module}
    end
  end

  defp module_segments(module) do
    case Atom.to_string(module) do
      "Elixir." <> name -> Enum.map(String.split(name, "."), &String.to_atom/1)
      _erlang -> [module]
    end
  end

  # --- following ---

  defp follow(rec, name, args, ctx, kinds) do
    arity = length(args)
    key = {rec.module, name, arity}
    clauses = clauses_for(rec, name, arity, kinds)

    cond do
      key in ctx.stack ->
        []

      clauses != [] ->
        bound = Enum.map(args, &{:expr, &1, ctx})
        Enum.flat_map(clauses, &follow_clause(&1, rec, bound, key, ctx))

      defines_macro?(rec, name, arity) ->
        [unclassified(ctx, "#{inspect(rec.module)}.#{name}/#{arity}, a macro")]

      true ->
        [unclassified(ctx, "#{inspect(rec.module)}.#{name}/#{arity}, which has no def in source")]
    end
  end

  defp follow_clause(clause, rec, bound_args, key, ctx) do
    base = %{
      ctx
      | rec: rec,
        fun: {clause.name, length(bound_args)},
        vars: %{},
        stack: [key | ctx.stack],
        line: clause.line,
        depth: ctx.depth + 1
    }

    vars = bind_params(clause, bound_args, base)
    walk_body(clause, %{base | vars: vars})
  end

  # Binds a clause's parameters to `bound` arguments; missing ones take their
  # default, as Elixir fills defaults left to right.
  defp bind_params(clause, bound, base) do
    filled = length(bound) - (clause.n - map_size(clause.defaults))

    {pairs, _rest, _filled} =
      clause.params
      |> Enum.with_index()
      |> Enum.reduce({[], bound, filled}, fn {param, idx}, {acc, args, fill} ->
        case Map.fetch(clause.defaults, idx) do
          {:ok, default} when fill <= 0 -> {[{param, {:expr, default, base}} | acc], args, fill}
          {:ok, _default} -> {[{param, hd(args)} | acc], tl(args), fill - 1}
          :error -> {[{param, hd(args)} | acc], tl(args), fill}
        end
      end)

    Enum.reduce(pairs, %{}, fn {param, value}, vars -> bind_param(param, value, vars) end)
  end

  defp bind_param(pattern, value, vars), do: bind_pattern(pattern, value, vars)

  # --- anonymous calls ---

  # `fexpr` evaluated in `fun_ctx`, applied to `args` (each `{:expr, e, ctx}`)
  # at the call site `ctx`.
  defp apply_fun({:fn, _meta, clauses}, fun_ctx, args, ctx) do
    Enum.flat_map(clauses, fn
      {:->, meta, [params, body]} ->
        params = fn_params(params)

        vars =
          if length(params) == length(args),
            do:
              params
              |> Enum.zip(args)
              |> Enum.reduce(fun_ctx.vars, fn {p, v}, acc -> bind_param(p, v, acc) end),
            else: fun_ctx.vars

        line = Keyword.get(meta, :line, fun_ctx.line) + Keyword.get(meta, :newlines, 0)
        tail(body, %{fun_ctx | vars: vars, line: line, depth: ctx.depth + 1, stack: ctx.stack})

      _other ->
        [unclassified(ctx, "an unrecognised fn clause")]
    end)
  end

  defp apply_fun({:&, _, [{:/, _, [callee, arity]}]}, fun_ctx, args, ctx)
       when is_integer(arity) do
    exprs = Enum.map(args, fn {:expr, e, _c} -> e end)
    call_ctx = %{fun_ctx | depth: ctx.depth, stack: ctx.stack}

    case callee do
      {{:., _, [receiver, name]}, _, []} ->
        remote_call(receiver, name, exprs, call_ctx)

      {name, _, context} when is_atom(name) and is_atom(context) ->
        local_call(name, exprs, call_ctx)

      _other ->
        [unclassified(ctx, "an unrecognised capture")]
    end
  end

  defp apply_fun({name, _meta, context}, fun_ctx, args, ctx)
       when is_atom(name) and is_atom(context) do
    case Map.fetch(fun_ctx.vars, name) do
      {:ok, {:expr, value, value_ctx}} ->
        apply_fun(value, value_ctx, args, ctx)

      _other ->
        [unclassified(ctx, "an anonymous call on `#{name}`, whose value the walk cannot resolve")]
    end
  end

  defp apply_fun({:|>, _, [lhs, rhs]}, fun_ctx, args, ctx),
    do: apply_fun(pipe(lhs, rhs), fun_ctx, args, ctx)

  defp apply_fun(
         {{:., _, [receiver, name]}, _meta, [opts, key | rest]} = expr,
         fun_ctx,
         args,
         ctx
       )
       when name in [:fetch!, :get] do
    case resolve_receiver(receiver, fun_ctx) do
      {:external, Keyword} ->
        Enum.flat_map(keyword_lookup(opts, key, fun_ctx), fn
          {:value, value, value_ctx} ->
            apply_fun(value, value_ctx, args, ctx)

          :absent when name == :get and rest != [] ->
            apply_fun(hd(rest), fun_ctx, args, ctx)

          :absent when name == :get ->
            [eventless(ctx, "nil (Keyword.get/2 with no default)", nil)]

          :absent ->
            []

          :unknown ->
            unresolved_options(name, key, ctx)
        end)

      _other ->
        [unclassified(ctx, "an anonymous call on `#{short(expr)}`")]
    end
  end

  defp apply_fun(expr, _fun_ctx, _args, ctx),
    do: [unclassified(ctx, "an anonymous call on `#{short(expr)}`")]

  defp fn_params([{:when, _, params_and_guard}]), do: Enum.drop(params_and_guard, -1)
  defp fn_params(params) when is_list(params), do: params

  # --- keyword options ---

  # The possible outcomes of reading `key` from `opts`: `{:value, expr, ctx}`,
  # `:absent` or `:unknown`.
  defp keyword_lookup(opts, key, ctx) when is_atom(key) do
    cond do
      is_list(opts) and keyword_literal?(opts) ->
        case List.keyfind(opts, key, 0) do
          {^key, value} -> [{:value, value, ctx}]
          nil -> [:absent]
        end

      true ->
        keyword_lookup_node(opts, key, ctx)
    end
  end

  defp keyword_lookup(_opts, _key, _ctx), do: [:unknown]

  defp keyword_lookup_node({name, _, context}, key, ctx)
       when is_atom(name) and is_atom(context) do
    case Map.fetch(ctx.vars, name) do
      {:ok, {:expr, value, value_ctx}} -> keyword_lookup(value, key, value_ctx)
      _other -> [:unknown]
    end
  end

  defp keyword_lookup_node({:|>, _, [lhs, rhs]}, key, ctx),
    do: keyword_lookup(pipe(lhs, rhs), key, ctx)

  defp keyword_lookup_node({:__block__, _, [single]}, key, ctx),
    do: keyword_lookup(single, key, ctx)

  defp keyword_lookup_node({{:., _, [receiver, op]}, _, args}, key, ctx) when is_list(args) do
    case resolve_receiver(receiver, ctx) do
      {:external, Keyword} -> keyword_op(op, args, key, ctx)
      _other -> [:unknown]
    end
  end

  defp keyword_lookup_node(_opts, _key, _ctx), do: [:unknown]

  defp keyword_op(:put, [base, k, v], key, ctx) when is_atom(k),
    do: if(k == key, do: [{:value, v, ctx}], else: keyword_lookup(base, key, ctx))

  defp keyword_op(:put_new, [base, k, v], key, ctx) when is_atom(k) do
    base_outcomes = keyword_lookup(base, key, ctx)

    if k == key,
      do: Enum.map(base_outcomes, &if(&1 == :absent, do: {:value, v, ctx}, else: &1)),
      else: base_outcomes
  end

  defp keyword_op(:merge, [left, right], key, ctx) do
    Enum.flat_map(keyword_lookup(right, key, ctx), fn
      :absent -> keyword_lookup(left, key, ctx)
      other -> [other]
    end)
  end

  defp keyword_op(:delete, [base, k], key, ctx) when is_atom(k),
    do: if(k == key, do: [:absent], else: keyword_lookup(base, key, ctx))

  defp keyword_op(_op, _args, _key, _ctx), do: [:unknown]

  defp keyword_literal?(list),
    do: Enum.all?(list, &match?({k, _v} when is_atom(k), &1))

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp pipe(lhs, {{:., _, _} = call, meta, args}) when is_list(args),
    do: {call, meta, [lhs | args]}

  defp pipe(lhs, {name, meta, args}) when is_atom(name) and is_list(args),
    do: {name, meta, [lhs | args]}

  defp pipe(lhs, {name, meta, context}) when is_atom(name) and is_atom(context),
    do: {name, meta, [lhs]}

  defp pipe(lhs, rhs), do: {:|>, [], [lhs, rhs]}

  # A node with a line moves the reported line to it.
  defp node_ctx(ctx, meta) do
    case Keyword.fetch(meta, :line) do
      {:ok, line} -> %{ctx | line: line}
      :error -> ctx
    end
  end

  # A literal written alone on the line after a `do` sits on the do's next line.
  defp do_ctx(ctx, meta) do
    case Keyword.fetch(meta, :do) do
      {:ok, do_meta} -> %{ctx | line: Keyword.get(do_meta, :line, ctx.line) + 1}
      :error -> node_ctx(ctx, meta)
    end
  end

  defp else_ctx(ctx, meta) do
    case Keyword.fetch(meta, :end) do
      {:ok, end_meta} -> %{ctx | line: Keyword.get(end_meta, :line, ctx.line + 1) - 1}
      :error -> node_ctx(ctx, meta)
    end
  end

  # The last expression of a block starts where the previous one ended, plus
  # its newlines.
  defp last_ctx(ctx, {_form, meta, _args}, _last) when is_list(meta) do
    case Keyword.fetch(meta, :end_of_expression) do
      {:ok, eoe} ->
        %{ctx | line: Keyword.get(eoe, :line, ctx.line) + Keyword.get(eoe, :newlines, 0)}

      :error ->
        ctx
    end
  end

  defp last_ctx(ctx, _previous, _last), do: ctx

  defp short(expr) do
    text = expr |> Macro.to_string() |> String.replace(~r/\s+/, " ")
    if String.length(text) > 60, do: String.slice(text, 0, 57) <> "...", else: text
  end

  defp eventless(ctx, shape, value), do: Map.put(finding(:eventless, ctx, shape), :value, value)
  defp unclassified(ctx, reason), do: finding(:unclassified, ctx, "unclassified: " <> reason)

  defp finding(kind, ctx, shape) do
    %{
      kind: kind,
      file: ctx.rec.file,
      line: ctx.line,
      module: ctx.rec.module,
      fun: ctx.fun,
      shape: shape,
      origin: ctx.origin
    }
  end

  defp with_message(%{module: module, fun: {name, arity}} = f) do
    reached =
      case f.origin do
        nil ->
          ""

        origin ->
          "  (reached from #{inspect(origin.module)}.#{origin.fun} clause at #{origin.file}:#{origin.line})"
      end

    Map.put(
      f,
      :message,
      "#{f.file}:#{f.line}  #{inspect(module)}.#{name}/#{arity}  #{f.shape}#{reached}"
    )
  end
end
