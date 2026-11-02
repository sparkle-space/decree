defmodule Decree.Decision.AppenderMap do
  @moduledoc """
  Finds the event types a decision's condition can miss.

  A condition holds only if every event it should see carries the tags it looks for, and
  only if whatever appends such an event is itself decided under a condition covering it. The
  store can check neither from one append: a plain aggregate appending a queried type has no
  condition to take kinds from. See `Decree.Event.RequiredTags`.

  It is opt-in. Nothing in decree calls it, and it reads no router. An application calls it
  from a build check of its own, with its rules and one map of who appends what:

      Decree.Decision.AppenderMap.verify!(
        [{ClaimEmail, ClaimEmail.query(fixture_command, fixture_context)}],
        %{EmailClaimed => [{ClaimEmailCommand, ClaimEmail}, {ImportUser, User}]},
        conditional: [ImportUser]
      )

  `rules` is a list of `{decision, items}`, `items` being every condition item of a fixture
  dispatch: what `c:Decree.Decision.query/2` and any `c:Decree.Decision.followup_query/3`
  answered, and on a create the fact item `[types: types, tags: [stream_tag]]` first. The
  stream tag is the route's, and this reads no router, so the application passes that item:

      {CreateClaim, [[types: types, tags: ["claim:" <> fixture_id]] | query_items]}

  `appenders` maps each event module to its `{command_module, target_module}` rows. The
  options:

    - `conditional:` command modules an aggregate decides under the rule's condition.
    - `dominant:` `{event_module, command_module}` pairs, the exceptions named by hand.

  **Fact item.** `c:Decree.Decision.query/2` may answer `[]` on a create, so a rule without
  the fact item names none of what the create reads. For a decision declaring `create:`, the
  rule's first item must name exactly the declared types, in any order, and exactly one tag,
  as it is first at a dispatch. An item further down does not count. Otherwise the problem is
  `{:no_fact_item, decision}`, ahead of the rule's other problems. A module with no
  `__decision__/0` is no create.

  Then, for each rule, for each type its items name, it checks two things.

  **Tag coverage.** Each tag's kind on an item naming the type must be one the module can
  produce, by `Decree.Event.RequiredTags.producible_kinds/1`. Otherwise the problem is
  `{:missing_tag_kind, type, kind, decision}`. A conditional kind counts for nothing here: an
  event that may lack a kind must still be able to carry it.

  **Condition coverage.** A type with no row is `{:no_appender_row, type, decision}`. Every
  row must be the rule's own decision, or another rule's decision whose items name that type
  with at least these kinds, or a command in `conditional:`, or a pair in `dominant:`. Every
  rule of that other decision naming the type must, because each stands for a dispatch
  appending it, and a rule not naming it says nothing. Otherwise the problem is
  `{:unchecked_appender, type, command, target, decision}`. A rule's fact item covers nothing
  for another rule, because a dispatch asks no event for its kinds.

  A type that names no event module is `{:unknown_type, type, decision}`.

  **A declared aggregate's rule.** A `Decree.Decision.Rule` goes in `rules` as a decision does,
  and it is no create, having no `__decision__/0`. List it by its own module,
  `{EmailRule, items}`, with the row `{command, aggregate}` and each command the rule's route
  decides in `conditional:`. A key covers an appender by its own name, as the rule's own
  decision or as another rule's, only when its module compiles and is a decision or a rule, by
  `__decision__/0` or `__rule__/0`, or exports neither `apply/2` nor `execute/2`. An aggregate
  covers none by its name, and nor does a key naming no module.
  """

  alias Decree.Decision.Read
  alias Decree.Event.RequiredTags
  alias Decree.EventStore.TypeProvider

  @type rule :: {decision :: module(), items :: [keyword() | map()]}
  @type appenders :: %{module() => [{command :: module(), target :: module()}]}
  @type option :: {:conditional, [module()]} | {:dominant, [{module(), module()}]}

  @type problem ::
          {:missing_tag_kind, type :: String.t(), kind :: String.t(), decision :: module()}
          | {:no_appender_row, type :: String.t(), decision :: module()}
          | {:unchecked_appender, type :: String.t(), command :: module(), target :: module(),
             decision :: module()}
          | {:unknown_type, type :: String.t(), decision :: module()}
          | {:no_fact_item, decision :: module()}

  @doc """
  Answers the problems, by rule and by type, in the order given.
  """
  @spec check([rule()], appenders(), [option()]) :: [problem()]
  def check(rules, appenders, opts \\ []) when is_list(rules) and is_map(appenders) do
    Enum.flat_map(rules, fn {decision, items} ->
      world = {rules, appenders, opts}

      fact_problems(decision, items) ++
        Enum.flat_map(Read.named_types(items), &type_problems(&1, decision, items, world))
    end)
  end

  @doc """
  Answers `:ok` when there are no problems, and raises `ArgumentError` naming every one.
  """
  @spec verify!([rule()], appenders(), [option()]) :: :ok
  def verify!(rules, appenders, opts \\ []) do
    case check(rules, appenders, opts) do
      [] ->
        :ok

      problems ->
        raise ArgumentError,
              "decisions a condition can miss an event of:\n" <>
                Enum.map_join(problems, "\n", &("  " <> inspect(&1)))
    end
  end

  defp fact_problems(decision, items) do
    case create_types(decision) do
      false ->
        []

      types ->
        if fact_head?(items, types), do: [], else: [{:no_fact_item, decision}]
    end
  end

  defp fact_head?([item | _items], types), do: fact_item?(item, types)
  defp fact_head?([], _types), do: false

  # What a dispatch prepends has one tag, so an item with more is a query about the type.
  defp fact_item?(item, types) do
    MapSet.new(Read.item_types(item)) == MapSet.new(types) and
      match?([_stream_tag], Read.item_tags(item))
  end

  # An aggregate's command runs under a rule's condition or under none, and no router says which,
  # so an aggregate covers nothing by its own name. A decision or a rule may still have helpers
  # named `execute/2` or `apply/2`, so its own export says what it is first.
  defp covers_by_name?(module) do
    resolves?(module) and
      (function_exported?(module, :__decision__, 0) or function_exported?(module, :__rule__, 0) or
         not (function_exported?(module, :apply, 2) or function_exported?(module, :execute, 2)))
  end

  # Under the compiler this waits for a module the same build is still compiling, where a load
  # would answer no and let it through, and outside it, it loads. A module that does not resolve
  # answers no, a misspelt key and one a wait would deadlock on alike.
  defp resolves?(module), do: match?({:module, _module}, Code.ensure_compiled(module))

  defp create_types(decision) do
    if resolves?(decision) and function_exported?(decision, :__decision__, 0) do
      decision.__decision__().create
    else
      false
    end
  end

  defp type_problems(type, decision, items, world) do
    case event_module(type) do
      {:ok, module} ->
        kinds = Read.kinds_naming(items, type)

        tag_problems(type, decision, module, kinds) ++
          appender_problems(type, decision, module, kinds, world)

      :error ->
        [{:unknown_type, type, decision}]
    end
  end

  defp tag_problems(type, decision, module, kinds) do
    for kind <- kinds -- RequiredTags.producible_kinds(module) do
      {:missing_tag_kind, type, kind, decision}
    end
  end

  defp appender_problems(type, decision, module, kinds, {_rules, appenders, _opts} = world) do
    case Map.get(appenders, module, []) do
      [] ->
        [{:no_appender_row, type, decision}]

      rows ->
        for {command, target} <- rows,
            not covered?({command, target}, {type, decision, module, kinds}, world) do
          {:unchecked_appender, type, command, target, decision}
        end
    end
  end

  defp covered?({command, target}, {type, decision, module, kinds}, {rules, _appenders, opts}) do
    (target == decision and covers_by_name?(decision)) or
      command in Keyword.get(opts, :conditional, []) or
      {module, command} in Keyword.get(opts, :dominant, []) or
      covers?(rules, target, type, kinds)
  end

  # Each rule naming the type is a dispatch appending it, so one short of `kinds` is a miss.
  defp covers?(rules, target, type, kinds) do
    naming = for {^target, items} <- rules, type in Read.named_types(items), do: items

    naming != [] and covers_by_name?(target) and
      Enum.all?(naming, &(kinds -- Read.kinds_naming(asked_items(target, &1), type) == []))
  end

  # As a dispatch asks: the target's condition reads the fact item, and no event is asked for it.
  defp asked_items(decision, items) do
    types = create_types(decision)

    if types != false and fact_head?(items, types), do: tl(items), else: items
  end

  # `to_struct/1` answers a struct built with no keys, so `@enforce_keys` does not come into it.
  defp event_module(type) do
    %module{} = TypeProvider.to_struct(type)

    {:ok, module}
  rescue
    _unknown -> :error
  end
end
