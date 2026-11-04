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
        [{{:decision, ClaimEmail}, ClaimEmail.query(fixture_command, fixture_context)}],
        %{EmailClaimed => [{ClaimEmailCommand, ClaimEmail}, {ImportUser, User}]},
        conditional: [ImportUser]
      )

  `rules` is a list of `{key, items}`, `items` being every condition item of a fixture
  dispatch: what `c:Decree.Decision.query/2` and any `c:Decree.Decision.followup_query/3`
  answered, and on a create the fact item `[types: types, tags: [stream_tag]]` first. The
  stream tag is the route's, and this reads no router, so the application passes that item:

      {{:decision, CreateClaim}, [[types: types, tags: ["claim:" <> fixture_id]] | query_items]}

  **Keys.** A key says what its module is, and nothing finds that out another way: not loading
  the module, its exports, its name or the order things compile in.

    - `{:decision, module}`: a `Decree.Decision`. Its `__decision__/0` is called for its
      `create:` types, so a module that is no decision raises. It covers every row whose
      target is the module, and another rule's rows as below.
    - `{:rule, module}`: a `Decree.Decision.Rule`. Its `__rule__/0` is called, so a module that
      is no rule raises. It is no create and covers no row: the commands its route decides go
      in `conditional:`.
    - `{:aggregate, module}`: nothing is called. Its items are checked as any rule's are, and it
      covers no row, its own commands' included.

  Any other key, a bare module included, raises `ArgumentError` naming it, before any problem
  is computed. Problems name the module, not the key.

  `appenders` maps each event module to its `{command_module, target_module}` rows. The
  options:

    - `conditional:` command modules an aggregate decides under the rule's condition.
    - `dominant:` `{event_module, command_module}` pairs, the exceptions named by hand.

  **Fact item.** `c:Decree.Decision.query/2` may answer `[]` on a create, so a rule without
  the fact item names none of what the create reads. For a decision declaring `create:`, the
  rule's first item must name exactly the declared types, in any order, and exactly one tag,
  as it is first at a dispatch. An item further down does not count. Otherwise the problem is
  `{:no_fact_item, decision}`, ahead of the rule's other problems. Only a `:decision` key is
  ever a create.

  Then, for each rule, for each type its items name, it checks two things.

  **Tag coverage.** Each tag's kind on an item naming the type must be one the module can
  produce, by `Decree.Event.RequiredTags.producible_kinds/1`. Otherwise the problem is
  `{:missing_tag_kind, type, kind, decision}`. A conditional kind counts for nothing here: an
  event that may lack a kind must still be able to carry it.

  **Condition coverage.** A type with no row is `{:no_appender_row, type, decision}`. Every
  row's target must be the rule's own `:decision` key, or another `:decision` key whose items
  name that type with at least these kinds, or its command must be in `conditional:`, or the
  pair in `dominant:`. Every rule of that other decision naming the type must, because each
  stands for a dispatch appending it, and a rule not naming it says nothing. Otherwise the
  problem is `{:unchecked_appender, type, command, target, decision}`. A rule's fact item
  covers nothing for another rule, because a dispatch asks no event for its kinds.

  A type that names no event module is `{:unknown_type, type, decision}`.

  **A declared aggregate's rule.** A `Decree.Decision.Rule` goes in `rules` under a `:rule`
  key, `{{:rule, EmailRule}, items}`, with the row `{command, aggregate}` and each command the
  rule's route decides in `conditional:`. An aggregate key, `{{:aggregate, User}, items}`,
  certifies none of its own commands: each still needs `conditional:` or `dominant:`.
  """

  alias Decree.Decision.Read
  alias Decree.Event.RequiredTags
  alias Decree.EventStore.TypeProvider

  @type role :: :decision | :rule | :aggregate
  @type rule :: {{role(), module()}, items :: [keyword() | map()]}
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

  Raises `ArgumentError` naming the first key that is not `{:decision, module}`,
  `{:rule, module}` or `{:aggregate, module}`, before any problem is computed.
  """
  @spec check([rule()], appenders(), [option()]) :: [problem()]
  def check(rules, appenders, opts \\ []) when is_list(rules) and is_map(appenders) do
    Enum.each(rules, &check_key!/1)

    world = {rules, appenders, opts}

    Enum.flat_map(rules, fn {{role, module}, items} ->
      fact_problems(role, module, items) ++
        Enum.flat_map(Read.named_types(items), &type_problems(&1, {role, module}, items, world))
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

  @roles [:decision, :rule, :aggregate]

  # The key says what its module is: nothing here finds that out another way.
  defp check_key!({{role, module}, _items}) when role in @roles and is_atom(module), do: :ok

  defp check_key!({key, _items}) do
    raise ArgumentError,
          "an appender map key is {:decision, module}, {:rule, module} or " <>
            "{:aggregate, module}, got: " <> inspect(key)
  end

  defp check_key!(rule) do
    raise ArgumentError,
          "an appender map rule is {key, items}, got: " <> inspect(rule)
  end

  defp fact_problems(role, module, items) do
    case create_types(role, module) do
      false ->
        []

      types ->
        if fact_head?(items, types), do: [], else: [{:no_fact_item, module}]
    end
  end

  defp fact_head?([item | _items], types), do: fact_item?(item, types)
  defp fact_head?([], _types), do: false

  # What a dispatch prepends has one tag, so an item with more is a query about the type.
  defp fact_item?(item, types) do
    MapSet.new(Read.item_types(item)) == MapSet.new(types) and
      match?([_stream_tag], Read.item_tags(item))
  end

  # A module of another role than its key's raises here, which is the loud answer to a
  # mislabelled key. An aggregate is asked nothing.
  defp create_types(:decision, module), do: module.__decision__().create

  defp create_types(:rule, module) do
    module.__rule__()
    false
  end

  defp create_types(:aggregate, _module), do: false

  defp type_problems(type, {_role, module} = key, items, world) do
    case event_module(type) do
      {:ok, event} ->
        kinds = Read.kinds_naming(items, type)

        tag_problems(type, module, event, kinds) ++
          appender_problems(type, key, event, kinds, world)

      :error ->
        [{:unknown_type, type, module}]
    end
  end

  defp tag_problems(type, module, event, kinds) do
    for kind <- kinds -- RequiredTags.producible_kinds(event) do
      {:missing_tag_kind, type, kind, module}
    end
  end

  defp appender_problems(
         type,
         {_role, module} = key,
         event,
         kinds,
         {_rules, appenders, _} = world
       ) do
    case Map.get(appenders, event, []) do
      [] ->
        [{:no_appender_row, type, module}]

      rows ->
        for {command, target} <- rows,
            not covered?({command, target}, {type, key, event, kinds}, world) do
          {:unchecked_appender, type, command, target, module}
        end
    end
  end

  # Only a decision key covers a row by its target: a rule's and an aggregate's commands are
  # covered through `conditional:` or `dominant:`.
  defp covered?({command, target}, {type, {role, module}, event, kinds}, {rules, _, opts}) do
    (role == :decision and target == module) or
      command in Keyword.get(opts, :conditional, []) or
      {event, command} in Keyword.get(opts, :dominant, []) or
      covers?(rules, target, type, kinds)
  end

  # Each rule naming the type is a dispatch appending it, so one short of `kinds` is a miss. Only
  # a decision key is consulted, never a rule's or an aggregate's.
  defp covers?(rules, target, type, kinds) do
    naming =
      for {{:decision, ^target}, items} <- rules, type in Read.named_types(items), do: items

    naming != [] and
      Enum.all?(naming, &(kinds -- Read.kinds_naming(asked_items(target, &1), type) == []))
  end

  # As a dispatch asks: the target's condition reads the fact item, and no event is asked for it.
  defp asked_items(decision, items) do
    types = create_types(:decision, decision)

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
