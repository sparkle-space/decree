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

  `rules` is a list of `{decision, items}`, `items` being what the decision's
  `c:Decree.Decision.query/2` answered for a fixture command. `appenders` maps each event
  module to its `{command_module, target_module}` rows. The options:

    - `conditional:` command modules an aggregate decides under the rule's condition.
    - `dominant:` `{event_module, command_module}` pairs, the exceptions named by hand.

  For each rule, for each type its items name, it checks two things.

  **Tag coverage.** Each tag's kind on an item naming the type must be one the module can
  produce, by `Decree.Event.RequiredTags.producible_kinds/1`. Otherwise the problem is
  `{:missing_tag_kind, type, kind, decision}`. A conditional kind counts for nothing here: an
  event that may lack a kind must still be able to carry it.

  **Condition coverage.** A type with no row is `{:no_appender_row, type, decision}`. Every
  row must be the rule's own decision, or another rule's decision whose items name that type
  with at least these kinds, or a command in `conditional:`, or a pair in `dominant:`.
  Otherwise the problem is `{:unchecked_appender, type, command, target, decision}`.

  A type that names no event module is `{:unknown_type, type, decision}`.
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

  @doc """
  Answers the problems, by rule and by type, in the order given.
  """
  @spec check([rule()], appenders(), [option()]) :: [problem()]
  def check(rules, appenders, opts \\ []) when is_list(rules) and is_map(appenders) do
    for {decision, items} <- rules,
        type <- Read.named_types(items),
        problem <- type_problems(type, decision, items, {rules, appenders, opts}) do
      problem
    end
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
    target == decision or
      command in Keyword.get(opts, :conditional, []) or
      {module, command} in Keyword.get(opts, :dominant, []) or
      Enum.any?(rules, &covers?(&1, target, type, kinds))
  end

  defp covers?({decision, items}, target, type, kinds) do
    decision == target and type in Read.named_types(items) and
      kinds -- Read.kinds_naming(items, type) == []
  end

  # `to_struct/1` answers a struct built with no keys, so `@enforce_keys` does not come into it.
  defp event_module(type) do
    %module{} = TypeProvider.to_struct(type)

    {:ok, module}
  rescue
    _unknown -> :error
  end
end
