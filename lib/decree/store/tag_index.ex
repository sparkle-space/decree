defmodule Decree.Store.TagIndex do
  @moduledoc """
  The grammar of the facts that say whether a tag index can be trusted.

  A condition finds an event by its tags, so a decision rests on the tag rows being complete.
  Whether they are is a fact on the ledger, in events the application appends. Decree ships no
  such event and no command: it ships the grammar, the store's guard on it, the fold a
  decision runs over it, and the release step's check.

  ## The grammar

  A fact is an event carrying `fact_tag/0`, `"tag_index:fact"`, and at least one state tag:

      tag_index_state:<family>:<key or ->:<enabled|disabled>

  `state_tag/3` builds one. `<family>` is a tag kind and `<key>` a fingerprint, or `-` for a
  family with no key. The store refuses an append, with `{:error, :invalid_tag_index_fact}`
  and before any statement, when an event carries a `tag_index` tag that is not exactly
  `tag_index:fact`, a `tag_index_state` tag that does not parse, or one of the two without
  the other. A disable with no fact tag would be invisible to the read that finds the facts,
  so an earlier enable would stand.

  `tag_index` and `tag_index_state` are `reserved_kinds/0`. Neither can be declared deletable,
  and `delete_stream_tags` refuses them, because a fact whose state tag is gone could have
  been a disable.

  ## The families

  A tag of a kind in `named_families/0` is in the family of that kind, and so is a tag of a
  kind a decision lists in its `index_families/0`. Any other kind is in the `entity` family.
  Decree's own named families are `command`, `command_kind` and `correlation`; the
  application adds its own kinds through the application environment:

      config :decree, :tag_index_named_families, ["tenant_slug", "plan_slug"]

  Every node must run the same list for the store's life. The list decides which facts a
  decision requires, so changing it changes what an existing fact covers, and a kind left out
  falls into the `entity` family without a word.

  A kind listed `:digest` holds `<fingerprint>:<digest>`, and its family's key is that
  fingerprint: `digest_key/1` reads it. Every other family has no key.

  ## The fold

  `fold/1` is the one fold. It takes the facts in position order: the last state per family
  and key wins, and a family and key with no fact is disabled. A fact the fold cannot
  attribute, one whose state tags are gone, refuses as a disable would. A decision runs it
  over the facts in its own read, and `states/2` over a read of every fact on the store, so
  the release check and every dispatch agree. See "Index facts" in `Decree.Decision`.

  ## The fingerprint

  `fingerprint/1` is DOC-1's form: the first 8 bytes of HMAC-SHA-256 keyed by the key material
  over `"tag-digest-fingerprint"`, in lowercase hex, 16 characters. It is derived from the key
  material and shows nothing about the key, and it holds no `:`, so it reads back from a
  `<fingerprint>:<digest>` value. It names a digest key without holding it, so one key's tags
  and its retirement meet under one name. The key and the digests stay the application's.

  ## The lifecycle

    - **A fresh store.** The application's release step appends the enabling batch only when
      `$all` is empty, so the facts sit from position 1 and every later event is covered by
      them. `bootstrap/3` is that step: the empty-store check, then the application's
      `enable`. The check is `stream_info(:all)` on the application's `Decree.Store` module:
      `$all`'s `stream_version` is the number of the last event appended to the store, and 0
      when none has been.

          case MyApp.EventStore.stream_info(:all) do
            {:ok, %Decree.Store.Streams.StreamInfo{stream_version: 0}} -> :empty
            {:ok, %Decree.Store.Streams.StreamInfo{}} -> :history
          end

      The check and the enable are two steps, with no guard between them: run both before
      any other writer starts, because an event appended between them is not checked.

      The check is the application's: decree accepts an enabling fact on a store with
      history, and the dispatch that appends it answers `:ok`. An enable states that its
      family's tag rows are complete for every event in the store, those before it too. On a
      store with history nothing has checked those rows: events appended before the
      application tagged a kind lack its tags. A decision then trusts a family whose rows are
      missing, so a condition can miss an earlier event that matches it, and a command its
      rule should refuse is accepted.
    - **A store with history.** `bootstrap/3` never enables here. It checks the requirements
      the application declares and answers `:verified` or
      `{:error, {:tag_index_not_enabled, missing}}`, and every decision whose families are not
      enabled refuses `{:error, :tag_index_not_enabled}` until they are repaired and enabled.
    - **Repair.** A disable for repair, the repair writing the tag rows, then an enable under
      a new index id. That is the supported way to retag. There is no shadow index to build
      beside the live one.
    - **Retirement.** A disable for a family and fingerprint whose key is retired. A key
      restored later has the same material, so the same fingerprint, and meets that disable.
    - **Never deleted.** A hard delete of a stream holding a fact, its own or through a link
      row written before links were removed, answers `{:error, :hard_delete_of_index_fact}`.
      A hard delete takes the stream's events from every stream, so a disable gone that way
      would leave an earlier enable standing. A fact stays for the store's life.
  """

  alias Decree.Store.{Query, ReadResult}
  alias Decree.Store.Streams.StreamInfo

  @fact_tag "tag_index:fact"
  @reserved_kinds ["tag_index", "tag_index_state"]
  @named_families ["command", "command_kind", "correlation"]
  @states %{enabled: "enabled", disabled: "disabled"}
  @fingerprint_message "tag-digest-fingerprint"

  @type family :: String.t()
  @type key :: String.t() | nil
  @type state :: :enabled | :disabled
  @type requirement :: family() | {family(), key()}
  @type states :: %{optional({family(), key()}) => state()}

  @doc """
  The tag every fact carries.
  """
  @spec fact_tag() :: String.t()
  def fact_tag, do: @fact_tag

  @doc """
  The tag kinds no application tag may hold, and no family is.
  """
  @spec reserved_kinds() :: [String.t()]
  def reserved_kinds, do: @reserved_kinds

  @doc """
  The families that are their own kind's, whatever a decision lists: decree's own `command`,
  `command_kind` and `correlation`, then the application's `:tag_index_named_families`, in
  order. Raises an `ArgumentError` when that is not a list of tag kinds that are not reserved.
  """
  @spec named_families() :: [family()]
  def named_families do
    configured = Application.get_env(:decree, :tag_index_named_families, [])

    if not (is_list(configured) and Enum.all?(configured, &family?/1)) do
      raise ArgumentError,
            "config :decree, :tag_index_named_families must be a list of tag kinds that are " <>
              "not reserved, got: #{inspect(configured)}"
    end

    @named_families ++ configured
  end

  @doc """
  A fact's state tag. `key` is nil for a family with no key. Raises an `ArgumentError` for a
  family that is not a tag kind or is reserved, a key that is empty, holds a `:` or is `-`, and
  a state other than `:enabled` and `:disabled`.
  """
  @spec state_tag(family(), key(), state()) :: String.t()
  def state_tag(family, key, state) do
    if not family?(family) do
      raise ArgumentError, "not a tag index family: #{inspect(family)}"
    end

    if not (is_nil(key) or key?(key)) do
      raise ArgumentError, "not a tag index key: #{inspect(key)}"
    end

    if not Map.has_key?(@states, state) do
      raise ArgumentError, "not a tag index state: #{inspect(state)}"
    end

    Enum.join(["tag_index_state", family, key || "-", Map.fetch!(@states, state)], ":")
  end

  @doc """
  The fingerprint of a digest key: 16 lowercase hex characters. Raises an `ArgumentError` for
  anything but a non-empty binary.
  """
  @spec fingerprint(binary()) :: String.t()
  def fingerprint(key_material) when is_binary(key_material) and key_material != "" do
    :hmac
    |> :crypto.mac(:sha256, key_material, @fingerprint_message)
    |> binary_part(0, 8)
    |> Base.encode16(case: :lower)
  end

  def fingerprint(key_material) do
    raise ArgumentError, "a fingerprint needs a non-empty binary, got: #{inspect(key_material)}"
  end

  @doc """
  The fingerprint a digest tag's value names, `{:ok, fingerprint}` for `<fingerprint>:<digest>`,
  or `:error`. The value is what follows the tag's kind and its `:`.
  """
  @spec digest_key(term()) :: {:ok, String.t()} | :error
  def digest_key(value) when is_binary(value) do
    case :binary.split(value, ":") do
      [fingerprint, digest] when digest != "" ->
        if key?(fingerprint), do: {:ok, fingerprint}, else: :error

      _value ->
        :error
    end
  end

  def digest_key(_value), do: :error

  @doc """
  The states the facts leave, each fact given as its tags, in position order. The last state
  per family and key wins, and a family and key with no fact has no entry, which reads as
  disabled. A fact naming one family and key both ways reads as disabled, since a read keeps
  no order within an event. Tags that are not state tags are ignored. A fact with no state
  tag, or one that does not parse, could have been a disable, so the whole answer is
  `:error`. It is the fold a decision runs: see "Index facts" in `Decree.Decision`.
  """
  @spec fold([[String.t()]]) :: {:ok, states()} | :error
  def fold(facts) do
    Enum.reduce_while(facts, {:ok, %{}}, fn tags, {:ok, states} ->
      case fact_states(tags) do
        {:ok, fact} -> {:cont, {:ok, Map.merge(states, fact)}}
        :error -> {:halt, :error}
      end
    end)
  end

  @doc """
  The states every fact on the store leaves, through `fold/1`. `store` is a module that uses
  `Decree.Store`. It reads every fact in one `read_query`, by position and tags, however many
  the store holds. Of `opts` it takes `name`, `conn` and `timeout`, and drops every other
  option. A fact whose state tags the read leaves out, its type declared deletable, folds as
  no tag, as it does in a dispatch, so the answer is
  `{:error, :unattributable_tag_index_fact}`. A read error is answered as is.
  """
  @spec states(module(), keyword()) :: {:ok, states()} | {:error, term()}
  def states(store, opts \\ []) do
    read_opts =
      Keyword.take(opts, [:name, :conn, :timeout]) ++ [tag_items: [0], position_items: [0]]

    with {:ok, %ReadResult{matched_items: matched, tags: tags}} <-
           store.read_query([[tags: [@fact_tag]]], read_opts) do
      facts = matched |> Map.keys() |> Enum.sort() |> Enum.map(&Map.get(tags, &1, []))

      case fold(facts) do
        {:ok, states} -> {:ok, states}
        :error -> {:error, :unattributable_tag_index_fact}
      end
    end
  end

  @doc """
  The requirements whose family and key `states` does not enable, in the order given, each as
  given. A family alone is that family with no key. Raises an `ArgumentError` for a
  requirement outside the grammar: a family that is not a tag kind or is reserved, a key that
  is empty, holds a `:` or is `-`, or anything but a family or a `{family, key}` pair.
  """
  @spec missing(states(), [requirement()]) :: [requirement()]
  def missing(states, requirements) do
    Enum.reject(requirements, &(Map.get(states, requirement!(&1)) == :enabled))
  end

  @doc ~S"""
  The release step: enables the tag index on an empty store, and checks it on a store with
  history. `store` is a module that uses `Decree.Store`, `requirements` the families and keys
  the application declares, and `enable` the application's own enable. Of `opts` it takes
  `name`, `conn` and `timeout`.

  `requirements` is a non-empty list of families and `{family, key}` pairs, with no family and
  key twice, `"entity"` and `{"entity", nil}` being the same. Anything else answers
  `{:error, :invalid_tag_index_requirements}` before any read. Then it reads
  `stream_info(:all)`, and answers a read error as is.

  On an empty store it calls `enable` once with `requirements`. When that answers `:ok` it
  reads `states/2` and answers `:enabled` when `missing/2` finds nothing, and
  `{:error, {:tag_index_not_enabled, missing}}` otherwise. An `{:error, reason}` from
  `enable` is answered as is.

  On a store with history it never calls `enable`. It reads `states/2` and answers
  `:verified` when `missing/2` finds nothing, and `{:error, {:tag_index_not_enabled, missing}}`
  otherwise. A late enable is the repair path in "The lifecycle", the operator's deliberate
  act, never this step's.

  The answer covers the requirements the application declares, not every family its rules
  read: a rule whose items need a family left out of the list still refuses
  `{:error, :tag_index_not_enabled}` at dispatch.

  `enable` must make one atomic append that enables every requirement: one command, whose
  events land in one batch. Decree cannot enforce it. An `enable` that dispatches twice and
  fails between the two leaves the second family disabled, and a rerun, now on a store with
  history, reports it and never enables it.

  No other writer may run between the empty-store check and the enable: an event appended
  between them is not checked. Run the step before any other writer starts.

      enable = fn families ->
        MyApp.App.dispatch(%MyApp.EnableTagIndexes{
          command_id: Decree.UUID.uuid4(),
          ledger_id: Decree.UUID.uuid4(),
          families: families
        })
      end

      case Decree.Store.TagIndex.bootstrap(MyApp.EventStore, ["entity"], enable) do
        :enabled -> :ok
        :verified -> :ok
        {:error, reason} -> raise "the tag index is not ready: #{inspect(reason)}"
      end

  `MyApp.EnableTagIndexes` is the command of the README's minimal application: one fact
  enabling every family it is given.
  """
  @spec bootstrap(
          module(),
          [requirement()],
          ([requirement()] -> :ok | {:error, term()}),
          keyword()
        ) ::
          :enabled
          | :verified
          | {:error, {:tag_index_not_enabled, [requirement()]}}
          | {:error, term()}
  def bootstrap(store, requirements, enable, opts \\ []) when is_function(enable, 1) do
    with :ok <- valid_requirements(requirements),
         {:ok, %StreamInfo{stream_version: version}} <-
           store.stream_info(:all, Keyword.take(opts, [:name, :conn, :timeout])) do
      if version == 0 do
        case enable.(requirements) do
          :ok -> check(store, requirements, opts, :enabled)
          {:error, _reason} = error -> error
        end
      else
        check(store, requirements, opts, :verified)
      end
    end
  end

  @doc false
  # A state tag's family, key and state, or `:error`.
  @spec parse_state(String.t()) :: {:ok, {family(), key(), state()}} | :error
  def parse_state("tag_index_state:" <> rest) do
    with [family, key, state] <- :binary.split(rest, ":", [:global]),
         true <- family?(family),
         {:ok, key} <- parse_key(key),
         {:ok, state} <- parse_state_name(state) do
      {:ok, {family, key, state}}
    else
      _invalid -> :error
    end
  end

  def parse_state(_tag), do: :error

  @doc false
  # The store's guard on one event's tags, which `Decree.Store.EventData.valid_tag?/1` has
  # already let through.
  @spec valid_tags?([String.t()]) :: boolean()
  def valid_tags?(tags) do
    facts = Enum.filter(tags, &(Query.tag_kind(&1) == "tag_index"))
    states = Enum.filter(tags, &(Query.tag_kind(&1) == "tag_index_state"))

    Enum.all?(facts, &(&1 == @fact_tag)) and
      Enum.all?(states, &(parse_state(&1) != :error)) and
      states != [] == (facts != [])
  end

  defp check(store, requirements, opts, answer) do
    with {:ok, states} <- states(store, opts) do
      case missing(states, requirements) do
        [] -> answer
        missing -> {:error, {:tag_index_not_enabled, missing}}
      end
    end
  end

  defp valid_requirements(requirements) when is_list(requirements) and requirements != [] do
    with {:ok, normalized} <- normalize(requirements, []),
         true <- Enum.uniq(normalized) == normalized do
      :ok
    else
      _invalid -> {:error, :invalid_tag_index_requirements}
    end
  end

  defp valid_requirements(_requirements), do: {:error, :invalid_tag_index_requirements}

  defp normalize([], normalized), do: {:ok, normalized}

  defp normalize([requirement | rest], normalized) do
    case requirement(requirement) do
      {:ok, family_key} -> normalize(rest, [family_key | normalized])
      :error -> :error
    end
  end

  defp normalize(_improper, _normalized), do: :error

  defp requirement!(requirement) do
    case requirement(requirement) do
      {:ok, family_key} -> family_key
      :error -> raise ArgumentError, "not a tag index requirement: #{inspect(requirement)}"
    end
  end

  defp requirement({family, key}) do
    if family?(family) and (is_nil(key) or key?(key)), do: {:ok, {family, key}}, else: :error
  end

  defp requirement(family), do: requirement({family, nil})

  # One fact's states. A fact naming one family and key both ways reads as disabled, since the
  # read keeps no order within an event.
  defp fact_states(tags) do
    parsed = for tag <- tags, Query.tag_kind(tag) == "tag_index_state", do: parse_state(tag)

    if parsed == [] or :error in parsed do
      :error
    else
      {:ok,
       Enum.reduce(parsed, %{}, fn {:ok, {family, key, state}}, states ->
         Map.update(states, {family, key}, state, &lower(&1, state))
       end)}
    end
  end

  defp lower(:enabled, :enabled), do: :enabled
  defp lower(_state, _other), do: :disabled

  defp family?(family) when is_binary(family) do
    family != "" and String.valid?(family) and not String.contains?(family, [":", <<0>>]) and
      family not in @reserved_kinds
  end

  defp family?(_family), do: false

  defp key?(key) when is_binary(key) do
    key not in ["", "-"] and String.valid?(key) and not String.contains?(key, [":", <<0>>])
  end

  defp key?(_key), do: false

  defp parse_key("-"), do: {:ok, nil}
  defp parse_key(key), do: if(key?(key), do: {:ok, key}, else: :error)

  defp parse_state_name("enabled"), do: {:ok, :enabled}
  defp parse_state_name("disabled"), do: {:ok, :disabled}
  defp parse_state_name(_state), do: :error
end
