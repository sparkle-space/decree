defmodule Decree.Store.TagIndex do
  @moduledoc """
  The grammar of the facts that say whether a tag index can be trusted.

  A condition finds an event by its tags, so a decision rests on the tag rows being complete.
  Whether they are is a fact on the ledger, in events the application appends. Decree ships no
  such event and no command: it ships the grammar, the store's guard on it, and the check a
  decision runs over it.

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

  A tag of a kind in `named_families/0`, `command`, `command_kind`, `correlation`,
  `tenant_slug`, `plan_slug`, `email` and `oauth_uid`, is in the family of that kind, and so
  is a tag of a kind a decision lists in its `index_families/0`. Any other kind is in the
  `entity` family. A kind listed `:digest` holds `<fingerprint>:<digest>`, and its family's
  key is that fingerprint: `digest_key/1` reads it. Every other family has no key.

  ## The fold

  A decision reads the facts in its own read and folds them in position order: the last
  state per family and key wins, and a family and key with no fact is disabled. A fact the
  fold cannot attribute, one whose state tags are gone, refuses as a disable would. See
  "Index facts" in `Decree.Decision`.

  ## The fingerprint

  `fingerprint/1` is DOC-1's form: the first 8 bytes of HMAC-SHA-256 keyed by the key material
  over `"tag-digest-fingerprint"`, in lowercase hex, 16 characters. It is derived from the key
  material and shows nothing about the key, and it holds no `:`, so it reads back from a
  `<fingerprint>:<digest>` value. It names a digest key without holding it, so one key's tags
  and its retirement meet under one name. The key and the digests stay the application's.

  ## The lifecycle

    - **A fresh store.** The application's release step appends the enabling batch only when
      `$all` is empty, so the facts sit from position 1 and every later event is covered by
      them.
    - **A store with history.** The same step does nothing, and every decision refuses
      `{:error, :tag_index_not_enabled}` until a family is repaired and enabled.
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

  alias Decree.Store.Query

  @fact_tag "tag_index:fact"
  @reserved_kinds ["tag_index", "tag_index_state"]
  @named_families [
    "command",
    "command_kind",
    "correlation",
    "tenant_slug",
    "plan_slug",
    "email",
    "oauth_uid"
  ]
  @states %{enabled: "enabled", disabled: "disabled"}
  @fingerprint_message "tag-digest-fingerprint"

  @type family :: String.t()
  @type key :: String.t() | nil
  @type state :: :enabled | :disabled

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
  The families that are their own kind's, whatever a decision lists.
  """
  @spec named_families() :: [family()]
  def named_families, do: @named_families

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
