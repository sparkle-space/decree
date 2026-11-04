defmodule Decree.TagIndexFixtures.TagIndexEnabled do
  @moduledoc false
  @derive Jason.Encoder
  defstruct [:index_id, :family, :key, :reason, :command_id]

  defimpl Decree.Event.Tags do
    alias Decree.Store.TagIndex

    def tags(%{family: family, key: key}, _context) do
      {:ok, [TagIndex.fact_tag(), TagIndex.state_tag(family, key, :enabled)]}
    end
  end
end

defmodule Decree.TagIndexFixtures.TagIndexDisabled do
  @moduledoc false
  @derive Jason.Encoder
  defstruct [:index_id, :family, :key, :reason, :command_id]

  defimpl Decree.Event.Tags do
    alias Decree.Store.TagIndex

    def tags(%{family: family, key: key}, _context) do
      {:ok, [TagIndex.fact_tag(), TagIndex.state_tag(family, key, :disabled)]}
    end
  end
end

defmodule Decree.TagIndexFixtures.EnableTagIndex do
  @moduledoc false

  # `indexes` are `%{index_id: id, family: family, key: key}`, each id the requester's.
  defstruct [:command_id, :ledger_id, indexes: []]
end

defmodule Decree.TagIndexFixtures.DisableTagIndex do
  @moduledoc false

  # `reason` is "repair" or "key_retired".
  defstruct [:command_id, :ledger_id, :index_id, :reason]
end

defmodule Decree.TagIndexFixtures.Ledger do
  @moduledoc false

  # One stream of facts. An index id names one enable: the same id again is a repeat and
  # appends nothing. A family and key holds one enabled index at a time, and a key once
  # retired is never enabled again.

  alias Decree.TagIndexFixtures.{DisableTagIndex, EnableTagIndex}
  alias Decree.TagIndexFixtures.{TagIndexDisabled, TagIndexEnabled}

  # `indexes` maps an index id to its family, key and state: `:enabled`, `"repair"` or
  # `"key_retired"`. `retired` holds the retired `{family, key}`s.
  defstruct indexes: %{}, retired: MapSet.new()

  def execute(%__MODULE__{} = ledger, %EnableTagIndex{indexes: indexes} = command) do
    new = Enum.reject(indexes, &Map.has_key?(ledger.indexes, &1.index_id))

    cond do
      Enum.any?(new, &MapSet.member?(ledger.retired, {&1.family, &1.key})) ->
        {:error, :tag_index_key_retired}

      Enum.any?(new, &enabled?(ledger, &1.family, &1.key)) ->
        {:error, :tag_index_already_enabled}

      true ->
        for %{index_id: index_id, family: family, key: key} <- new do
          %TagIndexEnabled{
            index_id: index_id,
            family: family,
            key: key,
            command_id: command.command_id
          }
        end
    end
  end

  def execute(%__MODULE__{} = ledger, %DisableTagIndex{reason: reason} = command)
      when reason in ["repair", "key_retired"] do
    case Map.fetch(ledger.indexes, command.index_id) do
      :error ->
        {:error, :tag_index_not_found}

      {:ok, %{state: state} = index} when state == :enabled or state == "repair" ->
        if state == reason do
          []
        else
          disabled(index, command)
        end

      {:ok, %{state: "key_retired"}} ->
        []
    end
  end

  def apply(%__MODULE__{} = ledger, %TagIndexEnabled{index_id: index_id} = event) do
    index = %{family: event.family, key: event.key, state: :enabled}

    %__MODULE__{ledger | indexes: Map.put(ledger.indexes, index_id, index)}
  end

  def apply(%__MODULE__{} = ledger, %TagIndexDisabled{index_id: index_id, reason: reason} = event) do
    indexes = Map.update!(ledger.indexes, index_id, &%{&1 | state: reason})

    retired =
      if reason == "key_retired" do
        MapSet.put(ledger.retired, {event.family, event.key})
      else
        ledger.retired
      end

    %__MODULE__{ledger | indexes: indexes, retired: retired}
  end

  defp enabled?(%__MODULE__{indexes: indexes}, family, key) do
    Enum.any?(indexes, fn {_index_id, index} ->
      index == %{family: family, key: key, state: :enabled}
    end)
  end

  defp disabled(%{family: family, key: key}, %DisableTagIndex{} = command) do
    %TagIndexDisabled{
      index_id: command.index_id,
      family: family,
      key: key,
      reason: command.reason,
      command_id: command.command_id
    }
  end
end

defmodule Decree.TagIndexFixtures.Router do
  @moduledoc false
  use Decree.Commands.Router

  alias Decree.TagIndexFixtures.{DisableTagIndex, EnableTagIndex, Ledger}

  identify Ledger, by: :ledger_id, prefix: "tag-index-"

  dispatch [EnableTagIndex, DisableTagIndex], to: Ledger
end

defmodule Decree.TagIndexFixtures do
  @moduledoc false

  # The application's half of the tag index, as decree's suite plays it: two fact events, a
  # ledger aggregate that appends them, and the helpers the store-backed cases use. Decree ships
  # none of this: the events and the commands are an application's.

  alias Decree.EventStore.RecordedEvent
  alias Decree.Store.{ReadResult, TagIndex}
  alias Decree.TagIndexFixtures.{DisableTagIndex, EnableTagIndex, Ledger, TagIndexEnabled}
  alias Decree.UUID

  @ledger_id "5d6c2a3e-6b1f-4c39-9a0e-7f1f0f6f0a01"
  @key "decree-fixture-key"

  def ledger_id, do: @ledger_id
  def ledger_stream, do: "tag-index-" <> @ledger_id

  @doc """
  The fixture's digest key material, and its fingerprint.
  """
  def key, do: @key
  def fingerprint, do: TagIndex.fingerprint(@key)

  @doc """
  A digest tag under the fixture key.
  """
  def digest_tag(kind, digest), do: kind <> ":" <> fingerprint() <> ":" <> digest

  @doc """
  Every family and key a store-backed case enables. `email` is enabled with no key as well,
  for the fixtures that tag an address without listing `email` as a digest.
  """
  def families do
    fingerprint = fingerprint()

    [
      {"command", nil},
      {"command_kind", nil},
      {"correlation", nil},
      {"tenant_slug", nil},
      {"plan_slug", nil},
      {"email", nil},
      {"email", fingerprint},
      {"oauth_uid", fingerprint},
      {"entity", nil}
    ]
  end

  @doc """
  One enabling batch for `families`, each with an index id of its own.
  """
  def enable_command(families \\ families()) do
    %EnableTagIndex{
      command_id: UUID.uuid4(),
      ledger_id: @ledger_id,
      indexes:
        for {family, key} <- families do
          %{index_id: UUID.uuid4(), family: family, key: key}
        end
    }
  end

  def disable_command(index_id, reason) do
    %DisableTagIndex{
      command_id: UUID.uuid4(),
      ledger_id: @ledger_id,
      index_id: index_id,
      reason: reason
    }
  end

  def enable!(application, families \\ families()) do
    :ok = application.dispatch(enable_command(families))
  end

  @doc """
  Disables the index enabled for a family and key, and answers its id.
  """
  def disable!(application, family, key \\ nil, reason \\ "repair") do
    index_id = enabled_index!(application, family, key)

    :ok = application.dispatch(disable_command(index_id, reason))

    index_id
  end

  @doc """
  A new enable for a family and key, under a new index id.
  """
  def reenable!(application, family, key \\ nil), do: enable!(application, [{family, key}])

  @doc """
  The id of the index enabled for a family and key.
  """
  def enabled_index!(application, family, key \\ nil) do
    %Ledger{indexes: indexes} = application.aggregate_state(Ledger, ledger_stream())

    [index_id] =
      for {index_id, %{family: ^family, key: ^key, state: :enabled}} <- indexes, do: index_id

    index_id
  end

  @doc """
  The enabling step a release runs: the batch, only when `$all` is empty. On a store with
  history it does nothing.
  """
  def enable_fresh!(application) do
    store = Module.concat([application, "FrameworkEventStore"])

    case Decree.FrameworkEventStore.read_all_streams_forward(0, 1, name: store) do
      {:ok, []} -> enable!(application)
      {:ok, [_event]} -> :ok
    end
  end

  @doc """
  What a mocked store's `read_query/3` answers: every family enabled, each fact matching the
  index item, wherever `items` holds it, and the facts' tags when `opts`' `tag_items` names
  that item, as a store answers them.
  """
  def mocked_read(items, opts) do
    index = Enum.find_index(items, &(&1 == [tags: [TagIndex.fact_tag()]]))
    facts = Enum.with_index(families(), 1)

    %ReadResult{
      events:
        for {{family, key}, position} <- facts do
          %RecordedEvent{
            event_number: position,
            data: %TagIndexEnabled{family: family, key: key},
            metadata: %{}
          }
        end,
      after: length(facts),
      read_at: DateTime.utc_now(),
      command_stamps: %{},
      matched_items: Map.new(facts, fn {_family_key, position} -> {position, [index]} end),
      tags:
        if index in Keyword.get(opts, :tag_items, []) do
          Map.new(facts, fn {{family, key}, position} ->
            {position, [TagIndex.fact_tag(), TagIndex.state_tag(family, key, :enabled)]}
          end)
        else
          %{}
        end
    }
  end
end
