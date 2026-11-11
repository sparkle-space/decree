defmodule Decree.PersonalFields.Fixtures.KeyStreams do
  @moduledoc false

  # The names and the wrap the key stream tests share. A wrap is AES-256-GCM under a test master
  # key, so it is bytes that are not text, as a host's are.

  @cipher :aes_256_gcm
  @aad "wrap"
  @master_key :crypto.hash(:sha256, "the test master key")

  def prefix, do: "SubjectKey-"
  def admission_prefix, do: "KeyAdmission-"
  def master_tag, do: "master-v1"

  def wrap(key) do
    nonce = :crypto.strong_rand_bytes(12)

    {ciphertext, tag} =
      :crypto.crypto_one_time_aead(@cipher, @master_key, nonce, key, @aad, 16, true)

    nonce <> ciphertext <> tag
  end

  def unwrap(<<nonce::binary-size(12), sealed::binary>>) when byte_size(sealed) > 16 do
    {ciphertext, tag} = :erlang.split_binary(sealed, byte_size(sealed) - 16)

    case :crypto.crypto_one_time_aead(@cipher, @master_key, nonce, ciphertext, @aad, tag, false) do
      key when is_binary(key) -> {:ok, key}
      :error -> :error
    end
  end

  def unwrap(_wrapped), do: :error

  # For a test that fails a clause on purpose.
  def needs_a_map(%{no_such_key: value}), do: value
end

defmodule Decree.PersonalFields.Fixtures.ShortTypes do
  @moduledoc false

  # A type provider that is not the module name.

  @behaviour Decree.EventStore.TypeProvider

  def to_string(%module{}), do: module |> Module.split() |> List.last()
  def to_struct(_type), do: raise("this provider only names events")
end

defmodule Decree.PersonalFields.Fixtures.DeclaredStore do
  @moduledoc false

  # The `use` line `Decree.PersonalFields.KeyStream`'s docs show. It never starts.

  use Decree.Store,
    otp_app: :decree,
    enable_hard_deletes: true,
    deletable: [
      stream_prefixes: ["SubjectKey-"],
      event_types: Decree.PersonalFields.KeyStream.event_types()
    ]
end

defmodule Decree.PersonalFields.Fixtures.AdmissionErased do
  @moduledoc false

  # What a host's admission stream holds once a subject is erased. It is never deleted.

  @derive Jason.Encoder
  defstruct [:subject_id]
end

defmodule Decree.PersonalFields.Fixtures.AdmissionGranted do
  @moduledoc false

  @derive Jason.Encoder
  defstruct [:subject_id]
end

defmodule Decree.PersonalFields.Fixtures.LedgerKeys do
  @moduledoc false

  # DOC-2 §5.4 as two store reads: admission first, then the key stream, then the unwrap. It
  # holds nothing, and reads nothing but the store, so it works with no handler started.

  @behaviour Decree.PersonalFields.KeySource

  alias Decree.EventStore
  alias Decree.EventStore.RecordedEvent
  alias Decree.PersonalFields.Fixtures.{AdmissionErased, KeyStreams, LedgerApp}
  alias Decree.PersonalFields.KeyStream
  alias Decree.PersonalFields.KeyStream.Head

  @impl Decree.PersonalFields.KeySource
  def lookup(subject_id) do
    stream = KeyStreams.admission_prefix() <> subject_id

    case EventStore.read_stream_head(LedgerApp, stream) do
      {:ok, %RecordedEvent{data: %AdmissionErased{}}} -> :erased
      {:ok, %RecordedEvent{}} -> read_key(subject_id)
      {:error, :stream_not_found} -> read_key(subject_id)
      {:error, _reason} -> :unknown
    end
  catch
    _kind, _value -> :unknown
  end

  defp read_key(subject_id) do
    case KeyStream.head(LedgerApp, KeyStreams.prefix(), subject_id) do
      {:ok, %Head{key_id: key_id, wrapped_key: wrapped_key}} ->
        case KeyStreams.unwrap(wrapped_key) do
          {:ok, key} -> {:ok, key_id, key}
          :error -> :unknown
        end

      {:error, _reason} ->
        :unknown
    end
  end
end

defmodule Decree.PersonalFields.Fixtures.FailingStore do
  @moduledoc false

  # The Postgres event store, with what a test flags per stream run in place of a head read
  # (`read_stream_backward/4` for the last event, which `Decree.EventStore.read_stream_head/2`
  # asks) or an append, so a test can have the store answer, raise, exit or throw whatever it
  # needs.

  use Decree.DelegatingStore

  alias Decree.FrameworkEventStore, as: Store

  def flag(stream_uuid, fun) when is_function(fun, 0) do
    :persistent_term.put(key(stream_uuid), fun)
  end

  def unflag(stream_uuid), do: :persistent_term.erase(key(stream_uuid))

  @impl Decree.Store
  def read_stream_backward(stream_uuid, -1 = start_version, 1 = count, opts) do
    case :persistent_term.get(key(stream_uuid), nil) do
      nil -> Store.read_stream_backward(stream_uuid, start_version, count, opts)
      fun -> fun.()
    end
  end

  def read_stream_backward(stream_uuid, start_version, count, opts),
    do: Store.read_stream_backward(stream_uuid, start_version, count, opts)

  @impl Decree.Store
  def append_to_stream(stream_uuid, expected_version, events, opts) do
    case :persistent_term.get(key({:append, stream_uuid}), nil) do
      nil -> Store.append_to_stream(stream_uuid, expected_version, events, opts)
      fun -> fun.()
    end
  end

  def flag_append(stream_uuid, fun) when is_function(fun, 0),
    do: flag({:append, stream_uuid}, fun)

  def unflag_append(stream_uuid), do: unflag({:append, stream_uuid})

  defp key(stream_uuid), do: {__MODULE__, stream_uuid}
end

defmodule Decree.PersonalFields.Fixtures.LedgerApp do
  @moduledoc false

  # Its store declares the key prefix and the two key event types, with hard deletes on, and a
  # pool that a test's blocked mints can all hold a connection of.

  alias Decree.FrameworkStorage
  alias Decree.PersonalFields.Fixtures.{FailingStore, KeyStreams, LedgerKeys, Router}
  alias Decree.PersonalFields.KeyStream

  use Decree.Application,
    otp_app: :decree,
    key_source: LedgerKeys,
    event_store:
      FrameworkStorage.event_store_config(
        event_store: FailingStore,
        enable_hard_deletes: true,
        pool_size: 12,
        deletable: [stream_prefixes: [KeyStreams.prefix()], event_types: KeyStream.event_types()]
      ),
    pubsub: :local,
    registry: :local

  router(Router)

  def store_name, do: __MODULE__.FrameworkEventStore

  def init(config) do
    instance = Keyword.get(config, :name) || Keyword.fetch!(config, :application)
    name = Module.concat([instance, "FrameworkEventStore"])

    {:ok, Keyword.update!(config, :event_store, &Keyword.put_new(&1, :name, name))}
  end
end

defmodule Decree.PersonalFields.Fixtures.RawSerializer do
  @moduledoc false

  # Writes the bytes of a `Raw`, so a test can store a row no reader can decode, on a store
  # that declares nothing.

  @behaviour Decree.Store.Serializer

  defmodule Raw do
    @moduledoc false
    defstruct [:bytes]
  end

  def serialize(%Raw{bytes: bytes}), do: bytes
  def serialize(term), do: Jason.encode!(term)

  def deserialize(binary, _config), do: Jason.decode!(binary)
end

defmodule Decree.PersonalFields.Fixtures.CatchAllHandler do
  @moduledoc false

  # Names one event and absorbs the rest, as every `$all` consumer does.

  use Decree.Event.Handler,
    application: Decree.PersonalFields.Fixtures.LedgerApp,
    name: __MODULE__,
    start_from: :origin

  alias Decree.PersonalFields.Fixtures.Events.Plain
  alias Decree.PersonalFields.Fixtures.Keys

  def handle(%Plain{}, _metadata), do: :ok

  def handle(_event, _metadata) do
    Keys.notify(:absorbed)

    :ok
  end
end
