defmodule Decree.PersonalFields.KeyStream do
  @moduledoc """
  A subject's wrapped data key, on a stream of its own that can be hard deleted.

  The stream is `prefix <> subject_id`. Its head holds the subject's current wrap, and the
  ledger is the only place it is read from: nothing here is a process, and nothing is held
  between calls.

  A host implements `Decree.PersonalFields.KeySource` with `head/3`. It also generates the 32
  bytes and wraps them, and unwraps what `head/3` answers: decree takes and answers wrapped
  bytes, and never sees the raw key.

  ## The store declaration

  A key stream's prefix must be declared deletable, along with `event_types/0`:

      defmodule MyApp.EventStore do
        use Decree.Store,
          otp_app: :my_app,
          enable_hard_deletes: true,
          deletable: [
            stream_prefixes: ["SubjectKey-"],
            event_types: Decree.PersonalFields.KeyStream.event_types()
          ]
      end

  `mint/5` and `rewrap/4` refuse a prefix the store does not declare, with
  `{:error, :undeclared_key_stream}`, because a stream outside the declaration gets none of
  its refusals: a query, a link or a stray event type would all be let in. A store without
  `enable_hard_deletes` answers `:undeclared_key_stream` for every prefix, whatever it declares,
  because it could never erase the key.

  A `use` line resolves `event_types/0` when the store compiles, so `:type_provider` belongs in
  compile-time config: a provider set only at runtime makes every key write answer
  `:undeclared_deletable_event_type`, with nothing written.

  ## What this layer can't tell

    - `head/3` answers `:stream_not_found` for a stream that never existed and for one hard
      deleted. Deletion leaves no tombstone. Only the host's own record of what it erased can
      tell the two apart, so a host's `lookup/1` reads that first, and maps this answer to
      `:unknown`.

    - Nothing here answers erased. A store failure is an `{:error, _}` that is not
      `:stream_not_found`, so it can never read as a shred.

    - A mint succeeds on a hard-deleted stream, with a fresh key.

    - An exact version can't tell a re-created stream. After a delete and a mint, a `%Head{}`
      at version 1 from before the delete passes `rewrap/4`, and old key material is back.

    - **So every write needs the host's admission re-check after it.** After a `mint/5` or a
      `rewrap/4` that answers success, the host reads its record again, and if the subject was
      erased it hard deletes the stream. No expected version here does that for it.

  ## Restores

  A restored stream is just a stream. After a whole-database restore `head/3` answers it, and
  nothing marks that it predates an erasure. That is the host's to re-apply.

  ## What a `$all` subscriber sees

  Key events reach every subscriber of `$all`: the store delivers them like any other, and a
  handler's catch-all clause absorbs them. Each holds the wrapped bytes as base64, the ids, the
  master tag and a timestamp, and no declared field. They carry no tags, so no query can reach
  them, and they skip `Decree.Event.Append.prepare/4`, which would add some.

  A delivered event is in the metadata of `[:decree, :event, :handle]` and of
  `Decree.EventStore.ack_event/3`'s telemetry, as every event is. `Inspect` leaves the wrap out
  there, so a telemetry handler may inspect that metadata, and must not serialize it.

  ## Key material

  Wrapped bytes never leave here but in a `%Head{}`, whose `Inspect` leaves them out, as the
  events' does. A failure answers an atom, `{:append_failed, class}` or
  `Decree.PersonalFields.RedactedError.reduce/1`'s reduction, and a raise, an exit or a throw
  is caught and reduced the same way. Every argument is checked inside the function, so a bad
  one answers a refusal, and no failed clause puts it in a stacktrace.

  A key append goes through `Decree.EventStore.append_without_span/4`, and a read through
  `Decree.EventStore.read_stream_head/2`, because a span's exception event would publish a
  raised term and its stacktrace before the catch here reduces anything.
  """

  alias Decree.Event.Mapper
  alias Decree.EventStore
  alias Decree.EventStore.RecordedEvent
  alias Decree.EventStore.TypeProvider
  alias Decree.PersonalFields.RedactedError
  alias Decree.PersonalFields.SubjectDataKeyMinted
  alias Decree.PersonalFields.SubjectDataKeyRewrapped
  alias Decree.Store.AppendError
  alias Decree.UUID

  defmodule Head do
    @moduledoc """
    A key stream's last event, read: its wrap, and the version a rewrap appends at.

    `Inspect` leaves out `wrapped_key`, which here is the bytes.
    """

    @derive {Inspect, except: [:wrapped_key]}
    defstruct [:stream_uuid, :version, :subject_id, :key_id, :wrapped_key, :master_tag]

    @type t :: %__MODULE__{
            stream_uuid: String.t(),
            version: pos_integer,
            subject_id: String.t(),
            key_id: String.t(),
            wrapped_key: binary,
            master_tag: String.t()
          }
  end

  @typedoc "A refusal or a failure, with nothing in it that came from a key."
  @type failure :: atom | {atom, atom}

  @doc """
  Appends a subject's first wrap at `:no_stream`, and mints its `key_id`.

  Every way of losing the stream to another writer answers `{:error, :stream_exists}`, and the
  loser then reads the winner's key with `head/3`.

  Refuses, with nothing written, a prefix the store does not declare
  (`:undeclared_key_stream`), a subject that is not a canonical UUID (`:invalid_subject`), an
  empty or non-binary wrap (`:invalid_wrapped_key`) and a master tag that is not non-empty
  text (`:invalid_master_tag`). A store without `enable_hard_deletes` answers
  `:undeclared_key_stream` for every prefix, because it could never erase the key.

  A failed append answers `{:error, {:append_failed, class}}`, with the class of
  `Decree.Store.AppendError`. An `:unavailable` one may have landed.
  """
  @spec mint(Decree.Application.t(), String.t(), String.t(), binary, String.t()) ::
          {:ok, key_id :: String.t()}
          | {:error, :stream_exists}
          | {:error, failure}
  def mint(application, prefix, subject_id, wrapped_key, master_tag) do
    with :ok <- check_subject(subject_id),
         :ok <- check_wrap(wrapped_key, master_tag),
         :ok <- check_declared(application, prefix) do
      key_id = UUID.uuid4()

      event = %SubjectDataKeyMinted{
        subject_id: subject_id,
        key_id: key_id,
        wrapped_key: Base.encode64(wrapped_key),
        master_tag: master_tag,
        minted_at: now()
      }

      case append(application, prefix <> subject_id, :no_stream, event) do
        :ok -> {:ok, key_id}
        {:error, reason} -> {:error, lost_no_stream(reason)}
      end
    end
  catch
    _kind, value -> {:error, RedactedError.reduce(value)}
  end

  @doc """
  Reads a subject's key stream, with one read of its head. Every call reads the store.

  Answers `{:ok, %Head{}}`, `{:error, :stream_not_found}` for a stream with no events, whether
  it never existed or was hard deleted, `{:error, :stream_deleted}` for a soft-deleted one,
  `{:error, :malformed_key_event}` for a head that is not a key event of this subject, or
  `{:error, :invalid_subject}` for a subject that is not a canonical UUID. Anything else is a
  failed read: an `{:error, _}` that says nothing about the subject.
  """
  @spec head(Decree.Application.t(), String.t(), String.t()) ::
          {:ok, Head.t()}
          | {:error, :stream_not_found}
          | {:error, :stream_deleted}
          | {:error, :malformed_key_event}
          | {:error, failure}
  def head(application, prefix, subject_id) do
    with :ok <- check_subject(subject_id),
         {:ok, %RecordedEvent{} = event} <-
           EventStore.read_stream_head(application, prefix <> subject_id) do
      to_head(event, subject_id)
    else
      {:error, reason} -> {:error, RedactedError.reduce(reason)}
    end
  catch
    _kind, value -> {:error, RedactedError.reduce(value)}
  end

  @doc """
  Appends a new wrap at the exact version of `head`, keeping its `key_id`.

  A `head` that is stale, or of a stream that has gone, answers
  `{:error, :wrong_expected_version}`. `{:error, :not_found}` is a stream hard deleted after
  this append had read it.

  It trusts `head`'s `key_id` and version, so pass a `%Head{}` that `head/3` answered on the
  same application.

  Refuses, with nothing written, what `mint/5` refuses, and `:invalid_head` for anything that
  is not a `%Head{}` as `head/3` answers one. See "What this layer can't tell" for what an
  exact version does not catch.
  """
  @spec rewrap(Decree.Application.t(), Head.t(), binary, String.t()) ::
          :ok
          | {:error, :wrong_expected_version}
          | {:error, :not_found}
          | {:error, failure}
  def rewrap(application, %Head{} = head, wrapped_key, master_tag) do
    %Head{stream_uuid: stream_uuid, version: version, subject_id: subject_id, key_id: key_id} =
      head

    with :ok <- check_subject(subject_id),
         {:ok, prefix} <- stream_prefix(stream_uuid, subject_id),
         :ok <- check_head(version, key_id),
         :ok <- check_wrap(wrapped_key, master_tag),
         :ok <- check_declared(application, prefix) do
      event = %SubjectDataKeyRewrapped{
        subject_id: subject_id,
        key_id: key_id,
        wrapped_key: Base.encode64(wrapped_key),
        master_tag: master_tag,
        rewrapped_at: now()
      }

      case append(application, stream_uuid, version, event) do
        :ok -> :ok
        {:error, reason} -> {:error, failed(reason)}
      end
    end
  catch
    _kind, value -> {:error, RedactedError.reduce(value)}
  end

  def rewrap(_application, _head, _wrapped_key, _master_tag), do: {:error, :invalid_head}

  @doc """
  The two stored type strings, for a store's `deletable: [event_types: ...]`.
  """
  @spec event_types() :: [String.t()]
  def event_types do
    [
      TypeProvider.to_string(%SubjectDataKeyMinted{}),
      TypeProvider.to_string(%SubjectDataKeyRewrapped{})
    ]
  end

  # Through the framework's own mapping, so a stored type is what `event_types/0` says, and
  # straight to the store: `Decree.Event.Append.prepare/4` would add tags a query could reach.
  # With no span, whose exception event would hold what `mint/5`'s catch has not yet reduced: a
  # failed clause under the adapter lists the `EventData`, and the wrap's base64 with it.
  defp append(application, stream_uuid, expected_version, event) do
    EventStore.append_without_span(
      application,
      stream_uuid,
      expected_version,
      [Mapper.map_to_event_data(event)]
    )
  end

  # `:wrong_expected_version` is a row at version 0 that another writer appended to first.
  defp lost_no_stream(:wrong_expected_version), do: :stream_exists
  defp lost_no_stream({:already_retried_once, :duplicate_stream_uuid}), do: :stream_exists
  defp lost_no_stream(reason), do: failed(reason)

  defp failed(%AppendError{class: class}), do: {:append_failed, class}
  defp failed(reason), do: RedactedError.reduce(reason)

  defp check_subject(subject_id) do
    if UUID.canonical?(subject_id), do: :ok, else: {:error, :invalid_subject}
  end

  defp check_wrap(wrapped_key, _master_tag) when not is_binary(wrapped_key) or wrapped_key == "",
    do: {:error, :invalid_wrapped_key}

  # A tag JSON can't hold would raise in the store's serializer, and answer no refusal.
  defp check_wrap(_wrapped_key, master_tag) do
    if text?(master_tag), do: :ok, else: {:error, :invalid_master_tag}
  end

  defp check_declared(application, prefix) do
    if prefix in EventStore.deletable_stream_prefixes(application) do
      :ok
    else
      {:error, :undeclared_key_stream}
    end
  end

  # What is left of the stream's name when the subject comes off its end.
  defp stream_prefix(stream_uuid, subject_id) when is_binary(stream_uuid) do
    if String.ends_with?(stream_uuid, subject_id) do
      {:ok, binary_part(stream_uuid, 0, byte_size(stream_uuid) - byte_size(subject_id))}
    else
      {:error, :invalid_subject}
    end
  end

  defp stream_prefix(_stream_uuid, _subject_id), do: {:error, :invalid_subject}

  # A version of 0 would pass on a stream that has gone, and create it with this wrap.
  defp check_head(version, key_id) when is_integer(version) and version > 0 do
    if text?(key_id), do: :ok, else: {:error, :invalid_head}
  end

  defp check_head(_version, _key_id), do: {:error, :invalid_head}

  defp to_head(%RecordedEvent{data: %SubjectDataKeyMinted{} = event} = recorded, subject_id) do
    %SubjectDataKeyMinted{key_id: key_id, wrapped_key: wrapped, master_tag: tag} = event

    to_head(recorded, event.subject_id, subject_id, {key_id, wrapped, tag})
  end

  defp to_head(%RecordedEvent{data: %SubjectDataKeyRewrapped{} = event} = recorded, subject_id) do
    %SubjectDataKeyRewrapped{key_id: key_id, wrapped_key: wrapped, master_tag: tag} = event

    to_head(recorded, event.subject_id, subject_id, {key_id, wrapped, tag})
  end

  defp to_head(%RecordedEvent{}, _subject_id), do: {:error, :malformed_key_event}

  defp to_head(%RecordedEvent{} = recorded, subject_id, subject_id, {key_id, encoded, tag}) do
    %RecordedEvent{stream_id: stream_uuid, stream_version: version} = recorded

    with true <- text?(key_id) and text?(tag) and is_binary(encoded),
         {:ok, wrapped_key} when wrapped_key != "" <- Base.decode64(encoded) do
      {:ok,
       %Head{
         stream_uuid: stream_uuid,
         version: version,
         subject_id: subject_id,
         key_id: key_id,
         wrapped_key: wrapped_key,
         master_tag: tag
       }}
    else
      _malformed -> {:error, :malformed_key_event}
    end
  end

  defp to_head(%RecordedEvent{}, _other, _subject_id, _parts) do
    {:error, :malformed_key_event}
  end

  defp text?(value), do: is_binary(value) and value != "" and String.valid?(value)

  defp now, do: DateTime.utc_now() |> DateTime.to_iso8601()
end
