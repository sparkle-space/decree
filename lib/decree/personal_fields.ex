defmodule Decree.PersonalFields do
  @moduledoc """
  Personal fields: values encrypted under their subject's key before they reach the store.

  An event declares its personal fields and metadata entries, and whose they are, through
  `Decree.Event.PersonalFields`. An application names where keys come from with `key_source:`
  on `use Decree.Application` (see `Decree.PersonalFields.KeySource`). Nothing else is needed,
  and an application or an event that declares nothing is unchanged.

  The store only ever sees JSON: everything here runs in the framework.

  ## What each path does

    - **Append.** `Decree.Event.Append.prepare/4` looks up each subject of each event, drops
      erased values, takes the tags from the plaintext that will be stored, then encrypts. A
      retired slot takes no lookup and is stored as given, and an envelope in one fails the
      append with `{:error, :personal_field_is_envelope}`. A metadata key the application
      lists in `personal_metadata_keys:` passes only when the event declares it under
      `personal_metadata:`; retiring it is not enough. An aggregate's append runs `prepare/4`,
      and a decision's, once per member with that member's stream.
      `Decree.EventStore.append_to_stream/5` and `Decree.Store` do not: an application's own
      direct append of a declared event stores what it is given, so a build check of its own
      must fence that.

    - **Delivery.** `Decree.Event.Handler` decrypts an event for a handler whose `handle/2`
      names it in a clause head, and for no other, retired slots included. The
      `RecordedEvent` is never rewritten.

    - **Rehydration.** An aggregate lists the events its state reads with
      `Decree.PersonalFields.Aggregate`, and those are decrypted in its fold.

    - **Decisions.** A `Decree.Decision` lists the events it reads the same way, and those
      reach `evolve/2` and `followup_query/3` decrypted, body only. Every other event reaches
      them as stored. There is no snapshot.

    - **Rules.** A `Decree.Decision.Rule` lists the events it reads the same way, and those
      reach `followup_query/3` and `execute/2` decrypted, body only. See "Personal fields" there.

    - **Snapshots.** The state an aggregate lists is encrypted on the way to the store and
      decrypted on the way back.

    - **Stream reads.** A bare read answers ciphertext. A reader that needs plaintext calls
      `decrypt_recorded_event/2`.

    - **Upcasters** run first, and see ciphertext. One may pass an envelope on, or move it to
      another struct. It can't read it, or move it to another field. The declaration read is
      the upcast struct's: a slot it neither declares nor retires reaches a reader as stored.

  Every read path walks an event's retired slots after its declared ones, the same way.

  ## Three answers

  A lookup answers a key, `:erased` or `:unknown`. Erased means absent: a field is nil and a
  metadata key is gone. Unknown is everything else, and it is never written or read as absent:
  an append fails with `{:error, :key_unknown}`, a delivery is held and not acked, an
  aggregate stops rather than fold a missing value, and a decision answers
  `{:error, :key_unknown}` with nothing appended. See `Decree.Event.Handler`,
  `Decree.Aggregates.Aggregate` and `Decree.Decision`.

  A nil stays nil. A bare value in a declared field passes through, and emits
  `[:decree, :personal_field, :plaintext_read]` unless its subject is `{:when_present, _}`. A
  bare value in a retired slot passes through with no lookup and emits nothing.

  ## Telemetry

    - `[:decree, :personal_field, :key_unknown]`, with `%{application:, subject_id:}`, for a
      lookup that answered unknown and for a value that a key could not read. `subject_id` is
      nil for a malformed envelope or a subject that is not an id.

    - `[:decree, :personal_field, :plaintext_read]`, with `%{application:, event_type:, field:}`
      and no value.
  """

  alias Decree.Application
  alias Decree.Event.PersonalFields, as: Protocol
  alias Decree.EventStore.EventData
  alias Decree.EventStore.RecordedEvent
  alias Decree.PersonalFields.Declaration
  alias Decree.PersonalFields.Envelope
  alias Decree.PersonalFields.Lookup
  alias Decree.PersonalFields.RedactedError

  @type answers :: Lookup.answers()

  # The event type of a snapshot, in place of a stored one.
  @snapshot "snapshot"

  # The subject a retired slot reads under. An atom would be a field name, so it is a shape no
  # declared subject can take.
  @retired {:retired}

  @doc """
  Decrypts a recorded event for a reader that needs plaintext.

  Answers `{:ok, event}`, with its fields and metadata decrypted, erased fields nil and erased
  metadata keys removed, or `{:error, :key_unknown}` if any value can't be read. An event that
  declares nothing comes back as it is.

  The declaration read is the one of the struct after upcasting, and the subject is each
  envelope's own, so a conditional declaration is not worked out again.
  """
  @spec decrypt_recorded_event(Application.t(), RecordedEvent.t()) ::
          {:ok, RecordedEvent.t()} | {:error, :key_unknown}
  def decrypt_recorded_event(application, %RecordedEvent{} = event) do
    case Protocol.declaration(event.data) do
      nil ->
        {:ok, event}

      %Declaration{} = declaration ->
        with {:ok, data, answers} <- decrypt_fields(application, event, declaration, %{}),
             {:ok, metadata, _answers} <-
               decrypt_metadata(application, event, declaration, answers) do
          {:ok, %RecordedEvent{event | data: data, metadata: metadata}}
        end
    end
  end

  ## Append

  @doc false
  # The list the metadata fence reads, once per append. An empty list when no event has
  # metadata, or when the application named none.
  @spec metadata_fence(Application.t(), [EventData.t()]) :: [atom]
  def metadata_fence(application, event_data) do
    if Enum.all?(event_data, &empty?(&1.metadata)) do
      []
    else
      Application.personal_metadata_keys(application)
    end
  end

  @doc false
  # The lookup and the drop. Answers the event data with erased values gone, and what is left
  # to encrypt: `nil` for an event with no declaration, else a list holding keys, for
  # `encrypt_append/3` only.
  def resolve_append(application, %EventData{} = event_data, fence) do
    declaration = Protocol.declaration(event_data.data)

    with :ok <- check_fence(event_data.metadata, declaration, fence) do
      case declaration do
        nil ->
          {:ok, event_data, nil}

        %Declaration{} ->
          with :ok <- check_retired(event_data, declaration) do
            resolve_declared(application, event_data, declaration)
          end
      end
    end
  end

  defp check_fence(_metadata, _declaration, []), do: :ok

  defp check_fence(metadata, declaration, fence) do
    declared =
      case declaration do
        nil -> []
        %Declaration{metadata: entries} -> Keyword.keys(entries)
      end

    if Enum.any?(fence -- declared, &present?(metadata, &1)) do
      {:error, :undeclared_personal_metadata}
    else
      :ok
    end
  end

  # A retired slot is stored as given, with no lookup, and never takes an envelope.
  defp check_retired(%EventData{data: event, metadata: metadata}, declaration) do
    %Declaration{retired_fields: fields, retired_metadata: names} = declaration

    values =
      Enum.map(fields, &Map.get(event, &1)) ++
        for name <- names, form <- key_forms(metadata, name), do: value(metadata, form)

    if Enum.any?(values, &Envelope.envelope?/1) do
      {:error, :personal_field_is_envelope}
    else
      :ok
    end
  end

  defp resolve_declared(application, event_data, declaration) do
    declaration
    |> append_slots(event_data)
    |> Enum.reduce_while({event_data, [], %{}}, fn slot, {event_data, plan, answers} ->
      case resolve_slot(application, slot, event_data.data, answers) do
        {:keep, answers} ->
          {:cont, {event_data, plan, answers}}

        {:drop, answers} ->
          {:cont, {drop(event_data, slot), plan, answers}}

        {:encrypt, step, answers} ->
          {:cont, {event_data, [step | plan], answers}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      {event_data, plan, _answers} -> {:ok, event_data, Enum.reverse(plan)}
    end
  end

  # A slot is a declared place: a field, or a metadata entry in the key form it has. A place
  # with no value still has a subject to check.
  defp append_slots(%Declaration{} = declaration, %EventData{} = event_data) do
    %Declaration{fields: fields, metadata: entries} = declaration
    %EventData{data: event, metadata: metadata} = event_data

    field_slots =
      for {name, subject} <- fields do
        {{:field, name}, subject, Map.get(event, name)}
      end

    metadata_slots =
      for {name, subject} <- entries, form <- key_forms(metadata, name) do
        {{:metadata, name, form}, subject, value(metadata, form)}
      end

    field_slots ++ metadata_slots
  end

  defp resolve_slot(application, {_place, subject, value} = slot, event, answers) do
    case subject(subject, event) do
      :missing ->
        {:error, :personal_subject_missing}

      :none ->
        if Envelope.envelope?(value),
          do: {:error, :personal_field_is_envelope},
          else: {:keep, answers}

      {:ok, subject_id} ->
        cond do
          is_nil(value) -> {:keep, answers}
          Envelope.envelope?(value) -> {:error, :personal_field_is_envelope}
          true -> lookup_slot(application, slot, subject_id, answers)
        end
    end
  end

  defp lookup_slot(application, {place, _subject, value}, subject_id, answers) do
    case Lookup.fetch(application, subject_id, answers) do
      {{:ok, key_id, key}, answers} ->
        {:encrypt, {place, value, {key_id, key}, subject_id}, answers}

      {:erased, answers} ->
        {:drop, answers}

      {:unknown, _answers} ->
        {:error, :key_unknown}
    end
  end

  defp subject(field, event) when is_atom(field) do
    case Map.get(event, field) do
      nil -> :missing
      subject_id -> {:ok, subject_id}
    end
  end

  defp subject({:first_present, fields}, event) do
    case Enum.find_value(fields, &Map.get(event, &1)) do
      nil -> :missing
      subject_id -> {:ok, subject_id}
    end
  end

  defp subject({:when_present, field}, event) do
    case Map.get(event, field) do
      nil -> :none
      subject_id -> {:ok, subject_id}
    end
  end

  defp drop(%EventData{data: event} = event_data, {{:field, name}, _subject, _value}) do
    %EventData{event_data | data: Map.put(event, name, nil)}
  end

  defp drop(%EventData{metadata: metadata} = event_data, {{:metadata, _name, form}, _, _}) do
    %EventData{event_data | metadata: Map.delete(metadata, form)}
  end

  @doc false
  # Replaces each value left by `resolve_append/3` with its envelope.
  def encrypt_append(%EventData{} = event_data, nil, _stream_uuid), do: {:ok, event_data}

  def encrypt_append(%EventData{event_type: event_type} = event_data, plan, stream_uuid) do
    Enum.reduce_while(plan, {:ok, event_data}, fn step, {:ok, event_data} ->
      {place, value, key, subject_id} = step
      location = {stream_uuid, event_type, field_name(place)}

      case Envelope.encrypt(value, key, subject_id, location) do
        {:ok, envelope} -> {:cont, {:ok, put(event_data, place, envelope)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp put(%EventData{data: event} = event_data, {:field, name}, envelope) do
    %EventData{event_data | data: Map.put(event, name, envelope)}
  end

  defp put(%EventData{metadata: metadata} = event_data, {:metadata, _name, form}, envelope) do
    %EventData{event_data | metadata: Map.put(metadata, form, envelope)}
  end

  defp field_name({:field, name}), do: Atom.to_string(name)
  defp field_name({:metadata, name, _form}), do: "metadata." <> Atom.to_string(name)

  ## Fold

  @doc false
  # The events a fold reads, or `nil` for an aggregate or a decision that doesn't use
  # `Decree.PersonalFields.Aggregate`. Read once per fold, because one that uses it can hold
  # decrypted state whatever it lists, and one that doesn't pays this one check.
  @spec fold_events(module) :: [module] | nil
  def fold_events(aggregate_module) do
    if adopted?(aggregate_module), do: aggregate_module.__personal_fold__()
  end

  @doc false
  # An aggregate or a decision that holds decrypted values, whose raises are redacted.
  @spec adopted?(module) :: boolean
  def adopted?(module), do: exports?(module, :__personal_fold__)

  @doc false
  # What a raise from such a module reports. Its code holds decrypted values, and a raise can
  # carry them in its message or in a stack frame's arguments. Any other module's is its own.
  @spec reported_exception(module, Exception.t(), Exception.stacktrace()) ::
          {Exception.t(), Exception.stacktrace()}
  def reported_exception(module, error, stacktrace) do
    if adopted?(module) do
      {RedactedError.redact(error), RedactedError.scrub_stacktrace(stacktrace)}
    else
      {error, stacktrace}
    end
  end

  @doc false
  # One event as a fold reads it: decrypted when `fold` lists it, and as stored otherwise.
  @spec readable_event(Application.t(), RecordedEvent.t(), [module], answers) ::
          {:ok, term, answers} | {:error, :key_unknown}
  def readable_event(application, %RecordedEvent{data: data} = event, fold, answers) do
    if in_fold?(data, fold) do
      decrypt_fold_event(application, event, answers)
    else
      {:ok, data, answers}
    end
  end

  defp in_fold?(%module{}, fold), do: module in fold
  defp in_fold?(_data, _fold), do: false

  @doc false
  # Decrypts one event's body for a fold. Metadata is never read: no fold sees it.
  @spec decrypt_fold_event(Application.t(), RecordedEvent.t(), answers) ::
          {:ok, struct, answers} | {:error, :key_unknown}
  def decrypt_fold_event(application, %RecordedEvent{} = event, answers) do
    case Protocol.declaration(event.data) do
      nil -> {:ok, event.data, answers}
      %Declaration{} = declaration -> decrypt_fields(application, event, declaration, answers)
    end
  end

  ## Snapshots

  @doc false
  # Encrypts on a copy of the state, for the store. There is no plaintext fallback: every
  # refusal here records no snapshot.
  @spec encrypt_state(Application.t(), String.t(), struct) :: {:ok, struct} | {:error, atom}
  def encrypt_state(application, source_uuid, %module{} = state) do
    with {:ok, state, _answers} <-
           reduce_paths(personal_state(module), {state, %{}}, fn path, {state, answers} ->
             encrypt_path(application, source_uuid, module, path, state, answers)
           end) do
      {:ok, state}
    end
  end

  def encrypt_state(_application, _source_uuid, state), do: {:ok, state}

  defp encrypt_path(application, source_uuid, module, {state_field, :self}, state, answers) do
    case Map.get(state, state_field) do
      nil ->
        {:ok, state, answers}

      value ->
        subject_id = Map.fetch!(state, module.__personal_state_subject__())
        context = {application, subject_id, scalar_location(source_uuid, state_field)}

        with {:ok, envelope, answers} <- encrypt_state_value(value, context, answers) do
          {:ok, Map.put(state, state_field, envelope), answers}
        end
    end
  end

  defp encrypt_path(application, source_uuid, module, {state_field, entry_key}, state, answers) do
    case Map.get(state, state_field) do
      empty when empty in [nil, []] ->
        {:ok, state, answers}

      entries when is_list(entries) ->
        subject_id = Map.fetch!(state, module.__personal_state_subject__())
        field = "#{state_field}.#{entry_key}"
        context = {application, subject_id, {source_uuid, @snapshot, field}}

        with {:ok, entries, answers} <- encrypt_entries(entries, entry_key, context, answers) do
          {:ok, Map.put(state, state_field, entries), answers}
        end

      _other ->
        {:error, :invalid_personal_state}
    end
  end

  defp encrypt_entries(entries, entry_key, context, answers) do
    entries
    |> Enum.reduce_while({[], answers}, fn entry, {encrypted, answers} ->
      case encrypt_entry(entry, entry_key, context, answers) do
        {:ok, entry, answers} -> {:cont, {[entry | encrypted], answers}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:error, _reason} = error -> error
      {entries, answers} -> {:ok, Enum.reverse(entries), answers}
    end
  end

  defp encrypt_entry(entry, entry_key, context, answers) when is_map(entry) do
    case key_forms(entry, entry_key) do
      [form] -> encrypt_entry_form(entry, form, context, answers)
      # One value would go in the clear beside the other's envelope.
      [_atom, _string] -> {:error, :invalid_personal_state}
    end
  end

  defp encrypt_entry(_entry, _entry_key, _context, _answers) do
    {:error, :invalid_personal_state}
  end

  defp encrypt_entry_form(entry, form, context, answers) do
    case value(entry, form) do
      nil ->
        {:ok, entry, answers}

      value ->
        with {:ok, envelope, answers} <- encrypt_state_value(value, context, answers) do
          {:ok, Map.put(entry, form, envelope), answers}
        end
    end
  end

  defp encrypt_state_value(_value, {_application, nil, _location}, _answers) do
    {:error, :personal_subject_missing}
  end

  defp encrypt_state_value(value, {application, subject_id, location}, answers) do
    if Envelope.envelope?(value) do
      {:error, :personal_field_is_envelope}
    else
      case Lookup.fetch(application, subject_id, answers) do
        {{:ok, key_id, key}, answers} ->
          with {:ok, envelope} <- Envelope.encrypt(value, {key_id, key}, subject_id, location) do
            {:ok, envelope, answers}
          end

        {:erased, _answers} ->
          {:error, :subject_erased}

        {:unknown, _answers} ->
          {:error, :key_unknown}
      end
    end
  end

  @doc false
  # Decrypts a snapshot's state, after the decoder has run. `{:erased, answers}` means the
  # snapshot can't be used, and the fold starts from the first event.
  @spec decrypt_state(Application.t(), String.t(), module, struct) ::
          {:ok, struct, answers} | {:erased, answers} | {:error, :key_unknown}
  def decrypt_state(application, source_uuid, aggregate_module, state) do
    reduce_paths(personal_state(aggregate_module), {state, %{}}, fn path, {state, answers} ->
      decrypt_path(application, source_uuid, path, state, answers)
    end)
  end

  defp decrypt_path(application, source_uuid, {state_field, :self}, state, answers) do
    location = scalar_location(source_uuid, state_field)

    case read(application, Map.get(state, state_field), :snapshot, location, answers) do
      {:ok, value, answers} -> {:ok, Map.put(state, state_field, value), answers}
      :nil_value -> {:ok, state, answers}
      {:absent, answers} -> {:erased, answers}
      {:error, :key_unknown} = error -> error
    end
  end

  defp decrypt_path(application, source_uuid, {state_field, entry_key}, state, answers) do
    case Map.get(state, state_field) do
      nil ->
        {:ok, state, answers}

      entries when is_list(entries) ->
        location = {source_uuid, @snapshot, "#{state_field}.#{entry_key}"}

        with {:ok, entries, answers} <-
               decrypt_entries(application, entries, entry_key, location, answers) do
          {:ok, Map.put(state, state_field, entries), answers}
        end

      _other ->
        unreadable(application, nil)
    end
  end

  defp decrypt_entries(application, entries, entry_key, location, answers) do
    entries
    |> Enum.reduce_while({[], answers}, fn entry, {decrypted, answers} ->
      case decrypt_entry(application, entry, entry_key, location, answers) do
        {:ok, entry, answers} -> {:cont, {[entry | decrypted], answers}}
        other -> {:halt, other}
      end
    end)
    |> case do
      {entries, answers} when is_list(entries) -> {:ok, Enum.reverse(entries), answers}
      other -> other
    end
  end

  defp decrypt_entry(application, entry, entry_key, location, answers) when is_map(entry) do
    form = entry_form(entry, entry_key)

    case read(application, value(entry, form), :snapshot, location, answers) do
      {:ok, value, answers} -> {:ok, Map.put(entry, form, value), answers}
      :nil_value -> {:ok, entry, answers}
      {:absent, answers} -> {:erased, answers}
      {:error, :key_unknown} = error -> error
    end
  end

  defp decrypt_entry(application, _entry, _entry_key, _location, _answers) do
    unreadable(application, nil)
  end

  defp scalar_location(source_uuid, state_field) do
    {source_uuid, @snapshot, Atom.to_string(state_field)}
  end

  defp personal_state(module) do
    if exports?(module, :__personal_state__), do: module.__personal_state__(), else: []
  end

  defp reduce_paths(paths, {state, answers}, fun) do
    Enum.reduce_while(paths, {:ok, state, answers}, fn path, {:ok, state, answers} ->
      case fun.(path, {state, answers}) do
        {:ok, _state, _answers} = result -> {:cont, result}
        other -> {:halt, other}
      end
    end)
  end

  ## Reading a value

  defp decrypt_fields(application, %RecordedEvent{} = event, declaration, answers) do
    %RecordedEvent{data: data, stream_id: stream_id, event_type: event_type} = event
    %Declaration{fields: fields, retired_fields: retired} = declaration
    slots = fields ++ Enum.map(retired, &{&1, @retired})

    Enum.reduce_while(slots, {:ok, data, answers}, fn {name, subject}, acc ->
      {:ok, data, answers} = acc
      location = {stream_id, event_type, Atom.to_string(name)}

      case read(application, Map.get(data, name), subject, location, answers) do
        {:ok, value, answers} -> {:cont, {:ok, Map.put(data, name, value), answers}}
        :nil_value -> {:cont, acc}
        {:absent, answers} -> {:cont, {:ok, Map.put(data, name, nil), answers}}
        {:error, :key_unknown} = error -> {:halt, error}
      end
    end)
  end

  defp decrypt_metadata(application, %RecordedEvent{} = event, declaration, answers) do
    %RecordedEvent{metadata: metadata, stream_id: stream_id, event_type: event_type} = event
    metadata = metadata || %{}
    %Declaration{metadata: entries, retired_metadata: retired} = declaration

    slots =
      for {name, subject} <- entries ++ Enum.map(retired, &{&1, @retired}),
          form <- key_forms(metadata, name),
          do: {name, subject, form}

    Enum.reduce_while(slots, {:ok, metadata, answers}, fn {name, subject, form}, acc ->
      {:ok, metadata, answers} = acc
      location = {stream_id, event_type, field_name({:metadata, name, form})}

      case read(application, value(metadata, form), subject, location, answers) do
        {:ok, value, answers} -> {:cont, {:ok, Map.put(metadata, form, value), answers}}
        :nil_value -> {:cont, acc}
        {:absent, answers} -> {:cont, {:ok, Map.delete(metadata, form), answers}}
        {:error, :key_unknown} = error -> {:halt, error}
      end
    end)
  end

  # The subject of a stored value is the envelope's, never the declaration's: `subject` here
  # only says whether a bare value is allowed for. A retired slot's always is.
  defp read(_application, nil, _subject, _location, _answers), do: :nil_value

  defp read(application, value, subject, location, answers) do
    if Envelope.envelope?(value) do
      read_envelope(application, value, location, answers)
    else
      plaintext_read(application, subject, location)

      {:ok, value, answers}
    end
  end

  defp read_envelope(application, value, location, answers) do
    case Envelope.parse(value) do
      {:ok, envelope} ->
        subject_id = Envelope.subject_id(envelope)

        case Lookup.fetch(application, subject_id, answers) do
          {{:ok, key_id, key}, answers} ->
            read_with_key(application, envelope, key_id, key, location, answers)

          {:erased, answers} ->
            {:absent, answers}

          {:unknown, _answers} ->
            {:error, :key_unknown}
        end

      :error ->
        unreadable(application, nil)
    end
  end

  defp read_with_key(application, envelope, key_id, key, location, answers) do
    with true <- Envelope.key_id(envelope) == key_id,
         {:ok, value} <- Envelope.decrypt(envelope, key, location) do
      {:ok, value, answers}
    else
      _unreadable -> unreadable(application, Envelope.subject_id(envelope))
    end
  end

  defp unreadable(application, subject_id) do
    Lookup.unknown(application, subject_id)

    {:error, :key_unknown}
  end

  defp plaintext_read(_application, {:when_present, _field}, _location), do: :ok
  defp plaintext_read(_application, @retired, _location), do: :ok

  defp plaintext_read(application, _subject, {_stream, event_type, field}) do
    :telemetry.execute([:decree, :personal_field, :plaintext_read], %{}, %{
      application: application,
      event_type: event_type,
      field: field
    })
  end

  ## Maps with either key form

  defp empty?(nil), do: true
  defp empty?(metadata), do: map_size(metadata) == 0

  defp present?(nil, _name), do: false

  defp present?(metadata, name) do
    not is_nil(value(metadata, name)) or not is_nil(value(metadata, Atom.to_string(name)))
  end

  # The key forms a map holds, or the atom form when it holds neither.
  defp key_forms(nil, name), do: [name]

  defp key_forms(map, name) do
    case Enum.filter([name, Atom.to_string(name)], &Map.has_key?(map, &1)) do
      [] -> [name]
      forms -> forms
    end
  end

  defp entry_form(entry, entry_key) do
    if Map.has_key?(entry, entry_key), do: entry_key, else: Atom.to_string(entry_key)
  end

  defp value(nil, _form), do: nil
  defp value(map, form), do: Map.get(map, form)

  # Not loaded is not the same as not exported, and a module read too early must not skip this.
  defp exports?(module, function) do
    Code.ensure_loaded?(module) and function_exported?(module, function, 0)
  end
end
