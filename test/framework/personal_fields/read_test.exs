defmodule Decree.PersonalFields.ReadTest do
  use Decree.PersonalFields.Case

  alias Decree.Aggregates.Aggregate
  alias Decree.PersonalFields.Fixtures.{Account, Handler}

  alias Decree.PersonalFields.Fixtures.Events.{
    BeforeRename,
    Linked,
    MetadataOnly,
    PassedThrough,
    Plain,
    PreAuthFailed,
    Registered,
    Renamed,
    Scored,
    TwoSubjects,
    Unfolded
  }

  setup do
    start_supervised!(App)

    :ok
  end

  describe "decrypt_recorded_event/2" do
    test "a bare read is ciphertext, and a decrypted one plaintext", context do
      %{stream: stream, subject: subject} = context
      event = %Registered{user_id: subject, email: email(), note: "a note"}

      dispatch!(stream, event, metadata: %{"email" => email(), "password_hash" => "hash"})

      assert [%RecordedEvent{data: %Registered{email: stored}, metadata: metadata} = recorded] =
               read(stream)

      assert %{"$pf": 1} = stored
      assert %{"$pf" => 1} = metadata["password_hash"]

      assert {:ok, %RecordedEvent{data: ^event, metadata: decrypted}} = decrypt(recorded)
      assert %{"email" => "alice@secret.example", "password_hash" => "hash"} = decrypted

      assert recorded.data.email == stored
    end

    test "an event with no declaration comes back as it is", %{stream: stream} do
      dispatch!(stream, %Plain{note: "a note"})

      [recorded] = read(stream)

      assert decrypt(recorded) == {:ok, recorded}
    end

    test "a value that is not a string keeps its type", %{stream: stream, subject: subject} do
      dispatch!(stream, %Scored{user_id: subject, score: %{"points" => 12}})

      assert {:ok, %RecordedEvent{data: %Scored{score: %{"points" => 12}}}} =
               stream |> read() |> hd() |> decrypt()
    end

    test "one lookup per subject per event", %{stream: stream, subject: subject} do
      dispatch!(stream, %Registered{user_id: subject, email: email()},
        metadata: %{"email" => email(), "password_hash" => "hash"}
      )

      [recorded] = read(stream)
      before = Keys.lookups(subject)

      assert {:ok, _event} = decrypt(recorded)
      assert Keys.lookups(subject) == before + 1
    end

    test "any unknown fails the whole event", %{stream: stream, subject: subject} do
      dispatch!(stream, %Registered{user_id: subject, email: email()},
        metadata: %{"password_hash" => "hash"}
      )

      Keys.set(subject, :unknown)

      assert stream |> read() |> hd() |> decrypt() == {:error, :key_unknown}
    end

    test "erased gives a nil field and no metadata key", %{stream: stream, subject: subject} do
      dispatch!(stream, %Registered{user_id: subject, email: email()},
        metadata: %{"email" => email(), "password_hash" => "hash", "who" => "alice"}
      )

      Keys.set(subject, :erased)

      assert {:ok, %RecordedEvent{data: %Registered{email: nil}, metadata: metadata}} =
               stream |> read() |> hd() |> decrypt()

      assert metadata == %{"who" => "alice"}
    end

    test "uses the envelope's subject, not the declaration's", context do
      %{stream: stream, subject: subject} = context
      other = Keys.mint(UUID.uuid4())

      # Stored for `subject`, and read as an event naming `other`.
      [%{data: %Registered{} = data}] =
        prepare!(stream, %Registered{user_id: subject, email: email()})

      store_raw!(stream, %Registered{data | user_id: other})

      assert {:ok, %RecordedEvent{data: %Registered{email: "alice@secret.example"}}} =
               stream |> read() |> hd() |> decrypt()

      assert Keys.lookups(other) == 0
    end
  end

  describe "a bare value" do
    setup %{} do
      id = attach_telemetry([[:decree, :personal_field, :plaintext_read]])

      [telemetry: id]
    end

    test "nil stays nil, and says nothing", %{stream: stream, subject: subject} do
      store_raw!(stream, %Registered{user_id: subject, email: nil})

      assert {:ok, %RecordedEvent{data: %Registered{email: nil}}} =
               stream |> read() |> hd() |> decrypt()

      assert flush_telemetry() == []
    end

    test "under a field or `:first_present` passes and says so", context do
      %{stream: stream, subject: subject} = context
      store_raw!(stream, %Registered{user_id: subject, email: email()}, %{"email" => "bare"})

      assert {:ok, %RecordedEvent{data: %Registered{email: "alice@secret.example"}} = read} =
               stream |> read() |> hd() |> decrypt()

      assert read.metadata["email"] == "bare"

      telemetry = flush_telemetry()
      type = "Elixir.Decree.PersonalFields.Fixtures.Events.Registered"

      assert length(telemetry) == 2

      for field <- ["email", "metadata.email"] do
        assert {:telemetry, _event, %{}, metadata} =
                 Enum.find(telemetry, fn message -> elem(message, 3).field == field end)

        assert metadata == %{application: App, event_type: type, field: field}
      end

      refute inspect(telemetry) =~ "secret.example"
    end

    test "under `:when_present` passes silently", %{stream: stream} do
      store_raw!(stream, %PreAuthFailed{user_id: nil, email: email(), ip_address: "192.0.2.1"})

      assert {:ok, %RecordedEvent{data: %PreAuthFailed{email: "alice@secret.example"}}} =
               stream |> read() |> hd() |> decrypt()

      assert flush_telemetry() == []
    end
  end

  describe "two subjects, one shredded" do
    setup %{stream: stream, subject: subject} do
      member = Keys.mint(UUID.uuid4())

      dispatch!(stream, %TwoSubjects{
        owner_id: subject,
        member_id: member,
        owner_email: email("owner"),
        member_email: email("member")
      })

      Keys.set(member, :erased)

      [member: member]
    end

    test "at append one field is stored nil", %{stream: stream, subject: subject, member: member} do
      other = UUID.uuid4()

      dispatch!(other, %TwoSubjects{
        owner_id: subject,
        member_id: member,
        owner_email: email("owner"),
        member_email: email("member")
      })

      assert [%RecordedEvent{data: %TwoSubjects{owner_email: %{"$pf": 1}, member_email: nil}}] =
               read(other)

      assert [%RecordedEvent{data: %TwoSubjects{member_email: %{"$pf": 1}}}] = read(stream)
    end

    test "decrypt_recorded_event/2 gives one nil and one plaintext", %{stream: stream} do
      assert {:ok, %RecordedEvent{data: data}} = stream |> read() |> hd() |> decrypt()
      assert %TwoSubjects{owner_email: "owner@secret.example", member_email: nil} = data
    end

    test "a handler gets one nil and one plaintext" do
      start_supervised!({Handler, start_from: :origin})

      assert_receive {:handled, %TwoSubjects{} = event, _metadata}
      assert %TwoSubjects{owner_email: "owner@secret.example", member_email: nil} = event
    end

    test "a fold gets one nil and one plaintext", %{stream: stream} do
      :ok = Aggregate.shutdown(App, Account, stream)

      assert %Account{email: "owner@secret.example", other_email: nil} =
               Aggregate.aggregate_state(App, Account, stream)
    end
  end

  describe "an upcaster" do
    test "passing an envelope through leaves it readable", context do
      %{stream: stream, subject: subject} = context

      dispatch!(stream, %PassedThrough{user_id: subject, email: email()})

      assert [%RecordedEvent{data: %PassedThrough{version: 2, email: %{"$pf": 1}}} = recorded] =
               read(stream)

      assert {:ok, %RecordedEvent{data: %PassedThrough{email: "alice@secret.example"}}} =
               decrypt(recorded)

      start_supervised!({Handler, start_from: :origin})
      assert_receive {:handled, %PassedThrough{email: "alice@secret.example"}, _metadata}

      :ok = Aggregate.shutdown(App, Account, stream)

      assert %Account{email: "alice@secret.example"} =
               Aggregate.aggregate_state(App, Account, stream)
    end

    test "to a different struct keeps the additional data on the stored type", context do
      %{stream: stream, subject: subject} = context

      dispatch!(stream, %BeforeRename{user_id: subject, email: email()})

      assert [%RecordedEvent{data: %Renamed{email: %{"$pf": 1}}} = recorded] = read(stream)
      assert recorded.event_type == to_string(BeforeRename)

      assert {:ok, %RecordedEvent{data: %Renamed{email: "alice@secret.example"}}} =
               decrypt(recorded)

      start_supervised!({Handler, start_from: :origin})
      assert_receive {:handled, %Renamed{email: "alice@secret.example"}, _metadata}

      :ok = Aggregate.shutdown(App, Account, stream)

      assert %Account{email: "alice@secret.example"} =
               Aggregate.aggregate_state(App, Account, stream)
    end
  end

  describe "a replay after a shred" do
    setup %{stream: stream, subject: subject} do
      dispatch!(
        stream,
        [
          %Registered{user_id: subject, email: email()},
          %Linked{user_id: subject, provider_uid: "u"}
        ],
        metadata: %{"password_hash" => "hash"}
      )

      :ok = Aggregate.shutdown(App, Account, stream)
      Keys.set(subject, :erased)

      :ok
    end

    test "folds an absent field", %{stream: stream} do
      assert %Account{email: nil, links: [%{provider_uid: nil}], seen: 2} =
               Aggregate.aggregate_state(App, Account, stream)
    end

    test "a subscription from origin gets an absent field, not ciphertext", %{subject: subject} do
      start_supervised!({Handler, start_from: :origin})

      assert_receive {:handled, %Registered{user_id: ^subject, email: nil}, metadata}
      refute Map.has_key?(metadata, "password_hash")
    end

    test "an upcaster's event reads absent too", %{stream: stream, subject: subject} do
      Keys.set(subject, :key)
      dispatch!(stream, %BeforeRename{user_id: subject, email: email()})
      Keys.set(subject, :erased)

      assert {:ok, %RecordedEvent{data: %Renamed{email: nil}}} =
               stream |> read() |> List.last() |> decrypt()

      start_supervised!({Handler, start_from: :origin})
      assert_receive {:handled, %Renamed{email: nil}, _metadata}
    end
  end

  describe "tampering with a stored value" do
    setup %{stream: stream, subject: subject} do
      other = Keys.mint(UUID.uuid4())

      [%{data: %{email: envelope}}] =
        prepare!(stream, %Registered{user_id: subject, email: email()})

      [%{data: %{email: second}}] = prepare!(stream, %Registered{user_id: subject, email: "x"})

      [envelope: envelope, second: second, other: other]
    end

    defp flip_byte(base64) do
      <<first, rest::binary>> = Base.decode64!(base64)

      Base.encode64(<<Bitwise.bxor(first, 1), rest::binary>>)
    end

    defp flip_last_byte(base64) do
      bytes = Base.decode64!(base64)
      size = byte_size(bytes) - 1
      <<head::binary-size(^size), last>> = bytes

      Base.encode64(<<head::binary, Bitwise.bxor(last, 1)>>)
    end

    defp assert_refused(stream, event) do
      store_raw!(stream, event)

      assert [recorded] = read(stream)
      assert decrypt(recorded) == {:error, :key_unknown}
    end

    test "control: the untouched envelope, stored raw, reads", context do
      %{stream: stream, subject: subject, envelope: envelope} = context

      store_raw!(stream, %Registered{user_id: subject, email: envelope})

      assert [recorded] = read(stream)

      assert {:ok, %RecordedEvent{data: %Registered{email: "alice@secret.example"}}} =
               decrypt(recorded)
    end

    test "a flipped ciphertext byte", context do
      %{stream: stream, subject: subject, envelope: envelope} = context
      tampered = Map.update!(envelope, "c", &flip_byte/1)

      assert_refused(stream, %Registered{user_id: subject, email: tampered})
    end

    test "a flipped tag", context do
      %{stream: stream, subject: subject, envelope: envelope} = context
      tampered = Map.update!(envelope, "c", &flip_last_byte/1)

      assert_refused(stream, %Registered{user_id: subject, email: tampered})
    end

    test "a swapped nonce", context do
      %{stream: stream, subject: subject, envelope: envelope, second: second} = context
      tampered = %{envelope | "n" => second["n"]}

      assert_refused(stream, %Registered{user_id: subject, email: tampered})
    end

    test "a changed key id", context do
      %{stream: stream, subject: subject, envelope: envelope} = context

      assert_refused(stream, %Registered{user_id: subject, email: %{envelope | "k" => "other"}})
    end

    test "a key id the lookup no longer names, with the same key", context do
      %{stream: stream, subject: subject, envelope: envelope} = context

      store_raw!(stream, %Registered{user_id: subject, email: envelope})
      Keys.relabel(subject, "another-id")

      assert [recorded] = read(stream)
      assert decrypt(recorded) == {:error, :key_unknown}
    end

    test "a subject changed to another", context do
      %{stream: stream, subject: subject, envelope: envelope, other: other} = context

      assert_refused(stream, %Registered{user_id: subject, email: %{envelope | "s" => other}})
    end

    test "a subject changed to another, with that subject's key id", context do
      %{stream: stream, subject: subject, envelope: envelope, other: other} = context
      {key_id, _key} = Keys.key(other)
      tampered = %{envelope | "s" => other, "k" => key_id}

      assert_refused(stream, %Registered{user_id: subject, email: tampered})
    end

    test "moved to another field", context do
      %{stream: stream, subject: subject, envelope: envelope} = context

      event = %TwoSubjects{owner_id: subject, member_id: subject, owner_email: envelope}

      assert_refused(stream, event)
    end

    test "moved to another stream", context do
      %{subject: subject, envelope: envelope} = context

      assert_refused(UUID.uuid4(), %Registered{user_id: subject, email: envelope})
    end

    test "moved to another event type", context do
      %{stream: stream, subject: subject, envelope: envelope} = context

      assert_refused(stream, %Unfolded{user_id: subject, email: envelope})
    end

    test "moved from body to metadata", context do
      %{stream: stream, subject: subject, envelope: envelope} = context

      store_raw!(stream, %Registered{user_id: subject}, %{"email" => envelope})

      assert [recorded] = read(stream)
      assert decrypt(recorded) == {:error, :key_unknown}
    end

    test "from metadata to body", %{stream: stream, subject: subject} do
      [%{metadata: %{"password_hash" => envelope}}] =
        prepare!(stream, %MetadataOnly{user_id: subject}, %{"password_hash" => "hash"})

      assert_refused(stream, %Registered{user_id: subject, email: envelope})
    end

    test "a malformed envelope is never plaintext or absent", context do
      %{stream: stream, subject: subject, envelope: envelope} = context

      assert_refused(stream, %Registered{user_id: subject, email: %{envelope | "$pf" => 2}})
    end

    test "is reported with the subject, and nothing else", context do
      %{stream: stream, subject: subject, envelope: envelope} = context
      attach_telemetry([[:decree, :personal_field, :key_unknown]])

      tampered = Map.update!(envelope, "c", &flip_byte/1)
      assert_refused(stream, %Registered{user_id: subject, email: tampered})

      assert [{:telemetry, _event, %{}, %{application: App, subject_id: ^subject} = metadata}] =
               flush_telemetry()

      assert map_size(metadata) == 2
    end

    test "the stored row is what the test wrote", %{conn: conn, stream: stream} = context do
      %{subject: subject, envelope: envelope} = context

      store_raw!(stream, %Registered{user_id: subject, email: envelope})

      assert [row] = rows(conn, "events", "data")
      assert Jason.decode!(row)["email"] == envelope
    end
  end

  describe "a raw scan of the store" do
    test "finds no plaintext", %{conn: conn, stream: stream, subject: subject} do
      password_hash = "hash-of-a-password"

      dispatch!(stream, %Registered{user_id: subject, email: email()},
        metadata: %{"email" => email(), "password_hash" => password_hash}
      )

      assert :ok = Aggregate.take_snapshot(App, Account, stream)

      rows = rows(conn, "events", "data, metadata") ++ rows(conn, "snapshots", "data, metadata")
      scan = Enum.join(rows, "\n")

      assert length(rows) >= 2
      assert scan =~ "$pf"
      refute scan =~ "secret.example"
      refute scan =~ password_hash
      refute scan =~ "uid-at-provider"
    end

    test "finds no plaintext in a snapshot row", context do
      %{conn: conn, stream: stream, subject: subject} = context

      stop_supervised!(App)

      start_supervised!(
        {App, snapshotting: %{Account => [snapshot_every: 1, snapshot_version: 1]}}
      )

      dispatch!(stream, %Linked{user_id: subject, provider_uid: "uid-at-provider"})

      Wait.until(fn -> assert [_row] = rows(conn, "snapshots", "data") end)

      assert [row] = rows(conn, "snapshots", "data")
      assert row =~ "$pf"
      refute row =~ "uid-at-provider"
    end
  end

  describe "the source stream" do
    defp source_stream_case(context, handler_opts) do
      %{stream: stream, subject: subject} = context

      dispatch!(stream, %Registered{user_id: subject, email: email()})

      start_supervised!({Handler, [start_from: :origin] ++ handler_opts})

      assert_receive {:handled, %Registered{email: "alice@secret.example"}, metadata}
      assert metadata.stream_id == stream
    end

    test "a subscription on $all decrypts", context do
      source_stream_case(context, subscribe_to: :all)
    end

    test "a subscription on a linked stream decrypts", context do
      %{stream: stream, subject: subject} = context
      linked = "linked-" <> stream
      name = Module.concat([App, "FrameworkEventStore"])

      dispatch!(stream, %Registered{user_id: subject, email: email()})

      assert [%RecordedEvent{event_id: event_id}] = read(stream)

      :ok =
        Decree.FrameworkEventStore.link_to_stream(linked, :any_version, [event_id], name: name)

      start_supervised!({Handler, start_from: :origin, subscribe_to: linked})

      assert_receive {:handled, %Registered{email: "alice@secret.example"}, metadata}
      assert metadata.stream_id == stream

      assert {:ok, %RecordedEvent{stream_id: ^stream}} = linked |> read() |> hd() |> decrypt()
    end
  end
end
