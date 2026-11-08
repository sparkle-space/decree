defmodule Decree.PersonalFields.SnapshotTest do
  use Decree.PersonalFields.Case

  alias Decree.Aggregates.Aggregate
  alias Decree.EventStore.SnapshotData
  alias Decree.PersonalFields.Fixtures.Account
  alias Decree.PersonalFields.Fixtures.Events.{Linked, Plain, Registered}
  alias Decree.Snapshotting

  defmodule Unlisted do
    @derive Jason.Encoder
    defstruct [:id, :email]
  end

  defmodule StringKeyed do
    use Decree.PersonalFields.Aggregate

    @personal_state [entries: :secret]
    @personal_state_subject :id

    @derive Jason.Encoder
    defstruct [:id, entries: []]

    def apply(state, _event), do: state
  end

  setup %{conn: conn, stream: stream} do
    start_supervised!(App)

    # A snapshot is recorded only for a stream that exists: a bare row, with no events.
    Postgrex.query!(conn, "INSERT INTO streams (stream_uuid) VALUES ($1)", [stream])

    :ok
  end

  defp snapshotting(stream) do
    Snapshotting.new(App, stream, snapshot_every: 1, snapshot_version: 1)
  end

  defp account(subject, links \\ [%{provider: "p", provider_uid: "uid-at-provider"}]) do
    %Account{user_id: subject, links: links, seen: 1}
  end

  defp take(stream, state, version \\ 1) do
    Snapshotting.take_snapshot(snapshotting(stream), version, state)
  end

  defp stored_snapshot(stream), do: EventStore.read_snapshot(App, stream, 1)

  defp populate(stream) do
    alias Decree.Aggregates.AggregateStateBuilder

    AggregateStateBuilder.populate(%Aggregate{
      application: App,
      aggregate_module: Account,
      aggregate_uuid: stream,
      snapshotting: snapshotting(stream)
    })
  end

  describe "taking a snapshot" do
    test "encrypts the listed paths on a copy", %{conn: conn, stream: stream, subject: subject} do
      state = account(subject)

      assert {:ok, %Snapshotting{snapshot_version: 1}} = take(stream, state)

      assert state == account(subject)

      assert {:ok, %SnapshotData{data: %Account{links: [%{provider_uid: %{"$pf": 1}}]}}} =
               stored_snapshot(stream)

      assert [row] = rows(conn, "versioned_snapshots", "data")
      refute row =~ "uid-at-provider"
    end

    test "the stream names the additional data, and the literal type", context do
      %{stream: stream, subject: subject} = context

      assert {:ok, _snapshotting} = take(stream, account(subject))

      assert {:ok, %SnapshotData{data: %Account{links: [%{provider_uid: stored}]}}} =
               stored_snapshot(stream)

      envelope = Map.new(stored, fn {key, value} -> {Atom.to_string(key), value} end)
      {_key_id, key} = Keys.key(subject)
      {:ok, parsed} = PersonalFields.Envelope.parse(envelope)

      assert PersonalFields.Envelope.decrypt(
               parsed,
               key,
               {stream, "snapshot", "links.provider_uid"}
             ) == {:ok, "uid-at-provider"}
    end

    test "nil and empty need no lookup", %{stream: stream, subject: subject} do
      Keys.set(subject, :unknown)

      assert {:ok, _snapshotting} = take(stream, account(subject, []))
      assert {:ok, _snapshotting} = take(stream, account(subject, nil), 2)

      assert {:ok, _snapshotting} =
               take(stream, account(subject, [%{provider: "p", provider_uid: nil}]), 3)

      assert Keys.lookups(subject) == 0
    end

    test "looks a subject up once", %{stream: stream, subject: subject} do
      links = [%{provider: "a", provider_uid: "1"}, %{provider: "b", provider_uid: "2"}]

      assert {:ok, _snapshotting} = take(stream, account(subject, links))
      assert Keys.lookups(subject) == 1
    end

    for {name, mode, reason} <- [
          {"an unknown key", :unknown, :key_unknown},
          {"an erased subject", :erased, :subject_erased}
        ] do
      test "#{name} records no snapshot", %{stream: stream, subject: subject} do
        Keys.set(subject, unquote(mode))

        assert take(stream, account(subject)) == {:error, unquote(reason)}
        assert stored_snapshot(stream) == {:error, :snapshot_not_found}
      end
    end

    test "a nil subject with a value records no snapshot", %{stream: stream} do
      assert take(stream, account(nil)) == {:error, :personal_subject_missing}
      assert stored_snapshot(stream) == {:error, :snapshot_not_found}
    end

    test "a state field that is neither a list nor nil records no snapshot", context do
      %{stream: stream, subject: subject} = context

      assert take(stream, account(subject, %{provider_uid: "uid"})) ==
               {:error, :invalid_personal_state}

      assert take(stream, account(subject, "uid")) == {:error, :invalid_personal_state}
      assert stored_snapshot(stream) == {:error, :snapshot_not_found}
    end

    test "an entry that is not a map records no snapshot", %{stream: stream, subject: subject} do
      assert take(stream, account(subject, ["uid"])) == {:error, :invalid_personal_state}
      assert stored_snapshot(stream) == {:error, :snapshot_not_found}
    end

    test "a value already an envelope records no snapshot", %{stream: stream, subject: subject} do
      links = [%{provider: "p", provider_uid: %{"$pf" => 1}}]

      assert take(stream, account(subject, links)) == {:error, :personal_field_is_envelope}
      assert stored_snapshot(stream) == {:error, :snapshot_not_found}
    end

    test "string-keyed entries keep their form", %{stream: stream, subject: subject} do
      state = %StringKeyed{id: subject, entries: [%{"secret" => "value", "other" => 1}]}

      assert {:ok, protected} = PersonalFields.encrypt_state(App, stream, state)

      assert [%{"secret" => %{"$pf" => 1}, "other" => 1}] = protected.entries
    end

    test "an entry holding both key forms records no snapshot", context do
      %{conn: conn, stream: stream, subject: subject} = context

      links = [%{:provider_uid => "atom-form", "provider_uid" => "string-form"}]

      assert take(stream, account(subject, links)) == {:error, :invalid_personal_state}
      assert stored_snapshot(stream) == {:error, :snapshot_not_found}
      assert rows(conn, "versioned_snapshots", "data") == []
      assert Keys.lookups(subject) == 0

      links = [%{:provider_uid => nil, "provider_uid" => "string-form"}]

      assert take(stream, account(subject, links)) == {:error, :invalid_personal_state}
    end

    test "an aggregate with no personal state is untouched", %{stream: stream} do
      state = %Unlisted{id: "1", email: "listed-nowhere"}

      assert PersonalFields.encrypt_state(App, stream, state) == {:ok, state}
    end

    test "a failed snapshot leaves the aggregate running", %{stream: stream, subject: subject} do
      stop_supervised!(App)

      start_supervised!(
        {App, snapshotting: %{Account => [snapshot_every: 1, snapshot_version: 1]}}
      )

      dispatch!(stream, %Linked{user_id: subject, provider_uid: "uid"})

      Wait.until(fn -> assert {:ok, _snapshot} = stored_snapshot(stream) end)

      Keys.set(subject, :erased)

      assert Aggregate.take_snapshot(App, Account, stream) == {:error, :subject_erased}
      assert %Account{} = Aggregate.aggregate_state(App, Account, stream)
    end
  end

  describe "populating from a snapshot" do
    test "decrypts after the decoder has run", %{stream: stream, subject: subject} do
      assert {:ok, _snapshotting} = take(stream, account(subject))

      assert %Aggregate{
               aggregate_version: 1,
               aggregate_state: %Account{links: [%{provider_uid: "uid-at-provider"}]}
             } = populate(stream)
    end

    test "erased drops the snapshot and folds from the first event", context do
      %{stream: stream, subject: subject} = context

      dispatch!(stream, [
        %Linked{user_id: subject, provider: "p", provider_uid: "uid"},
        %Plain{user_id: subject}
      ])

      assert {:ok, _snapshotting} = take(stream, account(subject), 2)

      Keys.set(subject, :erased)

      assert %Aggregate{
               aggregate_version: 2,
               aggregate_state: %Account{links: [%{provider: "p", provider_uid: nil}], seen: 2}
             } = populate(stream)
    end

    test "unknown is the typed error", %{stream: stream, subject: subject} do
      assert {:ok, _snapshotting} = take(stream, account(subject))

      Keys.set(subject, :unknown)

      assert populate(stream) == {:error, :key_unknown}
    end

    test "a bare value passes, and says so", %{stream: stream, subject: subject} do
      attach_telemetry([[:decree, :personal_field, :plaintext_read]])

      snapshot = %SnapshotData{
        source_uuid: stream,
        source_version: 1,
        source_type: to_string(Account),
        snapshot_version: 1,
        data: account(subject),
        metadata: %{"snapshot_module_version" => 1}
      }

      :ok = EventStore.record_snapshot(App, snapshot)

      assert %Aggregate{aggregate_state: %Account{links: [%{provider_uid: "uid-at-provider"}]}} =
               populate(stream)

      assert [{:telemetry, _event, %{}, %{event_type: "snapshot", field: field}}] =
               flush_telemetry()

      assert field == "links.provider_uid"
    end

    test "a malformed value is unknown", %{stream: stream, subject: subject} do
      snapshot = %SnapshotData{
        source_uuid: stream,
        source_version: 1,
        source_type: to_string(Account),
        snapshot_version: 1,
        data: account(subject, [%{provider: "p", provider_uid: %{"$pf" => 2}}]),
        metadata: %{"snapshot_module_version" => 1}
      }

      :ok = EventStore.record_snapshot(App, snapshot)

      assert populate(stream) == {:error, :key_unknown}
    end

    test "a state field that is not a list is unknown", %{stream: stream, subject: subject} do
      snapshot = %SnapshotData{
        source_uuid: stream,
        source_version: 1,
        source_type: to_string(Account),
        snapshot_version: 1,
        data: account(subject, %{provider_uid: "uid"}),
        metadata: %{"snapshot_module_version" => 1}
      }

      :ok = EventStore.record_snapshot(App, snapshot)

      assert populate(stream) == {:error, :key_unknown}
    end

    test "a cold read and a dispatch share the answer", %{stream: stream, subject: subject} do
      stop_supervised!(App)

      start_supervised!(
        {App, snapshotting: %{Account => [snapshot_every: 1, snapshot_version: 1]}}
      )

      dispatch!(stream, %Linked{user_id: subject, provider_uid: "uid"})

      Wait.until(fn -> assert {:ok, _snapshot} = stored_snapshot(stream) end)
      :ok = Aggregate.shutdown(App, Account, stream)

      Keys.set(subject, :unknown)

      assert App.dispatch(%Decree.PersonalFields.Fixtures.Commands.Emit{
               command_id: UUID.uuid4(),
               account_id: stream
             }) ==
               {:error, :key_unknown}

      assert {{:shutdown, :key_unknown}, {GenServer, :call, _call}} =
               catch_exit(Aggregate.aggregate_state(App, Account, stream))
    end
  end

  describe "a scalar" do
    defp with_email(subject), do: %Account{account(subject, []) | email: email()}

    defp stored_email(stream) do
      assert {:ok, %SnapshotData{data: %Account{email: stored}}} = stored_snapshot(stream)

      Map.new(stored, fn {key, value} -> {Atom.to_string(key), value} end)
    end

    defp record(stream, state) do
      :ok =
        EventStore.record_snapshot(App, %SnapshotData{
          source_uuid: stream,
          source_version: 1,
          source_type: to_string(Account),
          snapshot_version: 1,
          data: state,
          metadata: %{"snapshot_module_version" => 1}
        })
    end

    test "is stored as an envelope, and no SQL scan finds it", context do
      %{conn: conn, stream: stream, subject: subject} = context
      state = with_email(subject)

      assert {:ok, _snapshotting} = take(stream, state)

      assert state == with_email(subject)
      assert [row] = rows(conn, "versioned_snapshots", "data")
      assert row =~ "$pf"
      refute row =~ "secret.example"
    end

    test "round trips", %{stream: stream, subject: subject} do
      assert {:ok, _snapshotting} = take(stream, with_email(subject))

      assert %Aggregate{aggregate_state: %Account{email: "alice@secret.example"}} =
               populate(stream)
    end

    test "names its own path in the additional data", %{stream: stream, subject: subject} do
      assert {:ok, _snapshotting} = take(stream, with_email(subject))

      {_key_id, key} = Keys.key(subject)
      {:ok, parsed} = PersonalFields.Envelope.parse(stored_email(stream))

      assert PersonalFields.Envelope.decrypt(parsed, key, {stream, "snapshot", "email"}) ==
               {:ok, "alice@secret.example"}
    end

    test "nil needs no lookup", %{stream: stream, subject: subject} do
      Keys.set(subject, :unknown)

      assert {:ok, _snapshotting} = take(stream, account(subject, []))
      assert Keys.lookups(subject) == 0
    end

    test "a value that is not a string keeps its type", %{stream: stream, subject: subject} do
      assert {:ok, _snapshotting} = take(stream, %Account{with_email(subject) | email: 12})

      assert %Aggregate{aggregate_state: %Account{email: 12}} = populate(stream)
    end

    for {name, mode, reason} <- [
          {"an unknown key", :unknown, :key_unknown},
          {"an erased subject", :erased, :subject_erased}
        ] do
      test "with #{name} records no snapshot", %{stream: stream, subject: subject} do
        Keys.set(subject, unquote(mode))

        assert take(stream, with_email(subject)) == {:error, unquote(reason)}
        assert stored_snapshot(stream) == {:error, :snapshot_not_found}
      end
    end

    test "with a nil subject records no snapshot", %{stream: stream} do
      assert take(stream, with_email(nil)) == {:error, :personal_subject_missing}
      assert stored_snapshot(stream) == {:error, :snapshot_not_found}
    end

    test "already an envelope records no snapshot", %{stream: stream, subject: subject} do
      state = %Account{with_email(subject) | email: %{"$pf" => 1}}

      assert take(stream, state) == {:error, :personal_field_is_envelope}
      assert stored_snapshot(stream) == {:error, :snapshot_not_found}
    end

    test "erased on a read drops the snapshot and folds from the first event", context do
      %{stream: stream, subject: subject} = context

      dispatch!(stream, %Registered{user_id: subject, email: email()})

      assert {:ok, _snapshotting} = take(stream, with_email(subject))

      Keys.set(subject, :erased)

      assert %Aggregate{aggregate_version: 1, aggregate_state: %Account{email: nil, seen: 1}} =
               populate(stream)
    end

    test "unknown on a read is the typed error", %{stream: stream, subject: subject} do
      assert {:ok, _snapshotting} = take(stream, with_email(subject))

      Keys.set(subject, :unknown)

      assert populate(stream) == {:error, :key_unknown}
    end

    test "a bare value passes, and says so", %{stream: stream, subject: subject} do
      attach_telemetry([[:decree, :personal_field, :plaintext_read]])
      record(stream, with_email(subject))

      assert %Aggregate{aggregate_state: %Account{email: "alice@secret.example"}} =
               populate(stream)

      assert [{:telemetry, _event, %{}, %{event_type: "snapshot", field: "email"}}] =
               flush_telemetry()
    end

    test "a malformed value is unknown", %{stream: stream, subject: subject} do
      record(stream, %Account{with_email(subject) | email: %{"$pf" => 2}})

      assert populate(stream) == {:error, :key_unknown}
    end

    test "can't move to a list path", %{stream: stream, subject: subject} do
      assert {:ok, _snapshotting} = take(stream, with_email(subject))
      envelope = stored_email(stream)

      record(stream, account(subject, [%{provider: "p", provider_uid: envelope}]))

      assert populate(stream) == {:error, :key_unknown}
    end

    test "a list value can't move to the scalar", %{stream: stream, subject: subject} do
      assert {:ok, _snapshotting} = take(stream, account(subject))

      assert {:ok, %SnapshotData{data: %Account{links: [%{provider_uid: stored}]}}} =
               stored_snapshot(stream)

      envelope = Map.new(stored, fn {key, value} -> {Atom.to_string(key), value} end)

      record(stream, %Account{account(subject, []) | email: envelope})

      assert populate(stream) == {:error, :key_unknown}
    end

    test "can't move into an event", %{stream: stream, subject: subject} do
      assert {:ok, _snapshotting} = take(stream, with_email(subject))

      store_raw!(stream, %Registered{user_id: subject, email: stored_email(stream)})

      assert stream |> read() |> hd() |> decrypt() == {:error, :key_unknown}
    end

    test "a non-list value under an entry key stays invalid", context do
      %{stream: stream, subject: subject} = context

      assert take(stream, account(subject, "a scalar")) == {:error, :invalid_personal_state}
    end

    test "a field can't be `:self` and list an entry key" do
      assert_raise CompileError, ~r/can't list an entry key too/, fn ->
        Code.compile_string("""
        defmodule Decree.PersonalFields.SnapshotTest.Both do
          use Decree.PersonalFields.Aggregate

          @personal_state [email: :self, email: :provider_uid]
          @personal_state_subject :id

          defstruct [:id, :email]
        end
        """)
      end
    end
  end

  describe "a snapshot value" do
    test "can't move into an event", %{stream: stream, subject: subject} do
      assert {:ok, _snapshotting} = take(stream, account(subject))

      assert {:ok, %SnapshotData{data: %Account{links: [%{provider_uid: stored}]}}} =
               stored_snapshot(stream)

      envelope = Map.new(stored, fn {key, value} -> {Atom.to_string(key), value} end)

      store_raw!(stream, %Linked{user_id: subject, provider: "p", provider_uid: envelope})

      assert stream |> read() |> hd() |> decrypt() == {:error, :key_unknown}
    end

    test "an event value can't move into a snapshot", %{stream: stream, subject: subject} do
      [%{data: %{provider_uid: envelope}}] =
        prepare!(stream, %Linked{user_id: subject, provider: "p", provider_uid: "uid"})

      snapshot = %SnapshotData{
        source_uuid: stream,
        source_version: 1,
        source_type: to_string(Account),
        snapshot_version: 1,
        data: account(subject, [%{provider: "p", provider_uid: envelope}]),
        metadata: %{"snapshot_module_version" => 1}
      }

      :ok = EventStore.record_snapshot(App, snapshot)

      assert populate(stream) == {:error, :key_unknown}
    end
  end
end
