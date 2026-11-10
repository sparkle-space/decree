defmodule Decree.PersonalFields.RetiredDeclarationTest do
  use ExUnit.Case, async: true

  alias Decree.Event.PersonalFields
  alias Decree.PersonalFields.Declaration
  alias Decree.PersonalFields.Fixtures.Events.{EmailRetired, MixedRetired}

  defp compile_event(fields, options) do
    module =
      :"Decree.PersonalFields.RetiredDeclarationTest.E#{System.unique_integer([:positive])}"

    Code.compile_string("""
    defmodule #{inspect(module)} do
      @derive {Decree.Event.PersonalFields, #{options}}
      defstruct #{inspect(fields)}
    end
    """)

    module
  end

  test "both retired lists reach the declaration" do
    assert PersonalFields.declaration(%EmailRetired{}) == %Declaration{
             fields: [],
             metadata: [],
             retired_fields: [:email],
             retired_metadata: [:email]
           }

    assert PersonalFields.declaration(%MixedRetired{}) == %Declaration{
             fields: [ip_address: :user_id],
             metadata: [],
             retired_fields: [:email],
             retired_metadata: [:email]
           }
  end

  test "a retired-only derive answers a declaration" do
    module = compile_event([:a], "personal_retired: [:a]")

    assert %Declaration{fields: [], metadata: [], retired_fields: [:a], retired_metadata: []} =
             PersonalFields.declaration(struct(module))
  end

  test "a retired metadata entry need not be a field of the struct" do
    module = compile_event([:a], "personal_metadata_retired: [:password_hash]")

    assert %Declaration{retired_fields: [], retired_metadata: [:password_hash]} =
             PersonalFields.declaration(struct(module))
  end

  test "a name may be retired as a field and as a metadata entry, and declared in the other" do
    module =
      compile_event(
        [:a, :email],
        "personal: [email: :a], personal_metadata_retired: [:email]"
      )

    assert %Declaration{fields: [email: :a], retired_metadata: [:email]} =
             PersonalFields.declaration(struct(module))
  end

  for {name, fields, options, message} <- [
        {"every list empty, with a retired option", [:a],
         "personal: [], personal_retired: [], personal_metadata_retired: []", "are all empty"},
        {"a field both declared and retired", [:a, :b],
         "personal: [a: :b], personal_retired: [:a]",
         "the field :a is both declared personal and retired"},
        {"a metadata entry both declared and retired", [:b],
         "personal_metadata: [email: :b], personal_metadata_retired: [:email]",
         "the metadata entry :email is both declared personal and retired"},
        {"a retired field not in the struct", [:a], "personal_retired: [:typo]",
         "the retired field :typo is not in the struct"},
        {"a retired list that is a keyword list", [:a, :b], "personal_retired: [a: :b]",
         "must be a list of names"},
        {"a retired list that is not a list", [:a], "personal_retired: :a",
         "must be a list of names"},
        {"a retired metadata list of strings", [:a], ~s(personal_metadata_retired: ["email"]),
         "must be a list of names"},
        {"a repeated retired field", [:a], "personal_retired: [:a, :a]", "repeats"},
        {"a repeated retired metadata entry", [:a], "personal_metadata_retired: [:b, :b]",
         "repeats"},
        {"a field subject that is retired", [:a, :b], "personal: [a: :b], personal_retired: [:b]",
         "the subject field :b is itself retired"},
        {"a metadata subject that is retired", [:b],
         "personal_metadata: [email: :b], personal_retired: [:b]",
         "the subject field :b is itself retired"},
        {"a `:when_present` subject that is retired", [:a, :b],
         "personal: [a: {:when_present, :b}], personal_retired: [:b]",
         "the subject field :b is itself retired"},
        {"a `:first_present` subject that is retired", [:a, :b, :c],
         "personal: [a: {:first_present, [:b, :c]}], personal_retired: [:c]",
         "the subject field :c is itself retired"}
      ] do
    test "the derive refuses #{name}" do
      assert_raise CompileError, ~r/#{unquote(message)}/, fn ->
        compile_event(unquote(fields), unquote(options))
      end
    end
  end
end

defmodule Decree.PersonalFields.RetiredTest do
  use Decree.PersonalFields.Case

  import ExUnit.CaptureLog

  alias Decree.Aggregates.Aggregate
  alias Decree.PersonalFields.Fixtures.{Account, FencedApp, RetiredHandler}
  alias Decree.PersonalFields.Fixtures.Commands.Emit

  alias Decree.PersonalFields.Fixtures.Events.{
    EmailRetired,
    MixedRetired,
    RetiredBeforeRename,
    RetiredRenamed
  }

  @handler_name inspect(RetiredHandler)
  @plaintext_read [:decree, :personal_field, :plaintext_read]

  setup do
    start_supervised!(App)

    :ok
  end

  defp stored(conn) do
    %{rows: rows} = Postgrex.query!(conn, "SELECT data, metadata FROM events", [])

    Enum.map(rows, fn [data, metadata] -> {Jason.decode!(data), Jason.decode!(metadata)} end)
  end

  defp emit(application \\ App, stream, events, metadata) do
    application.dispatch(
      %Emit{account_id: stream, command_id: UUID.uuid4(), events: List.wrap(events)},
      metadata: metadata
    )
  end

  describe "an old envelope in a retired slot" do
    setup %{stream: stream, subject: subject} do
      body = old_envelope!(stream, EmailRetired, "email", subject, email())
      entry = old_envelope!(stream, EmailRetired, "metadata.email", subject, email())

      store_raw!(stream, %EmailRetired{user_id: subject, email: body, note: "n"}, %{
        "email" => entry,
        "who" => "alice"
      })

      :ok
    end

    test "decrypts through decrypt_recorded_event/2, body and metadata", %{stream: stream} do
      assert [%RecordedEvent{data: %EmailRetired{email: %{"$pf": 1}}} = recorded] = read(stream)
      assert %{"$pf" => 1} = recorded.metadata["email"]

      assert {:ok, %RecordedEvent{data: data, metadata: metadata}} = decrypt(recorded)
      assert %EmailRetired{email: "alice@secret.example", note: "n"} = data
      assert %{"email" => "alice@secret.example", "who" => "alice"} = metadata
    end

    test "decrypts on delivery to a handler whose head names it", %{conn: conn} do
      assert Map.has_key?(RetiredHandler.__personal_fields__(), EmailRetired)

      start_supervised!({RetiredHandler, start_from: :origin})

      assert_receive {:handled, %EmailRetired{email: "alice@secret.example"}, metadata}
      assert metadata["email"] == "alice@secret.example"
      Wait.until(fn -> assert acked(conn, @handler_name) == 1 end)
    end

    test "decrypts in an aggregate fold that lists it", %{stream: stream} do
      assert %Account{email: "alice@secret.example", seen: 1} =
               Aggregate.aggregate_state(App, Account, stream)
    end

    test "one lookup per subject for body and metadata", %{stream: stream, subject: subject} do
      before = Keys.lookups(subject)

      assert {:ok, _event} = stream |> read() |> hd() |> decrypt()
      assert Keys.lookups(subject) == before + 1
    end
  end

  describe "an old envelope for an erased subject" do
    setup %{stream: stream, subject: subject} do
      body = old_envelope!(stream, EmailRetired, "email", subject, email())
      entry = old_envelope!(stream, EmailRetired, "metadata.email", subject, email())

      store_raw!(stream, %EmailRetired{user_id: subject, email: body}, %{
        "email" => entry,
        "who" => "alice"
      })

      Keys.set(subject, :erased)

      :ok
    end

    test "reads absent through decrypt_recorded_event/2", %{stream: stream} do
      assert {:ok, %RecordedEvent{data: %EmailRetired{email: nil}, metadata: metadata}} =
               stream |> read() |> hd() |> decrypt()

      assert metadata == %{"who" => "alice"}
    end

    test "reads absent in the fold", %{stream: stream} do
      assert %Account{email: nil, seen: 1} = Aggregate.aggregate_state(App, Account, stream)
    end

    test "reads absent on delivery, and is acked", %{conn: conn} do
      start_supervised!({RetiredHandler, start_from: :origin})

      assert_receive {:handled, %EmailRetired{email: nil}, metadata}
      refute Map.has_key?(metadata, "email")
      Wait.until(fn -> assert acked(conn, @handler_name) == 1 end)
    end
  end

  describe "an old envelope under an unknown key" do
    setup %{stream: stream, subject: subject} do
      body = old_envelope!(stream, EmailRetired, "email", subject, email())
      entry = old_envelope!(stream, EmailRetired, "metadata.email", subject, email())

      store_raw!(stream, %EmailRetired{user_id: subject, email: body}, %{"email" => entry})
      Keys.set(subject, :unknown)

      :ok
    end

    test "fails decrypt_recorded_event/2", %{stream: stream} do
      assert stream |> read() |> hd() |> decrypt() == {:error, :key_unknown}
    end

    test "fails on metadata alone", %{stream: stream, subject: subject} do
      other = UUID.uuid4()
      entry = old_envelope!(other, EmailRetired, "metadata.email", subject, email())

      store_raw!(other, %EmailRetired{user_id: subject}, %{"email" => entry})

      assert other |> read() |> hd() |> decrypt() == {:error, :key_unknown}
      assert stream |> read() |> hd() |> decrypt() == {:error, :key_unknown}
    end

    test "holds the delivery with nothing acked", %{conn: conn} do
      Process.flag(:trap_exit, true)

      {:ok, handler} = RetiredHandler.start_link(start_from: :origin)

      assert_receive {:EXIT, ^handler, :key_unknown}
      assert acked(conn, @handler_name) == 0
      refute_received {:handled, _event, _metadata}
    end

    test "stops the fold, which recovers with the key", %{stream: stream, subject: subject} do
      capture_log(fn ->
        assert emit(stream, [], %{}) == {:error, :key_unknown}
      end)

      Keys.set(subject, :key)

      assert :ok = emit(stream, [], %{})

      assert %Account{email: "alice@secret.example"} =
               Aggregate.aggregate_state(App, Account, stream)
    end
  end

  describe "a bare value in a retired slot" do
    test "is stored as given, reads unchanged, with no lookup and no telemetry", context do
      %{conn: conn, stream: stream, subject: subject} = context
      attach_telemetry([@plaintext_read])

      dispatch!(stream, %EmailRetired{user_id: subject, email: email()},
        metadata: %{"email" => email("meta")}
      )

      assert [{data, metadata}] = stored(conn)
      assert data["email"] == "alice@secret.example"
      assert metadata["email"] == "meta@secret.example"

      assert [%RecordedEvent{data: %EmailRetired{email: "alice@secret.example"}} = recorded] =
               read(stream)

      assert {:ok, %RecordedEvent{data: data, metadata: metadata}} = decrypt(recorded)
      assert %EmailRetired{email: "alice@secret.example"} = data
      assert metadata["email"] == "meta@secret.example"

      :ok = Aggregate.shutdown(App, Account, stream)

      assert %Account{email: "alice@secret.example"} =
               Aggregate.aggregate_state(App, Account, stream)

      start_supervised!({RetiredHandler, start_from: :origin})

      assert_receive {:handled, %EmailRetired{email: "alice@secret.example"}, metadata}
      assert metadata["email"] == "meta@secret.example"
      Wait.until(fn -> assert acked(conn, @handler_name) == 1 end)

      assert flush_telemetry() == []
      assert Keys.lookups(subject) == 0
    end
  end

  describe "an envelope appended into a retired slot" do
    setup %{stream: stream, subject: subject} do
      [envelope: old_envelope!(stream, EmailRetired, "email", subject, email())]
    end

    test "is refused in a field", %{conn: conn, stream: stream, subject: subject} = context do
      event = %EmailRetired{user_id: subject, email: context.envelope}

      assert emit(stream, event, %{}) == {:error, :personal_field_is_envelope}
      assert stored(conn) == []
    end

    test "is refused in metadata, in either key form", context do
      %{conn: conn, stream: stream, subject: subject, envelope: envelope} = context

      for metadata <- [%{email: envelope}, %{"email" => envelope}] do
        assert emit(stream, %EmailRetired{user_id: subject}, metadata) ==
                 {:error, :personal_field_is_envelope}
      end

      assert stored(conn) == []
    end

    test "is refused when only shaped as one", %{conn: conn, stream: stream, subject: subject} do
      assert emit(stream, %EmailRetired{user_id: subject, email: %{"$pf" => 9}}, %{}) ==
               {:error, :personal_field_is_envelope}

      assert stored(conn) == []
    end

    test "is refused beside a declared field, before any lookup", context do
      %{conn: conn, stream: stream, subject: subject, envelope: envelope} = context
      event = %MixedRetired{user_id: subject, email: envelope, ip_address: "192.0.2.1"}

      assert emit(stream, event, %{}) == {:error, :personal_field_is_envelope}
      assert stored(conn) == []
      assert Keys.lookups(subject) == 0
    end
  end

  describe "the metadata fence" do
    setup do
      start_supervised!(FencedApp)

      :ok
    end

    test "refuses a fenced key the event only retires", context do
      %{conn: conn, stream: stream, subject: subject} = context

      for metadata <- [%{"email" => email()}, %{email: email()}] do
        assert emit(FencedApp, stream, %EmailRetired{user_id: subject}, metadata) ==
                 {:error, :undeclared_personal_metadata}
      end

      assert stored(conn) == []
    end

    test "allows the event without that key", %{stream: stream, subject: subject} do
      assert emit(FencedApp, stream, %EmailRetired{user_id: subject}, %{"who" => "alice"}) == :ok
    end
  end

  describe "a mixed event" do
    test "for an erased subject, drops the declared value and keeps the retired one", context do
      %{conn: conn, stream: stream, subject: subject} = context
      Keys.set(subject, :erased)

      dispatch!(
        stream,
        %MixedRetired{user_id: subject, email: email(), ip_address: "192.0.2.1"},
        metadata: %{"email" => email("meta")}
      )

      assert [{data, metadata}] = stored(conn)
      assert data["ip_address"] == nil
      assert data["email"] == "alice@secret.example"
      assert metadata["email"] == "meta@secret.example"
    end

    test "with a key, encrypts only the declared value", context do
      %{conn: conn, stream: stream, subject: subject} = context

      dispatch!(stream, %MixedRetired{user_id: subject, email: email(), ip_address: "192.0.2.1"})

      assert [{data, _metadata}] = stored(conn)
      assert %{"$pf" => 1} = data["ip_address"]
      assert data["email"] == "alice@secret.example"

      assert {:ok, %RecordedEvent{data: %MixedRetired{} = event}} =
               stream |> read() |> hd() |> decrypt()

      assert %MixedRetired{email: "alice@secret.example", ip_address: "192.0.2.1"} = event
    end

    test "an old one reads both values, with one lookup", context do
      %{stream: stream, subject: subject} = context

      store_raw!(
        stream,
        %MixedRetired{
          user_id: subject,
          email: old_envelope!(stream, MixedRetired, "email", subject, email()),
          ip_address: old_envelope!(stream, MixedRetired, "ip_address", subject, "192.0.2.1")
        },
        %{"email" => old_envelope!(stream, MixedRetired, "metadata.email", subject, email())}
      )

      before = Keys.lookups(subject)

      assert {:ok, %RecordedEvent{data: data, metadata: metadata}} =
               stream |> read() |> hd() |> decrypt()

      assert %MixedRetired{email: "alice@secret.example", ip_address: "192.0.2.1"} = data
      assert metadata["email"] == "alice@secret.example"
      assert Keys.lookups(subject) == before + 1
    end

    test "an old one for an erased subject reads both absent", context do
      %{conn: conn, stream: stream, subject: subject} = context

      store_raw!(
        stream,
        %MixedRetired{
          user_id: subject,
          email: old_envelope!(stream, MixedRetired, "email", subject, email()),
          ip_address: old_envelope!(stream, MixedRetired, "ip_address", subject, "192.0.2.1")
        },
        %{"email" => old_envelope!(stream, MixedRetired, "metadata.email", subject, email())}
      )

      Keys.set(subject, :erased)

      assert {:ok, %RecordedEvent{data: data, metadata: metadata}} =
               stream |> read() |> hd() |> decrypt()

      assert %MixedRetired{email: nil, ip_address: nil} = data
      refute Map.has_key?(metadata, "email")

      start_supervised!({RetiredHandler, start_from: :origin})

      assert_receive {:handled, %MixedRetired{email: nil, ip_address: nil}, metadata}
      refute Map.has_key?(metadata, "email")
      Wait.until(fn -> assert acked(conn, @handler_name) == 1 end)
    end
  end

  describe "an upcaster to another struct that retires the field" do
    setup %{stream: stream, subject: subject} do
      envelope = old_envelope!(stream, RetiredBeforeRename, "email", subject, email())

      store_raw!(stream, %RetiredBeforeRename{user_id: subject, email: envelope})

      :ok
    end

    test "carries the envelope, which decrypts after upcasting", %{stream: stream} do
      assert [%RecordedEvent{data: %RetiredRenamed{email: %{"$pf": 1}}} = recorded] =
               read(stream)

      assert recorded.event_type == to_string(RetiredBeforeRename)

      assert {:ok, %RecordedEvent{data: %RetiredRenamed{email: "alice@secret.example"}}} =
               decrypt(recorded)
    end

    test "decrypts in the fold", %{stream: stream} do
      assert %Account{email: "alice@secret.example", seen: 1} =
               Aggregate.aggregate_state(App, Account, stream)
    end

    test "decrypts on delivery", %{conn: conn} do
      start_supervised!({RetiredHandler, start_from: :origin})

      assert_receive {:handled, %RetiredRenamed{email: "alice@secret.example"}, _metadata}
      Wait.until(fn -> assert acked(conn, @handler_name) == 1 end)
    end
  end
end
