defmodule Decree.PersonalFields.AppendTest do
  use Decree.PostgresCase

  import Decree.PersonalFields.Case, only: [attach_telemetry: 1, flush_telemetry: 0]
  import Decree.PersonalFields.Fixtures, only: [email: 0, email: 1]
  import ExUnit.CaptureLog

  alias Decree.Aggregates.ExecutionContext
  alias Decree.Event.Append
  alias Decree.Event.Mapper
  alias Decree.EventStore
  alias Decree.EventStore.EventData
  alias Decree.FrameworkStorage
  alias Decree.PersonalFields
  alias Decree.PersonalFields.Envelope
  alias Decree.PersonalFields.Fixtures.{App, EnvApp, FencedApp, Keys}
  alias Decree.Aggregates.Supervisor, as: AggregateSupervisor
  alias Decree.PersonalFields.Fixtures.Commands.{Emit, PlainEmit}
  alias Decree.PersonalFields.Fixtures.PlainLedger

  alias Decree.PersonalFields.Fixtures.Events.{
    Invited,
    MetadataOnly,
    Plain,
    PreAuthFailed,
    RaisingPlain,
    Registered,
    Scored,
    Tagged,
    TaggedByMetadata,
    TwoSubjects
  }

  alias Decree.PersonalFields.RedactedError
  alias Decree.Registration
  alias Decree.Store.Config
  alias Decree.UUID

  setup do
    conn = start_supervised!({Postgrex, Config.default_postgrex_opts(FrameworkStorage.config())})

    start_supervised!(Keys)
    Keys.sink()
    start_supervised!(App)
    start_supervised!(FencedApp)
    start_supervised!(EnvApp)

    [conn: conn, subject: Keys.mint(UUID.uuid4())]
  end

  defp event_data(event, metadata \\ %{}) do
    Mapper.map_to_event_data(event, metadata: metadata)
  end

  defp prepare(event_data, opts \\ []) do
    context = %ExecutionContext{command: %Emit{}, metadata: Keyword.get(opts, :metadata, %{})}
    application = Keyword.get(opts, :application, App)

    Append.prepare(
      application,
      Keyword.get(opts, :stream, "stream-1"),
      List.wrap(event_data),
      context
    )
  end

  defp prepare_one(event, metadata \\ %{}, opts \\ []) do
    assert {:ok, [%EventData{} = prepared]} = prepare(event_data(event, metadata), opts)

    prepared
  end

  describe "with nothing declared" do
    test "the event and its metadata come out as they went in", %{subject: subject} do
      event = %Plain{user_id: subject, note: "a note"}
      metadata = %{"email" => "not-personal-here", "who" => "alice"}

      prepared = prepare_one(event, metadata)

      assert prepared.data === event
      assert prepared.metadata === metadata
      assert Keys.lookups(subject) == 0
    end

    test "the stored JSON and metadata are byte-identical", %{conn: conn, subject: subject} do
      stream = UUID.uuid4()
      event = %Plain{user_id: subject, note: "a note"}

      :ok =
        App.dispatch(%Emit{command_id: UUID.uuid4(), account_id: stream, events: [event]},
          metadata: %{"who" => "alice"}
        )

      %{rows: [[data, metadata]]} = Postgrex.query!(conn, "SELECT data, metadata FROM events", [])

      assert data == Jason.encode!(event)
      assert metadata == Jason.encode!(%{"who" => "alice"})
    end

    test "a raise from a tag implementation stays as it is", %{subject: subject} do
      assert_raise ArgumentError, "plain raise", fn ->
        prepare(event_data(%RaisingPlain{user_id: subject}))
      end
    end
  end

  describe "the three subject forms" do
    test "a field's subject is that field", %{subject: subject} do
      prepared = prepare_one(%Registered{user_id: subject, email: email()})

      assert %{"s" => ^subject, "$pf" => 1} = prepared.data.email
    end

    test "a nil subject fails the append, whatever the value is", %{subject: subject} do
      assert {:error, :personal_subject_missing} =
               prepare(event_data(%Registered{user_id: nil, email: email()}))

      assert {:error, :personal_subject_missing} =
               prepare(event_data(%Registered{user_id: nil, email: nil}))

      assert {:error, :personal_subject_missing} =
               prepare(event_data(%MetadataOnly{user_id: nil}))

      assert Keys.lookups(subject) == 0
    end

    test "`:first_present` takes the first non-nil id", %{subject: subject} do
      invitation_id = Keys.mint(UUID.uuid4())

      prepared =
        prepare_one(%Invited{
          target_user_id: subject,
          invitation_id: invitation_id,
          email: email()
        })

      assert %{"s" => ^subject} = prepared.data.email

      prepared =
        prepare_one(%Invited{target_user_id: nil, invitation_id: invitation_id, email: email()})

      assert %{"s" => ^invitation_id} = prepared.data.email
      assert Keys.lookups(subject) == 1
    end

    test "`:first_present` with all nil fails the append" do
      assert {:error, :personal_subject_missing} =
               prepare(event_data(%Invited{email: email()}))

      assert {:error, :personal_subject_missing} = prepare(event_data(%Invited{}))
    end

    test "`:when_present` with a nil subject leaves the value in the clear" do
      event = %PreAuthFailed{user_id: nil, email: email(), ip_address: "192.0.2.1"}

      assert prepare_one(event).data === event
    end

    test "`:when_present` with a subject encrypts", %{subject: subject} do
      prepared =
        prepare_one(%PreAuthFailed{user_id: subject, email: email(), ip_address: "192.0.2.1"})

      assert Envelope.envelope?(prepared.data.email)
      assert Envelope.envelope?(prepared.data.ip_address)
    end
  end

  describe "the values" do
    test "nil stays nil, with no lookup", %{subject: subject} do
      event = %Registered{user_id: subject, email: nil, note: "a note"}

      assert prepare_one(event).data === event
      assert Keys.lookups(subject) == 0
    end

    test "a value that is not a string keeps its type", %{subject: subject} do
      for score <- [12, 1.5, true, ["a"], %{"a" => 1}] do
        prepared = prepare_one(%Scored{user_id: subject, score: score})

        assert {:ok, %{data: %Scored{score: ^score}}} =
                 recorded(prepared, "stream-1") |> decrypt()
      end
    end

    test "a value with no JSON form refuses, and carries nothing", %{subject: subject} do
      assert prepare(event_data(%Scored{user_id: subject, score: {:a, "tuple"}})) ==
               {:error, :personal_field_not_encodable}
    end

    test "metadata keeps its key form", %{subject: subject} do
      atom_keyed = prepare_one(%MetadataOnly{user_id: subject}, %{password_hash: "hash"})
      string_keyed = prepare_one(%MetadataOnly{user_id: subject}, %{"password_hash" => "hash"})

      assert %{"$pf" => 1} = atom_keyed.metadata.password_hash
      refute Map.has_key?(atom_keyed.metadata, "password_hash")

      assert %{"$pf" => 1} = string_keyed.metadata["password_hash"]
      refute Map.has_key?(string_keyed.metadata, :password_hash)
    end

    test "metadata with both key forms encrypts both", %{subject: subject} do
      prepared =
        prepare_one(%MetadataOnly{user_id: subject}, %{
          :password_hash => "atom",
          "password_hash" => "string"
        })

      assert %{"$pf" => 1} = prepared.metadata.password_hash
      assert %{"$pf" => 1} = prepared.metadata["password_hash"]
    end

    test "a metadata entry that is nil or absent stays so, with no lookup", %{subject: subject} do
      assert prepare_one(%MetadataOnly{user_id: subject}, %{"who" => "alice"}).metadata ==
               %{"who" => "alice"}

      assert prepare_one(%MetadataOnly{user_id: subject}, %{"password_hash" => nil}).metadata ==
               %{"password_hash" => nil}

      assert Keys.lookups(subject) == 0
    end

    test "a value that is already an envelope is refused", %{subject: subject} do
      prepared = prepare_one(%Registered{user_id: subject, email: email()})
      envelope = prepared.data.email

      assert {:error, :personal_field_is_envelope} =
               prepare(event_data(%Registered{user_id: subject, email: envelope}))

      assert {:error, :personal_field_is_envelope} =
               prepare(
                 event_data(%MetadataOnly{user_id: subject}, %{"password_hash" => envelope})
               )

      assert {:error, :personal_field_is_envelope} =
               prepare(event_data(%PreAuthFailed{user_id: nil, email: envelope}))

      assert {:error, :personal_field_is_envelope} =
               prepare(event_data(%Registered{user_id: subject, email: %{"$pf" => 9}}))
    end
  end

  describe "the lookup" do
    test "runs once per subject per append", %{subject: subject} do
      assert {:ok, _prepared} =
               prepare([
                 event_data(%Registered{user_id: subject, email: email()}, %{
                   "email" => email(),
                   "password_hash" => "hash"
                 })
               ])

      assert Keys.lookups(subject) == 1
    end

    test "unknown fails the whole append", %{subject: subject} do
      Keys.set(subject, :unknown)

      assert {:error, :key_unknown} =
               prepare([
                 event_data(%Plain{user_id: subject}),
                 event_data(%Registered{user_id: subject, email: email()})
               ])
    end

    for mode <- [:raise, :exit, :throw, :garbage, :empty_id, :other] do
      test "that answers #{mode} fails the append and leaves the aggregate", context do
        %{conn: conn, subject: subject} = context
        stream = UUID.uuid4()

        :ok =
          App.dispatch(%Emit{
            command_id: UUID.uuid4(),
            account_id: stream,
            events: [%Plain{user_id: subject}]
          })

        Keys.set(subject, unquote(mode))

        assert {:error, :key_unknown} =
                 App.dispatch(%Emit{
                   command_id: UUID.uuid4(),
                   account_id: stream,
                   events: [%Registered{user_id: subject, email: email()}]
                 })

        assert %{rows: [[1]]} = Postgrex.query!(conn, "SELECT count(*)::int FROM events", [])
        assert :ok = App.dispatch(%Emit{command_id: UUID.uuid4(), account_id: stream})
      end
    end

    test "with no key source, a declared value fails the append", %{subject: subject} do
      assert {:error, :key_unknown} =
               prepare(event_data(%Registered{user_id: subject, email: email()}),
                 application: EnvApp
               )
    end

    test "erased makes a field nil and removes a metadata key", %{subject: subject} do
      Keys.set(subject, :erased)

      prepared =
        prepare_one(%Registered{user_id: subject, email: email(), note: "a note"}, %{
          "email" => email(),
          "password_hash" => "hash",
          "who" => "alice"
        })

      assert prepared.data === %Registered{user_id: subject, email: nil, note: "a note"}
      assert prepared.metadata == %{"who" => "alice"}
    end

    test "two subjects, one erased: one stored absent, one encrypted", %{subject: subject} do
      member = Keys.mint(UUID.uuid4())
      Keys.set(member, :erased)

      prepared =
        prepare_one(%TwoSubjects{
          owner_id: subject,
          member_id: member,
          owner_email: email("owner"),
          member_email: email("member")
        })

      assert %{"s" => ^subject} = prepared.data.owner_email
      assert prepared.data.member_email == nil
    end

    test "the context's metadata stays plaintext", %{subject: subject} do
      metadata = %{"password_hash" => "hash"}

      context = %ExecutionContext{command: %Emit{}, metadata: metadata}

      assert {:ok, _prepared} =
               Append.prepare(
                 App,
                 "stream-1",
                 [event_data(%MetadataOnly{user_id: subject}, metadata)],
                 context
               )

      assert context.metadata == metadata
    end
  end

  describe "tags" do
    test "see the plaintext", %{subject: subject} do
      prepared = prepare_one(%Tagged{user_id: subject, email: email()})

      digest = Base.encode16(:crypto.hash(:sha256, email()))

      assert ("digest:" <> digest) in prepared.tags
      assert Envelope.envelope?(prepared.data.email)
    end

    test "see plaintext metadata, and it stays out of the stored row", %{subject: subject} do
      prepared = prepare_one(%TaggedByMetadata{user_id: subject}, %{"email" => "plain"})

      assert "seen:plain" in prepared.tags
      assert Envelope.envelope?(prepared.metadata["email"])
    end

    test "an erased subject gets no digest", %{subject: subject} do
      Keys.set(subject, :erased)

      prepared = prepare_one(%Tagged{user_id: subject, email: email()})

      assert Enum.filter(prepared.tags, &String.starts_with?(&1, "digest:")) == []

      prepared = prepare_one(%TaggedByMetadata{user_id: subject}, %{"email" => "plain"})

      assert "no_email:true" in prepared.tags
    end

    test "an unknown key writes no row", %{conn: conn, subject: subject} do
      Keys.set(subject, :unknown)
      stream = UUID.uuid4()

      assert {:error, :key_unknown} =
               App.dispatch(%Emit{
                 command_id: UUID.uuid4(),
                 account_id: stream,
                 events: [%Tagged{user_id: subject, email: email()}]
               })

      assert %{rows: [[0]]} = Postgrex.query!(conn, "SELECT count(*)::int FROM events", [])
      assert %{rows: [[0]]} = Postgrex.query!(conn, "SELECT count(*)::int FROM event_tags", [])
    end

    test "no stored tag holds ciphertext or plaintext", %{conn: conn, subject: subject} do
      stream = UUID.uuid4()

      :ok =
        App.dispatch(%Emit{
          command_id: UUID.uuid4(),
          account_id: stream,
          events: [%Tagged{user_id: subject, email: email()}]
        })

      %{rows: rows} = Postgrex.query!(conn, "SELECT tag FROM event_tags", [])
      tags = List.flatten(rows)

      assert [_digest] = Enum.filter(tags, &String.starts_with?(&1, "digest:"))
      refute Enum.any?(tags, &(&1 =~ "$pf" or &1 =~ "secret.example"))
    end

    test "a raise on a declared event carries nothing", %{subject: subject} do
      event = %Tagged{user_id: subject, email: email(), mode: "raise"}

      assert {error, stacktrace} = raised(fn -> prepare(event_data(event)) end)

      assert %RedactedError{type: MatchError} = error
      refute inspect({error, stacktrace}) =~ "secret.example"

      event = %Tagged{user_id: subject, email: email(), mode: "invalid"}

      assert {%RedactedError{type: CaseClauseError}, _stacktrace} =
               raised(fn -> prepare(event_data(event)) end)
    end
  end

  describe "a tag implementation failing with the value in hand" do
    defp tag_failure(mode, subject) do
      event_data(%Tagged{user_id: subject, email: email(), mode: mode})
    end

    test "an `{:error, reason}` answers reduced, on a declared event", %{subject: subject} do
      assert prepare(tag_failure("error", subject)) == {:error, {:refused, :redacted}}
      assert prepare(tag_failure("bare_error", subject)) == {:error, :redacted}
      assert prepare(tag_failure("atom_error", subject)) == {:error, :refused}
    end

    test "an exit is reduced, on a declared event", %{subject: subject} do
      assert catch_exit(prepare(tag_failure("exit", subject))) == {:gave_up, :redacted}
    end

    test "a throw is reduced, and leaves as an exit, on a declared event", %{subject: subject} do
      assert catch_exit(prepare(tag_failure("throw", subject))) ==
               {:nocatch, {:thrown, :redacted}}
    end

    for mode <- ["error", "exit", "throw"] do
      test "(#{mode}) a real dispatch shows nothing in its logs or its answer", context do
        %{subject: subject} = context
        stream = UUID.uuid4()

        command = %Emit{
          command_id: UUID.uuid4(),
          account_id: stream,
          events: [%Tagged{user_id: subject, email: email(), mode: unquote(mode)}]
        }

        # The debug line printing a command is not this path: commands are not redacted.
        log =
          capture_log([level: :info], fn ->
            answer = App.dispatch(command)

            send(self(), {:answer, answer})
            Process.sleep(50)
          end)

        assert_received {:answer, {:error, _reason} = answer}

        refute inspect(answer, limit: :infinity) =~ "secret.example"
        refute log =~ "secret.example"
      end
    end

    # An aggregate that lists no personal fields lets a throw from `execute_command` leave as it
    # is, so a reduced one shaped as a `handle_call/3` return would be read as the answer: the
    # aggregate would carry on with its state set to `:redacted`, and the caller would wait.
    for {shape, mode} <- [{"{:noreply, secret}", "noreply"}, {"{:reply, :ok, secret}", "reply"}] do
      test "a throw of `#{shape}` stops an aggregate that lists no fields", %{subject: subject} do
        stream = UUID.uuid4()
        {:ok, ^stream} = AggregateSupervisor.open_aggregate(App, PlainLedger, stream)
        pid = Registration.whereis_name(App, {App, PlainLedger, stream})
        ref = Process.monitor(pid)

        attach_telemetry(
          for span <- [[:decree, :aggregate, :execute], [:decree, :application, :dispatch]],
              stage <- [:start, :stop, :exception],
              do: span ++ [stage]
        )

        event = %Tagged{user_id: subject, email: email(), mode: unquote(mode)}
        command = %PlainEmit{account_id: stream, command_id: UUID.uuid4(), events: [event]}

        # The debug line printing a command is not this path: commands are not redacted.
        log =
          capture_log([level: :info], fn ->
            send(self(), {:answer, App.dispatch(command, timeout: 2_000)})
            Process.sleep(50)
          end)

        assert_received {:answer, {:error, :aggregate_execution_failed}}

        assert_receive {:DOWN, ^ref, :process, ^pid, reason}
        assert reason == {:nocatch, {unquote(String.to_atom(mode)), :redacted}}

        refute log =~ "secret.example"

        # The command, and the context holding it, are in a span's metadata by contract: what
        # decree adds about the failure is what is checked.
        telemetry =
          for {:telemetry, event, measurements, metadata} <- flush_telemetry(),
              do: {event, measurements, Map.drop(metadata, [:command, :execution_context])}

        assert telemetry != []

        refute inspect(telemetry, limit: :infinity, printable_limit: :infinity) =~
                 "secret.example"

        # The next command opens a fresh aggregate.
        assert :ok = App.dispatch(%PlainEmit{account_id: stream, command_id: UUID.uuid4()})
        assert Registration.whereis_name(App, {App, PlainLedger, stream}) not in [pid, :undefined]
      end
    end

    test "on an event with no declaration stays exactly as it is" do
      assert prepare(event_data(%RaisingPlain{mode: "error", note: "shown"})) ==
               {:error, {:refused, "shown"}}

      assert catch_exit(prepare(event_data(%RaisingPlain{mode: "exit", note: "shown"}))) ==
               {:gave_up, "shown"}

      assert catch_throw(prepare(event_data(%RaisingPlain{mode: "throw", note: "shown"}))) ==
               {:thrown, "shown"}
    end
  end

  describe "the metadata fence" do
    test "refuses a listed key the event does not declare" do
      for metadata <- [%{email: email()}, %{"email" => email()}, %{"password_hash" => "hash"}] do
        assert {:error, :undeclared_personal_metadata} =
                 prepare(event_data(%Plain{}, metadata), application: FencedApp)
      end
    end

    test "refuses it on a declared event that does not declare that key", %{subject: subject} do
      assert {:error, :undeclared_personal_metadata} =
               prepare(event_data(%MetadataOnly{user_id: subject}, %{"email" => email()}),
                 application: FencedApp
               )
    end

    test "allows a declared key", %{subject: subject} do
      assert {:ok, _prepared} =
               prepare(event_data(%MetadataOnly{user_id: subject}, %{"password_hash" => "hash"}),
                 application: FencedApp
               )
    end

    test "allows a nil value, and a key it does not list" do
      for metadata <- [%{"email" => nil}, %{email: nil}, %{"who" => "alice"}, %{}] do
        assert {:ok, [%EventData{metadata: ^metadata}]} =
                 prepare(event_data(%Plain{}, metadata), application: FencedApp)
      end
    end

    test "an application with a key source and no list has no fence" do
      metadata = %{"email" => email(), "password_hash" => "hash"}

      assert {:ok, [%EventData{metadata: ^metadata}]} = prepare(event_data(%Plain{}, metadata))
    end

    test "checks each event of an append against its own declaration", %{subject: subject} do
      metadata = %{"password_hash" => "hash"}

      assert {:error, :undeclared_personal_metadata} =
               prepare(
                 [
                   event_data(%MetadataOnly{user_id: subject}, metadata),
                   event_data(%Plain{}, metadata)
                 ],
                 application: FencedApp
               )
    end
  end

  defp raised(fun) do
    fun.()
    flunk("expected a raise")
  rescue
    error -> {error, __STACKTRACE__}
  end

  defp recorded(%EventData{data: data, metadata: metadata, event_type: event_type}, stream) do
    %EventStore.RecordedEvent{
      data: data,
      metadata: metadata,
      event_type: event_type,
      stream_id: stream
    }
  end

  defp decrypt(event), do: PersonalFields.decrypt_recorded_event(App, event)
end
