defmodule Decree.Decision.PersonalFieldsTest do
  use Decree.DecisionStoreCase

  import Decree.PersonalFields.Case, only: [flush_telemetry: 0, rows: 3]
  import Decree.PersonalFields.Fixtures, only: [email: 1]
  import ExUnit.CaptureLog

  alias Decree.Aggregates.ExecutionContext
  alias Decree.DecisionFixtures.{Bare, Personal, PersonalApp, PersonalTight}
  alias Decree.Event.Append
  alias Decree.EventStore.{EventData, RecordedEvent}
  alias Decree.FrameworkStorage
  alias Decree.Middleware.Pipeline
  alias Decree.PersonalFields
  alias Decree.PersonalFields.{Envelope, RedactedError}
  alias Decree.PersonalFields.Fixtures.Keys

  alias Decree.PersonalFields.Fixtures.Events.{
    BeforeRename,
    Plain,
    RaisingPlain,
    Registered,
    Renamed,
    StateFails,
    Tagged,
    Unfolded
  }

  alias Decree.Store.{Config, ReadResult}

  @start [:decree, :decision, :execute, :start]
  @stop [:decree, :decision, :execute, :stop]
  @exception [:decree, :decision, :execute, :exception]
  @read [:decree, :decision, :read]
  @retry [:decree, :decision, :retry]
  @refused [:decree, :decision, :refused]

  @every_event [
    @start,
    @stop,
    @exception,
    @read,
    @retry,
    @refused,
    [:decree, :application, :dispatch, :start],
    [:decree, :application, :dispatch, :stop],
    [:decree, :event_store, :read_query, :start],
    [:decree, :event_store, :read_query, :stop],
    [:decree, :event_store, :read_tags, :start],
    [:decree, :event_store, :read_tags, :stop],
    [:decree, :event_store, :append_to_streams, :start],
    [:decree, :event_store, :append_to_streams, :stop]
  ]

  @types Enum.map(
           [BeforeRename, Plain, Registered, Renamed, StateFails, Tagged, Unfolded],
           &Atom.to_string/1
         )

  setup do
    start_supervised!(Keys)
    Keys.sink()
    start_supervised!(PersonalApp)
    attach_telemetry(@every_event)

    scope = scope()

    [scope: scope, tag: "scope:" <> scope, user: Keys.mint(UUID.uuid4())]
  end

  # Writes an event as a command would have, encrypted, to a stream no decision owns.
  defp seed!(event, tag) do
    stream = "other-" <> UUID.uuid4()
    context = %ExecutionContext{command: %Personal{}, metadata: %{}}

    {:ok, prepared} = Append.prepare_events(PersonalApp, stream, [event], context)

    prepared =
      Enum.map(prepared, fn %EventData{} = data -> Map.update!(data, :tags, &[tag | &1]) end)

    :ok = EventStore.append_to_stream(PersonalApp, stream, :any_version, prepared)
  end

  defp reading(tag), do: fn _command, _tag_context -> [[types: @types, tags: [tag]]] end

  defp stream_of(%Personal{claim_id: claim_id}), do: "personal-" <> claim_id
  defp stream_of(%Bare{claim_id: claim_id}), do: "bare-" <> claim_id

  defp stored_of(command), do: stored(stream_of(command), PersonalApp)

  defp decrypted(%RecordedEvent{} = event) do
    PersonalFields.decrypt_recorded_event(PersonalApp, event)
  end

  defp leaks?(term, value) do
    inspect(term, limit: :infinity, printable_limit: :infinity) =~ value
  end

  defp after_failure do
    assert_received {:middleware, :after_failure, %Pipeline{} = pipeline}

    pipeline
  end

  defp connect do
    start_supervised!({Postgrex, Config.default_postgrex_opts(FrameworkStorage.config())})
  end

  describe "what a decision appends" do
    test "is encrypted in the store, with tags taken from the plaintext", context do
      %{tag: tag, user: user} = context
      secret = email("stored-" <> UUID.uuid4())
      decide = fn _state, _command -> %Tagged{user_id: user, email: secret} end
      command = scripted([query: reading(tag), decide: decide], Personal)

      assert :ok = PersonalApp.dispatch(command)

      assert [%RecordedEvent{data: %Tagged{email: envelope}} = event] = stored_of(command)
      assert Envelope.envelope?(envelope)
      refute Enum.any?(rows(connect(), "events", "data, metadata"), &(&1 =~ secret))

      digest = Base.encode16(:crypto.hash(:sha256, secret))
      item = [types: [Atom.to_string(Tagged)], tags: ["digest:" <> digest]]

      assert {:ok, %ReadResult{events: [%RecordedEvent{event_id: event_id}]}} =
               EventStore.read_query(PersonalApp, [item])

      assert event_id == event.event_id
      assert {:ok, %RecordedEvent{data: %Tagged{email: ^secret}}} = decrypted(event)
    end

    test "is encrypted per member, each envelope bound to its own stream", context do
      %{tag: tag, user: user} = context
      other = Keys.mint(UUID.uuid4())
      first = email("first-" <> UUID.uuid4())
      second = email("second-" <> UUID.uuid4())

      decide = fn _state, _command ->
        {:streams,
         [
           {:user, user, %Registered{user_id: user, email: first}},
           {:order, other, %Registered{user_id: other, email: second}}
         ]}
      end

      command = scripted([query: reading(tag), decide: decide], Personal)

      assert :ok = PersonalApp.dispatch(command)

      assert [%RecordedEvent{} = first_event] = stored("personal-" <> user, PersonalApp)
      assert [%RecordedEvent{} = second_event] = stored("personal-" <> other, PersonalApp)

      assert Envelope.envelope?(first_event.data.email)
      assert Envelope.envelope?(second_event.data.email)

      assert {:ok, %RecordedEvent{data: %Registered{email: ^first}}} = decrypted(first_event)
      assert {:ok, %RecordedEvent{data: %Registered{email: ^second}}} = decrypted(second_event)

      moved = %RecordedEvent{first_event | stream_id: "personal-" <> other}

      capture_log(fn -> assert {:error, :key_unknown} = decrypted(moved) end)
    end

    test "keeps metadata it declares encrypted, and reads back", context do
      %{tag: tag, user: user} = context
      secret = email("metadata-" <> UUID.uuid4())
      decide = fn _state, _command -> %Registered{user_id: user} end
      command = scripted([query: reading(tag), decide: decide], Personal)

      assert :ok = PersonalApp.dispatch(command, metadata: %{"email" => secret})

      assert [%RecordedEvent{metadata: %{"email" => envelope}} = event] = stored_of(command)
      assert Envelope.envelope?(envelope)
      assert {:ok, %RecordedEvent{metadata: %{"email" => ^secret}}} = decrypted(event)
    end

    test "drops an erased subject's value before its tag is taken", context do
      %{tag: tag, user: user} = context
      Keys.set(user, :erased)

      decide = fn _state, _command -> %Tagged{user_id: user, email: email("gone")} end
      command = scripted([query: reading(tag), decide: decide], Personal)

      assert :ok = PersonalApp.dispatch(command)
      assert [%RecordedEvent{data: %Tagged{email: nil}}] = stored_of(command)
    end
  end

  describe "an answer from preparing" do
    setup %{tag: tag} do
      test = self()

      deciding = fn answer ->
        fn _state, _command ->
          send(test, :decided)

          answer.()
        end
      end

      [deciding: deciding, query: reading(tag)]
    end

    test "`:key_unknown` stands, once, with nothing appended", context do
      %{deciding: deciding, query: query, user: user} = context

      Keys.set(user, :unknown_once)

      decide = deciding.(fn -> %Registered{user_id: user, email: email("lost")} end)
      command = scripted([query: query, decide: decide], Personal)

      capture_log(fn -> assert {:error, :key_unknown} = PersonalApp.dispatch(command) end)

      assert_received :decided
      refute_received :decided
      refute_received {:telemetry, @retry, _, _}
      assert stored_of(command) == []
    end

    test "`:key_unknown` on one member appends none", context do
      %{deciding: deciding, query: query, user: user} = context
      lost = UUID.uuid4()

      decide =
        deciding.(fn ->
          {:streams,
           [
             {:user, user, %Registered{user_id: user, email: email("kept")}},
             {:user, lost, %Registered{user_id: lost, email: email("lost")}}
           ]}
        end)

      command = scripted([query: query, decide: decide], Personal)

      capture_log(fn -> assert {:error, :key_unknown} = PersonalApp.dispatch(command) end)

      assert_received :decided
      refute_received :decided
      assert stored("personal-" <> user, PersonalApp) == []
      assert stored("personal-" <> lost, PersonalApp) == []
    end

    test "`:personal_subject_missing` stands, once, with nothing appended", context do
      %{deciding: deciding, query: query} = context

      decide = deciding.(fn -> %Registered{email: email("nobody")} end)
      command = scripted([query: query, decide: decide], Personal)

      assert {:error, :personal_subject_missing} = PersonalApp.dispatch(command)

      assert_received :decided
      refute_received :decided
      refute_received {:telemetry, @retry, _, _}
      assert stored_of(command) == []
    end

    test "`:undeclared_personal_metadata` stands, once, with nothing appended", context do
      %{deciding: deciding, query: query} = context

      decide = deciding.(fn -> %Plain{} end)
      command = scripted([query: query, decide: decide], Personal)

      assert {:error, :undeclared_personal_metadata} =
               PersonalApp.dispatch(command, metadata: %{"email" => email("undeclared")})

      assert_received :decided
      refute_received :decided
      refute_received {:telemetry, @retry, _, _}
      assert stored_of(command) == []
    end
  end

  describe "a decision's fold" do
    test "decrypts a listed event, and leaves an unlisted one as stored", context do
      %{tag: tag, user: user} = context
      test = self()
      listed = email("listed-" <> UUID.uuid4())
      unlisted = email("unlisted-" <> UUID.uuid4())

      seed!(%Registered{user_id: user, email: listed}, tag)
      seed!(%Unfolded{user_id: user, email: unlisted}, tag)

      followup = fn _command, _tag_context, events ->
        send(test, {:followup, events})

        []
      end

      decide = fn state, _command ->
        send(test, {:folded, state})

        {:error, :seen}
      end

      command = scripted([query: reading(tag), followup: followup, decide: decide], Personal)

      assert {:error, :seen} = PersonalApp.dispatch(command)

      assert_received {:followup,
                       [%Registered{email: ^listed}, %Unfolded{email: followup_envelope}]}

      assert_received {:folded, [%Registered{email: ^listed}, %Unfolded{email: folded_envelope}]}

      assert Envelope.envelope?(followup_envelope)
      assert Envelope.envelope?(folded_envelope)
    end

    test "decrypts a listed event only the second read holds", context do
      %{tag: tag, user: user} = context
      test = self()
      secret = email("second-read-" <> UUID.uuid4())
      later = "later-" <> tag

      seed!(%Registered{user_id: user, email: secret}, later)

      decide = fn state, _command ->
        send(test, {:folded, state})

        {:error, :seen}
      end

      followup = fn _command, _tag_context, [] -> [[types: @types, tags: [later]]] end
      command = scripted([query: reading(tag), followup: followup, decide: decide], Personal)

      assert {:error, :seen} = PersonalApp.dispatch(command)
      assert_received {:folded, [%Registered{email: ^secret}]}
    end

    test "reads an upcast event at the place it was stored", context do
      %{tag: tag, user: user} = context
      test = self()
      secret = email("upcast-" <> UUID.uuid4())

      seed!(%BeforeRename{user_id: user, email: secret}, tag)

      decide = fn state, _command ->
        send(test, {:folded, state})

        {:error, :seen}
      end

      command = scripted([query: reading(tag), decide: decide], Personal)

      assert {:error, :seen} = PersonalApp.dispatch(command)
      assert_received {:folded, [%Renamed{email: ^secret}]}
    end

    test "gives an erased subject's field as nil", context do
      %{tag: tag, user: user} = context
      test = self()

      seed!(%Registered{user_id: user, email: email("erased"), note: "kept"}, tag)
      Keys.set(user, :erased)

      decide = fn state, _command ->
        send(test, {:folded, state})

        {:error, :seen}
      end

      command = scripted([query: reading(tag), decide: decide], Personal)

      assert {:error, :seen} = PersonalApp.dispatch(command)
      assert_received {:folded, [%Registered{email: nil, note: "kept"}]}
    end

    test "leaves everything as stored on a decision that lists nothing", context do
      %{tag: tag, user: user} = context
      test = self()

      seed!(%Registered{user_id: user, email: email("bare")}, tag)

      lookups = Keys.lookups(user)

      decide = fn state, _command ->
        send(test, {:folded, state})

        {:error, :seen}
      end

      command = scripted([query: reading(tag), decide: decide], Bare)

      assert {:error, :seen} = PersonalApp.dispatch(command)

      assert_received {:folded, [%Registered{email: envelope}]}
      assert Envelope.envelope?(envelope)
      assert Keys.lookups(user) == lookups
    end

    test "looks a subject up once in an attempt, across both reads", context do
      %{tag: tag, user: user} = context
      later = "later-" <> tag

      seed!(%Registered{user_id: user, email: email("first")}, tag)
      seed!(%Registered{user_id: user, email: email("second")}, tag)
      seed!(%Registered{user_id: user, email: email("third")}, later)

      lookups = Keys.lookups(user)

      followup = fn _command, _tag_context, _events -> [[types: @types, tags: [later]]] end
      decide = fn _state, _command -> {:error, :seen} end
      command = scripted([query: reading(tag), followup: followup, decide: decide], Personal)

      assert {:error, :seen} = PersonalApp.dispatch(command)
      assert Keys.lookups(user) == lookups + 1
    end
  end

  describe "`:key_unknown` on the fold" do
    setup %{tag: tag, user: user} do
      test = self()

      seed!(%Registered{user_id: user, email: email("unreadable")}, tag)

      Keys.set(user, :unknown_once)

      decide = fn _state, _command ->
        send(test, :decided)

        %Plain{}
      end

      [decide: decide, lookups: Keys.lookups(user)]
    end

    test "is answered once, with no retry and nothing appended", context do
      %{decide: decide, lookups: lookups, tag: tag, user: user} = context
      command = scripted([query: reading(tag), decide: decide], Personal)

      capture_log(fn -> assert {:error, :key_unknown} = PersonalApp.dispatch(command) end)

      assert Keys.lookups(user) == lookups + 1
      refute_received :decided
      refute_received {:telemetry, @retry, _, _}
      assert stored_of(command) == []

      assert_received {:telemetry, @stop, _, %{attempts: 1, error: :key_unknown}}
    end

    test "is answered from the second read's events too", context do
      %{decide: decide, lookups: lookups, tag: tag, user: user} = context
      test_tag = "unmatched-" <> tag

      followup = fn _command, _tag_context, [] -> [[types: @types, tags: [tag]]] end

      command =
        scripted([query: reading(test_tag), followup: followup, decide: decide], Personal)

      capture_log(fn -> assert {:error, :key_unknown} = PersonalApp.dispatch(command) end)

      assert Keys.lookups(user) == lookups + 1
      refute_received :decided
      refute_received {:telemetry, @retry, _, _}
      assert stored_of(command) == []
    end
  end

  describe "a retry after a condition failure" do
    test "derives from plaintext metadata and looks keys up afresh", context do
      %{tag: tag, user: user} = context
      test = self()
      writer = Keys.mint(UUID.uuid4())
      secret = email("metadata-" <> UUID.uuid4())
      folded = email("folded-" <> UUID.uuid4())

      seed!(%Registered{user_id: user, email: folded}, tag)

      decide = fn state, _command ->
        hold(test, {:deciding, state})

        %Registered{user_id: writer}
      end

      command = scripted([query: reading(tag), decide: decide], Personal)
      lookups = Keys.lookups(writer)

      task =
        Task.async(fn ->
          PersonalApp.dispatch(command, metadata: %{"email" => secret}, timeout: 60_000)
        end)

      assert_receive {{:deciding, [%Registered{email: ^folded}]}, pid}

      Keys.set(user, :erased)
      seed!(%Plain{note: "rival"}, tag)
      send(pid, :go)

      assert_receive {{:deciding, [%Registered{email: nil}, %Plain{note: "rival"}]}, ^pid}
      send(pid, :go)

      assert :ok = Task.await(task, :infinity)
      assert Keys.lookups(writer) == lookups + 2

      assert [%RecordedEvent{metadata: %{"email" => envelope}} = event] = stored_of(command)
      assert Envelope.envelope?(envelope)
      assert {:ok, %RecordedEvent{metadata: %{"email" => ^secret}}} = decrypted(event)
    end

    test "sees a key erased since, in what it appends", context do
      %{tag: tag, user: user} = context
      test = self()
      secret = email("written-" <> UUID.uuid4())

      decide = fn state, _command ->
        hold(test, {:deciding, state})

        %Tagged{user_id: user, email: secret}
      end

      command = scripted([query: reading(tag), decide: decide], Personal)

      task = Task.async(fn -> PersonalApp.dispatch(command, timeout: 60_000) end)

      assert_receive {{:deciding, []}, pid}

      seed!(%Plain{note: "rival"}, tag)
      Keys.set(user, :erased)
      send(pid, :go)

      assert_receive {{:deciding, [%Plain{note: "rival"}]}, ^pid}
      send(pid, :go)

      assert :ok = Task.await(task, :infinity)

      assert [%RecordedEvent{data: %Tagged{email: nil}}] = stored_of(command)

      digest = Base.encode16(:crypto.hash(:sha256, secret))
      item = [types: [Atom.to_string(Tagged)], tags: ["digest:" <> digest]]

      assert {:ok, %ReadResult{events: []}} = EventStore.read_query(PersonalApp, [item])
    end
  end

  describe "a raise on a decision that lists its fold" do
    setup %{tag: tag, user: user} do
      secret = email("decrypted-" <> UUID.uuid4())

      seed!(%Registered{user_id: user, email: secret}, tag)

      [secret: secret]
    end

    defp assert_redacted(command, type, secret) do
      log =
        capture_log(fn ->
          assert {:error, %RedactedError{type: ^type} = error} = PersonalApp.dispatch(command)

          refute leaks?(error, secret)
        end)

      assert log =~ inspect(type)
      refute log =~ secret

      assert_received {:telemetry, @exception, _,
                       %{kind: :error, reason: %RedactedError{}, stacktrace: stacktrace}}

      assert Enum.all?(stacktrace, fn frame -> is_integer(elem(frame, tuple_size(frame) - 2)) end)

      pipeline = after_failure()

      assert {:error, %RedactedError{}} = pipeline.response
      assert [_ | _] = pipeline.assigns.error_reason
      refute leaks?(pipeline, secret)

      refute leaks?(flush_telemetry(), secret)
    end

    test "from `evolve/2` holds no decrypted value", %{secret: secret, tag: tag} do
      seed!(%StateFails{mode: "match"}, tag)

      assert_redacted(scripted([query: reading(tag)], Personal), MatchError, secret)
    end

    test "from `evolve/2` holds none in a stack frame's arguments", context do
      %{secret: secret, tag: tag} = context

      seed!(%StateFails{mode: "clause"}, tag)

      assert_redacted(scripted([query: reading(tag)], Personal), FunctionClauseError, secret)
    end

    test "from `decide/2` holds no decrypted value", %{secret: secret, tag: tag} do
      decide = fn state, _command -> :never = state end

      assert_redacted(
        scripted([query: reading(tag), decide: decide], Personal),
        MatchError,
        secret
      )
    end

    test "from `decide/2` holds none in its message", %{secret: secret, tag: tag} do
      decide = fn state, _command -> raise "state: #{inspect(state)}" end

      assert_redacted(
        scripted([query: reading(tag), decide: decide], Personal),
        RuntimeError,
        secret
      )
    end

    test "from `decide/2` holds none in a stack frame's arguments", context do
      %{secret: secret, tag: tag} = context
      decide = fn [], _command -> :never end

      assert_redacted(
        scripted([query: reading(tag), decide: decide], Personal),
        FunctionClauseError,
        secret
      )
    end

    test "from `admit/2` holds no decrypted value", %{secret: secret, tag: tag} do
      admit = fn state, _command -> :never = state end

      assert_redacted(scripted([query: reading(tag), admit: admit], Personal), MatchError, secret)
    end

    test "from `followup_query/3` holds no decrypted value", %{secret: secret, tag: tag} do
      followup = fn _command, _tag_context, events -> :never = events end

      assert_redacted(
        scripted([query: reading(tag), followup: followup], Personal),
        MatchError,
        secret
      )
    end

    test "from `query/2` holds nothing of its arguments" do
      planted = "planted-" <> UUID.uuid4()
      query = fn _command, tag_context -> :never = tag_context end
      command = scripted([query: query], Personal)

      log =
        capture_log(fn ->
          assert {:error, %RedactedError{type: MatchError} = error} =
                   PersonalApp.dispatch(command, metadata: %{"actor" => planted})

          refute leaks?(error, planted)
        end)

      refute log =~ planted
    end

    test "from an undeclared event's `Tags`, in prepare, is redacted", %{tag: tag} do
      decide = fn _state, _command -> %RaisingPlain{} end
      command = scripted([query: reading(tag), decide: decide], Personal)

      capture_log(fn ->
        assert {:error, %RedactedError{type: ArgumentError}} = PersonalApp.dispatch(command)
      end)

      assert stored_of(command) == []
    end

    test "from a declared event's `Tags`, in prepare, holds no value", context do
      %{secret: secret, tag: tag, user: user} = context
      decide = fn _state, _command -> %Tagged{user_id: user, email: secret, mode: "raise"} end
      command = scripted([query: reading(tag), decide: decide], Personal)

      log =
        capture_log(fn ->
          assert {:error, %RedactedError{} = error} = PersonalApp.dispatch(command)

          refute leaks?(error, secret)
        end)

      refute log =~ secret
      assert stored_of(command) == []
    end
  end

  describe "a raise on a decision that doesn't list its fold" do
    test "keeps its exception, in `evolve/2`", %{tag: tag} do
      seed!(%Plain{note: "kept"}, tag)
      seed!(%StateFails{mode: "match"}, tag)

      command = scripted([query: reading(tag)], Bare)

      log =
        capture_log(fn ->
          assert {:error, %MatchError{term: [%Plain{note: "kept"}]}} =
                   PersonalApp.dispatch(command)
        end)

      assert log =~ "kept"

      assert_received {:telemetry, @exception, _,
                       %{kind: :error, reason: %MatchError{}, stacktrace: [_ | _]}}
    end

    test "keeps its exception, in `decide/2`", %{tag: tag} do
      planted = "kept-" <> UUID.uuid4()
      decide = fn _state, _command -> raise planted end
      command = scripted([query: reading(tag), decide: decide], Bare)

      log =
        capture_log(fn ->
          assert {:error, %RuntimeError{message: ^planted}} = PersonalApp.dispatch(command)
        end)

      assert log =~ planted
    end
  end

  defp leave(:exit, value), do: exit(value)
  defp leave(:throw, value), do: throw(value)

  defp leaving(:query, leave, _tag), do: [query: fn _command, _tag_context -> leave.() end]

  defp leaving(:followup, leave, tag) do
    [query: reading(tag), followup: fn _command, _tag_context, _events -> leave.() end]
  end

  defp leaving(:admit, leave, tag) do
    [query: reading(tag), admit: fn _state, _command -> leave.() end]
  end

  defp leaving(:decide, leave, tag) do
    [query: reading(tag), decide: fn _state, _command -> leave.() end]
  end

  # A crash report prints the task's arguments, so the value planted is one they hold.
  for module <- [Personal, Bare], kind <- [:exit, :throw] do
    describe "#{kind} on #{inspect(module)}" do
      for callback <- [:query, :followup, :admit, :decide] do
        test "from `#{callback}` leaves no crash report and no metadata", %{tag: tag} do
          kind = unquote(kind)
          planted = "planted-" <> UUID.uuid4()
          script = leaving(unquote(callback), fn -> leave(kind, {:gave_up, planted}) end, tag)
          command = scripted(script, unquote(module))

          log =
            capture_log(fn ->
              assert {:error, :decision_execution_failed} =
                       PersonalApp.dispatch(command, metadata: %{"actor" => planted})
            end)

          refute log =~ planted
          refute log =~ "terminating"
          assert log =~ "left by #{kind}: {:gave_up, :redacted}"

          assert_received {:telemetry, @exception, %{duration: _},
                           %{kind: ^kind, reason: {:gave_up, :redacted}, stacktrace: stacktrace}}

          refute leaks?(stacktrace, planted)
          refute_received {:telemetry, @stop, _, _}

          pipeline = after_failure()

          assert pipeline.response == {:error, :decision_execution_failed}
          refute Map.has_key?(pipeline.assigns, :error_reason)
        end
      end

      test "from `evolve` leaves no crash report and no state", %{tag: tag} do
        kind = unquote(kind)
        secret = email("in-state-" <> UUID.uuid4())

        seed!(%Plain{note: secret}, tag)
        seed!(%StateFails{mode: Atom.to_string(kind)}, tag)

        command = scripted([query: reading(tag)], unquote(module))

        log =
          capture_log(fn ->
            assert {:error, :decision_execution_failed} = PersonalApp.dispatch(command)
          end)

        refute log =~ secret
        refute log =~ "terminating"

        assert_received {:telemetry, @exception, _, %{kind: ^kind, reason: {_head, :redacted}}}

        refute leaks?(flush_telemetry(), secret)
      end
    end
  end

  describe "a killed task" do
    test "still answers `:decision_execution_failed`", %{tag: tag} do
      decide = fn _state, _command -> Process.exit(self(), :kill) end
      command = scripted([query: reading(tag), decide: decide], Personal)

      assert {:error, :decision_execution_failed} = PersonalApp.dispatch(command)
    end
  end

  describe "the telemetry of a success" do
    test "`:read` holds no value", %{tag: tag, user: user} do
      secret = email("decrypted-" <> UUID.uuid4())

      seed!(%Registered{user_id: user, email: secret}, tag)

      decide = fn _state, _command -> %Plain{} end
      command = scripted([query: reading(tag), decide: decide], Personal)

      assert :ok = PersonalApp.dispatch(command)

      assert_received {:telemetry, @read, measurements, metadata}

      assert Enum.sort(Map.keys(metadata)) == [:application, :attempt, :decision]
      assert Enum.all?(Map.values(measurements), &is_integer/1)
    end

    test "`:refused` holds no value", %{tag: tag, user: user} do
      secret = email("decrypted-" <> UUID.uuid4())

      seed!(%Registered{user_id: user, email: secret}, tag)

      decide = fn state, _command -> state ++ state end
      command = scripted([query: reading(tag), decide: decide], PersonalTight)

      capture_log(fn ->
        assert {:error, :decision_batch_too_large} = PersonalApp.dispatch(command)
      end)

      assert_received {:telemetry, @refused, %{value: 2, limit: 1}, metadata}

      assert Enum.sort(Map.keys(metadata)) == [:application, :bound, :decision]
      refute leaks?(metadata, secret)
    end
  end
end
