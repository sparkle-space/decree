defmodule Decree.Store.Storage.AppendConditionTest do
  use Decree.Store.StorageCase

  alias Decree.Store.{
    AppendCondition,
    AppendError,
    Config,
    EventData,
    EventFactory,
    LockSession,
    Query,
    ReadResult,
    RecordedEvent,
    StoreRows
  }

  alias Decree.Store.Sql.Statements
  alias Decree.Store.Storage.{Appender, CreateStream, Initializer, Schema}
  alias Decree.Store.Streams.Stream
  alias TestEventStore, as: EventStore

  @declared [stream_prefixes: ["decision-"], targets: [:declared_target]]
  @max_bigint 9_223_372_036_854_775_807
  @tag_matcher [[tags: ["tag"]]]

  defmodule PassThroughSerializer do
    @behaviour Decree.Store.Serializer

    def serialize(term), do: term
    def deserialize(term, _config), do: term
  end

  # Every other test here reads what a declared store does, so the store under test
  # declares. `TestEventStore`'s own default, nothing declared, has its own tests below.
  setup %{postgrex_config: postgrex_config} do
    stop_supervised!(TestEventStore)

    start_supervised!(
      {TestEventStore, conditional_appends: @declared, serializer: PassThroughSerializer}
    )

    [observer: LockSession.connect(postgrex_config)]
  end

  describe "compiling a condition" do
    test "holds the compiled query, the position and the target" do
      items = [[types: ["typed"], tags: ["first", "second"]]]
      {:ok, query} = Query.compile(items)

      assert AppendCondition.compile(items: items, after: 7, target: :aggregate) ==
               {:ok, %AppendCondition{query: query, after: 7, target: :aggregate}}
    end

    test "takes no target, and any term as one" do
      assert {:ok, %AppendCondition{target: nil}} =
               AppendCondition.compile(items: @tag_matcher, after: 0)

      assert {:ok, %AppendCondition{target: {:a, "b"}}} =
               AppendCondition.compile(items: @tag_matcher, after: 0, target: {:a, "b"})
    end

    test "takes every position whose successor fits a bigint" do
      assert {:ok, %AppendCondition{after: 0}} =
               AppendCondition.compile(items: @tag_matcher, after: 0)

      assert {:ok, %AppendCondition{after: position}} =
               AppendCondition.compile(items: @tag_matcher, after: @max_bigint - 1)

      assert position == @max_bigint - 1
    end

    test "refuses anything else" do
      for condition <- [
            nil,
            %{items: @tag_matcher, after: 0},
            :condition,
            [],
            [{:items, @tag_matcher} | :improper],
            [items: @tag_matcher],
            [after: 0],
            [items: @tag_matcher, after: nil],
            [items: @tag_matcher, after: -1],
            [items: @tag_matcher, after: 1.0],
            [items: @tag_matcher, after: "1"],
            [items: @tag_matcher, after: @max_bigint],
            [items: @tag_matcher, after: 0, aftr: 1],
            [items: @tag_matcher, after: 0, after: 9],
            [items: @tag_matcher, after: 0, target: :a, target: :b]
          ] do
        assert AppendCondition.compile(condition) == {:error, :invalid_condition},
               inspect(condition)
      end
    end

    test "passes on what the query refuses" do
      assert AppendCondition.compile(items: [], after: 0) == {:error, :invalid_query}
      assert AppendCondition.compile(items: [[tagz: ["a"]]], after: 0) == {:error, :invalid_query}

      assert AppendCondition.compile(items: [[tags: [long_tag()]]], after: 0) ==
               {:error, :tag_too_long}
    end
  end

  describe "the `after` boundary" do
    test "a match at `after` passes, one at `after + 1` fails, for each shape of item",
         context do
      for {items, match} <- [
            {[[types: ["t1"], tags: ["tag-1"]]], event("t1", ["tag-1"])},
            {[[tags: ["tag-2"]]], event("t2", ["tag-2"])},
            {[[types: ["t3"]]], event("t3", [])},
            {[[tags: ["tag-4", "tag-4b"]]], event("t4", ["tag-4b", "tag-4", "tag-4c"])},
            {[[types: ["t5", "t5b"], tags: ["tag-5"]]], event("t5b", ["tag-5"])},
            {[[types: ["t6", "t6b"]]], event("t6b", [])},
            {[[tags: ["absent"]], [types: ["t7"]]], event("t7", [])}
          ] do
        before = head(context)

        :ok = EventStore.append_to_stream("source", :any_version, [match])

        position = before + 1

        assert conditional(items, after: position - 1) ==
                 {:error, :append_condition_failed},
               inspect(items)

        assert conditional(items, after: position) == :ok, inspect(items)
      end
    end

    test "a head past the match passes, whatever moved it" do
      :ok = EventStore.append_to_stream("source", 0, [event("typed", ["tag"])])
      :ok = EventStore.append_to_stream("other", 0, [event("other", ["other"])])
      :ok = EventStore.append_to_stream("other", 1, [event("other", ["other"])])

      assert conditional(@tag_matcher, after: 0) == {:error, :append_condition_failed}
      assert conditional(@tag_matcher, after: 1) == :ok
      assert conditional(@tag_matcher, after: 2) == :ok
    end

    test "an event that is no match passes, for each shape of item" do
      :ok = EventStore.append_to_stream("source", 0, [event("typed", ["first"])])

      for items <- [
            [[types: ["other"], tags: ["first"]]],
            [[types: ["typed"], tags: ["other"]]],
            [[tags: ["other"]]],
            [[types: ["other"]]],
            [[tags: ["first", "second"]]],
            [[tags: ["second", "first"]]],
            [[types: ["other", "another"], tags: ["first"]]]
          ] do
        assert conditional(items, after: 0) == :ok, inspect(items)
      end
    end

    test "any item's match fails it" do
      :ok = EventStore.append_to_stream("source", 0, [event("typed", ["first"])])

      items = [[tags: ["absent"]], [types: ["absent"]], [types: ["typed"], tags: ["first"]]]

      assert conditional(items, after: 0) == {:error, :append_condition_failed}
    end

    test "the largest usable position is no error" do
      assert conditional(@tag_matcher, after: @max_bigint - 1) == :ok
    end
  end

  describe "a failed condition" do
    setup do
      :ok = EventStore.append_to_stream("source", 0, [event("typed", ["tag"])])
    end

    test "on a create leaves no `streams` row and no `events` row", context do
      assert EventStore.append_to_stream("decision-new", 0, [event("typed", [])],
               condition: [items: @tag_matcher, after: 0]
             ) == {:error, :append_condition_failed}

      assert_only_the_source(context)
      assert {:error, :stream_not_found} = EventStore.read_stream_forward("decision-new")
    end

    test "on an existing stream leaves it where it was", context do
      :ok = seed("decision-old", event("typed", []))

      assert EventStore.append_to_stream("decision-old", 1, [event("typed", ["more"])],
               condition: [items: @tag_matcher, after: 0]
             ) == {:error, :append_condition_failed}

      assert {:ok, [%RecordedEvent{stream_version: 1}]} =
               EventStore.read_stream_forward("decision-old")

      assert head(context) == 2
      assert count(context, "event_tags") == 1
      assert count(context, "event_types") == 2
    end

    test "fires neither notification", context do
      %{postgrex_config: postgrex_config, schema: schema} = context

      {:ok, listener} = Postgrex.Notifications.start_link(postgrex_config)
      {:ok, _reference} = Postgrex.Notifications.listen(listener, "#{schema}.events")

      assert EventStore.append_to_stream("decision-new", 0, [event("typed", [])],
               condition: [items: @tag_matcher, after: 0]
             ) == {:error, :append_condition_failed}

      :ok = EventStore.append_to_stream("sentinel", 0, [event("typed", [])])

      # Notifications arrive in commit order, so the sentinel's being the first two
      # proves that nothing came before them, on the stream's row or on `$all`.
      assert_receive {:notification, ^listener, _ref, _channel, first}
      assert_receive {:notification, ^listener, _ref, _channel, second}

      assert String.starts_with?(first, "sentinel,")
      assert String.starts_with?(second, "$all,")
    end

    test "answers the atom, not an `AppendError`", context do
      %{conn: conn, schema: schema} = context

      {:ok, stream_id} = CreateStream.execute(conn, "decision-direct", schema: schema)
      events = EventFactory.create_recorded_events(1, "decision-direct")

      {:ok, condition} = AppendCondition.compile(items: @tag_matcher, after: 0)

      reply = Appender.append(conn, stream_id, events, schema: schema, condition: condition)

      assert reply == {:error, :append_condition_failed}
      refute match?({:error, %AppendError{}}, reply)
    end

    # A create's `INSERT INTO streams` takes a value of the sequence, which no rollback
    # returns, so the sequence counts the calls of the function.
    test "runs once", context do
      before = stream_id_sequence(context)

      assert EventStore.append_to_stream("decision-new", 0, [event("typed", [])],
               condition: [items: @tag_matcher, after: 0]
             ) == {:error, :append_condition_failed}

      assert stream_id_sequence(context) == before + 1
    end

    test "of 1,000 or more events runs once", context do
      before = stream_id_sequence(context)

      assert EventStore.append_to_stream("decision-new", 0, events(1_001, []),
               condition: [items: @tag_matcher, after: 0]
             ) == {:error, :append_condition_failed}

      assert stream_id_sequence(context) == before + 1
    end

    test "leaves the stream appendable", context do
      :ok = seed("decision-old", event("typed", []))

      assert EventStore.append_to_stream("decision-old", 1, [event("typed", [])],
               condition: [items: @tag_matcher, after: 0]
             ) == {:error, :append_condition_failed}

      assert :ok =
               EventStore.append_to_stream("decision-old", 1, [event("typed", [])],
                 condition: [items: @tag_matcher, after: head(context)]
               )

      assert head(context) == 3
    end
  end

  describe "plain and conditional appends on one stream" do
    test "keep versions and positions gapless", context do
      # A declared target, because a declared prefix takes conditional appends alone.
      stream = "mixed"
      tagged = fn -> [event("typed", ["mixed"])] end

      assert :ok = EventStore.append_to_stream(stream, 0, tagged.())

      assert :ok =
               EventStore.append_to_stream(stream, 1, tagged.(),
                 condition: [items: [[tags: ["mixed"]]], after: 1, target: :declared_target]
               )

      # A plain append with a tag some condition reads evaluates nothing.
      assert :ok = EventStore.append_to_stream(stream, :any_version, tagged.())

      assert EventStore.append_to_stream(stream, 3, tagged.(),
               condition: [items: [[tags: ["mixed"]]], after: 2, target: :declared_target]
             ) == {:error, :append_condition_failed}

      assert :ok = EventStore.append_to_stream(stream, 3, tagged.())

      assert :ok =
               EventStore.append_to_stream(stream, :any_version, tagged.(),
                 condition: [items: [[tags: ["mixed"]]], after: 4, target: :declared_target]
               )

      assert :ok =
               EventStore.append_to_stream(stream, 5, tagged.(),
                 condition: [items: [[tags: ["other"]]], after: 0, target: :declared_target]
               )

      assert {:ok, events} = EventStore.read_stream_forward(stream)

      assert Enum.map(events, & &1.stream_version) == [1, 2, 3, 4, 5, 6]
      assert Enum.map(events, & &1.event_number) == [1, 2, 3, 4, 5, 6]
      assert positions(context) == [1, 2, 3, 4, 5, 6]
      assert head(context) == 6
    end

    test "a NULL `after` in the function reads as a plain append", context do
      %{conn: conn, schema: schema} = context

      :ok = EventStore.append_to_stream("source", 0, [event("typed", ["tag"])])

      {:ok, stream_id} = CreateStream.execute(conn, "plain", schema: schema)
      events = EventFactory.create_recorded_events(1, "plain")

      assert :ok = Appender.append(conn, stream_id, events, schema: schema)
      assert head(context) == 2
    end
  end

  describe "a stale append" do
    setup do
      :ok = seed("decision-stale", event("typed", ["tag"]))
    end

    test "answers `:wrong_expected_version` from the read, with a failing condition", context do
      assert EventStore.append_to_stream("decision-stale", 0, [event("typed", [])],
               condition: [items: @tag_matcher, after: 0]
             ) == {:error, :wrong_expected_version}

      assert head(context) == 1
    end

    test "answers `:wrong_expected_version` from the function, which never reaches it",
         context do
      %{conn: conn, schema: schema} = context

      %Postgrex.Result{rows: [[stream_id]]} =
        Postgrex.query!(
          conn,
          ~s(SELECT stream_id FROM "#{schema}".streams WHERE stream_uuid = 'decision-stale'),
          []
        )

      stale = EventFactory.create_recorded_events(1, "decision-stale", 1, 5)

      {:ok, condition} = AppendCondition.compile(items: @tag_matcher, after: 0)

      assert Appender.append(conn, stream_id, stale, schema: schema, condition: condition) ==
               {:error, :wrong_expected_version}

      assert head(context) == 1
    end
  end

  describe "an append of 1,000 or more events" do
    test "is not tripped by its own tags, across the batch split", context do
      :ok = EventStore.append_to_stream("before", 0, [event("typed", [])])

      assert :ok =
               EventStore.append_to_stream("decision-big", 0, events(1_001, ["tag"]),
                 condition: [items: @tag_matcher, after: 1]
               )

      assert head(context) == 1_002
      assert positions(context) == Enum.to_list(1..1_002)
      assert count(context, "event_tags") == 1_001
    end

    test "with a real prior match writes nothing", context do
      :ok = EventStore.append_to_stream("source", 0, [event("typed", ["tag"])])

      assert EventStore.append_to_stream("decision-big", 0, events(1_001, ["tag"]),
               condition: [items: @tag_matcher, after: 0]
             ) == {:error, :append_condition_failed}

      assert_only_the_source(context)
    end

    test "under an existing stream keeps numbering from it" do
      :ok = seed("decision-big", event("typed", ["tag"]))

      assert :ok =
               EventStore.append_to_stream("decision-big", 1, events(2_500, ["tag"]),
                 condition: [items: @tag_matcher, after: 1]
               )

      assert {:ok, events} = EventStore.read_stream_forward("decision-big", 0, 3_000)
      assert Enum.map(events, & &1.stream_version) == Enum.to_list(1..2_501)
    end
  end

  # The registration read and the release read are two statements, so the head moves
  # between them. A condition from the first read's `after` covers both parts.
  describe "a two-part decision (design doc Q-1)" do
    @registrations [types: ["UserRegistered"], tags: ["email:digest"]]
    @releases [types: ["UserReleased"], tags: ["user:candidate"]]

    test "a registration committed between the two reads fails the condition" do
      {:ok, %ReadResult{events: [], after: after_first}} =
        EventStore.read_query([@registrations])

      :ok =
        EventStore.append_to_stream("winner", 0, [
          event("UserRegistered", ["email:digest", "user:winner"])
        ])

      {:ok, %ReadResult{events: [], after: after_second}} =
        EventStore.read_query([@releases])

      assert after_second > after_first

      assert conditional([@registrations, @releases], after: after_first) ==
               {:error, :append_condition_failed}
    end

    test "a registration after the second read fails it too" do
      {after_first, after_second} = two_reads()

      assert after_second > after_first

      :ok =
        EventStore.append_to_stream("winner", 0, [
          event("UserRegistered", ["email:digest", "user:winner"])
        ])

      assert conditional([@registrations, @releases], after: after_first) ==
               {:error, :append_condition_failed}
    end

    # Spurious: the release is what the candidate waited for. It fails the append, and
    # the caller reads again.
    test "a release of the candidate committed after the first read fails it" do
      {:ok, %ReadResult{events: [], after: after_first}} =
        EventStore.read_query([@registrations])

      :ok = EventStore.append_to_stream("release", 0, [event("UserReleased", ["user:candidate"])])

      {:ok, %ReadResult{events: [%RecordedEvent{}], after: after_second}} =
        EventStore.read_query([@releases])

      assert after_second > after_first

      assert conditional([@registrations, @releases], after: after_first) ==
               {:error, :append_condition_failed}
    end

    test "a head that moved past no match of either part passes" do
      {after_first, _after_second} = two_reads()

      assert conditional([@registrations, @releases], after: after_first) == :ok
    end
  end

  describe "the `after` of a real read" do
    test "over a tag with a prior match, the match stays behind it" do
      :ok = EventStore.append_to_stream("source", 0, [event("typed", ["tag"])])

      {:ok, %ReadResult{events: [%RecordedEvent{}], after: head}} =
        EventStore.read_query(@tag_matcher)

      assert conditional(@tag_matcher, after: head) == :ok

      assert conditional(@tag_matcher, after: head, tags: ["tag"]) == :ok

      assert conditional(@tag_matcher, after: head) ==
               {:error, :append_condition_failed}
    end
  end

  describe "the declared target" do
    test "a declared prefix passes with no target" do
      assert conditional(@tag_matcher, after: 0) == :ok
    end

    test "a declared prefix refuses a target that is not declared", context do
      assert EventStore.append_to_stream("decision-x", 0, [event("typed", [])],
               condition: [items: @tag_matcher, after: 0, target: :unlisted]
             ) == {:error, :undeclared_conditional_target}

      assert head(context) == 0
    end

    test "a declared target passes on a bare UUID stream" do
      stream_uuid = Decree.Store.UUID.uuid4()

      assert :ok =
               EventStore.append_to_stream(stream_uuid, 0, [event("typed", [])],
                 condition: [items: @tag_matcher, after: 0, target: :declared_target]
               )
    end

    test "is refused for a target and a stream that are both undeclared", context do
      for condition <- [
            [items: @tag_matcher, after: 0],
            [items: @tag_matcher, after: 0, target: nil],
            [items: @tag_matcher, after: 0, target: :unlisted],
            [items: @tag_matcher, after: 0, target: "declared_target"]
          ] do
        assert EventStore.append_to_stream("stream", 0, [event("typed", [])],
                 condition: condition
               ) == {:error, :undeclared_conditional_target}
      end

      assert head(context) == 0
    end

    test "a stream that only contains the prefix is not declared" do
      assert EventStore.append_to_stream("no-decision-1", 0, [event("typed", [])],
               condition: [items: @tag_matcher, after: 0]
             ) == {:error, :undeclared_conditional_target}
    end

    test "a store that declares nothing refuses every conditional append", context do
      start_supervised!(
        {TestEventStore, name: :undeclared_store, serializer: PassThroughSerializer}
      )

      for {stream_uuid, target} <- [{"decision-x", nil}, {"stream", :declared_target}] do
        assert EventStore.append_to_stream(stream_uuid, 0, [event("typed", [])],
                 name: :undeclared_store,
                 condition: [items: @tag_matcher, after: 0, target: target]
               ) == {:error, :undeclared_conditional_target}
      end

      assert head(context) == 0
    end

    test "a store that declares nothing still takes plain appends", context do
      start_supervised!(
        {TestEventStore, name: :undeclared_store, serializer: PassThroughSerializer}
      )

      assert :ok =
               EventStore.append_to_stream("decision-x", 0, [event("typed", [])],
                 name: :undeclared_store
               )

      assert head(context) == 1
    end

    test "a caller cannot declare its own append" do
      for declared <- [
            [stream_prefixes: [""]],
            [stream_prefixes: ["stream"]],
            [targets: [:mine]]
          ] do
        assert EventStore.append_to_stream("stream", 0, [event("typed", [])],
                 conditional_appends: declared,
                 condition: [items: @tag_matcher, after: 0, target: :mine]
               ) == {:error, :undeclared_conditional_target}
      end
    end

    test "comes from the config file's shape, through the parser" do
      parsed = Config.parse(conditional_appends: @declared)

      assert Keyword.fetch!(parsed, :conditional_appends) == @declared
    end
  end

  # A connection that does not exist: any statement, a BEGIN included, would exit.
  describe "a refusal sends no statement at all" do
    @no_conn :a_connection_that_does_not_exist

    test "for a condition that cannot compile, in both clauses" do
      for {condition, answer} <- refusals(), count <- [1, 1_001] do
        assert EventStore.append_to_stream("decision-x", 0, events(count, []),
                 conn: @no_conn,
                 condition: condition
               ) == answer,
               inspect({condition, count})
      end
    end

    test "for a repeated `:condition`, in either order, in both clauses" do
      for conditions <- repeated_conditions(), count <- [1, 1_001] do
        opts = [conn: @no_conn] ++ Enum.map(conditions, &{:condition, &1})

        assert EventStore.append_to_stream("decision-x", 0, events(count, []), opts) ==
                 {:error, :invalid_condition},
               inspect({conditions, count})
      end
    end

    test "for a repeated `:condition`, and a caller's transaction stays alive", context do
      %{postgrex_config: postgrex_config} = context

      :ok = EventStore.append_to_stream("source", 0, [event("typed", ["tag"])])

      session = LockSession.start(postgrex_config)

      LockSession.run(session, fn tx ->
        for conditions <- repeated_conditions() do
          opts = [conn: tx] ++ Enum.map(conditions, &{:condition, &1})

          assert EventStore.append_to_stream("decision-x", 0, [event("typed", [])], opts) ==
                   {:error, :invalid_condition},
                 inspect(conditions)
        end

        assert %Postgrex.Result{rows: [[1]]} = Postgrex.query!(tx, "SELECT 1", [])
      end)

      assert {:ok, :ok} = LockSession.commit(session)
      assert_only_the_source(context)
    end

    test "for an undeclared target, in both clauses" do
      for count <- [1, 1_001] do
        assert EventStore.append_to_stream("stream", 0, events(count, []),
                 conn: @no_conn,
                 condition: [items: @tag_matcher, after: 0]
               ) == {:error, :undeclared_conditional_target}
      end
    end

    test "tags answer first, then the condition, then the target" do
      long = [event("typed", [long_tag()])]
      fine = [event("typed", [])]

      assert EventStore.append_to_stream("stream", 0, long,
               conn: @no_conn,
               condition: [after: nil]
             ) == {:error, :tag_too_long}

      assert EventStore.append_to_stream("stream", 0, fine,
               conn: @no_conn,
               condition: [items: @tag_matcher, after: nil]
             ) == {:error, :invalid_condition}

      assert EventStore.append_to_stream("stream", 0, fine,
               conn: @no_conn,
               condition: [items: [], after: 0]
             ) == {:error, :invalid_query}
    end

    test "for `condition: nil`, which is no plain append" do
      assert EventStore.append_to_stream("decision-x", 0, [event("typed", [])],
               conn: @no_conn,
               condition: nil
             ) == {:error, :invalid_condition}
    end

    test "and a refusal answers before a stale version would", context do
      :ok = seed("decision-x", event("typed", []))

      assert EventStore.append_to_stream("decision-x", 0, [event("typed", [])],
               condition: [items: @tag_matcher, after: nil]
             ) == {:error, :invalid_condition}

      assert head(context) == 1
    end

    test "and a caller's transaction stays alive", context do
      %{postgrex_config: postgrex_config} = context

      session = LockSession.start(postgrex_config)
      events = [event("typed", ["tag"])]

      LockSession.run(session, fn tx ->
        for {condition, answer} <- refusals() do
          assert EventStore.append_to_stream("decision-x", 0, events,
                   conn: tx,
                   condition: condition
                 ) == answer
        end

        assert EventStore.append_to_stream("stream", 0, events,
                 conn: tx,
                 condition: [items: @tag_matcher, after: 0]
               ) == {:error, :undeclared_conditional_target}

        assert :ok = EventStore.append_to_stream("stream", 0, events, conn: tx)
      end)

      assert {:ok, :ok} = LockSession.commit(session)
      assert head(context) == 1
    end
  end

  describe "a caller's transaction" do
    test "a failed condition aborts it until rollback", context do
      %{postgrex_config: postgrex_config} = context

      :ok = EventStore.append_to_stream("source", 0, [event("typed", ["tag"])])

      session = LockSession.start(postgrex_config)

      LockSession.run(session, fn tx ->
        assert :ok = EventStore.append_to_stream("mine", 0, [event("typed", [])], conn: tx)

        assert EventStore.append_to_stream("decision-x", 0, [event("typed", [])],
                 conn: tx,
                 condition: [items: @tag_matcher, after: 0]
               ) == {:error, :append_condition_failed}

        assert {:error, %Postgrex.Error{postgres: %{code: :in_failed_sql_transaction}}} =
                 Postgrex.query(tx, "SELECT 1", [])
      end)

      assert {:error, :rollback} = LockSession.commit(session)

      assert_only_the_source(context)
    end

    test "an event of its own past `after` fails its own condition", context do
      %{postgrex_config: postgrex_config} = context

      session = LockSession.start(postgrex_config)

      LockSession.run(session, fn tx ->
        assert :ok = EventStore.append_to_stream("mine", 0, [event("typed", ["tag"])], conn: tx)

        assert EventStore.append_to_stream("decision-x", 0, [event("typed", [])],
                 conn: tx,
                 condition: [items: @tag_matcher, after: 0]
               ) == {:error, :append_condition_failed}
      end)

      assert {:error, :rollback} = LockSession.commit(session)
      assert head(context) == 0
    end

    test "the function's settings are restored on a raise and on success", context do
      %{conn: conn, postgrex_config: postgrex_config, schema: schema} = context

      :ok = EventStore.append_to_stream("source", 0, [event("typed", ["tag"])])

      {:ok, stream_id} = CreateStream.execute(conn, "decision-x", schema: schema)

      {:ok, failing} = AppendCondition.compile(items: @tag_matcher, after: 0)
      {:ok, passing} = AppendCondition.compile(items: @tag_matcher, after: 1)

      session = LockSession.start(postgrex_config)

      LockSession.run(session, fn tx ->
        Postgrex.query!(tx, "SET LOCAL enable_seqscan TO on", [])

        events = EventFactory.create_recorded_events(1, "decision-x")

        # The raise rolls back to a savepoint, so the transaction lives to be read.
        assert Appender.append(tx, stream_id, events,
                 schema: schema,
                 condition: failing,
                 mode: :savepoint
               ) == {:error, :append_condition_failed}

        assert show(tx, "enable_seqscan") == "on"

        assert :ok = Appender.append(tx, stream_id, events, schema: schema, condition: passing)

        assert show(tx, "enable_seqscan") == "on"
      end)

      assert {:ok, :ok} = LockSession.commit(session)
    end
  end

  describe "a tag row with no `$all` row" do
    test "past `after` does not fail a condition", context do
      %{postgrex_config: postgrex_config, schema: schema} = context

      :ok = EventStore.append_to_stream("gone", 0, [event("typed", ["tag"])])

      :ok = StoreRows.orphan_stream(LockSession.connect(postgrex_config), schema, "gone")

      # Its rows are still there, and only the live-event check keeps them out.
      assert count(context, "event_tags") == 1
      assert count(context, "event_types") == 1

      assert conditional(@tag_matcher, after: 0) == :ok
      assert conditional([[types: ["typed"]]], after: 0, tags: ["tag"]) == :ok

      # The live event just appended is seen, as ever.
      assert conditional(@tag_matcher, after: 0) == {:error, :append_condition_failed}
    end
  end

  describe "a named schema" do
    test "reads its own rows, and takes the default schema's for nothing", context do
      schema = unique_schema("conditional")
      init_schema(context, schema)

      start_supervised!(
        {TestEventStore,
         name: :named_store,
         schema: schema,
         conditional_appends: @declared,
         serializer: PassThroughSerializer}
      )

      :ok = EventStore.append_to_stream("source", 0, [event("typed", ["tag"])])

      opts = [name: :named_store, condition: [items: @tag_matcher, after: 0]]

      assert :ok = EventStore.append_to_stream("decision-x", 0, [event("typed", ["tag"])], opts)

      assert EventStore.append_to_stream("decision-y", 0, [event("typed", [])], opts) ==
               {:error, :append_condition_failed}
    end
  end

  describe "the template" do
    setup do
      fragment =
        Statements.query_positions(
          "",
          "p_probe_items",
          "p_probe_tags",
          "p_probe_types",
          "p_rest_items",
          "p_rest_tags",
          "p_after + 1",
          "1",
          ">=",
          "ASC"
        )

      [fragment: IO.iodata_to_binary(fragment)]
    end

    test "is in the function of an initialised store, byte for byte", context do
      %{conn: conn, fragment: fragment} = context

      %Postgrex.Result{rows: [[definition]]} =
        Postgrex.query!(
          conn,
          "SELECT pg_get_functiondef('public.append_events_v5'::regproc)",
          []
        )

      assert String.contains?(definition, "SELECT EXISTS (\n" <> fragment <> "\n    ) INTO")
    end

    test "is in the migration, byte for byte", %{fragment: fragment} do
      migration =
        :decree
        |> Application.app_dir("priv/decree/store/migrations/v1.9.0.sql")
        |> File.read!()

      assert String.contains?(migration, "SELECT EXISTS (\n" <> fragment <> "\n    ) INTO")
    end
  end

  defp two_reads do
    {:ok, %ReadResult{events: [], after: after_first}} =
      EventStore.read_query([@registrations])

    :ok = EventStore.append_to_stream("noise", :any_version, [event("Noise", ["noise"])])

    {:ok, %ReadResult{events: [], after: after_second}} =
      EventStore.read_query([@releases])

    {after_first, after_second}
  end

  # Appends one untagged event, or one with the option's tags, to a new stream with a
  # declared prefix, and answers the store's reply.
  defp conditional(items, opts) do
    {tags, opts} = Keyword.pop(opts, :tags, [])
    position = Keyword.fetch!(opts, :after)

    stream_uuid = "decision-#{System.unique_integer([:positive])}"

    EventStore.append_to_stream(stream_uuid, 0, [event("written", tags)],
      condition: [items: items, after: position]
    )
  end

  # A declared prefix takes conditional appends and nothing else, so a test's starting rows
  # go in through a condition that nothing matches.
  defp seed(stream_uuid, event) do
    EventStore.append_to_stream(stream_uuid, 0, [event],
      condition: [items: [[tags: ["seed"]]], after: 0]
    )
  end

  # With the one event of "source" at position 1, the first passes and the second fails.
  defp repeated_conditions do
    passing = [items: @tag_matcher, after: 1]
    failing = [items: @tag_matcher, after: 0]

    [[passing, failing], [failing, passing], [passing, passing]]
  end

  defp refusals do
    [
      {[items: @tag_matcher, after: nil], {:error, :invalid_condition}},
      {[items: @tag_matcher, after: -1], {:error, :invalid_condition}},
      {[items: @tag_matcher, after: @max_bigint], {:error, :invalid_condition}},
      {[items: @tag_matcher, after: 0, unknown: 1], {:error, :invalid_condition}},
      {[items: @tag_matcher], {:error, :invalid_condition}},
      {%{items: @tag_matcher, after: 0}, {:error, :invalid_condition}},
      {[items: [], after: 0], {:error, :invalid_query}},
      {[items: [[tags: [long_tag()]]], after: 0], {:error, :tag_too_long}}
    ]
  end

  defp assert_only_the_source(context) do
    assert head(context) == 1
    assert count(context, "events") == 1
    assert count(context, "event_tags") == 1
    assert count(context, "event_types") == 1

    # `$all`, and "source".
    assert count(context, "streams") == 2
  end

  defp event(event_type, tags) do
    %EventData{event_type: event_type, data: "{}", metadata: "{}", tags: tags}
  end

  defp events(count, tags) do
    for _index <- 1..count, do: event("typed", tags)
  end

  defp long_tag, do: String.duplicate("a", 513)

  defp head(%{schema: schema} = context) do
    query_one(
      context,
      ~s(SELECT stream_version FROM "#{schema}".streams WHERE stream_id = 0)
    )
  end

  defp positions(%{observer: observer, schema: schema}) do
    %Postgrex.Result{rows: rows} =
      Postgrex.query!(
        observer,
        ~s(SELECT stream_version FROM "#{schema}".stream_events) <>
          " WHERE stream_id = 0 ORDER BY stream_version",
        []
      )

    Enum.map(rows, &hd/1)
  end

  defp count(%{schema: schema} = context, table) do
    query_one(context, ~s[SELECT count(*) FROM "#{schema}".#{table}])
  end

  defp stream_id_sequence(%{schema: schema} = context) do
    query_one(context, ~s(SELECT last_value FROM "#{schema}".streams_stream_id_seq))
  end

  defp query_one(%{observer: observer}, sql) do
    %Postgrex.Result{rows: [[value]]} = Postgrex.query!(observer, sql, [])

    value
  end

  defp show(conn, setting) do
    %Postgrex.Result{rows: [[value]]} = Postgrex.query!(conn, "SHOW #{setting}", [])

    value
  end

  defp unique_schema(prefix) do
    "#{prefix}_#{System.unique_integer([:positive])}"
  end

  defp init_schema(context, schema) do
    %{config: config} = context

    config = Keyword.put(config, :schema, schema)

    :ok = Schema.create(config)
    on_exit(fn -> Schema.drop(config) end)

    conn = config |> Config.default_postgrex_opts() |> LockSession.connect()

    {:ok, _} = Initializer.run!(conn, config)
    :ok = GenServer.stop(conn)
  end
end
