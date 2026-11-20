defmodule Decree.Store.TagIndexTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{Config, EventData, LockSession, TagIndex}
  alias Decree.Store.Streams.StreamInfo
  alias TestEventStore, as: EventStore

  # A connection that does not exist: any statement, a BEGIN included, would exit.
  @no_conn :a_connection_that_does_not_exist

  @fact "tag_index:fact"
  @enabled "tag_index_state:email:-:enabled"

  defmodule PassThroughSerializer do
    @behaviour Decree.Store.Serializer

    def serialize(term), do: term
    def deserialize(term, _config), do: term
  end

  setup %{postgrex_config: postgrex_config} do
    stop_supervised!(TestEventStore)

    start_supervised!(
      {TestEventStore,
       enable_hard_deletes: true,
       conditional_appends: [stream_prefixes: ["decision-"]],
       serializer: PassThroughSerializer}
    )

    [observer: LockSession.connect(postgrex_config)]
  end

  describe "the grammar" do
    test "a state tag round-trips, with `-` for no key" do
      fingerprint = TagIndex.fingerprint("key")

      for {family, key, state} <- [
            {"email", fingerprint, :enabled},
            {"email", fingerprint, :disabled},
            {"entity", nil, :enabled},
            {"command_kind", nil, :disabled}
          ] do
        tag = TagIndex.state_tag(family, key, state)

        assert tag ==
                 "tag_index_state:#{family}:#{key || "-"}:#{state}"

        assert TagIndex.parse_state(tag) == {:ok, {family, key, state}}
      end
    end

    test "a state tag refuses a family, a key or a state it cannot hold" do
      for {family, key, state} <- [
            {"", nil, :enabled},
            {"a:b", nil, :enabled},
            {:email, nil, :enabled},
            {"tag_index", nil, :enabled},
            {"tag_index_state", nil, :enabled},
            {"email", "", :enabled},
            {"email", "-", :enabled},
            {"email", "a:b", :enabled},
            {"email", :key, :enabled},
            {"email", nil, :on},
            {"email", nil, "enabled"}
          ] do
        assert_raise ArgumentError, fn -> TagIndex.state_tag(family, key, state) end
      end
    end

    test "a state tag that does not parse" do
      for tag <- [
            "tag_index_state:email:-",
            "tag_index_state:email:-:on",
            "tag_index_state::-:enabled",
            "tag_index_state:email::enabled",
            "tag_index_state:email:a:b:enabled",
            "tag_index_state:tag_index:-:enabled",
            "tag_index:fact",
            "email:-:enabled"
          ] do
        assert TagIndex.parse_state(tag) == :error, tag
      end
    end

    test "the fact tag, the reserved kinds and the named families" do
      assert TagIndex.fact_tag() == @fact
      assert TagIndex.reserved_kinds() == ["tag_index", "tag_index_state"]

      assert TagIndex.named_families() ==
               ~w(command command_kind correlation tenant_slug plan_slug email oauth_uid)
    end
  end

  describe "the fingerprint" do
    test "is 16 lowercase hex characters, the same for the same key" do
      fingerprint = TagIndex.fingerprint("key material")

      assert fingerprint =~ ~r/\A[0-9a-f]{16}\z/
      assert TagIndex.fingerprint("key material") == fingerprint
    end

    test "is DOC-1's form: the first 8 bytes of HMAC-SHA-256 keyed by the key material" do
      <<expected::binary-size(8), _rest::binary>> =
        :crypto.mac(:hmac, :sha256, "key material", "tag-digest-fingerprint")

      assert TagIndex.fingerprint("key material") == Base.encode16(expected, case: :lower)
    end

    test "differs between two keys" do
      refute TagIndex.fingerprint("one key") == TagIndex.fingerprint("another key")
    end

    test "refuses anything but a non-empty binary" do
      for key <- ["", nil, :key, 1] do
        assert_raise ArgumentError, fn -> TagIndex.fingerprint(key) end
      end
    end
  end

  describe "`digest_key/1`" do
    test "reads the fingerprint ahead of the digest" do
      fingerprint = TagIndex.fingerprint("key")

      assert TagIndex.digest_key(fingerprint <> ":digest") == {:ok, fingerprint}
      assert TagIndex.digest_key("fp:digest:with:colons") == {:ok, "fp"}
    end

    test "answers `:error` for a value with no readable fingerprint or no digest" do
      for value <- ["digest", ":digest", "-:digest", "fp:", "", nil, :fp] do
        assert TagIndex.digest_key(value) == :error, inspect(value)
      end
    end
  end

  describe "the store's guard" do
    @refused [
      {"a state tag without the fact tag", [@enabled]},
      {"the fact tag without a state tag", [@fact]},
      {"a malformed state", [@fact, "tag_index_state:email:-:on"]},
      {"a state with no key part", [@fact, "tag_index_state:email:enabled"]},
      {"`tag_index:` with another value", ["tag_index:other", @enabled]},
      {"`tag_index` alone as a kind", ["tag_index", @enabled]},
      {"another value beside the fact tag", [@fact, "tag_index:other", @enabled]}
    ]

    for {name, tags} <- @refused do
      test "refuses #{name} on `append_to_stream`, under and over the 1,000-event clause" do
        for count <- [1, 1_000, 1_001], expected_version <- [:any_version, :no_stream, 0] do
          events = events(count - 1) ++ [event(unquote(tags))]

          assert EventStore.append_to_stream("stream", expected_version, events, conn: @no_conn) ==
                   {:error, :invalid_tag_index_fact},
                 inspect({count, expected_version})
        end
      end

      test "refuses #{name} on `append_to_streams`, one member refusing the batch" do
        refusing = [event(unquote(tags))]

        for appends <- [
              [{"a", 0, refusing}],
              [{"a", 0, events(1)}, {"b", :any_version, refusing}]
            ] do
          assert EventStore.append_to_streams(appends, conn: @no_conn) ==
                   {:error, :invalid_tag_index_fact}
        end
      end

      test "refuses #{name} on a conditional append" do
        assert EventStore.append_to_stream("decision-1", 0, [event(unquote(tags))],
                 conn: @no_conn,
                 condition: [items: [[tags: ["never"]]], after: 0]
               ) == {:error, :invalid_tag_index_fact}
      end
    end

    test "comes after `:invalid_tag`, and before the required kinds" do
      assert EventStore.append_to_stream("a", 0, [event([<<0>>, @fact])], conn: @no_conn) ==
               {:error, :invalid_tag}

      refusing = %EventData{event([@fact]) | required_tag_kinds: ["email"]}

      assert EventStore.append_to_stream("a", 0, [refusing], conn: @no_conn) ==
               {:error, :invalid_tag_index_fact}
    end

    test "writes nothing", context do
      assert EventStore.append_to_stream("a", 0, [event([@enabled])]) ==
               {:error, :invalid_tag_index_fact}

      assert EventStore.append_to_streams([{"a", 0, events(1)}, {"b", 0, [event([@fact])]}]) ==
               {:error, :invalid_tag_index_fact}

      assert all_version(context) == 0
      assert stream_id(context, "a") == nil
    end

    test "lets a fact through, with every state tag it carries", context do
      disabled = TagIndex.state_tag("email", TagIndex.fingerprint("k"), :disabled)
      tags = [@fact, @enabled, disabled, "command:x"]

      assert :ok = EventStore.append_to_stream("a", 0, [event(tags)])

      assert %Postgrex.Result{rows: rows} =
               Postgrex.query!(context.conn, "SELECT tag FROM event_tags ORDER BY tag", [])

      assert List.flatten(rows) == Enum.sort(tags)
    end

    test "lets an event of no reserved kind through" do
      assert :ok = EventStore.append_to_stream("a", 0, [event(["tag_indexed:x", "index:fact"])])
    end
  end

  describe "a reserved kind" do
    test "cannot be declared deletable" do
      for kinds <- [["tag_index"], ["tag_index_state"], ["kind", "tag_index_state"]] do
        assert_raise ArgumentError, ~r/cannot declare the tag kind `tag_index/, fn ->
          Config.parse(deletable: [tag_kinds: kinds])
        end
      end
    end

    test "is refused by `delete_stream_tags`, with no statement sent" do
      for tag_kinds <- [["tag_index"], ["tag_index_state"], ["email", "tag_index_state"]] do
        assert EventStore.delete_stream_tags("a", tag_kinds, conn: @no_conn) ==
                 {:error, :invalid_tag_kinds}
      end
    end

    test "is left standing by `delete_stream_tags` on its neighbours", context do
      assert :ok = EventStore.append_to_stream("a", 0, [event([@fact, @enabled, "email:x"])])

      assert {:ok, 1} = EventStore.delete_stream_tags("a", ["email"])

      assert %Postgrex.Result{rows: rows} =
               Postgrex.query!(context.conn, "SELECT tag FROM event_tags ORDER BY tag", [])

      assert List.flatten(rows) == [@fact, @enabled]
    end
  end

  describe "`fold/1`" do
    test "no fact enables nothing" do
      assert TagIndex.fold([]) == {:ok, %{}}
    end

    test "the last state per family and key wins, in the facts' order" do
      key = TagIndex.fingerprint("k")

      facts = [
        [@fact, state("entity", :enabled), TagIndex.state_tag("email", key, :enabled)],
        [@fact, state("entity", :disabled), "command:x"],
        [@fact, state("command", :enabled)],
        [@fact, state("command", :disabled)],
        [@fact, state("command", :enabled)]
      ]

      assert TagIndex.fold(facts) ==
               {:ok,
                %{
                  {"entity", nil} => :disabled,
                  {"email", key} => :enabled,
                  {"command", nil} => :enabled
                }}
    end

    test "a fact naming one family and key both ways reads as disabled, whatever its tags' order" do
      enabled = state("entity", :enabled)
      disabled = state("entity", :disabled)

      for both_ways <- [[@fact, enabled, disabled], [disabled, @fact, enabled]] do
        assert TagIndex.fold([[@fact, enabled], both_ways]) ==
                 {:ok, %{{"entity", nil} => :disabled}}
      end
    end

    test "a fact it cannot attribute answers `:error`, wherever it sits" do
      enable = [@fact, state("entity", :enabled)]

      for unattributable <- [
            [],
            [@fact],
            [@fact, "command:x"],
            [@fact, "tag_index_state:entity:-:on"],
            [@fact, state("entity", :enabled), "tag_index_state:entity:-:on"]
          ] do
        for facts <- [[unattributable], [enable, unattributable], [unattributable, enable]] do
          assert TagIndex.fold(facts) == :error, inspect(facts)
        end
      end
    end
  end

  describe "`states/2`" do
    test "an empty store has no state" do
      assert TagIndex.states(EventStore) == {:ok, %{}}
    end

    test "folds every fact on the store, across streams, in position order" do
      :ok = EventStore.append_to_stream("a", 0, [fact(entity: :enabled), event(["entity:x"])])

      :ok =
        EventStore.append_to_stream("b", 0, [fact(entity: :disabled, command: :enabled)])

      :ok = EventStore.append_to_stream("a", 2, [fact(entity: :enabled)])

      assert TagIndex.states(EventStore) ==
               {:ok, %{{"entity", nil} => :enabled, {"command", nil} => :enabled}}
    end

    test "reads every fact, more than a read's count, whatever read options it is given" do
      facts =
        [fact(command: :enabled, entity: :enabled)] ++
          for(_fact <- 1..1_000, do: fact(entity: :enabled)) ++ [fact(entity: :disabled)]

      :ok = EventStore.append_to_stream("a", 0, facts)

      for opts <- [
            [],
            [count: 1],
            [from: 1_002],
            [direction: :backward],
            [tag_items: []],
            [position_items: []]
          ] do
        assert TagIndex.states(EventStore, opts) ==
                 {:ok, %{{"command", nil} => :enabled, {"entity", nil} => :disabled}},
               inspect(opts)
      end
    end

    test "a fact whose state tags the read leaves out is an error, never skipped" do
      restart_store(deletable: [event_types: ["hidden"]])

      hidden = %EventData{fact(entity: :disabled) | event_type: "hidden"}

      # The enable before it still reads, so skipping the disable would read as enabled.
      :ok = EventStore.append_to_stream("a", 0, [fact(entity: :enabled), hidden])

      assert TagIndex.states(EventStore) == {:error, :unattributable_tag_index_fact}
    end
  end

  describe "`missing/2`" do
    test "the requirements not enabled, in the order given, a family alone keyless" do
      key = TagIndex.fingerprint("k")

      states = %{
        {"entity", nil} => :enabled,
        {"command", nil} => :disabled,
        {"email", key} => :enabled
      }

      requirements = [
        "entity",
        {"entity", nil},
        "command",
        "tenant_slug",
        {"email", key},
        {"email", nil},
        {"entity", key}
      ]

      assert TagIndex.missing(states, requirements) ==
               ["command", "tenant_slug", {"email", nil}, {"entity", key}]

      assert TagIndex.missing(states, []) == []
      assert TagIndex.missing(%{}, ["entity"]) == ["entity"]
    end

    test "raises for a requirement outside the grammar" do
      for requirement <- [
            "",
            "a:b",
            :entity,
            "tag_index",
            "tag_index_state",
            {"entity", ""},
            {"entity", "-"},
            {"entity", "a:b"},
            {"entity", :key},
            {"entity"},
            {"entity", nil, nil}
          ] do
        assert_raise ArgumentError, fn -> TagIndex.missing(%{}, [requirement]) end
      end
    end
  end

  describe "`bootstrap/3`" do
    test "on an empty store, calls `enable` once with the requirements, and answers `:enabled`" do
      requirements = ["entity", {"email", TagIndex.fingerprint("k")}]

      assert TagIndex.bootstrap(EventStore, requirements, enabling()) == :enabled

      assert_received {:enable, ^requirements}
      refute_received {:enable, _requirements}
      assert all_head() == 1
    end

    test "a rerun on the store it enabled only verifies" do
      assert TagIndex.bootstrap(EventStore, ["entity", "command"], enabling()) == :enabled
      assert TagIndex.bootstrap(EventStore, ["entity", "command"], enabling()) == :verified

      assert_received {:enable, ["entity", "command"]}
      refute_received {:enable, _requirements}
      assert all_head() == 1
    end

    test "on an empty store, answers `enable`'s error" do
      assert TagIndex.bootstrap(EventStore, ["entity"], enabling({:error, :refused})) ==
               {:error, :refused}

      assert_received {:enable, ["entity"]}
      refute_received {:enable, _requirements}
      assert all_head() == 0
    end

    test "on an empty store, reports what `enable` left disabled" do
      assert TagIndex.bootstrap(EventStore, ["entity", "command"], enabling(["entity"])) ==
               {:error, {:tag_index_not_enabled, ["command"]}}

      assert_received {:enable, ["entity", "command"]}
      refute_received {:enable, _requirements}
    end

    test "on an empty store, reports every requirement when `enable` appends nothing" do
      assert TagIndex.bootstrap(EventStore, ["entity", "command"], enabling(:ok)) ==
               {:error, {:tag_index_not_enabled, ["entity", "command"]}}

      assert_received {:enable, ["entity", "command"]}
      refute_received {:enable, _requirements}
      assert all_head() == 0
    end

    test "on a store with history, never calls `enable`, and verifies" do
      :ok = EventStore.append_to_stream("a", 0, [fact(entity: :enabled, command: :enabled)])
      :ok = EventStore.append_to_stream("b", 0, [event(["entity:x"])])

      assert TagIndex.bootstrap(EventStore, ["command", {"entity", nil}], enabling()) ==
               :verified

      refute_received {:enable, _requirements}
      assert all_head() == 2
    end

    test "on a store with history and a requirement missing, reports it and never enables it" do
      key = TagIndex.fingerprint("k")

      :ok = EventStore.append_to_stream("a", 0, [fact(entity: :enabled, command: :enabled)])
      :ok = EventStore.append_to_stream("a", 1, [fact(command: :disabled)])

      requirements = ["entity", "extra_kind", "command", {"email", key}]

      assert TagIndex.bootstrap(EventStore, requirements, enabling()) ==
               {:error, {:tag_index_not_enabled, ["extra_kind", "command", {"email", key}]}}

      refute_received {:enable, _requirements}
      assert all_head() == 2
    end

    test "on a store with history and no fact, reports every requirement" do
      :ok = EventStore.append_to_stream("a", 0, [event(["entity:x"])])

      assert TagIndex.bootstrap(EventStore, ["entity"], enabling()) ==
               {:error, {:tag_index_not_enabled, ["entity"]}}

      refute_received {:enable, _requirements}
      assert all_head() == 1
    end

    test "answers the read's error for a fact it cannot attribute" do
      restart_store(deletable: [event_types: ["hidden"]])

      hidden = %EventData{fact(entity: :disabled) | event_type: "hidden"}
      :ok = EventStore.append_to_stream("a", 0, [fact(entity: :enabled), hidden])

      assert TagIndex.bootstrap(EventStore, ["entity"], enabling()) ==
               {:error, :unattributable_tag_index_fact}

      refute_received {:enable, _requirements}
    end

    test "refuses requirements that are empty, repeated or outside the grammar, reading nothing" do
      key = TagIndex.fingerprint("k")

      for requirements <- [
            [],
            ["entity", "entity"],
            ["entity", {"entity", nil}],
            [{"email", key}, "command", {"email", key}],
            ["a:b"],
            ["tag_index"],
            [{"entity", "-"}],
            [:entity],
            ["entity" | "command"],
            "entity",
            nil
          ] do
        assert TagIndex.bootstrap(EventStore, requirements, enabling(), conn: @no_conn) ==
                 {:error, :invalid_tag_index_requirements},
               inspect(requirements)
      end

      refute_received {:enable, _requirements}
    end
  end

  defp state(family, state), do: TagIndex.state_tag(family, nil, state)

  # One fact, a state tag for each keyless family.
  defp fact(states) do
    event([@fact | Enum.map(states, fn {family, state} -> state(to_string(family), state) end)])
  end

  # An `enable` that tells the test each call. By default it appends one fact enabling every
  # requirement; given families, one fact enabling those alone; given anything else, it answers
  # that and appends nothing.
  defp enabling(answer \\ :requirements) do
    test = self()

    fn requirements ->
      send(test, {:enable, requirements})

      case answer do
        :requirements -> append_enable(requirements)
        families when is_list(families) -> append_enable(families)
        answer -> answer
      end
    end
  end

  defp append_enable(requirements) do
    tags =
      for requirement <- requirements do
        case requirement do
          {family, key} -> TagIndex.state_tag(family, key, :enabled)
          family -> state(family, :enabled)
        end
      end

    EventStore.append_to_stream("tag-index", :any_version, [event([@fact | tags])])
  end

  defp all_head do
    {:ok, %StreamInfo{stream_version: version}} = EventStore.stream_info(:all)
    version
  end

  defp restart_store(opts) do
    stop_supervised!(TestEventStore)

    start_supervised!(
      {TestEventStore,
       [conditional_appends: [stream_prefixes: ["decision-"]], serializer: PassThroughSerializer] ++
         opts}
    )
  end

  defp event(tags) do
    %EventData{event_type: "typed", data: "{}", metadata: "{}", tags: tags}
  end

  defp events(count) do
    for _index <- 1..count//1, do: event([])
  end
end
