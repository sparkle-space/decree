defmodule Decree.Store.TagIndexTest do
  use Decree.Store.StorageCase

  import Decree.Store.StoreRows

  alias Decree.Store.{Config, EventData, LockSession, TagIndex}
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

  defp event(tags) do
    %EventData{event_type: "typed", data: "{}", metadata: "{}", tags: tags}
  end

  defp events(count) do
    for _index <- 1..count//1, do: event([])
  end
end
