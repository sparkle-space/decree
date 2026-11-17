defmodule Decree.Decision.RuleConformanceTest do
  use Decree.RuleCase

  alias Decree.FrameworkStorage
  alias Decree.RuleFixtures.DigestChecked
  alias Decree.Store.{Config, EventData, LockSession}

  @read [:decree, :decision, :read]
  @retry [:decree, :decision, :retry]

  setup do
    attach_telemetry([@read, @retry])
    :ok
  end

  defp holding_followup(test) do
    fn _registrations -> hold(test, :between_reads) end
  end

  defp deciding_on(test), do: &send(test, {:deciding, &1})

  defp after_of({:append_to_stream, _stream, _version, _count, opts}) do
    opts |> Keyword.fetch!(:condition) |> Keyword.fetch!(:after)
  end

  describe "the two-part condition" do
    test "CC-1: a registration between the reads fails through part 1, because `after` is p1" do
      test = self()
      digest = digest()

      released = registered!(digest)
      erase!(released)

      command =
        register(digest, between_reads: holding_followup(test), deciding: deciding_on(test))

      task = dispatch_async(command)

      assert_receive {:between_reads, pid}

      rival = registered!(digest)
      send(pid, :go)

      # The first attempt has no holder: part 2 released the one it read, and it read no rival.
      assert_receive {:deciding, [%Registered{user_id: ^released}, %Erased{user_id: ^released}]}

      assert_receive {:between_reads, ^pid}
      send(pid, :go)

      assert {:error, :email_taken} = Task.await(task, :infinity)

      assert_received {:deciding, events}
      assert %Registered{user_id: ^rival} = List.last(events)

      assert [{%{attempt: 1}, %{reason: :append_condition_failed, decision: decision}}] =
               received(@retry)

      assert decision == Decree.RuleFixtures.EmailRule
      assert stored(user_stream(command), App) == []
    end

    test "CC-2: a release between the reads fails through part 2, and the retry appends" do
      test = self()
      digest = digest()
      holder = registered!(digest)

      command =
        register(digest, between_reads: holding_followup(test), deciding: deciding_on(test))

      task = dispatch_async(command)

      assert_receive {:between_reads, pid}

      erase!(holder)
      send(pid, :go)

      assert_receive {:between_reads, ^pid}
      send(pid, :go)

      assert :ok = Task.await(task, :infinity)

      # A spurious conflict: both attempts saw the same two events, and both decided to append.
      assert_received {:deciding, [%Registered{}, %Erased{user_id: ^holder}]}
      assert_received {:deciding, [%Registered{}, %Erased{user_id: ^holder}]}

      assert [{%{attempt: 1}, %{reason: :append_condition_failed}}] = received(@retry)

      assert [%{attempt: 1}, %{attempt: 2}] =
               @read |> received() |> Enum.take(-2) |> Enum.map(&elem(&1, 1))

      assert [%{data: %Registered{}}] = stored(user_stream(command), App)
    end

    test "CC-3: no holder and no history: `after` is the store's head, and it appends" do
      Process.register(self(), :rule_recorder)

      head = seed(%Noted{id: "unrelated"}, ["unrelated:1"], App)
      assert head > 0

      command = register(digest())

      assert :ok = App.dispatch(command)

      assert_received {:append_to_stream, "user-" <> _, 0, 1, _opts} = append
      assert after_of(append) == head

      assert [%{data: %Registered{}}] = stored(user_stream(command), App)
    end

    test "CC-3: on a store holding nothing but the facts `after` is what the read said" do
      Process.register(self(), :rule_recorder)

      assert :ok = App.dispatch(register(digest()))

      assert_received {:append_to_stream, _stream, 0, 1, _opts} = append
      assert after_of(append) == fact_count()
    end

    test "CC-3: a released address registers again, which `after: 0` would fail for ever" do
      digest = digest()

      released = registered!(digest)
      erase!(released)

      command = register(digest)

      assert :ok = App.dispatch(command)
      assert [%{data: %Registered{}}] = stored(user_stream(command), App)
      assert [] = received(@retry)
    end

    test "CC-3: the address is held again after that" do
      digest = digest()

      erase!(registered!(digest))
      registered!(digest)

      assert {:error, :email_taken} = App.dispatch(register(digest))
    end

    test "CC-4: a nil email appends with no `email:` tag, and reads nothing" do
      Process.register(self(), :rule_recorder)

      command = register(nil)

      assert :ok = App.dispatch(command)

      assert_received {:append_to_stream, "user-" <> _, 0, 1, []}
      refute_received {:read_query, _items, _opts}
      refute_received {:read_tags, _positions}
      assert [] = received(@read)

      position = head()

      assert {:ok, %{^position => tags}} = EventStore.read_tags(App, [position])
      assert ("user:" <> command.user_id) in tags
      refute Enum.any?(tags, &String.starts_with?(&1, "email:"))
    end

    test "CC-7: a candidate whose `erased_by` names another isn't read as a release" do
      test = self()
      digest = digest()
      holder = registered!(digest)

      # `holder` erased someone else, which part 2 of `holder` must not find.
      erase!(registered!(digest()), holder)

      command = register(digest, deciding: deciding_on(test))

      assert {:error, :email_taken} = App.dispatch(command)
      assert_received {:deciding, [%Registered{user_id: ^holder}]}
    end

    test "CC-13, the nil-email half: a nil email holds nothing, with the kind in use" do
      digest = digest()
      registered!(digest)

      first = register(nil)
      second = register(nil)

      assert :ok = App.dispatch(first)
      assert :ok = App.dispatch(second)

      assert [_event] = stored(user_stream(first), App)
      assert [_event] = stored(user_stream(second), App)

      assert {:error, :email_taken} = App.dispatch(register(digest))
    end
  end

  describe "two registrations racing, by lock state" do
    setup do
      postgrex_config = Config.default_postgrex_opts(FrameworkStorage.config())

      [observer: LockSession.connect(postgrex_config), holder: LockSession.start(postgrex_config)]
    end

    # An uncommitted append holds `$all`.
    defp hold_all(%LockSession{} = holder) do
      store = Module.concat([App, "FrameworkEventStore"])

      event = %EventData{
        event_type: Atom.to_string(Noted),
        data: %Noted{id: "holder"},
        metadata: %{}
      }

      LockSession.run(holder, fn tx ->
        :ok =
          Decree.FrameworkEventStore.append_to_stream(
            "other-" <> UUID.uuid4(),
            :any_version,
            [event],
            name: store,
            conn: tx
          )
      end)
    end

    defp email_rows(observer, digest) do
      %Postgrex.Result{rows: [[count]]} =
        Postgrex.query!(observer, "SELECT count(*) FROM event_tags WHERE tag = $1", [
          "email:" <> digest
        ])

      count
    end

    test "exactly one commits, and the other's retry sees the holder", context do
      %{holder: holder, observer: observer} = context

      digest = digest()
      first = register(digest)
      second = register(digest)

      hold_all(holder)

      one = dispatch_async(first)
      other = dispatch_async(second)

      # Both are at the append while `$all` is held, so neither read can have seen the other.
      LockSession.wait_until_waiting(observer, holder.backend, 2)

      assert email_rows(observer, digest) == 0

      assert {:ok, :ok} = LockSession.commit(holder)

      answers = [Task.await(one, :infinity), Task.await(other, :infinity)]

      assert Enum.sort(answers) == Enum.sort([:ok, {:error, :email_taken}])
      assert email_rows(observer, digest) == 1

      assert [{%{attempt: 1}, %{reason: :append_condition_failed}}] = received(@retry)

      assert [_one] = stored(user_stream(first), App) ++ stored(user_stream(second), App)
    end
  end

  describe "the tag index" do
    defp noting(test, label) do
      fn %RuledAggregate{}, %{id: id} ->
        hold(test, label)
        %Noted{id: id, note: "held"}
      end
    end

    defp digest_checked(tag) do
      item = [types: [Atom.to_string(Read)], tags: [tag]]

      ruled([query: fn _command, _context -> [item] end], DigestChecked)
    end

    test "CC-5: an unavailable digest key: `query/2` answers an error, nothing stored" do
      Process.register(self(), :rule_recorder)

      command = ruled(query: fn _command, _context -> {:error, :digest_key_unavailable} end)

      assert {:error, :digest_key_unavailable} = App.dispatch(command)

      refute_received {:read_query, _items, _opts}
      assert stored(ruled_stream(command), App) == []
    end

    test "CC-6: a disable between the read and the append fails the condition, the retry refuses" do
      test = self()
      command = ruled(execute: noting(test, :executing))

      task = dispatch_async(command)

      assert_receive {:executing, pid}
      disable_index!("entity")
      send(pid, :go)

      assert {:error, :tag_index_not_enabled} = Task.await(task, :infinity)

      assert [{%{attempt: 1}, %{reason: :append_condition_failed}}] = received(@retry)
      assert stored(ruled_stream(command), App) == []
    end

    test "CC-6: a disable between the two reads fails the same way" do
      test = self()
      held = make_ref()

      # Held on the first attempt only: the retry reads the disable and refuses at step 3.
      followup = fn command, _context, _events ->
        if not Process.get(held, false) do
          Process.put(held, true)
          hold(test, :between_reads)
        end

        [[types: [Atom.to_string(Read)], tags: ["scope:" <> command.scope]]]
      end

      command = ruled(followup: followup)
      task = dispatch_async(command)

      assert_receive {:between_reads, pid}
      disable_index!("entity")
      send(pid, :go)

      assert {:error, :tag_index_not_enabled} = Task.await(task, :infinity)

      assert [{%{attempt: 1}, %{reason: :append_condition_failed}}] = received(@retry)
      assert stored(ruled_stream(command), App) == []
    end

    test "CC-9: a repair of a family with no key refuses between, and appends after" do
      disable_index!("entity")

      between = ruled()
      assert {:error, :tag_index_not_enabled} = App.dispatch(between)
      assert stored(ruled_stream(between), App) == []

      enable_index!("entity")

      after_repair = ruled()
      assert :ok = App.dispatch(after_repair)
      assert [_one] = stored(ruled_stream(after_repair), App)
    end

    test "CC-12: enable, retire, enable again refuses, and its dispatches refuse throughout" do
      fingerprint = TagIndexFixtures.fingerprint()
      tag = TagIndexFixtures.digest_tag("email", "d")

      assert :ok = App.dispatch(digest_checked(tag))

      disable_index!("email", fingerprint, "key_retired")
      assert {:error, :tag_index_not_enabled} = App.dispatch(digest_checked(tag))

      assert {:error, :tag_index_key_retired} =
               App.dispatch(TagIndexFixtures.enable_command([{"email", fingerprint}]))

      assert {:error, :tag_index_not_enabled} = App.dispatch(digest_checked(tag))
    end

    test "CC-12: retired while a repair holds it disabled, it is never enabled again" do
      fingerprint = TagIndexFixtures.fingerprint()
      tag = TagIndexFixtures.digest_tag("email", "d")

      assert :ok = App.dispatch(digest_checked(tag))

      index_id = disable_index!("email", fingerprint, "repair")
      assert {:error, :tag_index_not_enabled} = App.dispatch(digest_checked(tag))

      assert :ok = App.dispatch(TagIndexFixtures.disable_command(index_id, "key_retired"))
      assert {:error, :tag_index_not_enabled} = App.dispatch(digest_checked(tag))

      assert {:error, :tag_index_key_retired} =
               App.dispatch(TagIndexFixtures.enable_command([{"email", fingerprint}]))

      # A restored key: the same material, so the same fingerprint, meets the retirement.
      restored = Decree.Store.TagIndex.fingerprint(TagIndexFixtures.key())

      assert {:error, :tag_index_not_enabled} =
               App.dispatch(digest_checked("email:" <> restored <> ":d"))
    end

    test "CC-14: a disabled `command` family leaves a rule route appending, having no command item" do
      disable_index!("command")

      command = ruled()

      assert :ok = App.dispatch(command)
      assert [_one] = stored(ruled_stream(command), App)
      assert :ok = App.dispatch(register(digest()))
    end

    test "CC-14: a disabled `entity` family refuses a rule whose items carry an entity tag" do
      disable_index!("entity")

      command = ruled()

      assert {:error, :tag_index_not_enabled} = App.dispatch(command)
      assert stored(ruled_stream(command), App) == []
    end
  end

  describe "UF-1, item keys" do
    # Sends what rode to the test. `matched_keys` is read in the test process, so a result with
    # no such field fails the test there, not the aggregate.
    defp riding(test) do
      fn %RuledAggregate{}, %{id: id, rule_result: %Result{} = result} ->
        send(test, {:rode, result})

        %Noted{id: id, note: "keyed"}
      end
    end

    defp keyed(tags, key), do: [types: [Atom.to_string(Read)], tags: tags, key: key]

    defp unkeyed(tags), do: [types: [Atom.to_string(Read)], tags: tags]

    defp rode do
      assert_received {:rode, %Result{events: events} = result}

      matched_keys = Map.fetch!(result, :matched_keys)
      assert length(matched_keys) == length(events)

      {events, matched_keys}
    end

    defp notes(events), do: Enum.map(events, & &1.note)

    test "UF-1: an event two keyed items match carries both keys, parallel to `events`" do
      email = "email:" <> digest()
      uid = "uid:" <> scope()

      seed(%Read{note: "both"}, [email, uid], App)
      seed(%Read{note: "email"}, [email], App)
      seed(%Read{note: "uid"}, [uid], App)
      seed(%Read{note: "unrelated"}, ["uid:" <> scope()], App)

      query = fn _command, _context -> [keyed([email], :email), keyed([uid], :oauth_uid)] end

      assert :ok = App.dispatch(ruled(query: query, execute: riding(self())))

      assert {events, matched_keys} = rode()
      assert notes(events) == ["both", "email", "uid"]
      assert matched_keys == [[:email, :oauth_uid], [:email], [:oauth_uid]]
    end

    test "UF-1: a follow-up item's key rides after the first read's, a tuple key included" do
      email = "email:" <> digest()
      candidate = UUID.uuid4()
      release = "release:" <> candidate

      seed(%Read{note: "first only"}, [email], App)
      seed(%Read{note: "both reads"}, [email, release], App)
      seed(%Read{note: "follow-up only"}, [release], App)

      command =
        ruled(
          query: fn _command, _context -> [keyed([email], :email)] end,
          followup: fn _command, _context, _events ->
            [keyed([release], {:release, candidate})]
          end,
          execute: riding(self())
        )

      assert :ok = App.dispatch(command)

      assert {events, matched_keys} = rode()
      assert notes(events) == ["first only", "both reads", "follow-up only"]

      assert matched_keys == [
               [:email],
               [:email, {:release, candidate}],
               [{:release, candidate}]
             ]
    end

    test "UF-1: an unkeyed item contributes nothing, and a rule with no key gives `[]` each" do
      scope = scope()

      seed(%Read{note: "one"}, ["scope:" <> scope], App)
      seed(%Read{note: "two"}, ["scope:" <> scope], App)

      assert :ok = App.dispatch(ruled(scope: scope, execute: riding(self())))

      assert {events, matched_keys} = rode()
      assert notes(events) == ["one", "two"]
      assert matched_keys == [[], []]
    end

    test "UF-1: an event matched by a keyed and an unkeyed item carries the key alone" do
      tag = "scope:" <> scope()
      plain = "plain:" <> scope()

      seed(%Read{note: "both items"}, [tag], App)
      seed(%Read{note: "unkeyed only"}, [plain], App)

      query = fn _command, _context ->
        [keyed([tag], :scope), unkeyed([tag]), unkeyed([plain])]
      end

      assert :ok = App.dispatch(ruled(query: query, execute: riding(self())))

      assert {events, matched_keys} = rode()
      assert notes(events) == ["both items", "unkeyed only"]
      assert matched_keys == [[:scope], []]
    end

    test "UF-1: keys come in item order, each once, across both reads" do
      a = "a:" <> scope()
      b = "b:" <> scope()
      c = "c:" <> scope()

      seed(%Read{note: "all"}, [a, b, c], App)
      seed(%Read{note: "c"}, [c], App)

      # Item order is the reverse of term order, and `:z` names two items of the first read and
      # one of the follow-up.
      command =
        ruled(
          query: fn _command, _context -> [keyed([a], :z), keyed([b], :a), keyed([c], :z)] end,
          followup: fn _command, _context, _events -> [keyed([c], :z), keyed([a], :y)] end,
          execute: riding(self())
        )

      assert :ok = App.dispatch(command)

      assert {events, matched_keys} = rode()
      assert notes(events) == ["all", "c"]
      assert matched_keys == [[:z, :a, :y], [:z]]
    end

    test "UF-1: a `nil` key is a key" do
      tag = "keyed:" <> scope()
      plain = "plain:" <> scope()

      seed(%Read{note: "nil key"}, [tag], App)
      seed(%Read{note: "no key"}, [plain], App)

      query = fn _command, _context -> [keyed([tag], nil), unkeyed([plain])] end

      assert :ok = App.dispatch(ruled(query: query, execute: riding(self())))

      assert {events, matched_keys} = rode()
      assert notes(events) == ["nil key", "no key"]
      assert matched_keys == [[nil], []]
    end

    test "UF-1: a map item's `:key` works as the keyword one, in either read" do
      first = "first:" <> scope()
      second = "second:" <> scope()
      type = Atom.to_string(Read)

      seed(%Read{note: "first"}, [first], App)
      seed(%Read{note: "second"}, [second], App)

      command =
        ruled(
          query: fn _command, _context -> [%{types: [type], tags: [first], key: :first}] end,
          followup: fn _command, _context, _events ->
            [%{types: [type], tags: [second], key: :second}]
          end,
          execute: riding(self())
        )

      assert :ok = App.dispatch(command)

      assert {events, matched_keys} = rode()
      assert notes(events) == ["first", "second"]
      assert matched_keys == [[:first], [:second]]
    end

    test "UF-1: `key:` twice in one item is `:invalid_query`, with no read and nothing stored" do
      Process.register(self(), :rule_recorder)

      tags = ["scope:" <> scope()]
      twice = unkeyed(tags) ++ [key: :one, key: :two]

      command = ruled(query: fn _command, _context -> [twice] end)

      assert {:error, :invalid_query} = App.dispatch(command)

      refute_received {:read_query, _items, _opts}
      assert [] = received(@read)
      assert stored(ruled_stream(command), App) == []

      # The repeat is what refuses: the same item with one `key:` appends.
      once = ruled(query: fn _command, _context -> [unkeyed(tags) ++ [key: :one]] end)

      assert :ok = App.dispatch(once)
      assert [_one] = stored(ruled_stream(once), App)
    end

    test "UF-1: the condition's items carry no key, and equal the same items unkeyed" do
      Process.register(self(), :rule_recorder)

      tag = "scope:" <> scope()
      uid = "uid:" <> scope()
      release = "release:" <> scope()
      type = Atom.to_string(Read)

      seed(%Read{note: "read"}, [tag, uid], App)

      first = [unkeyed([tag]), %{types: [type], tags: [uid]}]
      second = [unkeyed([release])]

      keyed_first = [unkeyed([tag]) ++ [key: :scope], %{types: [type], tags: [uid], key: :uid}]
      keyed_second = [unkeyed([release]) ++ [key: {:release, "r"}]]

      condition = fn first, second ->
        command =
          ruled(
            query: fn _command, _context -> first end,
            followup: fn _command, _context, _events -> second end
          )

        assert :ok = App.dispatch(command)

        stream = ruled_stream(command)
        assert_received {:append_to_stream, ^stream, 0, 1, opts}

        opts |> Keyword.fetch!(:condition) |> Keyword.fetch!(:items)
      end

      keyed_items = condition.(keyed_first, keyed_second)
      unkeyed_items = condition.(first, second)

      index_item = [tags: [Decree.Store.TagIndex.fact_tag()]]

      assert unkeyed_items == first ++ second ++ [index_item]
      assert keyed_items == unkeyed_items

      refute Enum.any?(keyed_items, fn
               item when is_list(item) -> Keyword.has_key?(item, :key)
               item when is_map(item) -> Map.has_key?(item, :key)
             end)
    end

    test "UF-1: `[]` from `query/2` gives empty `events` and `matched_keys`" do
      command = ruled(query: fn _command, _context -> [] end, execute: riding(self()))

      assert :ok = App.dispatch(command)

      assert {[], []} = rode()
    end
  end

  # The `$all` head: the last position anything wrote.
  defp head do
    store = Module.concat([App, "FrameworkEventStore"])

    {:ok, events} = Decree.FrameworkEventStore.read_all_streams_forward(0, 1_000, name: store)

    List.last(events).event_number
  end
end
