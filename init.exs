# Creates and initialises the gate store, prints the store's append functions' md5 and the
# server's settings, and with GATE_FACTS=1 appends the eight index facts. GATE_RESET=1 drops
# the gate database first, so every run starts from a fresh store.
Logger.configure(level: :warning)
{:ok, _} = Application.ensure_all_started(:postgrex)
config = Decree.Store.Config.parsed(Gate.Store, :gate_bench)

if System.get_env("GATE_RESET") == "1" do
  admin = Keyword.put(Decree.Store.Config.default_postgrex_opts(config), :database, "postgres")
  {:ok, a} = Postgrex.start_link(admin)
  Postgrex.query!(a, ~s[DROP DATABASE IF EXISTS "#{config[:database]}" WITH (FORCE)], [])
  IO.puts("dropped #{config[:database]}")
  GenServer.stop(a)
end

Decree.Store.Tasks.Create.exec(config, [])
Decree.Store.Tasks.Init.exec(config, [])
{:ok, c} = Postgrex.start_link(Decree.Store.Config.default_postgrex_opts(config))

%{rows: [[md5]]} =
  Postgrex.query!(
    c,
    "select md5(string_agg(pg_get_functiondef(p.oid), E'\\n' order by p.proname)) from pg_proc p join pg_namespace n on n.oid = p.pronamespace where n.nspname = 'public' and p.proname like 'append_events%'",
    []
  )

IO.puts("#{System.get_env("BENCH_ARM")} append_events functions md5: #{md5}")
%{rows: [[version]]} = Postgrex.query!(c, "select version()", [])
%{rows: [[buffers]]} = Postgrex.query!(c, "show shared_buffers", [])
IO.puts("postgres: #{version}; shared_buffers #{buffers}")

if System.get_env("GATE_FACTS") == "1" do
  {:ok, _} = Application.ensure_all_started(:decree)
  {:ok, _} = Gate.Store.start_link(name: Gate.Store)
  fp = Gate.Shape.fp()

  facts =
    for {family, key} <- [
          {"command", "-"},
          {"command_kind", "-"},
          {"correlation", "-"},
          {"tenant_slug", "-"},
          {"plan_slug", "-"},
          {"email", fp},
          {"oauth_uid", fp},
          {"entity", "-"}
        ] do
      %Decree.Store.EventData{
        event_type: "Elixir.Gate.IndexFact",
        data: %Gate.IndexFact{family: family, key: key, state: "enabled"},
        metadata: %{},
        tags: ["tag_index:fact", "tag_index_state:#{family}:#{key}:enabled"]
      }
    end

  :ok = Gate.Store.append_to_stream("gate-index-facts", :no_stream, facts, name: Gate.Store)
  IO.puts("facts appended: #{length(facts)}")
end

# A small store: gate-shaped filler up to GATE_FILLER events in `$all`, then ANALYZE, so the planner
# has statistics as a production store's autovacuum would give it. Each pair is a Registered and a
# Released on a fresh holder stream, the same shape as the arms' own seed.
filler = String.to_integer(System.get_env("GATE_FILLER", "0"))

if filler > 0 do
  head = fn ->
    %{rows: [[v]]} = Postgrex.query!(c, "select stream_version from streams where stream_id = 0", [])
    v
  end

  uuid = &Decree.UUID.uuid4/0

  ev = fn type, data, tags ->
    %Decree.Store.EventData{event_type: type, data: data, metadata: %{}, tags: tags}
  end

  pair = fn ->
    digest = uuid.()
    holder = uuid.()

    :ok =
      Gate.Store.append_to_stream(
        holder,
        :any_version,
        [
          ev.("Elixir.Gate.Registered", %Gate.Registered{user_id: holder, digest: digest, note: "n"}, [Gate.Shape.email_tag(digest)]),
          ev.("Elixir.Gate.Released", %Gate.Released{user_id: holder}, ["user:" <> holder])
        ],
        name: Gate.Store
      )
  end

  need = filler - head.()
  if need < 0, do: raise("store already holds #{head.()} events, above GATE_FILLER #{filler}")
  for _ <- 1..div(need, 2)//1, do: pair.()

  if rem(need, 2) == 1 do
    digest = uuid.()
    holder = uuid.()

    :ok =
      Gate.Store.append_to_stream(
        holder,
        :any_version,
        [ev.("Elixir.Gate.Registered", %Gate.Registered{user_id: holder, digest: digest, note: "n"}, [Gate.Shape.email_tag(digest)])],
        name: Gate.Store
      )
  end

  %{rows: tables} = Postgrex.query!(c, "select tablename from pg_tables where schemaname = 'public'", [])
  for [t] <- tables, do: Postgrex.query!(c, ~s[ANALYZE "public"."#{t}"], [])
  IO.puts("analyzed #{length(tables)} store tables")
  IO.puts("store events after setup: #{head.()}")
end
