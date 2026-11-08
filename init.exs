# Creates and initialises the gate store, prints the store's append functions' md5 and the
# server's settings, and with GATE_FACTS=1 appends the eight index facts. GATE_RESET=1 drops
# the gate database first, so every run starts from a fresh store.
# GATE_PRODSHAPE=1 (sparklespace#7787, method A), after the facts: the production-shaped store
# from `AppendGate.Prefill` in $AG_HOME/prefill.ex at AG_SCALE (default 1.0), then VACUUM ANALYZE
# on every store table and a CHECKPOINT. It runs only with GATE_FACTS=1 and excludes
# GATE_FILLER > 0. The store's notification publisher reads every append back through the JSON
# serializer, so an event type with no loaded struct stops the store's supervisor: prefill.ex
# defines its own types (`AppendGate.Event<i>`, field `pad`) for that reason.
Logger.configure(level: :warning)
prodshape = System.get_env("GATE_PRODSHAPE") == "1"
filler = String.to_integer(System.get_env("GATE_FILLER", "0"))

if prodshape and filler > 0,
  do: raise("GATE_PRODSHAPE=1 and GATE_FILLER=#{filler} are exclusive: set one")

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
head = fn ->
  %{rows: [[v]]} =
    Postgrex.query!(c, "select stream_version from streams where stream_id = 0", [])

  v
end

if filler > 0 do
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
          ev.(
            "Elixir.Gate.Registered",
            %Gate.Registered{user_id: holder, digest: digest, note: "n"},
            [Gate.Shape.email_tag(digest)]
          ),
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
        [
          ev.(
            "Elixir.Gate.Registered",
            %Gate.Registered{user_id: holder, digest: digest, note: "n"},
            [Gate.Shape.email_tag(digest)]
          )
        ],
        name: Gate.Store
      )
  end

  %{rows: tables} =
    Postgrex.query!(c, "select tablename from pg_tables where schemaname = 'public'", [])

  for [t] <- tables, do: Postgrex.query!(c, ~s[ANALYZE "public"."#{t}"], [])
  IO.puts("analyzed #{length(tables)} store tables")
  IO.puts("store events after setup: #{head.()}")
end

# The production-shaped store: the prefill's appends, each through the store's own append (the
# JSON serializer included), then VACUUM ANALYZE per table (VACUUM cannot run in a transaction,
# so one plain statement each) and a CHECKPOINT, so every arm starts from a clean, flushed store.
if prodshape and System.get_env("GATE_FACTS") == "1" do
  Code.require_file(Path.join(System.fetch_env!("AG_HOME"), "prefill.ex"))
  {scale, ""} = Float.parse(System.get_env("AG_SCALE", "1.0"))

  append = fn stream_uuid, maps ->
    events =
      Enum.map(maps, fn map ->
        %Decree.Store.EventData{
          event_type: Map.fetch!(map, :event_type),
          data: Map.fetch!(map, :data),
          metadata: Map.fetch!(map, :metadata),
          tags: Map.fetch!(map, :tags)
        }
      end)

    :ok = Gate.Store.append_to_stream(stream_uuid, :any_version, events, name: Gate.Store)
  end

  counts = AppendGate.Prefill.run(append, scale: scale)
  schema = Keyword.get(config, :schema, "public")

  %{rows: tables} =
    Postgrex.query!(c, "select tablename from pg_tables where schemaname = $1", [schema])

  for [t] <- tables,
      do: Postgrex.query!(c, ~s[VACUUM ANALYZE "#{schema}"."#{t}"], [], timeout: :infinity)

  Postgrex.query!(c, "CHECKPOINT", [], timeout: :infinity)
  IO.puts("vacuum analyzed #{length(tables)} store tables; checkpoint done")
  IO.puts("prodshape: #{inspect(counts)}")
  IO.puts("store events after setup: #{head.()}")
end
