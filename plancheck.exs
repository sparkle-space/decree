# EXPLAINs this side's own read statement on the gate store, with the facts' query, and says
# whether the plan contains a Seq Scan on stream_events. PLAN_SIDE labels the output.
Logger.configure(level: :warning)
{:ok, _} = Application.ensure_all_started(:postgrex)
config = Decree.Store.Config.parsed(Gate.Store, :gate_bench)
{:ok, c} = Postgrex.start_link(Decree.Store.Config.default_postgrex_opts(config))

statement =
  "public" |> Decree.Store.Storage.Reader.Query.read_query_statement(:forward) |> IO.iodata_to_binary()

{:ok, query} = Decree.Store.Query.compile([[tags: ["tag_index:fact"]]])

base = [query.probe_items, query.probe_tags, query.probe_types, query.rest_items, query.rest_tags, 0, 1001, nil]
# The fix side's statement takes three more parameters (tag_items, tag_kinds, event_types).
params = if String.contains?(statement, "$9"), do: base ++ [[0], [], []], else: base

%{rows: rows} = Postgrex.query!(c, "EXPLAIN " <> statement, params)
plan = rows |> List.flatten() |> Enum.join("\n")
seq = Regex.match?(~r/Seq Scan on stream_events/, plan)
%{rows: [[head]]} = Postgrex.query!(c, "select stream_version from streams where stream_id = 0", [])
IO.puts("plan check #{System.fetch_env!("PLAN_SIDE")} at store #{head}: Seq Scan on stream_events: #{if seq, do: "yes", else: "no"}")
if System.get_env("PLAN_VERBOSE") == "1", do: IO.puts(plan)
