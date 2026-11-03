# One long-lived arm. BENCH_MODE: noop (quantity 2), rule_cold, rule_warm, decision (quantity 3).
# The decomposition modes (branch only) isolate the index facts' `read_tags` round trip:
# decision_rt (a decision, then one more `read_tags` of the facts, both timed), decision_tel (a
# decision; the value is its own `read_tags` span from telemetry), rule_cold_tel (the same for a
# cold rule route) and read_tags (that call alone).
# On each stdin line runs one block and prints its median in µs.
Logger.configure(level: :warning)
mode = System.fetch_env!("BENCH_MODE")
n = String.to_integer(System.get_env("BENCH_N", "501"))
uuid = &Decree.UUID.uuid4/0
median = fn l -> l |> Enum.sort() |> Enum.at(div(length(l), 2)) end
{:ok, _} = Application.ensure_all_started(:decree)

time = fn fun ->
  a = System.monotonic_time(:nanosecond)
  fun.()
  (System.monotonic_time(:nanosecond) - a) / 1000
end

event = fn type, data, tags ->
  %Decree.Store.EventData{event_type: type, data: data, metadata: %{}, tags: tags}
end

seed = fn ->
  digest = uuid.()
  holder = uuid.()

  :ok =
    Gate.Store.append_to_stream(
      holder,
      :any_version,
      [
        event.("Elixir.Gate.Registered", %Gate.Registered{user_id: holder, digest: digest, note: "n"}, [Gate.Shape.email_tag(digest)]),
        event.("Elixir.Gate.Released", %Gate.Released{user_id: holder}, ["user:" <> holder])
      ],
      name: Gate.Store
    )

  digest
end

one =
  case mode do
    "noop" ->
      {:ok, _} = Gate.NoopApp.start_link()
      user = uuid.()

      fn ->
        c = %Gate.Do{user_id: user, thing_id: uuid.(), tenant_id: uuid.(), command_id: uuid.()}
        time.(fn -> :ok = Gate.NoopApp.dispatch(c) end)
      end

    "rule_cold" ->
      {:ok, _} = Gate.App.start_link()

      fn ->
        c = %Gate.Register{user_id: uuid.(), digest: seed.(), command_id: uuid.()}
        time.(fn -> :ok = Gate.App.dispatch(c) end)
      end

    "rule_warm" ->
      {:ok, _} = Gate.App.start_link()
      user = uuid.()

      fn ->
        c = %Gate.Register{user_id: user, digest: seed.(), command_id: uuid.()}
        time.(fn -> :ok = Gate.App.dispatch(c) end)
      end

    "plain_cold" ->
      {:ok, _} = Gate.App.start_link()

      fn ->
        c = %Gate.PlainDo{user_id: uuid.(), digest: seed.(), command_id: uuid.()}
        time.(fn -> :ok = Gate.App.dispatch(c) end)
      end

    "plain_warm" ->
      {:ok, _} = Gate.App.start_link()
      user = uuid.()

      fn ->
        c = %Gate.PlainDo{user_id: user, digest: seed.(), command_id: uuid.()}
        time.(fn -> :ok = Gate.App.dispatch(c) end)
      end

    "decision" ->
      {:ok, _} = Gate.App.start_link()

      fn ->
        c = %Gate.Claim{user_id: uuid.(), digest: seed.(), command_id: uuid.()}
        time.(fn -> :ok = Gate.App.dispatch(c) end)
      end

    "decision_rt" ->
      {:ok, _} = Gate.App.start_link()
      positions = Gate.Decompose.fact_positions()

      fn ->
        c = %Gate.Claim{user_id: uuid.(), digest: seed.(), command_id: uuid.()}

        time.(fn ->
          :ok = Gate.App.dispatch(c)
          Gate.Decompose.read_facts(positions)
        end)
      end

    "decision_tel" ->
      {:ok, _} = Gate.App.start_link()
      Gate.Decompose.attach()

      fn ->
        c = %Gate.Claim{user_id: uuid.(), digest: seed.(), command_id: uuid.()}
        :ok = Gate.App.dispatch(c)
        Gate.Decompose.one_span()
      end

    "rule_cold_tel" ->
      {:ok, _} = Gate.App.start_link()
      Gate.Decompose.attach()

      fn ->
        c = %Gate.Register{user_id: uuid.(), digest: seed.(), command_id: uuid.()}
        :ok = Gate.App.dispatch(c)
        Gate.Decompose.one_span()
      end

    "read_tags" ->
      {:ok, _} = Gate.App.start_link()
      positions = Gate.Decompose.fact_positions()

      fn -> time.(fn -> Gate.Decompose.read_facts(positions) end) end
  end

for _ <- 1..String.to_integer(System.get_env("BENCH_WARM", "300")), do: one.()
IO.puts("READY")

loop = fn loop ->
  case IO.gets("") do
    :eof ->
      :ok

    _ ->
      IO.puts("BLOCK #{median.(for _ <- 1..n, do: one.())}")
      loop.(loop)
  end
end

loop.(loop)
