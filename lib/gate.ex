defmodule Gate.Store do
  use Decree.Store, otp_app: :gate_bench
end

defmodule Gate.Thing do
  @derive Jason.Encoder
  defstruct [:thing_id, :tenant_id, :note]

  defimpl Decree.Event.Tags do
    def tags(%{thing_id: thing, tenant_id: tenant}, _context) do
      {:ok, ["thing:" <> thing, "tenant:" <> tenant]}
    end
  end
end

defmodule Gate.Do do
  defstruct [:user_id, :thing_id, :tenant_id, :command_id]
end

defmodule Gate.Agg do
  defstruct n: 0

  def execute(%__MODULE__{}, %Gate.Do{} = c) do
    %Gate.Thing{thing_id: c.thing_id, tenant_id: c.tenant_id, note: String.duplicate("x", 300)}
  end

  def apply(%__MODULE__{n: n} = state, %Gate.Thing{}), do: %__MODULE__{state | n: n + 1}
end

defmodule Gate.Router do
  use Decree.Commands.Router

  dispatch Gate.Do, to: Gate.Agg, identity: :user_id
end

# Quantity 2: the framework's client side, no database. No declaration on this router.
defmodule Gate.NoopAdapter do
  def child_spec(_application, _config), do: {:ok, [], %{}}

  def append_to_stream(_meta, _uuid, _version, events, _opts) do
    _ = Enum.map(events, &Decree.EventStore.Adapters.EventStore.Mapper.to_event_data/1)
    :ok
  end

  def stream_forward(_meta, _uuid, _start, _size), do: {:error, :stream_not_found}
  def subscribe(_meta, _uuid), do: :ok
  def read_snapshot(_meta, _uuid), do: {:error, :snapshot_not_found}
  def record_snapshot(_meta, _snapshot), do: :ok
  def delete_snapshot(_meta, _uuid), do: :ok
  def conditional_targets(_meta), do: []
end

defmodule Gate.NoopApp do
  use Decree.Application, otp_app: :gate_bench

  router Gate.Router
end

# FW-6: an index fact, appended raw at init with its tags listed, so both arms share it.
defmodule Gate.IndexFact do
  @derive Jason.Encoder
  defstruct [:family, :key, :state]
end

# Quantity 3: design doc §4's two-part shape, on a rule and on a decision with the same query.
defmodule Gate.Registered do
  @derive Jason.Encoder
  defstruct [:user_id, :digest, :note]

  defimpl Decree.Event.Tags do
    def tags(%{digest: digest}, _context), do: {:ok, [Gate.Shape.email_tag(digest)]}
  end
end

defmodule Gate.Released do
  @derive Jason.Encoder
  defstruct [:user_id]

  defimpl Decree.Event.Tags do
    def tags(%{user_id: user}, _context), do: {:ok, ["user:" <> user]}
  end
end

defmodule Gate.Shape do
  @registered "Elixir.Gate.Registered"
  @released "Elixir.Gate.Released"

  # The rig key's fingerprint, the same literal on both arms (main has no helper).
  @fp "0123456789abcdef0123456789abcdef"

  def fp, do: @fp
  def email_tag(digest), do: "email:" <> @fp <> ":" <> digest

  def part1(digest), do: [[types: [@registered], tags: [email_tag(digest)]]]

  def part2(events) do
    for %Gate.Registered{user_id: user} <- events do
      [types: [@released], tags: ["user:" <> user]]
    end
  end

  def holders(events) do
    Enum.reduce(events, MapSet.new(), fn
      %Gate.Registered{user_id: user}, set -> MapSet.put(set, user)
      %Gate.Released{user_id: user}, set -> MapSet.delete(set, user)
    end)
  end
end

defmodule Gate.Register do
  defstruct [:user_id, :digest, :command_id, :rule_result]
end

defmodule Gate.PlainDo do
  defstruct [:user_id, :digest, :command_id]
end

defmodule Gate.Claim do
  defstruct [:user_id, :digest, :command_id]
end

if Code.ensure_loaded?(Decree.Decision.Rule) do
  defmodule Gate.EmailRule do
    use Decree.Decision.Rule

    def query(%Gate.Register{digest: digest}, _context), do: Gate.Shape.part1(digest)
    def followup_query(%Gate.Register{}, _context, events), do: Gate.Shape.part2(events)
    def index_families, do: %{"email" => :digest}
  end

  defmodule Gate.UserAgg do
    defstruct n: 0

    def execute(%__MODULE__{}, %Gate.Register{rule_result: result} = command) do
      if MapSet.size(Gate.Shape.holders(result.events)) == 0 do
        %Gate.Registered{
          user_id: command.user_id,
          digest: command.digest,
          note: String.duplicate("x", 300)
        }
      else
        {:error, :taken}
      end
    end

    def apply(%__MODULE__{n: n} = state, _event), do: %__MODULE__{state | n: n + 1}
  end

  defmodule Gate.EmailDecision do
    use Decree.Decision, create: false

    def query(%Gate.Claim{digest: digest}, _context), do: Gate.Shape.part1(digest)
    def followup_query(%Gate.Claim{}, _context, events), do: Gate.Shape.part2(events)
    def index_families, do: %{"email" => :digest}
    def command_window_ms, do: 300_000
    def initial_state, do: MapSet.new()

    def evolve(state, %Gate.Registered{user_id: user}), do: MapSet.put(state, user)
    def evolve(state, %Gate.Released{user_id: user}), do: MapSet.delete(state, user)

    def decide(state, %Gate.Claim{} = command) do
      if MapSet.size(state) == 0 do
        %Gate.Registered{
          user_id: command.user_id,
          digest: command.digest,
          note: String.duplicate("x", 300)
        }
      else
        {:error, :taken}
      end
    end
  end

  # The same event as `Gate.UserAgg`, with no rule: the open's cost on its own.
  defmodule Gate.PlainAgg do
    defstruct n: 0

    def execute(%__MODULE__{}, %Gate.PlainDo{} = command) do
      %Gate.Registered{
        user_id: command.user_id,
        digest: command.digest,
        note: String.duplicate("x", 300)
      }
    end

    def apply(%__MODULE__{n: n} = state, _event), do: %__MODULE__{state | n: n + 1}
  end

  defmodule Gate.ConditionalRouter do
    use Decree.Commands.Router

    dispatch Gate.PlainDo, to: Gate.PlainAgg, identity: :user_id

    dispatch Gate.Register, to: Gate.UserAgg, identity: :user_id, rule: Gate.EmailRule

    decide Gate.Claim,
      to: Gate.EmailDecision,
      identity: :user_id,
      identity_prefix: "gd-",
      stream_tag: :user
  end

  defmodule Gate.App do
    use Decree.Application, otp_app: :gate_bench

    router Gate.ConditionalRouter
  end
end
