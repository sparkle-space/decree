defmodule Decree.Store.Sql.Statements do
  @moduledoc false

  require EEx

  alias Decree.Store.Sql.{Init, Reset}

  defdelegate initializers(config), to: Init, as: :statements
  defdelegate reset(config), to: Reset, as: :statements

  for {fun, args} <- [
        {:count_streams, [:schema]},
        {:create_stream, [:schema]},
        {:append_events, [:schema]},
        {:lock_link_events, [:schema]},
        {:query_link_sources, [:schema]},
        {:insert_link_events, [:schema, :number_of_events]},
        {:soft_delete_stream, [:schema]},
        {:lock_stream, [:schema]},
        {:lock_deleted_events, [:schema]},
        {:query_index_facts, [:schema]},
        {:hard_delete_stream, [:schema]},
        {:delete_stream_row, [:schema]},
        {:delete_stream_tags, [:schema]},
        {:insert_subscription, [:schema]},
        {:delete_subscription, [:schema]},
        {:try_advisory_lock, [:schema]},
        {:advisory_unlock, [:schema]},
        {:try_session_advisory_lock, [:schema]},
        {:query_held_advisory_locks, [:schema]},
        {:query_subscription_locks, [:schema]},
        {:subscription_ack, [:schema]},
        {:subscription_ack_by_id, [:schema]},
        {:insert_snapshot, [:schema]},
        {:delete_snapshot, [:schema]},
        {:query_all_subscriptions, [:schema]},
        {:query_positions,
         [
           :prefix,
           :items,
           :tags,
           :types,
           :rest_items,
           :rest_tags,
           :bound,
           :limit,
           :op,
           :order
         ]},
        {:query_snapshot, [:schema]},
        {:query_stream_info, [:schema]},
        {:query_stream_events_backward, [:schema]},
        {:query_stream_events_forward, [:schema]},
        {:query_streams, [:schema, :sort_by, :sort_dir]},
        {:query_subscription, [:schema]},
        {:read_query, [:schema, :positions, :order]},
        {:read_tags, [:schema]}
      ] do
    file = Path.expand("statements/#{fun}.sql.eex", __DIR__)

    @external_resource file

    EEx.function_from_file(:def, fun, file, args, engine: Decree.Store.EExIOListEngine)
  end
end
