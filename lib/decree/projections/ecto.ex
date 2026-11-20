defmodule Decree.Projections.Ecto do
  @moduledoc """
  Read model projections for Decree using Ecto.

  A projector is an event handler (`Decree.Event.Handler`) that writes each event it handles
  into an Ecto read model. It runs your `Ecto.Multi` in one transaction with an update to its
  row in the `projection_versions` table, which holds the number of the last event it
  projected, so an event delivered again is skipped.

  ## Example

  A projector for the README's minimal application. It writes each `MyApp.AccountRegistered`
  into an `accounts` table:

      # lib/my_app/accounts.ex
      defmodule MyApp.Accounts.Account do
        use Ecto.Schema

        @primary_key {:account_id, :binary_id, autogenerate: false}
        schema "accounts" do
          field :handle, :string
        end
      end

      defmodule MyApp.Accounts.Projector do
        use Decree.Projections.Ecto,
          application: MyApp.App,
          repo: MyApp.Repo,
          name: "accounts"

        project %MyApp.AccountRegistered{} = event, _metadata, fn multi ->
          Ecto.Multi.insert(multi, :account, %MyApp.Accounts.Account{
            account_id: event.account_id,
            handle: event.handle
          })
        end
      end

  `application:` is your `Decree.Application`, `MyApp.App`. `repo:` is your `Ecto.Repo`;
  without it, the projector takes `config :decree, repo: MyApp.Repo` at compile time. `name:`
  names the projector's subscription and its row in `projection_versions`. `schema_prefix:`
  (below) and `timeout:`, the transaction's timeout (`:infinity` by default), are the
  projector's own. Every other option goes to `Decree.Event.Handler`.

  ## The repo

  The read model lives in your own Ecto repo:

      # lib/my_app/repo.ex
      defmodule MyApp.Repo do
        use Ecto.Repo, otp_app: :my_app, adapter: Ecto.Adapters.Postgres
      end

  Configure it beside the README's event store config:

      # config/config.exs
      import Config

      config :my_app, ecto_repos: [MyApp.Repo]

      config :my_app, MyApp.Repo,
        username: "postgres",
        password: "postgres",
        hostname: "localhost",
        database: "my_app_read_models"

  ## The `projection_versions` table

  decree ships no migration for this table. It lives in your repo's database, so create it
  with an Ecto migration of your own before any projector starts. Every projector on the repo
  shares it, one row per `name:`. This migration creates it and the example's read model:

      # priv/repo/migrations/20261120000000_create_accounts.exs
      defmodule MyApp.Repo.Migrations.CreateAccounts do
        use Ecto.Migration

        def change do
          create table(:projection_versions, primary_key: false) do
            add :projection_name, :text, primary_key: true
            add :last_seen_event_number, :bigint

            timestamps(type: :naive_datetime_usec)
          end

          create table(:accounts, primary_key: false) do
            add :account_id, :uuid, primary_key: true
            add :handle, :text, null: false
          end
        end
      end

  Create the database with `mix ecto.create` and run the migration with `mix ecto.migrate`.

  With `schema_prefix:`, a projector reads and writes `projection_versions` in that schema, so
  each schema it names needs the table too: create it there with
  `table(:projection_versions, primary_key: false, prefix: "my_prefix")`. The option sets only
  that table's schema. Your read model's own queries take their own `prefix:`.

  ## Starting it

  Start the projector in your OTP application's supervision tree, after the repo and the
  decree application:

      # lib/my_app/application.ex
      defmodule MyApp.Application do
        use Application

        @impl Application
        def start(_type, _args) do
          children = [MyApp.Repo, MyApp.App, MyApp.Accounts.Projector]

          Supervisor.start_link(children, strategy: :one_for_one, name: MyApp.Supervisor)
        end
      end

  `MyApp.Application` is the OTP application module, named by `mod:` in `mix.exs`. `MyApp.App`
  is the decree application. They are two modules.
  """

  defmacro __using__(opts) do
    opts = opts || []

    schema_prefix =
      opts[:schema_prefix] || Application.get_env(:decree, :schema_prefix)

    quote location: :keep do
      @behaviour Decree.Projections.Ecto

      @opts unquote(opts)
      @repo @opts[:repo] || Application.compile_env(:decree, :repo) ||
              raise("Decree Ecto projections expects :repo to be configured in environment")
      @timeout @opts[:timeout] || :infinity

      # Pass through any other configuration to the event handler
      @handler_opts Keyword.drop(@opts, [:repo, :schema_prefix, :timeout])

      unquote(__include_schema_prefix__(schema_prefix))
      unquote(__include_projection_version_schema__())

      use Ecto.Schema
      use Decree.Event.Handler, @handler_opts

      import Ecto.Query
      import unquote(__MODULE__)

      def update_projection(event, metadata, multi_fn) do
        projection_name = Map.fetch!(metadata, :handler_name)
        event_number = Map.fetch!(metadata, :event_number)

        projection_version = %ProjectionVersion{
          projection_name: projection_name,
          last_seen_event_number: event_number
        }

        prefix = schema_prefix(event, metadata)

        # Query to update an existing projection version with the last seen event number with
        # a check to ensure that the event has not already been projected.
        update_projection_version =
          from(pv in ProjectionVersion,
            where:
              pv.projection_name == ^projection_name and pv.last_seen_event_number < ^event_number,
            update: [set: [last_seen_event_number: ^event_number]]
          )

        multi =
          Ecto.Multi.new()
          |> Ecto.Multi.run(:track_projection_version, fn repo, _changes ->
            try do
              repo.insert(projection_version,
                prefix: prefix,
                on_conflict: update_projection_version,
                conflict_target: [:projection_name]
              )
            rescue
              exception in Ecto.StaleEntryError ->
                # Attempted to insert a projection version for an already seen event
                {:error, :already_seen_event}

              exception ->
                reraise exception, __STACKTRACE__
            end
          end)

        with %Ecto.Multi{} = multi <- apply(multi_fn, [multi]),
             {:ok, changes} <- transaction(multi) do
          if function_exported?(__MODULE__, :after_update, 3) do
            apply(__MODULE__, :after_update, [event, metadata, changes])
          else
            :ok
          end
        else
          {:error, :track_projection_version, :already_seen_event, _changes} -> :ok
          {:error, _stage, error, _changes} -> {:error, error}
          {:error, _error} = reply -> reply
        end
      end

      defp transaction(%Ecto.Multi{} = multi) do
        @repo.transaction(multi, timeout: @timeout, pool_timeout: @timeout)
      end

      defoverridable schema_prefix: 1, schema_prefix: 2
    end
  end

  ## User callbacks

  @optional_callbacks [after_update: 3, schema_prefix: 1, schema_prefix: 2]

  @doc """
  The optional `after_update/3` callback function defined in a projector is
  called after each projected event.

  The function receives the event, its metadata, and all changes from the
  `Ecto.Multi` struct that were executed within the database transaction.

  You could use this function to notify subscribers that the read model has been
  updated, such as by publishing changes via Phoenix PubSub channels.

  ## Example

  The moduledoc's projector, with the callback:

      # lib/my_app/accounts.ex
      defmodule MyApp.Accounts.Projector do
        use Decree.Projections.Ecto,
          application: MyApp.App,
          repo: MyApp.Repo,
          name: "accounts"

        project %MyApp.AccountRegistered{} = event, _metadata, fn multi ->
          Ecto.Multi.insert(multi, :account, %MyApp.Accounts.Account{
            account_id: event.account_id,
            handle: event.handle
          })
        end

        @impl Decree.Projections.Ecto
        def after_update(_event, _metadata, %{account: %MyApp.Accounts.Account{}}) do
          # Use the event, metadata, or `Ecto.Multi` changes, such as to broadcast the
          # account with Phoenix.PubSub, and return `:ok`
          :ok
        end
      end

  """
  @callback after_update(event :: struct, metadata :: map, changes :: Ecto.Multi.changes()) ::
              :ok | {:error, any}

  @doc """
  The optional `schema_prefix/1` callback function defined in a projector is
  used to set the schema of the `projection_versions` table used by the
  projector for idempotency checks.

  It is passed the event and its metadata and must return the schema name, as a
  string, or `nil`.
  """
  @callback schema_prefix(event :: struct) :: String.t() | nil

  @doc """
  The optional `schema_prefix/2` callback function defined in a projector is
  used to set the schema of the `projection_versions` table used by the
  projector for idempotency checks.

  It is passed the event and its metadata, and must return the schema name, as a
  string, or `nil`
  """
  @callback schema_prefix(event :: struct(), metadata :: map()) :: String.t() | nil

  defp __include_schema_prefix__(schema_prefix) do
    quote do
      cond do
        is_nil(unquote(schema_prefix)) ->
          def schema_prefix(_event), do: nil
          def schema_prefix(event, _metadata), do: schema_prefix(event)

        is_binary(unquote(schema_prefix)) ->
          def schema_prefix(_event), do: nil
          def schema_prefix(_event, _metadata), do: unquote(schema_prefix)

        is_function(unquote(schema_prefix), 1) ->
          def schema_prefix(event), do: nil
          def schema_prefix(event, _metadata), do: apply(unquote(schema_prefix), [event])

        is_function(unquote(schema_prefix), 2) ->
          def schema_prefix(event), do: nil
          def schema_prefix(event, metadata), do: apply(unquote(schema_prefix), [event, metadata])

        true ->
          raise ArgumentError,
            message:
              "expected :schema_prefix option to be a string or a one-arity or two-arity function, but got: " <>
                inspect(unquote(schema_prefix))
      end
    end
  end

  defp __include_projection_version_schema__ do
    quote do
      defmodule ProjectionVersion do
        @moduledoc false
        use Ecto.Schema

        @primary_key {:projection_name, :string, []}

        schema "projection_versions" do
          field(:last_seen_event_number, :integer)

          timestamps(type: :naive_datetime_usec)
        end
      end
    end
  end

  defmacro project(event, do: block) do
    IO.warn(
      "project macro with \"do end\" block is deprecated; use project/2 with function instead",
      Macro.Env.stacktrace(__ENV__)
    )

    quote do
      def handle(unquote(event) = event, metadata) do
        update_projection(event, metadata, fn var!(multi) ->
          unquote(block)
        end)
      end
    end
  end

  @doc """
  Project a domain event into a read model by appending one or more operations
  to the `Ecto.Multi` struct passed to the projection function you define

  The operations will be executed in a database transaction including an
  idempotency check to guarantee an event cannot be projected more than once.

  ## Example

  The moduledoc's projector, with a projection that takes no metadata:

      # lib/my_app/accounts.ex
      defmodule MyApp.Accounts.Projector do
        use Decree.Projections.Ecto,
          application: MyApp.App,
          repo: MyApp.Repo,
          name: "accounts"

        project %MyApp.AccountRegistered{} = event, fn multi ->
          Ecto.Multi.insert(multi, :account, %MyApp.Accounts.Account{
            account_id: event.account_id,
            handle: event.handle
          })
        end
      end

  """
  defmacro project(event, lambda) do
    quote do
      def handle(unquote(event) = event, metadata) do
        update_projection(event, metadata, unquote(lambda))
      end
    end
  end

  defmacro project(event, metadata, do: block) do
    IO.warn(
      "project macro with \"do end\" block is deprecated; use project/3 with function instead",
      Macro.Env.stacktrace(__ENV__)
    )

    quote do
      def handle(unquote(event) = event, unquote(metadata) = metadata) do
        update_projection(event, metadata, fn var!(multi) ->
          unquote(block)
        end)
      end
    end
  end

  @doc """
  Project a domain event and its metadata map into a read model by appending one
  or more operations to the `Ecto.Multi` struct passed to the projection
  function you define.

  The operations will be executed in a database transaction including an
  idempotency check to guarantee an event cannot be projected more than once.

  The metadata is the map `c:Decree.Event.Handler.handle/2` receives, and the second argument
  is a pattern on it, as the first is on the event.

  ## Example

  The moduledoc's projector:

      # lib/my_app/accounts.ex
      defmodule MyApp.Accounts.Projector do
        use Decree.Projections.Ecto,
          application: MyApp.App,
          repo: MyApp.Repo,
          name: "accounts"

        project %MyApp.AccountRegistered{} = event, _metadata, fn multi ->
          Ecto.Multi.insert(multi, :account, %MyApp.Accounts.Account{
            account_id: event.account_id,
            handle: event.handle
          })
        end
      end

  """
  defmacro project(event, metadata, lambda) do
    quote do
      def handle(unquote(event) = event, unquote(metadata) = metadata) do
        update_projection(event, metadata, unquote(lambda))
      end
    end
  end
end
