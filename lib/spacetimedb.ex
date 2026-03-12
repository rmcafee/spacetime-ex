defmodule SpacetimeDB do
  @moduledoc ~S"""
  Elixir client for [SpacetimeDB](https://spacetimedb.com) v2.

  ## Quick start

      {:ok, conn} = SpacetimeDB.start_link(
        host: "localhost",
        database: "my_module",
        handler: %{
          on_connected: fn conn_info, _ ->
            IO.puts("Connected as #{conn_info.identity}")
          end,
          on_transaction_update: fn update, _ ->
            IO.inspect(update.query_sets, label: "query sets changed")
          end
        }
      )

      SpacetimeDB.subscribe(conn, ["SELECT * FROM Player"])
      SpacetimeDB.call_reducer(conn, "CreatePlayer", ["Alice"])

  ## Connecting to SpacetimeDB Cloud (Maincloud)

      {:ok, conn} = SpacetimeDB.start_link(
        uri: "https://maincloud.spacetimedb.com",
        database: "my_module",
        handler: MyHandler
      )

  ## Using a handler module

      defmodule MyHandler do
        @behaviour SpacetimeDB.Handler

        @impl true
        def on_transaction_update(update, _arg) do
          Enum.each(update.query_sets, fn qs ->
            Enum.each(qs.tables, &process_table/1)
          end)
        end
      end

      {:ok, conn} = SpacetimeDB.start_link(
        uri: "https://maincloud.spacetimedb.com",
        database: "game-prod",
        token: System.get_env("SPACETIMEDB_TOKEN"),
        handler: MyHandler
      )

  ## BSATN (binary protocol, default)

  By default the connection uses `v2.bsatn.spacetimedb` — binary WebSocket frames
  that are 3–5× smaller than JSON.  Row data in `TableUpdate` structs arrives as
  raw BSATN binaries; decode them with `SpacetimeDB.BSATN.Schema`:

      defmodule MyGame.Player do
        use SpacetimeDB.BSATN.Schema
        bsatn_schema do
          field :id,     :u32
          field :name,   :string
          field :health, :u32
        end
      end

      def on_transaction_update(update, _) do
        for qs <- update.query_sets,
            table <- qs.tables,
            table.table_name == "Player" do
          for row_bin <- table.inserts do
            {:ok, player, ""} = MyGame.Player.decode(row_bin)
            IO.inspect(player)
          end
        end
      end

  Pass `protocol: :json` to use text frames and plain decoded JSON maps instead.

  ## Options

  See `SpacetimeDB.Connection` for the full option reference.
  """

  alias SpacetimeDB.Connection

  @doc """
  Start a connection process and link it to the caller.

  The process is not supervised — wrap in your application's supervision tree
  for production use:

      children = [
        {SpacetimeDB, uri: "https://maincloud.spacetimedb.com", database: "my_module", handler: MyHandler}
      ]
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  defdelegate start_link(opts), to: Connection

  @doc "Subscribe to one or more SQL queries under a query set."
  @spec subscribe(GenServer.server(), [String.t()], non_neg_integer()) :: :ok
  defdelegate subscribe(conn, query_strings, query_set_id \\ 0), to: Connection

  @doc "Unsubscribe from a query set."
  @spec unsubscribe(GenServer.server(), non_neg_integer()) :: :ok
  defdelegate unsubscribe(conn, query_set_id), to: Connection

  @doc """
  Call a reducer.

  Returns `{:ok, request_id}`. The result arrives asynchronously via the
  handler's `on_reducer_result/2` callback; match on `request_id`.
  """
  @spec call_reducer(GenServer.server(), String.t(), list() | binary()) ::
          {:ok, non_neg_integer()}
  defdelegate call_reducer(conn, reducer, args), to: Connection

  @doc """
  Call a procedure.

  Returns `{:ok, request_id}`. The result arrives asynchronously via the
  handler's `on_procedure_result/2` callback.
  """
  @spec call_procedure(GenServer.server(), String.t(), list() | binary()) ::
          {:ok, non_neg_integer()}
  defdelegate call_procedure(conn, procedure, args), to: Connection

  @doc """
  Run a one-off SQL query without subscribing.

  Returns `{:ok, request_id}`. The result arrives asynchronously via the
  handler's `on_one_off_query_result/2` callback.
  """
  @spec one_off_query(GenServer.server(), String.t()) :: {:ok, non_neg_integer()}
  defdelegate one_off_query(conn, query_string), to: Connection

  @doc "Return the current connection status: `:connected`, `:connecting`, or `:disconnected`."
  @spec status(GenServer.server()) :: :connected | :connecting | :disconnected
  defdelegate status(conn), to: Connection

  @doc "Disconnect and stop the connection process."
  @spec stop(GenServer.server()) :: :ok
  defdelegate stop(conn), to: Connection

  # Allow SpacetimeDB to be used directly as a child_spec in supervision trees.
  @doc false
  def child_spec(opts) do
    %{
      id: Keyword.get(opts, :name, __MODULE__),
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      type: :worker
    }
  end
end
