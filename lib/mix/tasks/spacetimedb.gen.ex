defmodule Mix.Tasks.Spacetimedb.Gen do
  use Mix.Task

  @shortdoc "Generate BSATN schema modules from a live SpacetimeDB database"

  @moduledoc ~S"""
  Connects to a SpacetimeDB instance, fetches the module schema, and generates
  one `SpacetimeDB.BSATN.Schema` Elixir module per table.

  ## Usage

      mix spacetimedb.gen --uri URI --database NAME [options]
      mix spacetimedb.gen --host HOST --database NAME [options]

  ## Options

  | Flag | Default | Description |
  |------|---------|-------------|
  | `--uri` | — | Full URI (e.g. `https://maincloud.spacetimedb.com`). Extracts host/port/tls. |
  | `--host` | required (unless `--uri`) | SpacetimeDB host |
  | `--database` | required | Database name or address |
  | `--port` | `3000` | Port (443 when TLS detected from URI) |
  | `--tls` | false | Use TLS (`wss://`) |
  | `--token` | — | Auth token |
  | `--namespace` | `SpacetimeDB` | Elixir module namespace prefix |
  | `--out` | `lib/spacetimedb/` | Output directory |

  ## Examples

      # Local dev server
      mix spacetimedb.gen --host localhost --database my-module

      # SpacetimeDB cloud (Maincloud)
      mix spacetimedb.gen \
        --uri https://maincloud.spacetimedb.com \
        --database my-module \
        --namespace MyApp.SpacetimeDB \
        --out lib/my_app/spacetimedb/

      # With custom namespace and output directory
      mix spacetimedb.gen \
        --host localhost \
        --database my-module \
        --namespace MyApp.SpacetimeDB \
        --out lib/my_app/spacetimedb/

      # Production with TLS and auth token
      mix spacetimedb.gen \
        --host maincloud.spacetimedb.com \
        --database my-prod-module \
        --tls \
        --token $SPACETIMEDB_TOKEN \
        --namespace MyApp.SpacetimeDB \
        --out lib/my_app/spacetimedb/

  ## Generated files

  For a table called `Player` with a `--namespace` of `MyApp.SpacetimeDB`:

      # lib/my_app/spacetimedb/player.ex
      defmodule MyApp.SpacetimeDB.Player do
        @moduledoc "Live mirror of the `Player` SpacetimeDB table. Auto-generated."

        use SpacetimeDB.BSATN.Schema

        bsatn_schema do
          field :id,     :u32
          field :name,   :string
          field :health, :u32
        end

        def primary_key, do: :id
        def table_name,  do: "Player"
      end

  Re-run the task any time the SpacetimeDB module schema changes.  Files are
  overwritten, so avoid hand-editing generated modules — extend them in
  separate files instead.
  """

  @switches [
    uri: :string,
    host: :string,
    database: :string,
    port: :integer,
    tls: :boolean,
    token: :string,
    namespace: :string,
    out: :string
  ]

  @impl Mix.Task
  def run(args) do
    # Ensure the app is started so Jason etc. are available
    Mix.Task.run("app.start", [])

    {opts, _rest, _invalid} = OptionParser.parse(args, strict: @switches)

    unless opts[:uri] || opts[:host] do
      Mix.raise("--uri or --host is required")
    end

    database = opts[:database] || Mix.raise("--database is required")

    gen_opts =
      Enum.reject(
        [
          uri: opts[:uri],
          host: opts[:host],
          port: opts[:port],
          tls: opts[:tls],
          token: opts[:token],
          namespace: opts[:namespace] || "SpacetimeDB",
          out: opts[:out] || "lib/spacetimedb/"
        ],
        fn {_k, v} -> is_nil(v) end
      )

    label = opts[:uri] || opts[:host]
    Mix.shell().info("Fetching schema from #{label} / #{database}...")

    case SpacetimeDB.CodeGen.run([database: database] ++ gen_opts) do
      :ok ->
        Mix.shell().info("Done.")

      {:error, {:http_error, status, body}} ->
        Mix.raise("HTTP #{status}: #{body}")

      {:error, reason} ->
        Mix.raise("Failed: #{inspect(reason)}")
    end
  end
end
