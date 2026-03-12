defmodule SpacetimeDB.CodeGen do
  @moduledoc ~S"""
  Generates `SpacetimeDB.BSATN.Schema` modules from a live SpacetimeDB database.

  Fetches the module schema from SpacetimeDB's REST endpoint, parses the
  algebraic type system, and emits one Elixir source file per table.

  ## Programmatic usage

      SpacetimeDB.CodeGen.run(
        host: "localhost",
        database: "my-module",
        namespace: "MyApp.SpacetimeDB",
        out: "lib/my_app/spacetimedb/"
      )

  ## Mix task

      mix spacetimedb.gen \
        --host localhost \
        --database my-module \
        --namespace MyApp.SpacetimeDB \
        --out lib/my_app/spacetimedb/

  ## Example output — `lib/my_app/spacetimedb/player.ex`

      defmodule MyApp.SpacetimeDB.Player do
        @moduledoc "Live mirror of the `Player` SpacetimeDB table. Auto-generated — do not edit."

        use SpacetimeDB.BSATN.Schema

        bsatn_schema do
          field :id,     :u32
          field :name,   :string
          field :health, :u32
        end

        @doc "Primary key field for `SpacetimeDB.Table`."
        def primary_key, do: :id

        @doc "SpacetimeDB table name."
        def table_name, do: "Player"
      end

  ## SpacetimeDB schema format

  SpacetimeDB v1 returns a `RawModuleDef` from `GET /v1/database/{name}/schema`:

      {
        "typespace": { "types": [ ... ] },
        "tables": [
          { "name": "Player", "product_type_ref": 0, "primary_key": [0], ... }
        ]
      }

  Each table's columns come from `typespace.types[product_type_ref]`, which is a
  `Product` type whose elements are `{name, algebraic_type}` pairs.

  The parser handles both nested-object and string-literal type representations
  for compatibility across SpacetimeDB versions.
  """

  require Logger

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc """
  Fetch the schema, generate modules, and write them to disk.

  ## Options

  | Key | Default | Description |
  |-----|---------|-------------|
  | `:uri` | — | Full URI (e.g. `"https://maincloud.spacetimedb.com"`). Extracts host/port/tls. |
  | `:host` | required (unless `:uri`) | SpacetimeDB host |
  | `:database` | required | Database name or address |
  | `:port` | `3000` | Port (443 when TLS detected from URI) |
  | `:tls` | `false` | Use TLS |
  | `:token` | `nil` | Auth token |
  | `:namespace` | `"SpacetimeDB"` | Elixir module namespace prefix |
  | `:out` | `"lib/spacetimedb/"` | Output directory |
  """
  @spec run(keyword()) :: :ok | {:error, term()}
  def run(opts) do
    opts = maybe_parse_uri(opts)
    host = Keyword.fetch!(opts, :host)
    database = Keyword.fetch!(opts, :database)
    port = Keyword.get(opts, :port, 3000)
    tls = Keyword.get(opts, :tls, false)
    token = Keyword.get(opts, :token)
    namespace = Keyword.get(opts, :namespace, "SpacetimeDB")
    out = Keyword.get(opts, :out, "lib/spacetimedb/")

    with {:ok, schema} <- fetch_schema(host, database, port: port, tls: tls, token: token),
         modules = generate_modules(schema, namespace: namespace),
         :ok <- write_files(modules, out) do
      :ok
    end
  end

  @doc "Fetch the schema JSON from SpacetimeDB's REST API."
  @spec fetch_schema(String.t(), String.t(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def fetch_schema(host, database, opts \\ []) do
    port = Keyword.get(opts, :port, 3000)
    tls = Keyword.get(opts, :tls, false)
    token = Keyword.get(opts, :token)
    scheme = if tls, do: :https, else: :http
    path = "/v1/database/#{URI.encode(database)}/schema"
    headers = if token, do: [{"authorization", "Bearer #{token}"}], else: []

    with {:ok, body} <- http_get(scheme, host, port, path, headers) do
      Jason.decode(body)
    end
  end

  @doc """
  Generate module source strings from a parsed schema map.

  Returns `[{filename, source_code}]`.
  """
  @spec generate_modules(map(), keyword()) :: [{String.t(), String.t()}]
  def generate_modules(schema, opts \\ []) do
    namespace = Keyword.get(opts, :namespace, "SpacetimeDB")
    typespace = extract_typespace(schema)

    schema
    |> extract_tables()
    |> Enum.flat_map(&generate_table_module(&1, typespace, namespace))
  end

  @doc "Write generated `{filename, source}` pairs to `output_dir`."
  @spec write_files([{String.t(), String.t()}], String.t()) :: :ok
  def write_files(modules, output_dir) do
    File.mkdir_p!(output_dir)

    Enum.each(modules, fn {filename, source} ->
      path = Path.join(output_dir, filename)
      File.write!(path, source)
      Logger.info("[SpacetimeDB.CodeGen] wrote #{path}")
    end)
  end

  # ---------------------------------------------------------------------------
  # Private — schema extraction
  # ---------------------------------------------------------------------------

  # Pull the indexed type list out — handle both "typespace"/"types" nesting
  # and a flat "types" list.
  defp extract_typespace(%{"typespace" => %{"types" => types}}), do: types
  defp extract_typespace(%{"types" => types}), do: types
  defp extract_typespace(_), do: []

  defp extract_tables(%{"tables" => tables}), do: tables
  defp extract_tables(_), do: []

  # ---------------------------------------------------------------------------
  # Private — module generation
  # ---------------------------------------------------------------------------

  defp generate_table_module(table, typespace, namespace) do
    table_name = table["name"]

    unless is_binary(table_name) and table_name != "" do
      Logger.warning("[SpacetimeDB.CodeGen] skipping table with no name: #{inspect(table)}")
      []
    else
      columns = resolve_columns(table, typespace)
      pk_index = primary_key_index(table)
      pk_field = columns |> Enum.at(pk_index, hd(columns)) |> elem(0)

      module_name = "#{namespace}.#{pascal_case(table_name)}"
      filename = "#{snake_case(table_name)}.ex"
      source = render_module(module_name, table_name, columns, pk_field)

      [{filename, source}]
    end
  end

  # Resolve a table's column list from its product_type_ref or inline schema.
  defp resolve_columns(table, typespace) do
    cond do
      # v1: product_type_ref points into the typespace
      is_integer(table["product_type_ref"]) ->
        idx = table["product_type_ref"]
        type = Enum.at(typespace, idx)
        elements = get_in(type, ["Product", "elements"]) || []
        Enum.map(elements, &{&1["name"], map_algebraic_type(&1["algebraic_type"], typespace)})

      # Older: inline "schema" with "columns"
      is_map(table["schema"]) ->
        cols = table["schema"]["columns"] || table["schema"]["elements"] || []
        Enum.map(cols, fn c ->
          name = c["name"] || c["col_name"]
          type = map_algebraic_type(c["col_type"] || c["algebraic_type"], typespace)
          {name, type}
        end)

      true ->
        Logger.warning("[SpacetimeDB.CodeGen] could not resolve columns for table #{table["name"]}")
        []
    end
    |> Enum.reject(fn {name, _} -> is_nil(name) end)
  end

  # Extract the 0-based column index of the primary key.
  defp primary_key_index(%{"primary_key" => [idx | _]}) when is_integer(idx), do: idx

  defp primary_key_index(%{"primary_key" => [%{"col_pos" => idx} | _]}), do: idx

  defp primary_key_index(%{"constraints" => constraints}) do
    pk =
      Enum.find(constraints, fn c ->
        kind = c["constraint_data"] || c["kind"] || %{}
        Map.has_key?(kind, "PrimaryKey") or Map.has_key?(kind, "primary_key")
      end)

    case pk do
      %{"columns" => [idx | _]} -> idx
      _ -> 0
    end
  end

  defp primary_key_index(_), do: 0

  # ---------------------------------------------------------------------------
  # Private — algebraic type mapping
  # ---------------------------------------------------------------------------

  # String literals (most common in recent SpacetimeDB versions)
  defp map_algebraic_type("Bool", _ts), do: :bool
  defp map_algebraic_type("U8", _ts), do: :u8
  defp map_algebraic_type("U16", _ts), do: :u16
  defp map_algebraic_type("U32", _ts), do: :u32
  defp map_algebraic_type("U64", _ts), do: :u64
  defp map_algebraic_type("U128", _ts), do: :u128
  defp map_algebraic_type("I8", _ts), do: :i8
  defp map_algebraic_type("I16", _ts), do: :i16
  defp map_algebraic_type("I32", _ts), do: :i32
  defp map_algebraic_type("I64", _ts), do: :i64
  defp map_algebraic_type("I128", _ts), do: :i128
  defp map_algebraic_type("F32", _ts), do: :f32
  defp map_algebraic_type("F64", _ts), do: :f64
  defp map_algebraic_type("String", _ts), do: :string
  defp map_algebraic_type("Bytes", _ts), do: :bytes

  # Nested-object form: {"U32": {}} or {"U32": null}
  defp map_algebraic_type(%{"Bool" => _}, _ts), do: :bool
  defp map_algebraic_type(%{"U8" => _}, _ts), do: :u8
  defp map_algebraic_type(%{"U16" => _}, _ts), do: :u16
  defp map_algebraic_type(%{"U32" => _}, _ts), do: :u32
  defp map_algebraic_type(%{"U64" => _}, _ts), do: :u64
  defp map_algebraic_type(%{"U128" => _}, _ts), do: :u128
  defp map_algebraic_type(%{"I8" => _}, _ts), do: :i8
  defp map_algebraic_type(%{"I16" => _}, _ts), do: :i16
  defp map_algebraic_type(%{"I32" => _}, _ts), do: :i32
  defp map_algebraic_type(%{"I64" => _}, _ts), do: :i64
  defp map_algebraic_type(%{"I128" => _}, _ts), do: :i128
  defp map_algebraic_type(%{"F32" => _}, _ts), do: :f32
  defp map_algebraic_type(%{"F64" => _}, _ts), do: :f64
  defp map_algebraic_type(%{"String" => _}, _ts), do: :string
  defp map_algebraic_type(%{"Bytes" => _}, _ts), do: :bytes

  # Wrapped in {"Builtin": T} (some SpacetimeDB versions)
  defp map_algebraic_type(%{"Builtin" => inner}, ts), do: map_algebraic_type(inner, ts)

  # Array[T]
  defp map_algebraic_type(%{"Array" => %{"elem_ty" => inner}}, ts),
    do: {:array, map_algebraic_type(inner, ts)}

  defp map_algebraic_type(%{"ArrayType" => %{"elem_ty" => inner}}, ts),
    do: {:array, map_algebraic_type(inner, ts)}

  # Option[T] — inner may be a map, string, or wrapped in "some"
  defp map_algebraic_type(%{"Option" => %{"some" => inner}}, ts),
    do: {:option, map_algebraic_type(inner, ts)}

  defp map_algebraic_type(%{"Option" => inner}, ts),
    do: {:option, map_algebraic_type(inner, ts)}

  # Ref — resolved inline as :bytes (opaque) with a comment; advanced users
  # can customise the generated file after running the task.
  defp map_algebraic_type(%{"Ref" => idx}, ts) do
    case Enum.at(ts, idx) do
      nil -> :bytes
      type -> map_algebraic_type(type, ts)
    end
  end

  defp map_algebraic_type(%{"AlgebraicTypeRef" => idx}, ts),
    do: map_algebraic_type(%{"Ref" => idx}, ts)

  # Product type inline — flatten to :bytes (nested structs need their own module)
  defp map_algebraic_type(%{"Product" => _}, _ts), do: :bytes

  # Sum type inline — flatten to :u8 discriminant
  defp map_algebraic_type(%{"Sum" => _}, _ts), do: :u8

  defp map_algebraic_type(unknown, _ts) do
    Logger.warning("[SpacetimeDB.CodeGen] unknown algebraic type: #{inspect(unknown)}, using :bytes")
    :bytes
  end

  # ---------------------------------------------------------------------------
  # Private — source rendering
  # ---------------------------------------------------------------------------

  defp render_module(module_name, table_name, columns, pk_field) do
    fields_source =
      columns
      |> Enum.map(fn {name, type} ->
        "      field #{inspect(String.to_atom(name))}, #{inspect_type(type)}"
      end)
      |> Enum.join("\n")

    pk_atom = inspect(String.to_atom(pk_field))

    """
    # Generated by SpacetimeDB.CodeGen — do not edit manually.
    # Re-generate with: mix spacetimedb.gen
    defmodule #{module_name} do
      @moduledoc "Live mirror of the `#{table_name}` SpacetimeDB table. Auto-generated."

      use SpacetimeDB.BSATN.Schema

      bsatn_schema do
    #{fields_source}
      end

      @doc "Primary key field for use with `SpacetimeDB.Table`."
      def primary_key, do: #{pk_atom}

      @doc "SpacetimeDB table name."
      def table_name, do: #{inspect(table_name)}
    end
    """
  end

  # Pretty-print a type term for use in source code
  defp inspect_type(atom) when is_atom(atom), do: inspect(atom)
  defp inspect_type({:array, inner}), do: "{:array, #{inspect_type(inner)}}"
  defp inspect_type({:option, inner}), do: "{:option, #{inspect_type(inner)}}"

  # ---------------------------------------------------------------------------
  # Private — naming helpers
  # ---------------------------------------------------------------------------

  defp pascal_case(str) do
    str
    |> String.split(~r/[_\s-]+/)
    |> Enum.map(&String.capitalize/1)
    |> Enum.join()
  end

  defp snake_case(str) do
    str
    |> String.replace(~r/([A-Z])/, "_\\1")
    |> String.downcase()
    |> String.trim_leading("_")
    |> String.replace(~r/[^a-z0-9]+/, "_")
  end

  # ---------------------------------------------------------------------------
  # Private — synchronous Mint HTTP GET
  # ---------------------------------------------------------------------------

  defp http_get(scheme, host, port, path, headers) do
    with {:ok, conn} <- Mint.HTTP.connect(scheme, host, port, protocols: [:http1]),
         {:ok, conn, ref} <- Mint.HTTP.request(conn, "GET", path, headers, nil) do
      collect_response(conn, ref, nil, "")
    end
  end

  defp collect_response(conn, ref, status, body) do
    receive do
      msg ->
        case Mint.HTTP.stream(conn, msg) do
          {:ok, conn, responses} ->
            {new_status, new_body, done?} = fold_responses(responses, ref, status, body)

            if done? do
              Mint.HTTP.close(conn)

              if new_status in 200..299 do
                {:ok, new_body}
              else
                {:error, {:http_error, new_status, new_body}}
              end
            else
              collect_response(conn, ref, new_status, new_body)
            end

          {:error, _conn, reason, _} ->
            {:error, reason}

          :unknown ->
            collect_response(conn, ref, status, body)
        end
    after
      15_000 -> {:error, :timeout}
    end
  end

  defp fold_responses(responses, ref, status, body) do
    Enum.reduce(responses, {status, body, false}, fn
      {:status, ^ref, s}, {_, b, d} -> {s, b, d}
      {:headers, ^ref, _}, acc -> acc
      {:data, ^ref, data}, {s, b, d} -> {s, b <> data, d}
      {:done, ^ref}, {s, b, _} -> {s, b, true}
      _, acc -> acc
    end)
  end

  defp maybe_parse_uri(opts) do
    case Keyword.pop(opts, :uri) do
      {nil, opts} -> opts
      {uri, opts} ->
        parsed = URI.parse(uri)
        tls = parsed.scheme in ["https", "wss"]
        default_port = if tls, do: 443, else: 3000

        opts
        |> Keyword.put_new(:host, parsed.host)
        |> Keyword.put_new(:port, parsed.port || default_port)
        |> Keyword.put_new(:tls, tls)
    end
  end
end
