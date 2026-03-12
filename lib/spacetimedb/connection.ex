defmodule SpacetimeDB.Connection do
  @moduledoc ~S"""
  A GenServer that owns a single WebSocket connection to a SpacetimeDB module.

  ## Lifecycle

  1. `start_link/1` opens a TCP/TLS connection and performs the WebSocket upgrade
     with the negotiated subprotocol (`v2.json.spacetimedb` or `v2.bsatn.spacetimedb`).
  2. After the upgrade the server sends an `InitialConnection` frame.  The token is
     stored in state and the handler's `on_connected/2` callback is fired.
  3. The process drives the Mint connection inside its own receive loop
     (`handle_info/2`) and dispatches decoded server messages to the handler.
  4. On disconnect the process waits `reconnect_delay_ms` (doubling on each
     failure, capped at `max_reconnect_delay_ms`) before reconnecting.

  ## Protocols

  Pass `protocol: :bsatn` (default) or `protocol: :json`.

  `:bsatn` uses binary WebSocket frames and the `v2.bsatn.spacetimedb` subprotocol.
  It produces 3–5× smaller payloads.  Row data in `TableUpdate` structs arrives as
  raw BSATN binaries; decode them with `SpacetimeDB.BSATN.Schema`.

  `:json` uses text WebSocket frames and the `v2.json.spacetimedb` subprotocol.
  Row data arrives as decoded JSON terms (maps/lists).

  ## call_reducer and BSATN args

  In `:bsatn` mode the `args` argument to `call_reducer/3` must be a pre-encoded
  BSATN binary.  Use `SpacetimeDB.BSATN` to encode reducer arguments:

      args = SpacetimeDB.BSATN.encode_string("Alice") <>
             SpacetimeDB.BSATN.encode_u32(100)
      SpacetimeDB.call_reducer(conn, "CreatePlayer", args)

  In `:json` mode `args` is a list that is JSON-encoded automatically.

  ## Options

  | Key | Type | Default | Description |
  |-----|------|---------|-------------|
  | `:uri` | `String.t()` | — | Full URI (e.g. `"https://maincloud.spacetimedb.com"`). Extracts host/port/tls automatically. |
  | `:host` | `String.t()` | required (unless `:uri`) | SpacetimeDB host |
  | `:port` | `non_neg_integer()` | `3000` | Port (443 when TLS detected from URI) |
  | `:tls` | `boolean()` | `false` | Use TLS |
  | `:database` | `String.t()` | required | Database name or identity hex |
  | `:token` | `String.t() \| nil` | `nil` | Auth token (re-uses identity on reconnect) |
  | `:protocol` | `:bsatn \| :json` | `:bsatn` | Wire protocol |
  | `:handler` | `module \| {module, term} \| map` | required | Callback module |
  | `:reconnect` | `boolean()` | `true` | Auto-reconnect on disconnect |
  | `:reconnect_delay_ms` | `non_neg_integer()` | `500` | Initial reconnect delay |
  | `:max_reconnect_delay_ms` | `non_neg_integer()` | `30_000` | Max reconnect delay |
  | `:name` | `GenServer.name()` | — | Optional registered name |
  """

  use GenServer, restart: :permanent

  require Logger

  alias SpacetimeDB.{Protocol, Types}
  alias SpacetimeDB.Protocol.BSATN, as: ProtocolBSATN

  @default_port 3000
  @default_protocol :bsatn
  @default_reconnect_delay_ms 500
  @default_max_reconnect_delay_ms 30_000
  @request_id_start 1

  # ---------------------------------------------------------------------------
  # Public API
  # ---------------------------------------------------------------------------

  @doc "Start and link the connection process."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name_opt, opts} = Keyword.pop(opts, :name)
    gen_opts = if name_opt, do: [name: name_opt], else: []
    GenServer.start_link(__MODULE__, opts, gen_opts)
  end

  @doc "Subscribe to one or more SQL queries under a query set."
  @spec subscribe(GenServer.server(), [String.t()], non_neg_integer()) :: :ok
  def subscribe(conn, query_strings, query_set_id \\ 0) when is_list(query_strings) do
    GenServer.cast(conn, {:subscribe, query_strings, query_set_id})
  end

  @doc "Unsubscribe from a query set."
  @spec unsubscribe(GenServer.server(), non_neg_integer()) :: :ok
  def unsubscribe(conn, query_set_id) do
    GenServer.cast(conn, {:unsubscribe, query_set_id})
  end

  @doc """
  Call a reducer.  Returns `{:ok, request_id}` immediately.

  In `:bsatn` mode `args` must be a pre-encoded BSATN binary.
  In `:json` mode `args` is a list that is JSON-encoded automatically.
  """
  @spec call_reducer(GenServer.server(), String.t(), list() | binary()) ::
          {:ok, non_neg_integer()}
  def call_reducer(conn, reducer, args) do
    GenServer.call(conn, {:call_reducer, reducer, args})
  end

  @doc """
  Call a procedure.  Returns `{:ok, request_id}` immediately.

  Same encoding rules as `call_reducer/3`.
  """
  @spec call_procedure(GenServer.server(), String.t(), list() | binary()) ::
          {:ok, non_neg_integer()}
  def call_procedure(conn, procedure, args) do
    GenServer.call(conn, {:call_procedure, procedure, args})
  end

  @doc """
  Run a one-off SQL query.  Returns `{:ok, request_id}`.
  The result arrives via the handler's `on_one_off_query_result/2` callback.
  """
  @spec one_off_query(GenServer.server(), String.t()) :: {:ok, non_neg_integer()}
  def one_off_query(conn, query_string) do
    GenServer.call(conn, {:one_off_query, query_string})
  end

  @doc "Return current connection status: `:connected`, `:connecting`, or `:disconnected`."
  @spec status(GenServer.server()) :: :connected | :connecting | :disconnected
  def status(conn), do: GenServer.call(conn, :status)

  @doc "Disconnect and stop the connection process."
  @spec stop(GenServer.server()) :: :ok
  def stop(conn), do: GenServer.stop(conn, :normal)

  # ---------------------------------------------------------------------------
  # GenServer init
  # ---------------------------------------------------------------------------

  @impl GenServer
  def init(opts) do
    opts = maybe_parse_uri(opts)
    host = Keyword.fetch!(opts, :host)
    port = Keyword.get(opts, :port, @default_port)
    tls = Keyword.get(opts, :tls, false)
    database = Keyword.fetch!(opts, :database)
    token = Keyword.get(opts, :token)
    protocol = Keyword.get(opts, :protocol, @default_protocol)
    handler = build_handler(Keyword.fetch!(opts, :handler))
    reconnect = Keyword.get(opts, :reconnect, true)
    reconnect_delay_ms = Keyword.get(opts, :reconnect_delay_ms, @default_reconnect_delay_ms)

    max_reconnect_delay_ms =
      Keyword.get(opts, :max_reconnect_delay_ms, @default_max_reconnect_delay_ms)

    state = %{
      host: host,
      port: port,
      tls: tls,
      database: database,
      token: token,
      protocol: protocol,
      handler: handler,
      reconnect: reconnect,
      reconnect_delay_ms: reconnect_delay_ms,
      max_reconnect_delay_ms: max_reconnect_delay_ms,
      # runtime connection state
      status: :disconnected,
      conn: nil,
      websocket: nil,
      ws_ref: nil,
      request_id: @request_id_start,
      current_reconnect_delay_ms: reconnect_delay_ms
    }

    send(self(), :connect)
    {:ok, state}
  end

  # ---------------------------------------------------------------------------
  # Casts
  # ---------------------------------------------------------------------------

  @impl GenServer
  def handle_cast({:subscribe, query_strings, query_set_id}, state) do
    {state, request_id} = next_request_id(state)
    send_msg(state, enc(state, :encode_subscribe, [query_strings, request_id, query_set_id]))
    {:noreply, state}
  end

  def handle_cast({:unsubscribe, query_set_id}, state) do
    {state, request_id} = next_request_id(state)
    send_msg(state, enc(state, :encode_unsubscribe, [request_id, query_set_id]))
    {:noreply, state}
  end

  # ---------------------------------------------------------------------------
  # Calls
  # ---------------------------------------------------------------------------

  @impl GenServer
  def handle_call({:call_reducer, reducer, args}, _from, state) do
    {state, request_id} = next_request_id(state)
    send_msg(state, enc(state, :encode_call_reducer, [reducer, args, request_id]))
    {:reply, {:ok, request_id}, state}
  end

  def handle_call({:call_procedure, procedure, args}, _from, state) do
    {state, request_id} = next_request_id(state)
    send_msg(state, enc(state, :encode_call_procedure, [procedure, args, request_id]))
    {:reply, {:ok, request_id}, state}
  end

  def handle_call({:one_off_query, query_string}, _from, state) do
    {state, request_id} = next_request_id(state)
    send_msg(state, enc(state, :encode_one_off_query, [query_string, request_id]))
    {:reply, {:ok, request_id}, state}
  end

  def handle_call(:status, _from, state), do: {:reply, state.status, state}

  # ---------------------------------------------------------------------------
  # Info — connection lifecycle
  # ---------------------------------------------------------------------------

  @impl GenServer
  def handle_info(:connect, state) do
    case do_connect(state) do
      {:ok, state} ->
        {:noreply, state}

      {:error, reason} ->
        Logger.warning(
          "[SpacetimeDB] connect failed: #{inspect(reason)}, retrying in #{state.current_reconnect_delay_ms}ms"
        )

        schedule_reconnect(state.current_reconnect_delay_ms)
        {:noreply, %{state | status: :disconnected}}
    end
  end

  def handle_info(:reconnect, state) do
    send(self(), :connect)
    {:noreply, state}
  end

  def handle_info(message, %{conn: conn} = state) when not is_nil(conn) do
    case Mint.WebSocket.stream(conn, message) do
      {:ok, conn, responses} ->
        state = %{state | conn: conn}
        state = Enum.reduce(responses, state, &handle_response(&2, &1))
        {:noreply, state}

      {:error, _conn, %Mint.TransportError{reason: :closed}, _responses} ->
        handle_disconnect(state, :closed)

      {:error, _conn, reason, _responses} ->
        Logger.warning("[SpacetimeDB] stream error: #{inspect(reason)}")
        handle_disconnect(state, reason)

      :unknown ->
        {:noreply, state}
    end
  end

  def handle_info(_, state), do: {:noreply, state}

  # ---------------------------------------------------------------------------
  # Terminate
  # ---------------------------------------------------------------------------

  @impl GenServer
  def terminate(_reason, %{conn: conn}) when not is_nil(conn) do
    Mint.HTTP.close(conn)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  # ---------------------------------------------------------------------------
  # Private — connection setup
  # ---------------------------------------------------------------------------

  defp do_connect(state) do
    scheme = if state.tls, do: :https, else: :http
    path = "/v1/database/#{URI.encode(state.database)}/subscribe"

    headers =
      [{"sec-websocket-protocol", subprotocol(state.protocol)}] ++
        if state.token, do: [{"authorization", "Bearer #{state.token}"}], else: []

    ws_scheme = if state.tls, do: :wss, else: :ws

    with {:ok, conn} <-
           Mint.HTTP.connect(scheme, state.host, state.port, protocols: [:http1]),
         {:ok, conn, ref} <- Mint.WebSocket.upgrade(ws_scheme, conn, path, headers),
         state = %{state | conn: conn, ws_ref: ref, status: :connecting},
         {:ok, state} <- await_upgrade(state, ref) do
      Logger.info(
        "[SpacetimeDB] connected (#{state.protocol}) to #{state.host}:#{state.port}/#{state.database}"
      )

      {:ok,
       %{state | status: :connected, current_reconnect_delay_ms: state.reconnect_delay_ms}}
    end
  end

  defp await_upgrade(state, ref, acc \\ %{}) do
    receive do
      message ->
        case Mint.WebSocket.stream(state.conn, message) do
          {:ok, conn, responses} ->
            process_upgrade_responses(%{state | conn: conn}, ref, acc, responses)

          {:error, _conn, reason, _} ->
            {:error, reason}

          :unknown ->
            await_upgrade(state, ref, acc)
        end
    after
      10_000 -> {:error, :upgrade_timeout}
    end
  end

  defp process_upgrade_responses(state, ref, acc, []) do
    if Map.has_key?(acc, :done) do
      finalize_upgrade(state, ref, acc)
    else
      await_upgrade(state, ref, acc)
    end
  end

  defp process_upgrade_responses(state, ref, acc, [{:status, ref, status} | rest]) do
    process_upgrade_responses(state, ref, Map.put(acc, :status, status), rest)
  end

  defp process_upgrade_responses(state, ref, acc, [{:headers, ref, headers} | rest]) do
    process_upgrade_responses(state, ref, Map.put(acc, :headers, headers), rest)
  end

  defp process_upgrade_responses(state, ref, acc, [{:done, ref} | rest]) do
    process_upgrade_responses(state, ref, Map.put(acc, :done, true), rest)
  end

  defp process_upgrade_responses(state, ref, acc, [_ | rest]) do
    process_upgrade_responses(state, ref, acc, rest)
  end

  defp finalize_upgrade(state, ref, acc) do
    case Mint.WebSocket.new(state.conn, ref, acc.status, acc[:headers] || []) do
      {:ok, conn, websocket} ->
        {:ok, %{state | conn: conn, websocket: websocket}}

      {:error, _conn, reason} ->
        {:error, reason}
    end
  end

  # ---------------------------------------------------------------------------
  # Private — response dispatch
  # ---------------------------------------------------------------------------

  defp handle_response(state, {:data, _ref, data}) do
    case Mint.WebSocket.decode(state.websocket, data) do
      {:ok, websocket, frames} ->
        state = %{state | websocket: websocket}
        Enum.reduce(frames, state, &dispatch_frame(&2, &1))

      {:error, websocket, reason} ->
        Logger.warning("[SpacetimeDB] frame decode error: #{inspect(reason)}")
        %{state | websocket: websocket}
    end
  end

  defp handle_response(state, {:error, _ref, reason}) do
    Logger.warning("[SpacetimeDB] HTTP error: #{inspect(reason)}")
    state
  end

  defp handle_response(state, _), do: state

  # JSON protocol — text frames
  defp dispatch_frame(%{protocol: :json} = state, {:text, json}) do
    case Protocol.decode(json) do
      {:ok, msg} -> handle_server_message(state, msg)
      {:error, reason} ->
        Logger.warning("[SpacetimeDB] JSON decode error: #{inspect(reason)}")
        state
    end
  end

  # BSATN protocol — binary frames
  defp dispatch_frame(%{protocol: :bsatn} = state, {:binary, bin}) do
    case ProtocolBSATN.decode(bin) do
      {:ok, msg} -> handle_server_message(state, msg)
      {:error, reason} ->
        tag = if byte_size(bin) > 0, do: :binary.at(bin, 0), else: :empty
        hex = bin |> binary_part(0, min(byte_size(bin), 60)) |> Base.encode16()
        Logger.warning("[SpacetimeDB] BSATN decode error: #{inspect(reason)}, tag=#{tag}, #{byte_size(bin)} bytes, hex=#{hex}")
        state
    end
  end

  defp dispatch_frame(state, {:close, _code, _reason}) do
    elem(handle_disconnect(state, :peer_closed), 1)
  end

  defp dispatch_frame(state, :ping) do
    send_ws_frame(state, :pong)
    state
  end

  defp dispatch_frame(state, _), do: state

  # ---------------------------------------------------------------------------
  # Private — server message routing
  # ---------------------------------------------------------------------------

  defp handle_server_message(state, %Types.InitialConnection{} = msg) do
    state = %{state | token: msg.token}
    invoke_handler(state, :on_connected, [msg])
    state
  end

  defp handle_server_message(state, %Types.SubscribeApplied{} = msg) do
    invoke_handler(state, :on_subscribe_applied, [msg])
    state
  end

  defp handle_server_message(state, %Types.UnsubscribeApplied{} = msg) do
    invoke_handler(state, :on_unsubscribe_applied, [msg])
    state
  end

  defp handle_server_message(state, %Types.SubscriptionError{} = msg) do
    invoke_handler(state, :on_subscription_error, [msg])
    state
  end

  defp handle_server_message(state, %Types.TransactionUpdate{} = msg) do
    invoke_handler(state, :on_transaction_update, [msg])
    state
  end

  defp handle_server_message(state, %Types.ReducerResult{} = msg) do
    invoke_handler(state, :on_reducer_result, [msg])
    state
  end

  defp handle_server_message(state, %Types.ProcedureResult{} = msg) do
    invoke_handler(state, :on_procedure_result, [msg])
    state
  end

  defp handle_server_message(state, %Types.OneOffQueryResult{} = msg) do
    invoke_handler(state, :on_one_off_query_result, [msg])
    state
  end

  defp handle_server_message(state, {:unknown, raw}) do
    Logger.debug("[SpacetimeDB] unknown message: #{inspect(raw)}")
    state
  end

  defp handle_server_message(state, {:unknown_tag, tag}) do
    Logger.debug("[SpacetimeDB] unknown BSATN tag: #{tag}")
    state
  end

  # ---------------------------------------------------------------------------
  # Private — disconnect / reconnect
  # ---------------------------------------------------------------------------

  defp handle_disconnect(state, reason) do
    Logger.warning("[SpacetimeDB] disconnected: #{inspect(reason)}")
    if state.conn, do: Mint.HTTP.close(state.conn)
    invoke_handler(state, :on_disconnect, [reason])
    state = %{state | conn: nil, websocket: nil, ws_ref: nil, status: :disconnected}

    if state.reconnect do
      schedule_reconnect(state.current_reconnect_delay_ms)
      next_delay = min(state.current_reconnect_delay_ms * 2, state.max_reconnect_delay_ms)
      {:noreply, %{state | current_reconnect_delay_ms: next_delay}}
    else
      {:noreply, state}
    end
  end

  defp schedule_reconnect(delay_ms) do
    Process.send_after(self(), :reconnect, delay_ms)
  end

  # ---------------------------------------------------------------------------
  # Private — frame sending
  # ---------------------------------------------------------------------------

  # JSON protocol: text WebSocket frames
  defp send_msg(%{protocol: :json} = state, data), do: send_ws_frame(state, {:text, data})
  # BSATN protocol: binary WebSocket frames
  defp send_msg(%{protocol: :bsatn} = state, data), do: send_ws_frame(state, {:binary, data})

  defp send_ws_frame(%{conn: nil}, _frame), do: :ok

  defp send_ws_frame(%{conn: conn, websocket: ws, ws_ref: ref}, frame) do
    case Mint.WebSocket.encode(ws, frame) do
      {:ok, _ws, data} ->
        Mint.WebSocket.stream_request_body(conn, ref, data)

      {:error, reason} ->
        Logger.warning("[SpacetimeDB] encode error: #{inspect(reason)}")
    end
  end

  # ---------------------------------------------------------------------------
  # Private — helpers
  # ---------------------------------------------------------------------------

  # Route an encode call to the right protocol module
  defp enc(%{protocol: :json}, fun, args), do: apply(Protocol, fun, args)
  defp enc(%{protocol: :bsatn}, fun, args), do: apply(ProtocolBSATN, fun, args)

  defp subprotocol(:json), do: Protocol.subprotocol()
  defp subprotocol(:bsatn), do: ProtocolBSATN.subprotocol()

  # When a `:uri` option is provided, parse it into `:host`, `:port`, and `:tls`
  # so users can pass `uri: "https://maincloud.spacetimedb.com"` like the official SDKs.
  defp maybe_parse_uri(opts) do
    case Keyword.pop(opts, :uri) do
      {nil, opts} -> opts
      {uri, opts} ->
        parsed = URI.parse(uri)
        tls = parsed.scheme in ["https", "wss"]
        default_port = if tls, do: 443, else: @default_port

        opts
        |> Keyword.put_new(:host, parsed.host)
        |> Keyword.put_new(:port, parsed.port || default_port)
        |> Keyword.put_new(:tls, tls)
    end
  end

  defp next_request_id(state) do
    id = state.request_id
    {%{state | request_id: id + 1}, id}
  end

  defp build_handler({mod, arg}) when is_atom(mod), do: {mod, arg}
  defp build_handler(mod) when is_atom(mod), do: {mod, nil}
  defp build_handler(map) when is_map(map), do: {map, nil}

  defp invoke_handler(%{handler: {mod, arg}}, callback, args) when is_atom(mod) do
    if function_exported?(mod, callback, length(args) + 1) do
      apply(mod, callback, args ++ [arg])
    end
  rescue
    e ->
      Logger.warning("[SpacetimeDB] handler #{callback} raised: #{Exception.message(e)}")
  end

  defp invoke_handler(%{handler: {map, _}}, callback, args) when is_map(map) do
    case Map.get(map, callback) do
      fun when is_function(fun) -> apply(fun, args)
      nil -> :ok
    end
  rescue
    e ->
      Logger.warning("[SpacetimeDB] handler #{callback} raised: #{Exception.message(e)}")
  end
end
