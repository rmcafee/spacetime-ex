defmodule SpacetimeDB.Protocol do
  @moduledoc """
  Encodes and decodes SpacetimeDB WebSocket messages using the
  `v2.json.spacetimedb` text subprotocol.

  ## Wire format

  Both client→server and server→client messages are tagged JSON objects:

      # client → server
      {"Subscribe":     {"request_id": N, "query_set_id": N, "query_strings": [...]}}
      {"Unsubscribe":   {"request_id": N, "query_set_id": N, "flags": 0}}
      {"OneOffQuery":   {"request_id": N, "query_string": "..."}}
      {"CallReducer":   {"request_id": N, "flags": 0, "reducer": "...", "args": "..."}}
      {"CallProcedure": {"request_id": N, "flags": 0, "procedure": "...", "args": "..."}}

      # server → client
      {"InitialConnection":  {...}}
      {"SubscribeApplied":   {...}}
      {"UnsubscribeApplied": {...}}
      {"SubscriptionError":  {...}}
      {"TransactionUpdate":  {...}}
      {"OneOffQueryResult":  {...}}
      {"ReducerResult":      {...}}
      {"ProcedureResult":    {...}}
  """

  alias SpacetimeDB.Types

  @subprotocol "v2.json.spacetimedb"

  @doc "The WebSocket subprotocol header value."
  def subprotocol, do: @subprotocol

  # ---------------------------------------------------------------------------
  # Encoding  (client → server)
  # ---------------------------------------------------------------------------

  @doc "Subscribe to one or more queries under a query set."
  def encode_subscribe(query_strings, request_id, query_set_id \\ 0)
      when is_list(query_strings) do
    Jason.encode!(%{
      "Subscribe" => %{
        "request_id" => request_id,
        "query_set_id" => %{"id" => query_set_id},
        "query_strings" => query_strings
      }
    })
  end

  @doc "Remove a subscription."
  def encode_unsubscribe(request_id, query_set_id, flags \\ 0) do
    Jason.encode!(%{
      "Unsubscribe" => %{
        "request_id" => request_id,
        "query_set_id" => %{"id" => query_set_id},
        "flags" => flags_to_string(flags)
      }
    })
  end

  @doc """
  Call a reducer.

  `args` must be JSON-serialisable (list of reducer arguments in order).
  In the JSON wire format, args are sent as a JSON-encoded string.
  """
  def encode_call_reducer(reducer, args, request_id) do
    Jason.encode!(%{
      "CallReducer" => %{
        "request_id" => request_id,
        "flags" => "Default",
        "reducer" => reducer,
        "args" => Jason.encode!(args)
      }
    })
  end

  @doc "Call a procedure."
  def encode_call_procedure(procedure, args, request_id) do
    Jason.encode!(%{
      "CallProcedure" => %{
        "request_id" => request_id,
        "flags" => "Default",
        "procedure" => procedure,
        "args" => Jason.encode!(args)
      }
    })
  end

  @doc "Execute a one-off query without establishing a subscription."
  def encode_one_off_query(query_string, request_id) do
    Jason.encode!(%{
      "OneOffQuery" => %{
        "request_id" => request_id,
        "query_string" => query_string
      }
    })
  end

  # ---------------------------------------------------------------------------
  # Decoding  (server → client)
  # ---------------------------------------------------------------------------

  @doc "Decode a JSON text frame from the server into a typed struct."
  @spec decode(binary()) :: {:ok, term()} | {:error, term()}
  def decode(json) when is_binary(json) do
    case Jason.decode(json) do
      {:ok, map} -> {:ok, decode_map(map)}
      {:error, _} = err -> err
    end
  end

  defp decode_map(%{"InitialConnection" => d}) do
    %Types.InitialConnection{
      identity: d["identity"],
      token: d["token"],
      connection_id: d["connection_id"] || d["connectionId"]
    }
  end

  defp decode_map(%{"SubscribeApplied" => d}) do
    %Types.SubscribeApplied{
      request_id: d["request_id"],
      query_set_id: get_query_set_id(d),
      tables: decode_query_rows(d["rows"])
    }
  end

  defp decode_map(%{"UnsubscribeApplied" => d}) do
    %Types.UnsubscribeApplied{
      request_id: d["request_id"],
      query_set_id: get_query_set_id(d),
      tables: decode_query_rows(d["rows"])
    }
  end

  defp decode_map(%{"SubscriptionError" => d}) do
    %Types.SubscriptionError{
      request_id: d["request_id"],
      query_set_id: get_query_set_id(d),
      error: d["error"]
    }
  end

  defp decode_map(%{"TransactionUpdate" => d}) do
    query_sets =
      (d["query_sets"] || [])
      |> Enum.map(fn qs ->
        %Types.QuerySetUpdate{
          query_set_id: get_query_set_id(qs),
          tables: decode_table_updates(qs["tables"] || [])
        }
      end)

    %Types.TransactionUpdate{query_sets: query_sets}
  end

  defp decode_map(%{"OneOffQueryResult" => d}) do
    case d["result"] do
      %{"Ok" => rows} ->
        %Types.OneOffQueryResult{
          request_id: d["request_id"],
          tables: decode_query_rows(rows)
        }

      %{"Err" => msg} ->
        %Types.OneOffQueryResult{
          request_id: d["request_id"],
          error: msg
        }

      _ ->
        %Types.OneOffQueryResult{
          request_id: d["request_id"],
          tables: decode_query_rows(d["rows"])
        }
    end
  end

  defp decode_map(%{"ReducerResult" => d}) do
    %Types.ReducerResult{
      request_id: d["request_id"],
      timestamp: decode_timestamp(d["timestamp"]),
      outcome: decode_reducer_outcome(d["result"])
    }
  end

  defp decode_map(%{"ProcedureResult" => d}) do
    %Types.ProcedureResult{
      request_id: d["request_id"],
      timestamp: decode_timestamp(d["timestamp"]),
      status: decode_procedure_status(d["status"]),
      execution_duration_micros: d["total_host_execution_duration"] || 0
    }
  end

  defp decode_map(other), do: {:unknown, other}

  # ---------------------------------------------------------------------------
  # Helpers — QueryRows / TableUpdate
  # ---------------------------------------------------------------------------

  # QueryRows: { tables: [SingleTableRows] }
  defp decode_query_rows(nil), do: []

  defp decode_query_rows(%{"tables" => tables}) when is_list(tables) do
    Enum.map(tables, fn t ->
      %Types.TableUpdate{
        table_name: t["table"] || t["table_name"],
        inserts: decode_rows(t["rows"] || []),
        deletes: []
      }
    end)
  end

  defp decode_query_rows(rows) when is_list(rows) do
    Enum.map(rows, fn t ->
      %Types.TableUpdate{
        table_name: t["table_name"] || t["tableName"] || t["table"],
        inserts: decode_rows(t["inserts"] || t["rows"] || []),
        deletes: decode_rows(t["deletes"] || [])
      }
    end)
  end

  defp decode_query_rows(_), do: []

  defp decode_table_updates(tables) when is_list(tables) do
    Enum.map(tables, fn t ->
      {inserts, deletes} = decode_table_update_rows(t["rows"] || [])

      %Types.TableUpdate{
        table_name: t["table_name"],
        inserts: inserts,
        deletes: deletes
      }
    end)
  end

  defp decode_table_updates(_), do: []

  defp decode_table_update_rows(row_groups) when is_list(row_groups) do
    Enum.reduce(row_groups, {[], []}, fn
      %{"PersistentTable" => pt}, {i_acc, d_acc} ->
        {i_acc ++ decode_rows(pt["inserts"] || []),
         d_acc ++ decode_rows(pt["deletes"] || [])}

      %{"EventTable" => et}, {i_acc, d_acc} ->
        {i_acc ++ decode_rows(et["events"] || []), d_acc}

      _, acc ->
        acc
    end)
  end

  defp decode_table_update_rows(_), do: {[], []}

  defp decode_rows(rows) when is_list(rows) do
    Enum.map(rows, fn
      row when is_binary(row) ->
        case Jason.decode(row) do
          {:ok, decoded} -> decoded
          _ -> row
        end

      row ->
        row
    end)
  end

  defp decode_rows(_), do: []

  # ---------------------------------------------------------------------------
  # Helpers — ReducerOutcome / ProcedureStatus
  # ---------------------------------------------------------------------------

  defp decode_reducer_outcome(%{"Ok" => ok}) do
    tx = ok["transaction_update"]
    query_sets = if tx, do: decode_tx_query_sets(tx), else: []
    {:ok, ok["ret_value"] || <<>>, %Types.TransactionUpdate{query_sets: query_sets}}
  end

  defp decode_reducer_outcome(%{"OkEmpty" => _}), do: :ok_empty
  defp decode_reducer_outcome("OkEmpty"), do: :ok_empty

  defp decode_reducer_outcome(%{"Err" => err_data}), do: {:error, err_data}

  defp decode_reducer_outcome(%{"InternalError" => msg}), do: {:internal_error, msg}

  defp decode_reducer_outcome(_), do: :ok_empty

  defp decode_procedure_status(%{"Returned" => data}), do: {:returned, data}
  defp decode_procedure_status(%{"InternalError" => msg}), do: {:internal_error, msg}
  defp decode_procedure_status(_), do: {:internal_error, "unknown status"}

  defp decode_tx_query_sets(%{"query_sets" => sets}) when is_list(sets) do
    Enum.map(sets, fn qs ->
      %Types.QuerySetUpdate{
        query_set_id: get_query_set_id(qs),
        tables: decode_table_updates(qs["tables"] || [])
      }
    end)
  end

  defp decode_tx_query_sets(_), do: []

  # ---------------------------------------------------------------------------
  # Helpers — common
  # ---------------------------------------------------------------------------

  defp decode_timestamp(nil), do: nil
  defp decode_timestamp(us) when is_integer(us), do: %Types.Timestamp{microseconds_since_epoch: us}

  defp decode_timestamp(%{"microseconds" => us}),
    do: %Types.Timestamp{microseconds_since_epoch: us}

  defp decode_timestamp(%{"__time_duration_micros__" => us}),
    do: %Types.Timestamp{microseconds_since_epoch: us}

  defp get_query_set_id(%{"query_set_id" => %{"id" => id}}), do: id
  defp get_query_set_id(%{"query_set_id" => id}) when is_integer(id), do: id
  defp get_query_set_id(%{"querySetId" => %{"id" => id}}), do: id
  defp get_query_set_id(%{"querySetId" => id}) when is_integer(id), do: id
  defp get_query_set_id(_), do: nil

  defp flags_to_string(0), do: "Default"
  defp flags_to_string(1), do: "SendDroppedRows"
  defp flags_to_string(n) when is_integer(n), do: "Default"
  defp flags_to_string(s) when is_binary(s), do: s
end
