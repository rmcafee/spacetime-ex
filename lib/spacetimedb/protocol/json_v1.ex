defmodule SpacetimeDB.Protocol.JsonV1 do
  @moduledoc """
  Encodes and decodes SpacetimeDB WebSocket messages using the
  `v1.json.spacetimedb` text subprotocol.

  ## Wire format

  Messages use named-tag JSON envelopes with SATS-JSON encoding:

      # client → server
      {"Subscribe":    {"request_id": N, "query_strings": [...]}}
      {"Unsubscribe":  {"request_id": N, "query_id": {"id": N}}}
      {"OneOffQuery":  {"message_id": "<hex>", "query_string": "..."}}
      {"CallReducer":  {"request_id": N, "reducer": "...", "args": "<json>", "flags": 0}}

      # server → client
      {"IdentityToken":        {...}}
      {"InitialSubscription":  {...}}
      {"SubscribeApplied":     {...}}
      {"UnsubscribeApplied":   {...}}
      {"SubscriptionError":    {...}}
      {"TransactionUpdate":    {...}}
      {"OneOffQueryResponse":  {...}}

  ## Key differences from v2

  - No `query_set_id` on Subscribe (v1 has no query set concept)
  - Initial rows arrive via `InitialSubscription` (flat `database_update.tables`)
  - `TransactionUpdate` has flat `status.Committed.tables` (not nested `query_sets`)
  - Identity message is `IdentityToken` (not `InitialConnection`)
  - Row data is double-JSON-encoded: each row is a JSON string inside the array
  - Identity/Timestamp/ConnectionId use SATS wrapper objects
  """

  alias SpacetimeDB.Types

  @subprotocol "v1.json.spacetimedb"

  @doc "The WebSocket subprotocol header value."
  def subprotocol, do: @subprotocol

  # ---------------------------------------------------------------------------
  # Encoding  (client → server)
  # ---------------------------------------------------------------------------

  @doc "Subscribe to one or more queries."
  def encode_subscribe(query_strings, request_id, _query_set_id \\ 0)
      when is_list(query_strings) do
    Jason.encode!(%{
      "Subscribe" => %{
        "request_id" => request_id,
        "query_strings" => query_strings
      }
    })
  end

  @doc "Remove a subscription."
  def encode_unsubscribe(request_id, query_set_id, _flags \\ 0) do
    Jason.encode!(%{
      "Unsubscribe" => %{
        "request_id" => request_id,
        "query_id" => %{"id" => query_set_id}
      }
    })
  end

  @doc """
  Call a reducer.

  `args` must be JSON-serialisable (list of reducer arguments in order).
  In v1, args are sent as a JSON-encoded string.
  """
  def encode_call_reducer(reducer, args, request_id) do
    Jason.encode!(%{
      "CallReducer" => %{
        "request_id" => request_id,
        "reducer" => reducer,
        "args" => Jason.encode!(args),
        "flags" => 0
      }
    })
  end

  @doc "Call a procedure."
  def encode_call_procedure(procedure, args, request_id) do
    Jason.encode!(%{
      "CallProcedure" => %{
        "request_id" => request_id,
        "procedure" => procedure,
        "args" => Jason.encode!(args),
        "flags" => 0
      }
    })
  end

  @doc "Execute a one-off query without establishing a subscription."
  def encode_one_off_query(query_string, request_id) do
    Jason.encode!(%{
      "OneOffQuery" => %{
        "message_id" => encode_message_id(request_id),
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

  # IdentityToken → Types.InitialConnection (same struct, different wire name)
  defp decode_map(%{"IdentityToken" => d}) do
    %Types.InitialConnection{
      identity: decode_identity(d["identity"]),
      token: d["token"],
      connection_id: decode_connection_id(d["connection_id"])
    }
  end

  # InitialSubscription → Types.SubscribeApplied (initial rows for subscription)
  defp decode_map(%{"InitialSubscription" => d}) do
    db_update = d["database_update"] || %{}

    %Types.SubscribeApplied{
      request_id: d["request_id"],
      query_set_id: nil,
      tables: decode_database_update_tables(db_update["tables"] || [])
    }
  end

  defp decode_map(%{"SubscribeApplied" => d}) do
    rows = d["rows"] || %{}
    table_rows = rows["table_rows"] || rows

    %Types.SubscribeApplied{
      request_id: d["request_id"],
      query_set_id: get_query_id(d),
      tables: decode_single_table_rows(rows["table_name"] || table_rows["table_name"], table_rows)
    }
  end

  defp decode_map(%{"UnsubscribeApplied" => d}) do
    %Types.UnsubscribeApplied{
      request_id: d["request_id"],
      query_set_id: get_query_id(d),
      tables: []
    }
  end

  defp decode_map(%{"SubscriptionError" => d}) do
    %Types.SubscriptionError{
      request_id: unwrap_option(d["request_id"]),
      query_set_id: unwrap_option(d["query_id"]),
      error: d["error"]
    }
  end

  defp decode_map(%{"TransactionUpdate" => d}) do
    tables = decode_transaction_status(d["status"])

    # Wrap in a single QuerySetUpdate for compatibility with v2 struct shape
    %Types.TransactionUpdate{
      query_sets: [
        %Types.QuerySetUpdate{
          query_set_id: nil,
          tables: tables
        }
      ]
    }
  end

  # TransactionUpdateLight → same as TransactionUpdate
  defp decode_map(%{"TransactionUpdateLight" => d}) do
    db_update = d["update"] || %{}

    %Types.TransactionUpdate{
      query_sets: [
        %Types.QuerySetUpdate{
          query_set_id: nil,
          tables: decode_database_update_tables(db_update["tables"] || [])
        }
      ]
    }
  end

  defp decode_map(%{"OneOffQueryResponse" => d}) do
    error = unwrap_option(d["error"])

    if error do
      %Types.OneOffQueryResult{
        request_id: decode_message_id(d["message_id"]),
        error: error
      }
    else
      tables =
        (d["tables"] || [])
        |> Enum.map(fn t ->
          %Types.TableUpdate{
            table_name: t["table_name"],
            inserts: decode_rows(t["rows"] || []),
            deletes: []
          }
        end)

      %Types.OneOffQueryResult{
        request_id: decode_message_id(d["message_id"]),
        tables: tables
      }
    end
  end

  defp decode_map(%{"ProcedureResult" => d}) do
    %Types.ProcedureResult{
      request_id: d["request_id"],
      timestamp: decode_timestamp(d["timestamp"]),
      status: decode_procedure_status(d["status"]),
      execution_duration_micros: decode_duration(d["total_host_execution_duration"])
    }
  end

  defp decode_map(other), do: {:unknown, other}

  # ---------------------------------------------------------------------------
  # Helpers — DatabaseUpdate tables (v1 flat structure)
  # ---------------------------------------------------------------------------

  defp decode_database_update_tables(tables) when is_list(tables) do
    Enum.map(tables, fn t ->
      updates = t["updates"] || []

      {inserts, deletes} =
        Enum.reduce(updates, {[], []}, fn update, {i_acc, d_acc} ->
          {i_acc ++ decode_rows(update["inserts"] || []),
           d_acc ++ decode_rows(update["deletes"] || [])}
        end)

      %Types.TableUpdate{
        table_name: t["table_name"],
        inserts: inserts,
        deletes: deletes
      }
    end)
  end

  defp decode_database_update_tables(_), do: []

  defp decode_single_table_rows(nil, _), do: []

  defp decode_single_table_rows(table_name, table_rows) do
    updates = table_rows["updates"] || []

    {inserts, deletes} =
      Enum.reduce(updates, {[], []}, fn update, {i_acc, d_acc} ->
        {i_acc ++ decode_rows(update["inserts"] || []),
         d_acc ++ decode_rows(update["deletes"] || [])}
      end)

    [
      %Types.TableUpdate{
        table_name: table_name,
        inserts: inserts,
        deletes: deletes
      }
    ]
  end

  # ---------------------------------------------------------------------------
  # Helpers — TransactionUpdate status
  # ---------------------------------------------------------------------------

  defp decode_transaction_status(%{"Committed" => db_update}) do
    decode_database_update_tables(db_update["tables"] || [])
  end

  defp decode_transaction_status(%{"Failed" => _msg}), do: []
  defp decode_transaction_status(%{"OutOfEnergy" => _}), do: []
  defp decode_transaction_status(_), do: []

  # ---------------------------------------------------------------------------
  # Helpers — row decoding (double-JSON-encoded strings)
  # ---------------------------------------------------------------------------

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
  # Helpers — SATS-JSON wrapper types
  # ---------------------------------------------------------------------------

  defp decode_identity(%{"__identity__" => hex}) when is_binary(hex), do: hex
  defp decode_identity(%{"__identity__" => n}) when is_integer(n), do: Integer.to_string(n, 16)
  defp decode_identity(hex) when is_binary(hex), do: hex
  defp decode_identity(_), do: nil

  defp decode_connection_id(%{"__connection_id__" => hex}) when is_binary(hex), do: hex

  defp decode_connection_id(%{"__connection_id__" => n}) when is_integer(n),
    do: Integer.to_string(n, 16)

  defp decode_connection_id(hex) when is_binary(hex), do: hex
  defp decode_connection_id(_), do: nil

  defp decode_timestamp(nil), do: nil

  defp decode_timestamp(%{"__timestamp_micros_since_unix_epoch__" => us}),
    do: %Types.Timestamp{microseconds_since_epoch: us}

  defp decode_timestamp(us) when is_integer(us),
    do: %Types.Timestamp{microseconds_since_epoch: us}

  defp decode_timestamp(_), do: nil

  defp decode_duration(%{"__time_duration_micros__" => us}), do: us
  defp decode_duration(us) when is_integer(us), do: us
  defp decode_duration(_), do: 0

  defp decode_procedure_status(%{"Returned" => data}), do: {:returned, data}
  defp decode_procedure_status(%{"InternalError" => msg}), do: {:internal_error, msg}
  defp decode_procedure_status(_), do: {:internal_error, "unknown status"}

  # Unwrap SATS Option: {"some": val} → val, {"none": {}} → nil
  defp unwrap_option(%{"some" => val}), do: val
  defp unwrap_option(%{"none" => _}), do: nil
  defp unwrap_option(val), do: val

  defp get_query_id(%{"query_id" => %{"id" => id}}), do: id
  defp get_query_id(%{"query_id" => id}) when is_integer(id), do: id
  defp get_query_id(_), do: nil

  # OneOffQuery uses a hex-encoded message_id (Box<[u8]> → hex string)
  defp encode_message_id(request_id) when is_integer(request_id) do
    request_id
    |> :binary.encode_unsigned()
    |> Base.encode16(case: :lower)
  end

  defp decode_message_id(hex) when is_binary(hex) do
    case Base.decode16(hex, case: :mixed) do
      {:ok, bin} -> :binary.decode_unsigned(bin)
      :error -> nil
    end
  end

  defp decode_message_id(_), do: nil
end
