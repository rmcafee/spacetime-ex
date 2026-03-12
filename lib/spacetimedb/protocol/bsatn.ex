defmodule SpacetimeDB.Protocol.BSATN do
  @moduledoc """
  Encodes and decodes SpacetimeDB WebSocket messages using the
  `v2.bsatn.spacetimedb` binary subprotocol.

  ## Wire format

  Every WebSocket binary frame carries exactly one message, encoded as a
  BSATN sum type:

      <<tag::u8, ...fields>>

  ### Client → server message tags

  | Tag | Message |
  |-----|---------|
  | 0 | `Subscribe` |
  | 1 | `Unsubscribe` |
  | 2 | `OneOffQuery` |
  | 3 | `CallReducer` |
  | 4 | `CallProcedure` |

  ### Server → client message tags

  | Tag | Message |
  |-----|---------|
  | 0 | `InitialConnection` |
  | 1 | `SubscribeApplied` |
  | 2 | `UnsubscribeApplied` |
  | 3 | `SubscriptionError` |
  | 4 | `TransactionUpdate` |
  | 5 | `OneOffQueryResult` |
  | 6 | `ReducerResult` |
  | 7 | `ProcedureResult` |

  Row data within table updates uses BsatnRowList — a packed binary with a
  size hint.  Rows are unpacked into individual `binary()` values in
  `SpacetimeDB.Types.TableUpdate`; decode them with `SpacetimeDB.BSATN.Schema`.
  """

  alias SpacetimeDB.{BSATN, Types}

  @subprotocol "v2.bsatn.spacetimedb"

  @doc "The WebSocket subprotocol header value."
  def subprotocol, do: @subprotocol

  # ---------------------------------------------------------------------------
  # Client → Server encoding
  # ---------------------------------------------------------------------------

  @doc "Encode a `Subscribe` message (tag 0)."
  def encode_subscribe(query_strings, request_id, query_set_id \\ 0)
      when is_list(query_strings) do
    queries = BSATN.encode_array(query_strings, &BSATN.encode_string/1)

    <<0::8,
      BSATN.encode_u32(request_id)::binary,
      BSATN.encode_u32(query_set_id)::binary,
      queries::binary>>
  end

  @doc "Encode an `Unsubscribe` message (tag 1)."
  def encode_unsubscribe(request_id, query_set_id, flags \\ 0) do
    <<1::8,
      BSATN.encode_u32(request_id)::binary,
      BSATN.encode_u32(query_set_id)::binary,
      BSATN.encode_u8(flags)::binary>>
  end

  @doc "Encode a `OneOffQuery` message (tag 2)."
  def encode_one_off_query(query_string, request_id) do
    <<2::8,
      BSATN.encode_u32(request_id)::binary,
      BSATN.encode_string(query_string)::binary>>
  end

  @doc "Encode a `CallReducer` message (tag 3)."
  def encode_call_reducer(reducer, args_bsatn, request_id)
      when is_binary(args_bsatn) do
    <<3::8,
      BSATN.encode_u32(request_id)::binary,
      BSATN.encode_u8(0)::binary,
      BSATN.encode_string(reducer)::binary,
      BSATN.encode_bytes(args_bsatn)::binary>>
  end

  @doc "Encode a `CallProcedure` message (tag 4)."
  def encode_call_procedure(procedure, args_bsatn, request_id)
      when is_binary(args_bsatn) do
    <<4::8,
      BSATN.encode_u32(request_id)::binary,
      BSATN.encode_u8(0)::binary,
      BSATN.encode_string(procedure)::binary,
      BSATN.encode_bytes(args_bsatn)::binary>>
  end

  # ---------------------------------------------------------------------------
  # Server → Client decoding
  # ---------------------------------------------------------------------------

  @doc "Decode a BSATN binary frame from the server into a typed struct."
  @spec decode(binary()) :: {:ok, term()} | {:error, term()}
  def decode(<<tag::8, rest::binary>>), do: decode_tag(tag, rest)
  def decode(_), do: {:error, :empty_frame}

  # tag 0: InitialConnection
  defp decode_tag(0, bin) do
    with {:ok, identity, bin} <- BSATN.decode_bytes(bin),
         {:ok, conn_id, bin} <- BSATN.decode_bytes(bin),
         {:ok, token, _rest} <- BSATN.decode_string(bin) do
      {:ok,
       %Types.InitialConnection{
         identity: Base.encode16(identity, case: :lower),
         connection_id: Base.encode16(conn_id, case: :lower),
         token: token
       }}
    end
  end

  # tag 1: SubscribeApplied
  defp decode_tag(1, bin) do
    with {:ok, request_id, bin} <- BSATN.decode_u32(bin),
         {:ok, query_set_id, bin} <- BSATN.decode_u32(bin),
         {:ok, tables, _rest} <- decode_query_rows(bin) do
      {:ok,
       %Types.SubscribeApplied{
         request_id: request_id,
         query_set_id: query_set_id,
         tables: tables
       }}
    end
  end

  # tag 2: UnsubscribeApplied
  defp decode_tag(2, bin) do
    with {:ok, request_id, bin} <- BSATN.decode_u32(bin),
         {:ok, query_set_id, bin} <- BSATN.decode_u32(bin),
         {:ok, tables, _rest} <- decode_option_query_rows(bin) do
      {:ok,
       %Types.UnsubscribeApplied{
         request_id: request_id,
         query_set_id: query_set_id,
         tables: tables || []
       }}
    end
  end

  # tag 3: SubscriptionError
  defp decode_tag(3, bin) do
    with {:ok, request_id, bin} <- BSATN.decode_option(bin, &BSATN.decode_u32/1),
         {:ok, query_set_id, bin} <- BSATN.decode_u32(bin),
         {:ok, error, _rest} <- BSATN.decode_string(bin) do
      {:ok,
       %Types.SubscriptionError{
         request_id: request_id,
         query_set_id: query_set_id,
         error: error
       }}
    end
  end

  # tag 4: TransactionUpdate
  defp decode_tag(4, bin) do
    with {:ok, query_sets, _rest} <- BSATN.decode_array(bin, &decode_query_set_update/1) do
      {:ok, %Types.TransactionUpdate{query_sets: query_sets}}
    end
  end

  # tag 5: OneOffQueryResult
  defp decode_tag(5, bin) do
    with {:ok, request_id, bin} <- BSATN.decode_u32(bin),
         {:ok, result, _rest} <- decode_result_query_rows(bin) do
      case result do
        {:ok, tables} ->
          {:ok, %Types.OneOffQueryResult{request_id: request_id, tables: tables}}

        {:error, msg} ->
          {:ok, %Types.OneOffQueryResult{request_id: request_id, error: msg}}
      end
    end
  end

  # tag 6: ReducerResult
  defp decode_tag(6, bin) do
    with {:ok, request_id, bin} <- BSATN.decode_u32(bin),
         {:ok, timestamp_us, bin} <- BSATN.decode_u64(bin),
         {:ok, outcome, _rest} <- decode_reducer_outcome(bin) do
      {:ok,
       %Types.ReducerResult{
         request_id: request_id,
         timestamp: %Types.Timestamp{microseconds_since_epoch: timestamp_us},
         outcome: outcome
       }}
    end
  end

  # tag 7: ProcedureResult
  defp decode_tag(7, bin) do
    with {:ok, status, bin} <- decode_procedure_status(bin),
         {:ok, timestamp_us, bin} <- BSATN.decode_u64(bin),
         {:ok, duration_us, bin} <- BSATN.decode_u64(bin),
         {:ok, request_id, _rest} <- BSATN.decode_u32(bin) do
      {:ok,
       %Types.ProcedureResult{
         request_id: request_id,
         timestamp: %Types.Timestamp{microseconds_since_epoch: timestamp_us},
         status: status,
         execution_duration_micros: duration_us
       }}
    end
  end

  defp decode_tag(tag, _bin), do: {:ok, {:unknown_tag, tag}}

  # ---------------------------------------------------------------------------
  # Helpers — QueryRows / SingleTableRows
  # ---------------------------------------------------------------------------

  # QueryRows = { tables: [SingleTableRows] }
  # SingleTableRows = { table: RawIdentifier, rows: BsatnRowList }
  defp decode_query_rows(bin) do
    BSATN.decode_array(bin, &decode_single_table_rows/1)
  end

  defp decode_option_query_rows(bin) do
    BSATN.decode_option(bin, &decode_query_rows/1)
  end

  # Result<QueryRows, String> encoded as a sum: 0=Ok, 1=Err
  defp decode_result_query_rows(<<0::8, rest::binary>>) do
    with {:ok, tables, bin} <- decode_query_rows(rest) do
      {:ok, {:ok, tables}, bin}
    end
  end

  defp decode_result_query_rows(<<1::8, rest::binary>>) do
    with {:ok, msg, bin} <- BSATN.decode_string(rest) do
      {:ok, {:error, msg}, bin}
    end
  end

  defp decode_result_query_rows(_), do: {:error, :invalid_result}

  defp decode_single_table_rows(bin) do
    with {:ok, table_name, bin} <- decode_raw_identifier(bin),
         {:ok, rows, rest} <- decode_bsatn_row_list(bin) do
      {:ok,
       %Types.TableUpdate{
         table_name: table_name,
         inserts: rows,
         deletes: []
       }, rest}
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers — QuerySetUpdate / TableUpdate
  # ---------------------------------------------------------------------------

  defp decode_query_set_update(bin) do
    with {:ok, query_set_id, bin} <- BSATN.decode_u32(bin),
         {:ok, tables, rest} <- BSATN.decode_array(bin, &decode_table_update/1) do
      {:ok,
       %Types.QuerySetUpdate{
         query_set_id: query_set_id,
         tables: tables
       }, rest}
    end
  end

  # TableUpdate = { table_name: RawIdentifier, rows: [TableUpdateRows] }
  defp decode_table_update(bin) do
    with {:ok, table_name, bin} <- decode_raw_identifier(bin),
         {:ok, row_groups, rest} <- BSATN.decode_array(bin, &decode_table_update_rows/1) do
      # Merge all row groups into a single TableUpdate
      {inserts, deletes} =
        Enum.reduce(row_groups, {[], []}, fn
          {:persistent, ins, dels}, {i_acc, d_acc} ->
            {i_acc ++ ins, d_acc ++ dels}

          {:event, events}, {i_acc, d_acc} ->
            {i_acc ++ events, d_acc}
        end)

      {:ok,
       %Types.TableUpdate{
         table_name: table_name,
         inserts: inserts,
         deletes: deletes
       }, rest}
    end
  end

  # TableUpdateRows: 0=PersistentTable, 1=EventTable
  defp decode_table_update_rows(<<0::8, rest::binary>>) do
    with {:ok, inserts, bin} <- decode_bsatn_row_list(rest),
         {:ok, deletes, rest2} <- decode_bsatn_row_list(bin) do
      {:ok, {:persistent, inserts, deletes}, rest2}
    end
  end

  defp decode_table_update_rows(<<1::8, rest::binary>>) do
    with {:ok, events, rest2} <- decode_bsatn_row_list(rest) do
      {:ok, {:event, events}, rest2}
    end
  end

  defp decode_table_update_rows(_), do: {:error, :invalid_table_update_rows}

  # ---------------------------------------------------------------------------
  # Helpers — BsatnRowList
  # ---------------------------------------------------------------------------

  # BsatnRowList = { size_hint: RowSizeHint, rows_data: Bytes }
  # RowSizeHint: 0=FixedSize(u16), 1=RowOffsets([u64])
  defp decode_bsatn_row_list(bin) do
    with {:ok, hint, bin} <- decode_row_size_hint(bin),
         {:ok, rows_data, rest} <- BSATN.decode_bytes(bin) do
      rows = split_rows(hint, rows_data)
      {:ok, rows, rest}
    end
  end

  defp decode_row_size_hint(<<0::8, rest::binary>>) do
    with {:ok, row_size, bin} <- BSATN.decode_u16(rest) do
      {:ok, {:fixed, row_size}, bin}
    end
  end

  defp decode_row_size_hint(<<1::8, rest::binary>>) do
    with {:ok, offsets, bin} <- BSATN.decode_array(rest, &BSATN.decode_u64/1) do
      {:ok, {:offsets, offsets}, bin}
    end
  end

  defp decode_row_size_hint(_), do: {:error, :invalid_row_size_hint}

  # Split packed rows_data into individual row binaries
  defp split_rows({:fixed, 0}, _data), do: []

  defp split_rows({:fixed, row_size}, data) do
    for <<row::binary-size(row_size) <- data>>, do: row
  end

  defp split_rows({:offsets, offsets}, data) do
    total = byte_size(data)

    offsets
    |> Enum.chunk_every(2, 1)
    |> Enum.map(fn
      [start, stop] -> binary_part(data, start, stop - start)
      [start] -> binary_part(data, start, total - start)
    end)
  end

  # ---------------------------------------------------------------------------
  # Helpers — RawIdentifier
  # ---------------------------------------------------------------------------

  # RawIdentifier is encoded as a BSATN string
  defp decode_raw_identifier(bin), do: BSATN.decode_string(bin)

  # ---------------------------------------------------------------------------
  # Helpers — ReducerOutcome / ProcedureStatus
  # ---------------------------------------------------------------------------

  # ReducerOutcome: 0=Ok(ReducerOk), 1=OkEmpty, 2=Err(Bytes), 3=InternalError(String)
  defp decode_reducer_outcome(<<0::8, rest::binary>>) do
    with {:ok, ret_value, bin} <- BSATN.decode_bytes(rest),
         {:ok, query_sets, bin2} <- BSATN.decode_array(bin, &decode_query_set_update/1) do
      tx_update = %Types.TransactionUpdate{query_sets: query_sets}
      {:ok, {:ok, ret_value, tx_update}, bin2}
    end
  end

  defp decode_reducer_outcome(<<1::8, rest::binary>>), do: {:ok, :ok_empty, rest}

  defp decode_reducer_outcome(<<2::8, rest::binary>>) do
    with {:ok, err_bytes, bin} <- BSATN.decode_bytes(rest) do
      {:ok, {:error, err_bytes}, bin}
    end
  end

  defp decode_reducer_outcome(<<3::8, rest::binary>>) do
    with {:ok, msg, bin} <- BSATN.decode_string(rest) do
      {:ok, {:internal_error, msg}, bin}
    end
  end

  defp decode_reducer_outcome(_), do: {:error, :invalid_reducer_outcome}

  # ProcedureStatus: 0=Returned(Bytes), 1=InternalError(String)
  defp decode_procedure_status(<<0::8, rest::binary>>) do
    with {:ok, ret_bytes, bin} <- BSATN.decode_bytes(rest) do
      {:ok, {:returned, ret_bytes}, bin}
    end
  end

  defp decode_procedure_status(<<1::8, rest::binary>>) do
    with {:ok, msg, bin} <- BSATN.decode_string(rest) do
      {:ok, {:internal_error, msg}, bin}
    end
  end

  defp decode_procedure_status(_), do: {:error, :invalid_procedure_status}
end
