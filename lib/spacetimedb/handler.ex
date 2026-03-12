defmodule SpacetimeDB.Handler do
  @moduledoc """
  Callback behaviour for receiving SpacetimeDB v2 server messages.

  Implement this behaviour in a module and pass it as the `:handler` option to
  `SpacetimeDB.start_link/1` or `SpacetimeDB.Connection.start_link/1`.

  All callbacks are optional — unimplemented callbacks are silently ignored.
  The second argument `arg` is whatever you passed as the second element of the
  `{module, arg}` tuple (or `nil` when passing just a module name).

  ## Example

      defmodule MyApp.SpacetimeHandler do
        @behaviour SpacetimeDB.Handler

        @impl true
        def on_connected(%SpacetimeDB.Types.InitialConnection{} = conn, _arg) do
          MyApp.Auth.store_token(conn.token)
        end

        @impl true
        def on_transaction_update(%SpacetimeDB.Types.TransactionUpdate{} = update, _arg) do
          Enum.each(update.query_sets, fn qs ->
            Enum.each(qs.tables, fn table ->
              MyApp.Cache.apply_diff(table.table_name, table.inserts, table.deletes)
            end)
          end)
        end
      end
  """

  alias SpacetimeDB.Types

  @doc "Called once after connecting when the server sends the `InitialConnection` message."
  @callback on_connected(Types.InitialConnection.t(), arg :: term()) :: term()

  @doc "Called when a `Subscribe` request is confirmed with initial rows."
  @callback on_subscribe_applied(Types.SubscribeApplied.t(), arg :: term()) :: term()

  @doc "Called when an `Unsubscribe` request is confirmed."
  @callback on_unsubscribe_applied(Types.UnsubscribeApplied.t(), arg :: term()) :: term()

  @doc "Called when a subscription is rejected or invalidated by the server."
  @callback on_subscription_error(Types.SubscriptionError.t(), arg :: term()) :: term()

  @doc "Called when subscribed rows change due to a committed transaction."
  @callback on_transaction_update(Types.TransactionUpdate.t(), arg :: term()) :: term()

  @doc "Called with the result of a `call_reducer/3` call."
  @callback on_reducer_result(Types.ReducerResult.t(), arg :: term()) :: term()

  @doc "Called with the result of a `call_procedure/3` call."
  @callback on_procedure_result(Types.ProcedureResult.t(), arg :: term()) :: term()

  @doc "Called with the result of a `one_off_query/2` call."
  @callback on_one_off_query_result(Types.OneOffQueryResult.t(), arg :: term()) :: term()

  @doc "Called when the WebSocket connection drops. `reason` is `:closed`, `:peer_closed`, or a `Mint.TransportError`."
  @callback on_disconnect(reason :: term(), arg :: term()) :: term()

  @optional_callbacks [
    on_connected: 2,
    on_subscribe_applied: 2,
    on_unsubscribe_applied: 2,
    on_subscription_error: 2,
    on_transaction_update: 2,
    on_reducer_result: 2,
    on_procedure_result: 2,
    on_one_off_query_result: 2,
    on_disconnect: 2
  ]
end
