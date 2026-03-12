defmodule SpacetimeDB.Types do
  @moduledoc "Structs representing SpacetimeDB v2 WebSocket protocol types."

  # ---------------------------------------------------------------------------
  # Identifiers
  # ---------------------------------------------------------------------------

  defmodule Identity do
    @moduledoc "A 256-bit public key identifying a SpacetimeDB user or module."
    @enforce_keys [:hex]
    defstruct [:hex]
    @type t :: %__MODULE__{hex: String.t()}
  end

  defmodule ConnectionId do
    @moduledoc "An opaque identifier for a single client WebSocket connection."
    @enforce_keys [:hex]
    defstruct [:hex]
    @type t :: %__MODULE__{hex: String.t()}
  end

  defmodule Timestamp do
    @moduledoc "A point in time, in microseconds since the Unix epoch."
    @enforce_keys [:microseconds_since_epoch]
    defstruct [:microseconds_since_epoch]
    @type t :: %__MODULE__{microseconds_since_epoch: integer()}

    @doc "Convert to an Elixir `DateTime`."
    @spec to_datetime(t()) :: DateTime.t()
    def to_datetime(%__MODULE__{microseconds_since_epoch: us}),
      do: DateTime.from_unix!(us, :microsecond)
  end

  # ---------------------------------------------------------------------------
  # Row data
  # ---------------------------------------------------------------------------

  defmodule TableUpdate do
    @moduledoc """
    Row changes for a single table within a transaction or subscription response.

    In v2 the protocol distinguishes persistent tables (inserts + deletes) from
    event tables (transient events).  For simplicity both are normalised here:
    event table rows appear in `inserts` with `deletes` empty.

    Row data in BSATN mode is raw binaries; decode with `SpacetimeDB.BSATN.Schema`.
    """
    defstruct table_name: nil, inserts: [], deletes: []

    @type t :: %__MODULE__{
            table_name: String.t() | nil,
            inserts: [term()],
            deletes: [term()]
          }
  end

  defmodule QuerySetUpdate do
    @moduledoc "Row changes grouped by query set, as seen in `TransactionUpdate`."
    defstruct query_set_id: nil, tables: []

    @type t :: %__MODULE__{
            query_set_id: non_neg_integer() | nil,
            tables: [TableUpdate.t()]
          }
  end

  # ---------------------------------------------------------------------------
  # Server → Client messages
  # ---------------------------------------------------------------------------

  defmodule InitialConnection do
    @moduledoc """
    First message after connecting — contains the client's identity and session token.

    (Named `IdentityToken` in v1.)
    """
    @enforce_keys [:identity, :token, :connection_id]
    defstruct [:identity, :token, :connection_id]

    @type t :: %__MODULE__{
            identity: String.t(),
            token: String.t(),
            connection_id: String.t()
          }
  end

  # Backward-compatibility alias
  defmodule IdentityToken do
    @moduledoc false
    defstruct [:identity, :token, :connection_id]
    @type t :: %__MODULE__{identity: String.t(), token: String.t(), connection_id: String.t()}
  end

  defmodule SubscribeApplied do
    @moduledoc "Confirmation + initial rows for a `Subscribe` request."
    defstruct request_id: nil, query_set_id: nil, tables: []

    @type t :: %__MODULE__{
            request_id: non_neg_integer() | nil,
            query_set_id: non_neg_integer() | nil,
            tables: [TableUpdate.t()]
          }
  end

  defmodule UnsubscribeApplied do
    @moduledoc "Confirmation that an `Unsubscribe` was processed."
    defstruct request_id: nil, query_set_id: nil, tables: []

    @type t :: %__MODULE__{
            request_id: non_neg_integer() | nil,
            query_set_id: non_neg_integer() | nil,
            tables: [TableUpdate.t()]
          }
  end

  defmodule SubscriptionError do
    @moduledoc "The server rejected or invalidated a subscription."
    defstruct request_id: nil, query_set_id: nil, error: nil

    @type t :: %__MODULE__{
            request_id: non_neg_integer() | nil,
            query_set_id: non_neg_integer() | nil,
            error: String.t()
          }
  end

  defmodule TransactionUpdate do
    @moduledoc """
    Pushed when subscribed rows are changed by a committed transaction.

    In v2 this contains a list of `QuerySetUpdate` structs — one per subscription
    query set affected by the transaction.
    """
    defstruct query_sets: []

    @type t :: %__MODULE__{
            query_sets: [QuerySetUpdate.t()]
          }
  end

  defmodule ReducerCallInfo do
    @moduledoc "Metadata about a reducer call result."
    defstruct reducer_name: nil, request_id: nil, status: nil

    @type t :: %__MODULE__{
            reducer_name: String.t() | nil,
            request_id: non_neg_integer() | nil,
            status: :committed | {:failed, String.t()} | :out_of_energy | nil
          }
  end

  defmodule ReducerResult do
    @moduledoc """
    Result of a `CallReducer` request, sent back to the caller.

    `outcome` is one of:
    - `{:ok, ret_value, transaction_update}` — success with return value and table changes
    - `:ok_empty` — success with no return value or table changes
    - `{:error, error_bytes}` — structured error from the reducer
    - `{:internal_error, message}` — unstructured server-side error
    """
    defstruct request_id: nil, timestamp: nil, outcome: nil

    @type outcome ::
            {:ok, binary(), TransactionUpdate.t()}
            | :ok_empty
            | {:error, binary()}
            | {:internal_error, String.t()}

    @type t :: %__MODULE__{
            request_id: non_neg_integer() | nil,
            timestamp: Timestamp.t() | nil,
            outcome: outcome()
          }
  end

  defmodule ProcedureResult do
    @moduledoc """
    Result of a `CallProcedure` request.

    `status` is one of:
    - `{:returned, bytes}` — success with return value
    - `{:internal_error, message}` — server-side error
    """
    defstruct request_id: nil, timestamp: nil, status: nil, execution_duration_micros: 0

    @type status :: {:returned, binary()} | {:internal_error, String.t()}

    @type t :: %__MODULE__{
            request_id: non_neg_integer() | nil,
            timestamp: Timestamp.t() | nil,
            status: status(),
            execution_duration_micros: non_neg_integer()
          }
  end

  defmodule OneOffQueryResult do
    @moduledoc "Response to a `OneOffQuery` request."
    defstruct request_id: nil, error: nil, tables: []

    @type t :: %__MODULE__{
            request_id: non_neg_integer() | nil,
            error: String.t() | nil,
            tables: [TableUpdate.t()]
          }
  end
end
