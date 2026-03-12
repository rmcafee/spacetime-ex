defmodule SpacetimeDB.ProtocolTest do
  use ExUnit.Case, async: true

  alias SpacetimeDB.Protocol
  alias SpacetimeDB.Types

  # ---------------------------------------------------------------------------
  # Encoding
  # ---------------------------------------------------------------------------

  describe "encode_subscribe/3" do
    test "produces correct JSON" do
      json = Protocol.encode_subscribe(["SELECT * FROM Player"], 1)
      assert {:ok, map} = Jason.decode(json)
      assert %{"Subscribe" => %{"query_strings" => ["SELECT * FROM Player"], "request_id" => 1}} = map
    end

    test "includes query_set_id" do
      json = Protocol.encode_subscribe(["SELECT * FROM Player"], 1, 42)
      assert {:ok, %{"Subscribe" => d}} = Jason.decode(json)
      assert d["query_set_id"] == %{"id" => 42}
    end
  end

  describe "encode_unsubscribe/3" do
    test "produces correct JSON" do
      json = Protocol.encode_unsubscribe(4, 7)
      assert {:ok, %{"Unsubscribe" => d}} = Jason.decode(json)
      assert d["request_id"] == 4
      assert d["query_set_id"] == %{"id" => 7}
    end
  end

  describe "encode_call_reducer/3" do
    test "encodes args as a JSON string" do
      json = Protocol.encode_call_reducer("CreatePlayer", ["Alice", 100], 5)
      assert {:ok, %{"CallReducer" => d}} = Jason.decode(json)
      assert d["reducer"] == "CreatePlayer"
      assert d["request_id"] == 5
      assert d["flags"] == "Default"
      # args must be a JSON-encoded string, not a raw array
      assert is_binary(d["args"])
      assert {:ok, ["Alice", 100]} = Jason.decode(d["args"])
    end
  end

  describe "encode_call_procedure/3" do
    test "encodes procedure call" do
      json = Protocol.encode_call_procedure("FetchData", ["param1"], 6)
      assert {:ok, %{"CallProcedure" => d}} = Jason.decode(json)
      assert d["procedure"] == "FetchData"
      assert d["request_id"] == 6
      assert d["flags"] == "Default"
    end
  end

  describe "encode_one_off_query/2" do
    test "encodes with request_id" do
      json = Protocol.encode_one_off_query("SELECT * FROM T", 10)
      assert {:ok, %{"OneOffQuery" => d}} = Jason.decode(json)
      assert d["query_string"] == "SELECT * FROM T"
      assert d["request_id"] == 10
    end
  end

  # ---------------------------------------------------------------------------
  # Decoding
  # ---------------------------------------------------------------------------

  describe "decode/1 — InitialConnection" do
    test "decodes connection info" do
      json = Jason.encode!(%{
        "InitialConnection" => %{
          "identity" => "abc123",
          "token" => "tok",
          "connection_id" => "conn-1"
        }
      })

      assert {:ok, %Types.InitialConnection{} = msg} = Protocol.decode(json)
      assert msg.identity == "abc123"
      assert msg.token == "tok"
      assert msg.connection_id == "conn-1"
    end
  end

  describe "decode/1 — SubscribeApplied" do
    test "decodes query_set_id from nested map" do
      json = Jason.encode!(%{
        "SubscribeApplied" => %{
          "request_id" => 2,
          "query_set_id" => %{"id" => 42},
          "rows" => %{"tables" => []}
        }
      })

      assert {:ok, %Types.SubscribeApplied{query_set_id: 42, request_id: 2}} = Protocol.decode(json)
    end

    test "decodes query_set_id as integer" do
      json = Jason.encode!(%{
        "SubscribeApplied" => %{"request_id" => 3, "query_set_id" => 7, "rows" => %{"tables" => []}}
      })

      assert {:ok, %Types.SubscribeApplied{query_set_id: 7}} = Protocol.decode(json)
    end
  end

  describe "decode/1 — UnsubscribeApplied" do
    test "decodes correctly" do
      json = Jason.encode!(%{
        "UnsubscribeApplied" => %{"request_id" => 6, "query_set_id" => 9, "rows" => %{"tables" => []}}
      })

      assert {:ok, %Types.UnsubscribeApplied{request_id: 6, query_set_id: 9}} = Protocol.decode(json)
    end
  end

  describe "decode/1 — SubscriptionError" do
    test "decodes error message" do
      json = Jason.encode!(%{
        "SubscriptionError" => %{
          "request_id" => 7,
          "query_set_id" => %{"id" => 3},
          "error" => "table not found: Foo"
        }
      })

      assert {:ok, %Types.SubscriptionError{error: "table not found: Foo"}} = Protocol.decode(json)
    end
  end

  describe "decode/1 — TransactionUpdate" do
    test "decodes query_sets with table updates" do
      json = Jason.encode!(%{
        "TransactionUpdate" => %{
          "query_sets" => [
            %{
              "query_set_id" => %{"id" => 1},
              "tables" => [
                %{
                  "table_name" => "Player",
                  "rows" => [
                    %{
                      "PersistentTable" => %{
                        "inserts" => [%{"name" => "Alice"}],
                        "deletes" => []
                      }
                    }
                  ]
                }
              ]
            }
          ]
        }
      })

      assert {:ok, %Types.TransactionUpdate{} = msg} = Protocol.decode(json)
      assert [%Types.QuerySetUpdate{query_set_id: 1, tables: tables}] = msg.query_sets
      assert [%Types.TableUpdate{table_name: "Player"}] = tables
    end
  end

  describe "decode/1 — OneOffQueryResult" do
    test "decodes Ok result" do
      json = Jason.encode!(%{
        "OneOffQueryResult" => %{
          "request_id" => 10,
          "result" => %{
            "Ok" => %{
              "tables" => [
                %{"table" => "Player", "rows" => [%{"name" => "Alice"}]}
              ]
            }
          }
        }
      })

      assert {:ok, %Types.OneOffQueryResult{} = msg} = Protocol.decode(json)
      assert msg.request_id == 10
      assert msg.error == nil
      assert [%Types.TableUpdate{table_name: "Player"}] = msg.tables
    end

    test "decodes Err result" do
      json = Jason.encode!(%{
        "OneOffQueryResult" => %{
          "request_id" => 11,
          "result" => %{"Err" => "table not found"}
        }
      })

      assert {:ok, %Types.OneOffQueryResult{error: "table not found"}} = Protocol.decode(json)
    end
  end

  describe "decode/1 — ReducerResult" do
    test "decodes OkEmpty outcome" do
      json = Jason.encode!(%{
        "ReducerResult" => %{
          "request_id" => 12,
          "timestamp" => 1_700_000_000_000_000,
          "result" => "OkEmpty"
        }
      })

      assert {:ok, %Types.ReducerResult{} = msg} = Protocol.decode(json)
      assert msg.request_id == 12
      assert msg.outcome == :ok_empty
    end

    test "decodes InternalError outcome" do
      json = Jason.encode!(%{
        "ReducerResult" => %{
          "request_id" => 13,
          "timestamp" => 1_700_000_000_000_000,
          "result" => %{"InternalError" => "something went wrong"}
        }
      })

      assert {:ok, %Types.ReducerResult{outcome: {:internal_error, "something went wrong"}}} =
               Protocol.decode(json)
    end
  end

  describe "decode/1 — ProcedureResult" do
    test "decodes Returned status" do
      json = Jason.encode!(%{
        "ProcedureResult" => %{
          "request_id" => 14,
          "timestamp" => 1_700_000_000_000_000,
          "status" => %{"Returned" => "some_data"},
          "total_host_execution_duration" => 500
        }
      })

      assert {:ok, %Types.ProcedureResult{} = msg} = Protocol.decode(json)
      assert msg.request_id == 14
      assert msg.status == {:returned, "some_data"}
      assert msg.execution_duration_micros == 500
    end
  end

  describe "decode/1 — unknown message" do
    test "returns :unknown tuple" do
      json = Jason.encode!(%{"FutureMessage" => %{"data" => 1}})
      assert {:ok, {:unknown, %{"FutureMessage" => _}}} = Protocol.decode(json)
    end
  end

  describe "decode/1 — invalid JSON" do
    test "returns error" do
      assert {:error, _} = Protocol.decode("not json {{{")
    end
  end

  # ---------------------------------------------------------------------------
  # Timestamp helpers
  # ---------------------------------------------------------------------------

  describe "Types.Timestamp.to_datetime/1" do
    test "converts microseconds to DateTime" do
      ts = %Types.Timestamp{microseconds_since_epoch: 1_700_000_000_000_000}
      dt = Types.Timestamp.to_datetime(ts)
      assert %DateTime{year: 2023} = dt
    end
  end
end
