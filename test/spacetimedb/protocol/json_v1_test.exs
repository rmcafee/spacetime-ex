defmodule SpacetimeDB.Protocol.JsonV1Test do
  use ExUnit.Case, async: true

  alias SpacetimeDB.Protocol.JsonV1
  alias SpacetimeDB.Types

  # ---------------------------------------------------------------------------
  # Encoding
  # ---------------------------------------------------------------------------

  describe "subprotocol/0" do
    test "returns v1.json.spacetimedb" do
      assert JsonV1.subprotocol() == "v1.json.spacetimedb"
    end
  end

  describe "encode_subscribe/3" do
    test "produces correct JSON without query_set_id" do
      json = JsonV1.encode_subscribe(["SELECT * FROM Player"], 1)
      assert {:ok, %{"Subscribe" => d}} = Jason.decode(json)
      assert d["query_strings"] == ["SELECT * FROM Player"]
      assert d["request_id"] == 1
      refute Map.has_key?(d, "query_set_id")
    end

    test "ignores query_set_id argument" do
      json = JsonV1.encode_subscribe(["SELECT * FROM Player"], 1, 42)
      assert {:ok, %{"Subscribe" => d}} = Jason.decode(json)
      refute Map.has_key?(d, "query_set_id")
    end
  end

  describe "encode_unsubscribe/3" do
    test "produces correct JSON with query_id" do
      json = JsonV1.encode_unsubscribe(4, 7)
      assert {:ok, %{"Unsubscribe" => d}} = Jason.decode(json)
      assert d["request_id"] == 4
      assert d["query_id"] == %{"id" => 7}
    end
  end

  describe "encode_call_reducer/3" do
    test "encodes args as a JSON string" do
      json = JsonV1.encode_call_reducer("CreatePlayer", ["Alice", 100], 5)
      assert {:ok, %{"CallReducer" => d}} = Jason.decode(json)
      assert d["reducer"] == "CreatePlayer"
      assert d["request_id"] == 5
      assert d["flags"] == 0
      # args must be a JSON-encoded string, not a raw array
      assert is_binary(d["args"])
      assert {:ok, ["Alice", 100]} = Jason.decode(d["args"])
    end
  end

  describe "encode_call_procedure/3" do
    test "encodes procedure call" do
      json = JsonV1.encode_call_procedure("FetchData", ["param1"], 6)
      assert {:ok, %{"CallProcedure" => d}} = Jason.decode(json)
      assert d["procedure"] == "FetchData"
      assert d["request_id"] == 6
    end
  end

  describe "encode_one_off_query/2" do
    test "encodes with hex message_id" do
      json = JsonV1.encode_one_off_query("SELECT * FROM T", 10)
      assert {:ok, %{"OneOffQuery" => d}} = Jason.decode(json)
      assert d["query_string"] == "SELECT * FROM T"
      assert is_binary(d["message_id"])
    end
  end

  # ---------------------------------------------------------------------------
  # Decoding
  # ---------------------------------------------------------------------------

  describe "decode/1 — IdentityToken" do
    test "decodes to InitialConnection struct" do
      json =
        Jason.encode!(%{
          "IdentityToken" => %{
            "identity" => %{"__identity__" => "abc123"},
            "token" => "tok",
            "connection_id" => %{"__connection_id__" => "conn-1"}
          }
        })

      assert {:ok, %Types.InitialConnection{} = msg} = JsonV1.decode(json)
      assert msg.identity == "abc123"
      assert msg.token == "tok"
      assert msg.connection_id == "conn-1"
    end

    test "handles plain string identity" do
      json =
        Jason.encode!(%{
          "IdentityToken" => %{
            "identity" => "abc123",
            "token" => "tok",
            "connection_id" => "conn-1"
          }
        })

      assert {:ok, %Types.InitialConnection{identity: "abc123"}} = JsonV1.decode(json)
    end
  end

  describe "decode/1 — InitialSubscription" do
    test "decodes to SubscribeApplied with flat tables" do
      json =
        Jason.encode!(%{
          "InitialSubscription" => %{
            "database_update" => %{
              "tables" => [
                %{
                  "table_name" => "Player",
                  "table_id" => 1,
                  "num_rows" => 2,
                  "updates" => [
                    %{
                      "inserts" => [
                        Jason.encode!(%{"id" => 1, "name" => "Alice"}),
                        Jason.encode!(%{"id" => 2, "name" => "Bob"})
                      ],
                      "deletes" => []
                    }
                  ]
                }
              ]
            },
            "request_id" => 1
          }
        })

      assert {:ok, %Types.SubscribeApplied{} = msg} = JsonV1.decode(json)
      assert msg.request_id == 1
      assert msg.query_set_id == nil
      assert [%Types.TableUpdate{table_name: "Player"} = table] = msg.tables
      assert length(table.inserts) == 2
      assert Enum.at(table.inserts, 0)["name"] == "Alice"
    end
  end

  describe "decode/1 — SubscribeApplied" do
    test "decodes v1 SubscribeApplied" do
      json =
        Jason.encode!(%{
          "SubscribeApplied" => %{
            "request_id" => 2,
            "query_id" => %{"id" => 42},
            "rows" => %{
              "table_name" => "Player",
              "table_rows" => %{
                "table_name" => "Player",
                "updates" => [
                  %{
                    "inserts" => [Jason.encode!(%{"id" => 1})],
                    "deletes" => []
                  }
                ]
              }
            }
          }
        })

      assert {:ok, %Types.SubscribeApplied{} = msg} = JsonV1.decode(json)
      assert msg.request_id == 2
      assert msg.query_set_id == 42
    end
  end

  describe "decode/1 — UnsubscribeApplied" do
    test "decodes correctly" do
      json =
        Jason.encode!(%{
          "UnsubscribeApplied" => %{
            "request_id" => 6,
            "query_id" => %{"id" => 9}
          }
        })

      assert {:ok, %Types.UnsubscribeApplied{request_id: 6, query_set_id: 9}} = JsonV1.decode(json)
    end
  end

  describe "decode/1 — SubscriptionError" do
    test "decodes error with SATS Option fields" do
      json =
        Jason.encode!(%{
          "SubscriptionError" => %{
            "request_id" => %{"some" => 7},
            "query_id" => %{"some" => 3},
            "table_id" => %{"none" => %{}},
            "error" => "table not found: Foo"
          }
        })

      assert {:ok, %Types.SubscriptionError{} = msg} = JsonV1.decode(json)
      assert msg.request_id == 7
      assert msg.query_set_id == 3
      assert msg.error == "table not found: Foo"
    end

    test "handles none option values" do
      json =
        Jason.encode!(%{
          "SubscriptionError" => %{
            "request_id" => %{"none" => %{}},
            "query_id" => %{"none" => %{}},
            "error" => "unknown error"
          }
        })

      assert {:ok, %Types.SubscriptionError{request_id: nil, query_set_id: nil}} =
               JsonV1.decode(json)
    end
  end

  describe "decode/1 — TransactionUpdate" do
    test "decodes committed status with flat tables" do
      json =
        Jason.encode!(%{
          "TransactionUpdate" => %{
            "status" => %{
              "Committed" => %{
                "tables" => [
                  %{
                    "table_name" => "Player",
                    "table_id" => 1,
                    "num_rows" => 1,
                    "updates" => [
                      %{
                        "inserts" => [Jason.encode!(%{"name" => "Alice"})],
                        "deletes" => []
                      }
                    ]
                  }
                ]
              }
            },
            "timestamp" => %{
              "__timestamp_micros_since_unix_epoch__" => 1_700_000_000_000_000
            },
            "caller_identity" => %{"__identity__" => "caller1"}
          }
        })

      assert {:ok, %Types.TransactionUpdate{} = msg} = JsonV1.decode(json)
      # v1 flat tables are wrapped in a single QuerySetUpdate for v2 compat
      assert [%Types.QuerySetUpdate{tables: tables}] = msg.query_sets
      assert [%Types.TableUpdate{table_name: "Player"} = table] = tables
      assert [%{"name" => "Alice"}] = table.inserts
    end

    test "handles Failed status" do
      json =
        Jason.encode!(%{
          "TransactionUpdate" => %{
            "status" => %{"Failed" => "reducer error"},
            "timestamp" => %{"__timestamp_micros_since_unix_epoch__" => 0}
          }
        })

      assert {:ok, %Types.TransactionUpdate{query_sets: [%{tables: []}]}} = JsonV1.decode(json)
    end

    test "handles OutOfEnergy status" do
      json =
        Jason.encode!(%{
          "TransactionUpdate" => %{
            "status" => %{"OutOfEnergy" => %{}},
            "timestamp" => %{"__timestamp_micros_since_unix_epoch__" => 0}
          }
        })

      assert {:ok, %Types.TransactionUpdate{query_sets: [%{tables: []}]}} = JsonV1.decode(json)
    end
  end

  describe "decode/1 — TransactionUpdateLight" do
    test "decodes to TransactionUpdate struct" do
      json =
        Jason.encode!(%{
          "TransactionUpdateLight" => %{
            "request_id" => 2,
            "update" => %{
              "tables" => [
                %{
                  "table_name" => "Messages",
                  "updates" => [
                    %{
                      "inserts" => [Jason.encode!(%{"text" => "hello"})],
                      "deletes" => []
                    }
                  ]
                }
              ]
            }
          }
        })

      assert {:ok, %Types.TransactionUpdate{} = msg} = JsonV1.decode(json)
      assert [%Types.QuerySetUpdate{tables: [%Types.TableUpdate{table_name: "Messages"}]}] =
               msg.query_sets
    end
  end

  describe "decode/1 — OneOffQueryResponse" do
    test "decodes successful response" do
      json =
        Jason.encode!(%{
          "OneOffQueryResponse" => %{
            "message_id" => "0a",
            "error" => %{"none" => %{}},
            "tables" => [
              %{
                "table_name" => "Player",
                "rows" => [Jason.encode!(%{"name" => "Alice"})]
              }
            ]
          }
        })

      assert {:ok, %Types.OneOffQueryResult{} = msg} = JsonV1.decode(json)
      assert msg.request_id == 10
      assert msg.error == nil
      assert [%Types.TableUpdate{table_name: "Player"}] = msg.tables
    end

    test "decodes error response" do
      json =
        Jason.encode!(%{
          "OneOffQueryResponse" => %{
            "message_id" => "01",
            "error" => %{"some" => "table not found"}
          }
        })

      assert {:ok, %Types.OneOffQueryResult{error: "table not found"}} = JsonV1.decode(json)
    end
  end

  describe "decode/1 — unknown message" do
    test "returns :unknown tuple" do
      json = Jason.encode!(%{"FutureMessage" => %{"data" => 1}})
      assert {:ok, {:unknown, %{"FutureMessage" => _}}} = JsonV1.decode(json)
    end
  end

  describe "decode/1 — invalid JSON" do
    test "returns error" do
      assert {:error, _} = JsonV1.decode("not json {{{")
    end
  end

  # ---------------------------------------------------------------------------
  # SATS-JSON helpers
  # ---------------------------------------------------------------------------

  describe "SATS-JSON identity decoding" do
    test "decodes wrapped identity" do
      json =
        Jason.encode!(%{
          "IdentityToken" => %{
            "identity" => %{"__identity__" => "deadbeef"},
            "token" => "t",
            "connection_id" => "c"
          }
        })

      assert {:ok, %Types.InitialConnection{identity: "deadbeef"}} = JsonV1.decode(json)
    end
  end

  describe "SATS-JSON timestamp decoding" do
    test "decodes wrapped timestamp in TransactionUpdate" do
      json =
        Jason.encode!(%{
          "TransactionUpdate" => %{
            "status" => %{"Committed" => %{"tables" => []}},
            "timestamp" => %{
              "__timestamp_micros_since_unix_epoch__" => 1_700_000_000_000_000
            }
          }
        })

      assert {:ok, %Types.TransactionUpdate{}} = JsonV1.decode(json)
    end
  end

  describe "encode/decode message_id roundtrip" do
    test "request_id survives encode → decode" do
      json = JsonV1.encode_one_off_query("SELECT 1", 42)
      assert {:ok, %{"OneOffQuery" => %{"message_id" => hex}}} = Jason.decode(json)

      response_json =
        Jason.encode!(%{
          "OneOffQueryResponse" => %{
            "message_id" => hex,
            "error" => %{"none" => %{}},
            "tables" => []
          }
        })

      assert {:ok, %Types.OneOffQueryResult{request_id: 42}} = JsonV1.decode(response_json)
    end
  end
end
