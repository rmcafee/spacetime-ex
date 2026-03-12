defmodule SpacetimeDB.TableTest do
  use ExUnit.Case, async: false

  alias SpacetimeDB.{Table, Types}

  # ---------------------------------------------------------------------------
  # Test schema
  # ---------------------------------------------------------------------------

  defmodule Player do
    use SpacetimeDB.BSATN.Schema

    bsatn_schema do
      field :id,     :u32
      field :name,   :string
      field :health, :u32
      field :alive,  :bool
    end
  end

  defmodule Item do
    use SpacetimeDB.BSATN.Schema

    bsatn_schema do
      field :id,    :u32
      field :owner, :u32
      field :name,  :string
    end
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp player(id, name, health, alive) do
    %Player{id: id, name: name, health: health, alive: alive}
  end

  defp player_row(id, name, health \\ 100, alive \\ true) do
    Player.encode(player(id, name, health, alive))
  end

  defp table_update(table_name, inserts, deletes \\ []) do
    %Types.TableUpdate{
      table_name: table_name,
      inserts: inserts,
      deletes: deletes
    }
  end

  defp transaction_update(tables) do
    query_sets = [%Types.QuerySetUpdate{query_set_id: 0, tables: tables}]
    %Types.TransactionUpdate{query_sets: query_sets}
  end

  defp subscribe_applied(tables) do
    %Types.SubscribeApplied{tables: tables}
  end

  # ---------------------------------------------------------------------------
  # Setup — start a fresh Table for each test
  # ---------------------------------------------------------------------------

  setup do
    name = :"test_players_#{System.unique_integer([:positive])}"
    start_supervised!({Table, name: name, schema: Player, table_name: "Player", primary_key: :id})
    {:ok, name: name}
  end

  # ---------------------------------------------------------------------------
  # Initial state
  # ---------------------------------------------------------------------------

  describe "initial state" do
    test "all/1 returns empty list", %{name: name} do
      assert Table.all(name) == []
    end

    test "count/1 returns 0", %{name: name} do
      assert Table.count(name) == 0
    end

    test "get/2 returns :error", %{name: name} do
      assert :error = Table.get(name, 1)
    end
  end

  # ---------------------------------------------------------------------------
  # apply_update — TransactionUpdate
  # ---------------------------------------------------------------------------

  describe "apply_update/2 with TransactionUpdate" do
    test "inserts rows", %{name: name} do
      update =
        transaction_update([
          table_update("Player", [player_row(1, "Alice"), player_row(2, "Bob")])
        ])

      Table.apply_update(name, update)

      assert Table.count(name) == 2
      assert {:ok, %Player{name: "Alice"}} = Table.get(name, 1)
      assert {:ok, %Player{name: "Bob"}} = Table.get(name, 2)
    end

    test "deletes rows", %{name: name} do
      Table.apply_update(name, transaction_update([
        table_update("Player", [player_row(1, "Alice"), player_row(2, "Bob")])
      ]))

      Table.apply_update(name, transaction_update([
        table_update("Player", [], [player_row(1, "Alice")])
      ]))

      assert Table.count(name) == 1
      assert :error = Table.get(name, 1)
      assert {:ok, %Player{name: "Bob"}} = Table.get(name, 2)
    end

    test "insert overwrites existing row with same key", %{name: name} do
      Table.apply_update(name, transaction_update([
        table_update("Player", [player_row(1, "Alice", 100)])
      ]))

      Table.apply_update(name, transaction_update([
        table_update("Player", [player_row(1, "Alice", 50)])
      ]))

      assert {:ok, %Player{health: 50}} = Table.get(name, 1)
      assert Table.count(name) == 1
    end

    test "ignores updates for other tables", %{name: name} do
      Table.apply_update(name, transaction_update([
        table_update("Enemy", [player_row(1, "Goblin")])
      ]))

      assert Table.all(name) == []
    end

    test "processes multiple table updates in one message", %{name: name} do
      Table.apply_update(name, transaction_update([
        table_update("Enemy", [player_row(99, "Goblin")]),
        table_update("Player", [player_row(1, "Alice")]),
        table_update("Item", [])
      ]))

      assert Table.count(name) == 1
      assert {:ok, %Player{name: "Alice"}} = Table.get(name, 1)
    end
  end

  # ---------------------------------------------------------------------------
  # apply_update — SubscribeApplied
  # ---------------------------------------------------------------------------

  describe "apply_update/2 with SubscribeApplied" do
    test "seeds the table from subscribe rows", %{name: name} do
      applied = subscribe_applied([
        table_update("Player", [player_row(10, "Charlie")])
      ])

      Table.apply_update(name, applied)

      assert {:ok, %Player{name: "Charlie"}} = Table.get(name, 10)
    end
  end

  # ---------------------------------------------------------------------------
  # apply_update — ReducerResult
  # ---------------------------------------------------------------------------

  describe "apply_update/2 with ReducerResult" do
    test "applies table changes from successful reducer result", %{name: name} do
      tx = transaction_update([table_update("Player", [player_row(20, "Dave")])])
      result = %Types.ReducerResult{
        request_id: 1,
        outcome: {:ok, <<>>, tx}
      }

      Table.apply_update(name, result)

      assert {:ok, %Player{name: "Dave"}} = Table.get(name, 20)
    end
  end

  # ---------------------------------------------------------------------------
  # apply_update — bare TableUpdate
  # ---------------------------------------------------------------------------

  describe "apply_update/2 with bare TableUpdate" do
    test "applies directly without table_name filtering", %{name: name} do
      tu = %Types.TableUpdate{
        table_name: "Player",
        inserts: [player_row(5, "Dave")],
        deletes: []
      }

      Table.apply_update(name, tu)

      assert {:ok, %Player{name: "Dave"}} = Table.get(name, 5)
    end
  end

  # ---------------------------------------------------------------------------
  # Query functions
  # ---------------------------------------------------------------------------

  describe "all/1" do
    test "returns all rows as structs", %{name: name} do
      Table.apply_update(name, transaction_update([
        table_update("Player", [
          player_row(1, "Alice"),
          player_row(2, "Bob"),
          player_row(3, "Carol")
        ])
      ]))

      rows = Table.all(name)
      assert length(rows) == 3
      names = Enum.map(rows, & &1.name) |> Enum.sort()
      assert names == ["Alice", "Bob", "Carol"]
    end
  end

  describe "get/2" do
    test "returns row by primary key", %{name: name} do
      Table.apply_update(name, transaction_update([
        table_update("Player", [player_row(7, "Eve", 75)])
      ]))

      assert {:ok, %Player{id: 7, name: "Eve", health: 75}} = Table.get(name, 7)
    end

    test "returns :error for missing key", %{name: name} do
      assert :error = Table.get(name, 999)
    end
  end

  describe "filter/2" do
    test "returns only matching rows", %{name: name} do
      Table.apply_update(name, transaction_update([
        table_update("Player", [
          player_row(1, "Alice", 100, true),
          player_row(2, "Bob", 0, false),
          player_row(3, "Carol", 50, true)
        ])
      ]))

      alive = Table.filter(name, & &1.alive)
      assert length(alive) == 2
      assert Enum.all?(alive, & &1.alive)
    end

    test "returns empty list when nothing matches", %{name: name} do
      Table.apply_update(name, transaction_update([
        table_update("Player", [player_row(1, "Alice", 100, false)])
      ]))

      assert [] = Table.filter(name, & &1.alive)
    end
  end

  describe "count/1" do
    test "tracks count through inserts and deletes", %{name: name} do
      assert Table.count(name) == 0

      Table.apply_update(name, transaction_update([
        table_update("Player", [player_row(1, "Alice"), player_row(2, "Bob")])
      ]))

      assert Table.count(name) == 2

      Table.apply_update(name, transaction_update([
        table_update("Player", [], [player_row(1, "Alice")])
      ]))

      assert Table.count(name) == 1
    end
  end

  # ---------------------------------------------------------------------------
  # JSON mode rows (string-keyed maps)
  # ---------------------------------------------------------------------------

  describe "JSON mode rows" do
    test "decodes string-keyed map rows into structs", %{name: name} do
      json_row = %{"id" => 1, "name" => "Alice", "health" => 100, "alive" => true}

      tu = %Types.TableUpdate{
        table_name: "Player",
        inserts: [json_row],
        deletes: []
      }

      Table.apply_update(name, tu)

      assert {:ok, %Player{id: 1, name: "Alice"}} = Table.get(name, 1)
    end

    test "deletes via JSON row", %{name: name} do
      Table.apply_update(name, transaction_update([
        table_update("Player", [player_row(1, "Alice")])
      ]))

      json_row = %{"id" => 1, "name" => "Alice", "health" => 100, "alive" => true}
      tu = %Types.TableUpdate{table_name: "Player", inserts: [], deletes: [json_row]}
      Table.apply_update(name, tu)

      assert :error = Table.get(name, 1)
    end
  end

  # ---------------------------------------------------------------------------
  # Multiple independent tables
  # ---------------------------------------------------------------------------

  describe "multiple table instances" do
    test "two tables with different names and schemas are isolated" do
      item_name = :"test_items_#{System.unique_integer([:positive])}"
      player_name = :"test_players_multi_#{System.unique_integer([:positive])}"

      start_supervised!({Table, name: item_name, schema: Item, table_name: "Item", primary_key: :id})
      start_supervised!({Table, name: player_name, schema: Player, table_name: "Player", primary_key: :id})

      item_row = Item.encode(%Item{id: 1, owner: 42, name: "Sword"})

      Table.apply_update(item_name, transaction_update([
        table_update("Item", [item_row])
      ]))

      Table.apply_update(player_name, transaction_update([
        table_update("Player", [player_row(42, "Alice")])
      ]))

      assert Table.count(item_name) == 1
      assert Table.count(player_name) == 1
      assert {:ok, %Item{name: "Sword"}} = Table.get(item_name, 1)
      assert {:ok, %Player{name: "Alice"}} = Table.get(player_name, 42)
    end
  end
end
