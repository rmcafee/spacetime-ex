defmodule SpacetimeDB.BSATN.SchemaTest do
  use ExUnit.Case, async: true

  alias SpacetimeDB.BSATN

  # ---------------------------------------------------------------------------
  # Test schemas
  # ---------------------------------------------------------------------------

  defmodule Point do
    use SpacetimeDB.BSATN.Schema

    bsatn_schema do
      field :x, :f32
      field :y, :f32
    end
  end

  defmodule Player do
    use SpacetimeDB.BSATN.Schema

    bsatn_schema do
      field :id,     :u32
      field :name,   :string
      field :health, :u32
      field :alive,  :bool
      field :score,  :i64
    end
  end

  defmodule WithOptional do
    use SpacetimeDB.BSATN.Schema

    bsatn_schema do
      field :id,    :u32
      field :label, {:option, :string}
    end
  end

  defmodule WithArray do
    use SpacetimeDB.BSATN.Schema

    bsatn_schema do
      field :id,    :u32
      field :items, {:array, :string}
    end
  end

  defmodule Direction do
    use SpacetimeDB.BSATN.Schema

    bsatn_sum do
      variant :north, 0
      variant :south, 1
      variant :east,  2
      variant :west,  3
    end
  end

  # ---------------------------------------------------------------------------
  # Product type tests
  # ---------------------------------------------------------------------------

  describe "Point schema" do
    test "encodes and decodes" do
      p = %Point{x: 1.5, y: -3.0}
      bin = Point.encode(p)
      assert {:ok, ^p, ""} = Point.decode(bin)
    end

    test "returns leftover bytes" do
      p = %Point{x: 0.0, y: 0.0}
      bin = Point.encode(p) <> <<99>>
      assert {:ok, ^p, <<99>>} = Point.decode(bin)
    end

    test "fields/0 returns name/type pairs in order" do
      assert [{:x, :f32}, {:y, :f32}] = Point.fields()
    end
  end

  describe "Player schema" do
    test "round-trips all field types" do
      p = %Player{id: 42, name: "Alice", health: 100, alive: true, score: -500}
      bin = Player.encode(p)
      assert {:ok, ^p, ""} = Player.decode(bin)
    end

    test "struct is zero-initialized for missing fields" do
      # struct! will raise if required field missing — so all fields get defaults
      assert %Player{id: nil, name: nil, health: nil, alive: nil, score: nil} = struct(Player)
    end
  end

  describe "WithOptional schema" do
    test "Some value round-trips" do
      s = %WithOptional{id: 1, label: "hello"}
      bin = WithOptional.encode(s)
      assert {:ok, ^s, ""} = WithOptional.decode(bin)
    end

    test "None value round-trips" do
      s = %WithOptional{id: 2, label: nil}
      bin = WithOptional.encode(s)
      assert {:ok, ^s, ""} = WithOptional.decode(bin)
    end
  end

  describe "WithArray schema" do
    test "empty array round-trips" do
      s = %WithArray{id: 1, items: []}
      bin = WithArray.encode(s)
      assert {:ok, ^s, ""} = WithArray.decode(bin)
    end

    test "non-empty array round-trips" do
      s = %WithArray{id: 5, items: ["sword", "shield", "potion"]}
      bin = WithArray.encode(s)
      assert {:ok, ^s, ""} = WithArray.decode(bin)
    end
  end

  # ---------------------------------------------------------------------------
  # Sum type tests
  # ---------------------------------------------------------------------------

  describe "Direction sum type" do
    test "encodes each variant as a single byte" do
      assert Direction.encode(:north) == <<0>>
      assert Direction.encode(:south) == <<1>>
      assert Direction.encode(:east) == <<2>>
      assert Direction.encode(:west) == <<3>>
    end

    test "decodes all variants" do
      assert {:ok, :north, ""} = Direction.decode(<<0>>)
      assert {:ok, :south, ""} = Direction.decode(<<1>>)
      assert {:ok, :east, ""} = Direction.decode(<<2>>)
      assert {:ok, :west, ""} = Direction.decode(<<3>>)
    end

    test "unknown tag returns error" do
      assert {:error, {:unknown_variant_tag, 99}} = Direction.decode(<<99>>)
    end

    test "round-trips all variants" do
      for v <- [:north, :south, :east, :west] do
        bin = Direction.encode(v)
        assert {:ok, ^v, ""} = Direction.decode(bin)
      end
    end

    test "unknown variant raises ArgumentError" do
      assert_raise ArgumentError, fn -> Direction.encode(:diagonal) end
    end
  end

  # ---------------------------------------------------------------------------
  # BSATN.decode/2 dispatch to schema module
  # ---------------------------------------------------------------------------

  describe "BSATN.decode/2 with schema module" do
    test "dispatches to module decode/1" do
      p = %Player{id: 1, name: "Bob", health: 50, alive: false, score: 0}
      bin = Player.encode(p)
      assert {:ok, ^p, ""} = BSATN.decode(Player, bin)
    end
  end

  # ---------------------------------------------------------------------------
  # Decode error propagation
  # ---------------------------------------------------------------------------

  describe "error propagation" do
    test "truncated binary returns error" do
      # encode a full Player then strip the last byte
      p = %Player{id: 1, name: "x", health: 0, alive: true, score: 0}
      bin = Player.encode(p)
      truncated = binary_part(bin, 0, byte_size(bin) - 1)
      assert {:error, _} = Player.decode(truncated)
    end
  end
end
