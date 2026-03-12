defmodule SpacetimeDB.BSATNTest do
  use ExUnit.Case, async: true

  alias SpacetimeDB.BSATN

  # ---------------------------------------------------------------------------
  # Round-trip helpers
  # ---------------------------------------------------------------------------

  defp roundtrip(type, value) do
    encoded = BSATN.encode(type, value)
    assert {:ok, ^value, ""} = BSATN.decode(type, encoded)
  end

  # ---------------------------------------------------------------------------
  # Boolean
  # ---------------------------------------------------------------------------

  describe "bool" do
    test "true encodes to <<1>>" do
      assert BSATN.encode_bool(true) == <<1>>
    end

    test "false encodes to <<0>>" do
      assert BSATN.encode_bool(false) == <<0>>
    end

    test "round-trips" do
      roundtrip(:bool, true)
      roundtrip(:bool, false)
    end

    test "invalid byte returns error" do
      assert {:error, {:invalid_bool, 2}} = BSATN.decode_bool(<<2>>)
    end
  end

  # ---------------------------------------------------------------------------
  # Unsigned integers
  # ---------------------------------------------------------------------------

  describe "u8" do
    test "encodes correctly" do
      assert BSATN.encode_u8(0) == <<0>>
      assert BSATN.encode_u8(255) == <<255>>
    end

    test "round-trips" do
      roundtrip(:u8, 0)
      roundtrip(:u8, 127)
      roundtrip(:u8, 255)
    end
  end

  describe "u16" do
    test "little-endian encoding" do
      # 0x0102 → <<02, 01>> in LE
      assert BSATN.encode_u16(0x0102) == <<0x02, 0x01>>
    end

    test "round-trips" do
      roundtrip(:u16, 0)
      roundtrip(:u16, 65_535)
    end
  end

  describe "u32" do
    test "little-endian encoding" do
      assert BSATN.encode_u32(1) == <<1, 0, 0, 0>>
      assert BSATN.encode_u32(256) == <<0, 1, 0, 0>>
    end

    test "round-trips" do
      roundtrip(:u32, 0)
      roundtrip(:u32, 4_294_967_295)
    end
  end

  describe "u64" do
    test "round-trips" do
      roundtrip(:u64, 0)
      roundtrip(:u64, 18_446_744_073_709_551_615)
    end
  end

  describe "u128" do
    test "round-trips" do
      roundtrip(:u128, 0)
      roundtrip(:u128, 340_282_366_920_938_463_463_374_607_431_768_211_455)
    end
  end

  describe "u256" do
    test "round-trips zero" do
      roundtrip(:u256, 0)
    end

    test "round-trips max value" do
      max_u256 = Bitwise.bsl(1, 256) - 1
      roundtrip(:u256, max_u256)
    end

    test "encodes to exactly 32 bytes" do
      assert byte_size(BSATN.encode_u256(0)) == 32
      assert byte_size(BSATN.encode_u256(1)) == 32
    end

    test "little-endian encoding" do
      <<first_byte, _::binary>> = BSATN.encode_u256(1)
      assert first_byte == 1
    end

    test "not enough bytes" do
      assert {:error, :not_enough_bytes} = BSATN.decode_u256(<<0::size(248)>>)
    end
  end

  # ---------------------------------------------------------------------------
  # Signed integers
  # ---------------------------------------------------------------------------

  describe "i8" do
    test "round-trips positive and negative" do
      roundtrip(:i8, 0)
      roundtrip(:i8, 127)
      roundtrip(:i8, -128)
      roundtrip(:i8, -1)
    end
  end

  describe "i32" do
    test "round-trips" do
      roundtrip(:i32, 0)
      roundtrip(:i32, 2_147_483_647)
      roundtrip(:i32, -2_147_483_648)
    end
  end

  describe "i64" do
    test "round-trips" do
      roundtrip(:i64, 0)
      roundtrip(:i64, 9_223_372_036_854_775_807)
      roundtrip(:i64, -9_223_372_036_854_775_808)
    end
  end

  # ---------------------------------------------------------------------------
  # Floats
  # ---------------------------------------------------------------------------

  describe "f32" do
    test "round-trips zero" do
      roundtrip(:f32, 0.0)
    end

    test "positive float round-trips within f32 precision" do
      encoded = BSATN.encode_f32(1.5)
      assert {:ok, 1.5, ""} = BSATN.decode_f32(encoded)
    end
  end

  describe "f64" do
    test "round-trips" do
      roundtrip(:f64, 0.0)
      roundtrip(:f64, 3.141592653589793)
      roundtrip(:f64, -1.0e100)
    end
  end

  # ---------------------------------------------------------------------------
  # String
  # ---------------------------------------------------------------------------

  describe "string" do
    test "empty string" do
      encoded = BSATN.encode_string("")
      assert encoded == <<0, 0, 0, 0>>
      assert {:ok, "", ""} = BSATN.decode_string(encoded)
    end

    test "ASCII string" do
      roundtrip(:string, "hello")
    end

    test "UTF-8 string" do
      roundtrip(:string, "héllo wörld 🌍")
    end

    test "length prefix is byte length not character count" do
      s = "🌍"
      encoded = BSATN.encode_string(s)
      <<len::little-32, _rest::binary>> = encoded
      assert len == byte_size(s)
    end

    test "returns leftover bytes" do
      bin = BSATN.encode_string("hi") <> <<99>>
      assert {:ok, "hi", <<99>>} = BSATN.decode_string(bin)
    end
  end

  # ---------------------------------------------------------------------------
  # Bytes
  # ---------------------------------------------------------------------------

  describe "bytes" do
    test "empty" do
      roundtrip(:bytes, <<>>)
    end

    test "round-trips arbitrary bytes" do
      roundtrip(:bytes, <<1, 2, 3, 255, 0>>)
    end
  end

  # ---------------------------------------------------------------------------
  # Option
  # ---------------------------------------------------------------------------

  describe "option" do
    test "None encodes as <<0>>" do
      assert BSATN.encode_option(nil, &BSATN.encode_u32/1) == <<0>>
    end

    test "Some encodes as <<1>> + value" do
      assert BSATN.encode_option(42, &BSATN.encode_u32/1) == <<1>> <> BSATN.encode_u32(42)
    end

    test "round-trips None" do
      enc = BSATN.encode_option(nil, &BSATN.encode_u32/1)
      assert {:ok, nil, ""} = BSATN.decode_option(enc, &BSATN.decode_u32/1)
    end

    test "round-trips Some" do
      enc = BSATN.encode_option(42, &BSATN.encode_u32/1)
      assert {:ok, 42, ""} = BSATN.decode_option(enc, &BSATN.decode_u32/1)
    end

    test "via decode/2 dispatch" do
      roundtrip({:option, :u32}, nil)
      roundtrip({:option, :u32}, 7)
      roundtrip({:option, :string}, nil)
      roundtrip({:option, :string}, "hello")
    end
  end

  # ---------------------------------------------------------------------------
  # Array
  # ---------------------------------------------------------------------------

  describe "array" do
    test "empty array" do
      enc = BSATN.encode_array([], &BSATN.encode_u32/1)
      assert {:ok, [], ""} = BSATN.decode_array(enc, &BSATN.decode_u32/1)
    end

    test "round-trips u32 list" do
      roundtrip({:array, :u32}, [1, 2, 3, 1000])
    end

    test "round-trips string list" do
      roundtrip({:array, :string}, ["foo", "bar", "baz"])
    end

    test "returns leftover bytes" do
      bin = BSATN.encode_array([1, 2], &BSATN.encode_u8/1) <> <<99>>
      assert {:ok, [1, 2], <<99>>} = BSATN.decode_array(bin, &BSATN.decode_u8/1)
    end
  end

  # ---------------------------------------------------------------------------
  # Not enough bytes
  # ---------------------------------------------------------------------------

  describe "error on short input" do
    test "u32 with 3 bytes" do
      assert {:error, :not_enough_bytes} = BSATN.decode_u32(<<1, 2, 3>>)
    end

    test "string with truncated body" do
      # length says 10 bytes but only 2 present
      bin = <<10::little-32, "hi"::binary>>
      assert {:error, :not_enough_bytes} = BSATN.decode_string(bin)
    end
  end
end
