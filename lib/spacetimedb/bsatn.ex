defmodule SpacetimeDB.BSATN do
  @moduledoc """
  Binary SpacetimeDB Algebraic Type Notation (BSATN) codec.

  BSATN is SpacetimeDB's compact binary serialisation format.  All integers are
  little-endian; there is no framing or self-description — the decoder must know
  the schema in advance.

  ## Primitive wire sizes

  | Type | Bytes |
  |------|-------|
  | `bool` | 1 |
  | `u8` / `i8` | 1 |
  | `u16` / `i16` | 2 |
  | `u32` / `i32` | 4 |
  | `u64` / `i64` | 8 |
  | `u128` / `i128` | 16 |
  | `f32` | 4 (IEEE 754 LE) |
  | `f64` | 8 (IEEE 754 LE) |
  | `string` | 4-byte LE length + UTF-8 bytes |
  | `bytes` | 4-byte LE length + raw bytes |

  ## Compound types

  - **Array**: `u32` element count followed by N encoded elements
  - **Option**: `0x00` for `None`; `0x01` followed by the encoded value for `Some`
  - **Product** (struct): fields encoded in declaration order, no framing
  - **Sum** (enum / tagged union): `u8` discriminant followed by the variant's fields
  """

  # ---------------------------------------------------------------------------
  # Encoding
  # ---------------------------------------------------------------------------

  @doc "Encode a boolean (1 byte: 0 or 1)."
  @spec encode_bool(boolean()) :: binary()
  def encode_bool(true), do: <<1::8>>
  def encode_bool(false), do: <<0::8>>

  @doc "Encode an unsigned 8-bit integer."
  @spec encode_u8(0..255) :: binary()
  def encode_u8(n), do: <<n::little-unsigned-8>>

  @doc "Encode an unsigned 16-bit integer (little-endian)."
  @spec encode_u16(non_neg_integer()) :: binary()
  def encode_u16(n), do: <<n::little-unsigned-16>>

  @doc "Encode an unsigned 32-bit integer (little-endian)."
  @spec encode_u32(non_neg_integer()) :: binary()
  def encode_u32(n), do: <<n::little-unsigned-32>>

  @doc "Encode an unsigned 64-bit integer (little-endian)."
  @spec encode_u64(non_neg_integer()) :: binary()
  def encode_u64(n), do: <<n::little-unsigned-64>>

  @doc "Encode an unsigned 128-bit integer (little-endian)."
  @spec encode_u128(non_neg_integer()) :: binary()
  def encode_u128(n), do: <<n::little-unsigned-128>>

  @doc "Encode a signed 8-bit integer."
  @spec encode_i8(integer()) :: binary()
  def encode_i8(n), do: <<n::little-signed-8>>

  @doc "Encode a signed 16-bit integer (little-endian)."
  @spec encode_i16(integer()) :: binary()
  def encode_i16(n), do: <<n::little-signed-16>>

  @doc "Encode a signed 32-bit integer (little-endian)."
  @spec encode_i32(integer()) :: binary()
  def encode_i32(n), do: <<n::little-signed-32>>

  @doc "Encode a signed 64-bit integer (little-endian)."
  @spec encode_i64(integer()) :: binary()
  def encode_i64(n), do: <<n::little-signed-64>>

  @doc "Encode a signed 128-bit integer (little-endian)."
  @spec encode_i128(integer()) :: binary()
  def encode_i128(n), do: <<n::little-signed-128>>

  @doc "Encode a 32-bit float (IEEE 754 little-endian)."
  @spec encode_f32(float()) :: binary()
  def encode_f32(n), do: <<n::little-float-32>>

  @doc "Encode a 64-bit float (IEEE 754 little-endian)."
  @spec encode_f64(float()) :: binary()
  def encode_f64(n), do: <<n::little-float-64>>

  @doc "Encode a UTF-8 string (4-byte LE length prefix + bytes)."
  @spec encode_string(String.t()) :: binary()
  def encode_string(s) when is_binary(s) do
    encode_u32(byte_size(s)) <> s
  end

  @doc "Encode a raw binary (4-byte LE length prefix + bytes)."
  @spec encode_bytes(binary()) :: binary()
  def encode_bytes(b) when is_binary(b) do
    encode_u32(byte_size(b)) <> b
  end

  @doc "Encode an `Option`: `None` → `<<0>>`, `Some(v)` → `<<1>> <> encode_fn.(v)`."
  @spec encode_option(term(), (term() -> binary())) :: binary()
  def encode_option(nil, _encode_fn), do: <<0::8>>
  def encode_option(value, encode_fn), do: <<1::8>> <> encode_fn.(value)

  @doc "Encode a list as a BSATN array (4-byte LE count + elements)."
  @spec encode_array([term()], (term() -> binary())) :: binary()
  def encode_array(list, encode_fn) when is_list(list) do
    encoded = Enum.map_join(list, &encode_fn.(&1))
    encode_u32(length(list)) <> encoded
  end

  # ---------------------------------------------------------------------------
  # Decoding
  # ---------------------------------------------------------------------------

  @doc "Decode a boolean from the head of `bin`."
  @spec decode_bool(binary()) :: {:ok, boolean(), binary()} | {:error, term()}
  def decode_bool(<<0::8, rest::binary>>), do: {:ok, false, rest}
  def decode_bool(<<1::8, rest::binary>>), do: {:ok, true, rest}
  def decode_bool(<<n::8, _::binary>>), do: {:error, {:invalid_bool, n}}
  def decode_bool(_), do: {:error, :not_enough_bytes}

  @doc "Decode an unsigned 8-bit integer."
  @spec decode_u8(binary()) :: {:ok, non_neg_integer(), binary()} | {:error, term()}
  def decode_u8(<<n::little-unsigned-8, rest::binary>>), do: {:ok, n, rest}
  def decode_u8(_), do: {:error, :not_enough_bytes}

  @doc "Decode an unsigned 16-bit integer (little-endian)."
  @spec decode_u16(binary()) :: {:ok, non_neg_integer(), binary()} | {:error, term()}
  def decode_u16(<<n::little-unsigned-16, rest::binary>>), do: {:ok, n, rest}
  def decode_u16(_), do: {:error, :not_enough_bytes}

  @doc "Decode an unsigned 32-bit integer (little-endian)."
  @spec decode_u32(binary()) :: {:ok, non_neg_integer(), binary()} | {:error, term()}
  def decode_u32(<<n::little-unsigned-32, rest::binary>>), do: {:ok, n, rest}
  def decode_u32(_), do: {:error, :not_enough_bytes}

  @doc "Decode an unsigned 64-bit integer (little-endian)."
  @spec decode_u64(binary()) :: {:ok, non_neg_integer(), binary()} | {:error, term()}
  def decode_u64(<<n::little-unsigned-64, rest::binary>>), do: {:ok, n, rest}
  def decode_u64(_), do: {:error, :not_enough_bytes}

  @doc "Decode an unsigned 128-bit integer (little-endian)."
  @spec decode_u128(binary()) :: {:ok, non_neg_integer(), binary()} | {:error, term()}
  def decode_u128(<<n::little-unsigned-128, rest::binary>>), do: {:ok, n, rest}
  def decode_u128(_), do: {:error, :not_enough_bytes}

  @doc "Decode a signed 8-bit integer."
  @spec decode_i8(binary()) :: {:ok, integer(), binary()} | {:error, term()}
  def decode_i8(<<n::little-signed-8, rest::binary>>), do: {:ok, n, rest}
  def decode_i8(_), do: {:error, :not_enough_bytes}

  @doc "Decode a signed 16-bit integer (little-endian)."
  @spec decode_i16(binary()) :: {:ok, integer(), binary()} | {:error, term()}
  def decode_i16(<<n::little-signed-16, rest::binary>>), do: {:ok, n, rest}
  def decode_i16(_), do: {:error, :not_enough_bytes}

  @doc "Decode a signed 32-bit integer (little-endian)."
  @spec decode_i32(binary()) :: {:ok, integer(), binary()} | {:error, term()}
  def decode_i32(<<n::little-signed-32, rest::binary>>), do: {:ok, n, rest}
  def decode_i32(_), do: {:error, :not_enough_bytes}

  @doc "Decode a signed 64-bit integer (little-endian)."
  @spec decode_i64(binary()) :: {:ok, integer(), binary()} | {:error, term()}
  def decode_i64(<<n::little-signed-64, rest::binary>>), do: {:ok, n, rest}
  def decode_i64(_), do: {:error, :not_enough_bytes}

  @doc "Decode a signed 128-bit integer (little-endian)."
  @spec decode_i128(binary()) :: {:ok, integer(), binary()} | {:error, term()}
  def decode_i128(<<n::little-signed-128, rest::binary>>), do: {:ok, n, rest}
  def decode_i128(_), do: {:error, :not_enough_bytes}

  @doc "Decode a 32-bit float (IEEE 754 little-endian)."
  @spec decode_f32(binary()) :: {:ok, float(), binary()} | {:error, term()}
  def decode_f32(<<n::little-float-32, rest::binary>>), do: {:ok, n, rest}
  def decode_f32(_), do: {:error, :not_enough_bytes}

  @doc "Decode a 64-bit float (IEEE 754 little-endian)."
  @spec decode_f64(binary()) :: {:ok, float(), binary()} | {:error, term()}
  def decode_f64(<<n::little-float-64, rest::binary>>), do: {:ok, n, rest}
  def decode_f64(_), do: {:error, :not_enough_bytes}

  @doc "Decode a length-prefixed UTF-8 string."
  @spec decode_string(binary()) :: {:ok, String.t(), binary()} | {:error, term()}
  def decode_string(bin) do
    with {:ok, len, rest} <- decode_u32(bin),
         <<str::binary-size(len), tail::binary>> <- rest do
      case :unicode.characters_to_binary(str, :utf8) do
        {:error, _, _} -> {:error, :invalid_utf8}
        s -> {:ok, s, tail}
      end
    else
      bin when is_binary(bin) -> {:error, :not_enough_bytes}
      err -> err
    end
  end

  @doc "Decode a length-prefixed raw binary."
  @spec decode_bytes(binary()) :: {:ok, binary(), binary()} | {:error, term()}
  def decode_bytes(bin) do
    with {:ok, len, rest} <- decode_u32(bin) do
      case rest do
        <<data::binary-size(len), tail::binary>> -> {:ok, data, tail}
        _ -> {:error, :not_enough_bytes}
      end
    end
  end

  @doc "Decode an Option: `0x00` → `nil`; `0x01` + decode_fn → `{:ok, value}`."
  @spec decode_option(binary(), (binary() -> {:ok, term(), binary()} | {:error, term()})) ::
          {:ok, term(), binary()} | {:error, term()}
  def decode_option(<<0::8, rest::binary>>, _decode_fn), do: {:ok, nil, rest}

  def decode_option(<<1::8, rest::binary>>, decode_fn), do: decode_fn.(rest)

  def decode_option(_, _), do: {:error, :not_enough_bytes}

  @doc "Decode a BSATN array (4-byte LE count + elements decoded with `decode_fn`)."
  @spec decode_array(binary(), (binary() -> {:ok, term(), binary()} | {:error, term()})) ::
          {:ok, [term()], binary()} | {:error, term()}
  def decode_array(bin, decode_fn) do
    with {:ok, count, rest} <- decode_u32(bin) do
      decode_n(rest, count, decode_fn, [])
    end
  end

  defp decode_n(rest, 0, _fn, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_n(bin, n, decode_fn, acc) do
    case decode_fn.(bin) do
      {:ok, value, rest} -> decode_n(rest, n - 1, decode_fn, [value | acc])
      err -> err
    end
  end

  # ---------------------------------------------------------------------------
  # Convenience — decode a specific primitive by atom name
  # ---------------------------------------------------------------------------

  @doc """
  Decode a single value by type atom.  Useful when building generic decoders.

  Supported atoms: `:bool`, `:u8`, `:u16`, `:u32`, `:u64`, `:u128`,
  `:i8`, `:i16`, `:i32`, `:i64`, `:i128`, `:f32`, `:f64`, `:string`, `:bytes`.
  """
  @spec decode(atom(), binary()) :: {:ok, term(), binary()} | {:error, term()}
  def decode(:bool, bin), do: decode_bool(bin)
  def decode(:u8, bin), do: decode_u8(bin)
  def decode(:u16, bin), do: decode_u16(bin)
  def decode(:u32, bin), do: decode_u32(bin)
  def decode(:u64, bin), do: decode_u64(bin)
  def decode(:u128, bin), do: decode_u128(bin)
  def decode(:i8, bin), do: decode_i8(bin)
  def decode(:i16, bin), do: decode_i16(bin)
  def decode(:i32, bin), do: decode_i32(bin)
  def decode(:i64, bin), do: decode_i64(bin)
  def decode(:i128, bin), do: decode_i128(bin)
  def decode(:f32, bin), do: decode_f32(bin)
  def decode(:f64, bin), do: decode_f64(bin)
  def decode(:string, bin), do: decode_string(bin)
  def decode(:bytes, bin), do: decode_bytes(bin)
  def decode({:array, type}, bin), do: decode_array(bin, &decode(type, &1))
  def decode({:option, type}, bin), do: decode_option(bin, &decode(type, &1))

  def decode(mod, bin) when is_atom(mod) do
    if function_exported?(mod, :decode, 1) do
      mod.decode(bin)
    else
      {:error, {:unknown_type, mod}}
    end
  end

  @doc "Same as `decode/2` but for encoding by type atom."
  @spec encode(atom(), term()) :: binary()
  def encode(:bool, v), do: encode_bool(v)
  def encode(:u8, v), do: encode_u8(v)
  def encode(:u16, v), do: encode_u16(v)
  def encode(:u32, v), do: encode_u32(v)
  def encode(:u64, v), do: encode_u64(v)
  def encode(:u128, v), do: encode_u128(v)
  def encode(:i8, v), do: encode_i8(v)
  def encode(:i16, v), do: encode_i16(v)
  def encode(:i32, v), do: encode_i32(v)
  def encode(:i64, v), do: encode_i64(v)
  def encode(:i128, v), do: encode_i128(v)
  def encode(:f32, v), do: encode_f32(v)
  def encode(:f64, v), do: encode_f64(v)
  def encode(:string, v), do: encode_string(v)
  def encode(:bytes, v), do: encode_bytes(v)
  def encode({:array, type}, list), do: encode_array(list, &encode(type, &1))
  def encode({:option, type}, v), do: encode_option(v, &encode(type, &1))

  def encode(mod, v) when is_atom(mod) do
    if function_exported?(mod, :encode, 1) do
      mod.encode(v)
    else
      raise ArgumentError, "unknown BSATN type: #{inspect(mod)}"
    end
  end
end
