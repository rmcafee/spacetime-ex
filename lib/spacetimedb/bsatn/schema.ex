defmodule SpacetimeDB.BSATN.Schema do
  @moduledoc ~S"""
  Macro DSL for defining BSATN-serialisable product types (table row schemas).

  ## Usage

      defmodule MyGame.Player do
        use SpacetimeDB.BSATN.Schema

        bsatn_schema do
          field :id,       :u32
          field :name,     :string
          field :pos_x,    :f32
          field :pos_y,    :f32
          field :health,   :u32
          field :alive,    :bool
        end
      end

  This generates:

  - A struct with the declared fields
  - `decode(binary) :: {:ok, t(), rest :: binary()} | {:error, term()}`
  - `encode(t()) :: binary()`
  - `fields() :: [{atom(), atom()}]` — field name / type pairs in order

  ## Supported field types

  All BSATN primitive atoms (`:u8`, `:u16`, `:u32`, `:u64`, `:u128`,
  `:i8`, `:i16`, `:i32`, `:i64`, `:i128`, `:f32`, `:f64`, `:bool`,
  `:string`, `:bytes`) plus compound forms:

  - `{:array, inner_type}` — a BSATN array of `inner_type`
  - `{:option, inner_type}` — an optional value
  - Any module that itself `use`s `SpacetimeDB.BSATN.Schema`

  ## Sum types (enums)

  Use `bsatn_sum` to define a sum type where each variant is identified by a
  `u8` discriminant:

      defmodule MyGame.MoveDir do
        use SpacetimeDB.BSATN.Schema

        bsatn_sum do
          variant :north, 0
          variant :south, 1
          variant :east,  2
          variant :west,  3
        end
      end

  This generates `encode/1` and `decode/1` only (no struct).
  """

  alias SpacetimeDB.BSATN

  defmacro __using__(_opts) do
    quote do
      import SpacetimeDB.BSATN.Schema, only: [bsatn_schema: 1, bsatn_sum: 1]
      Module.register_attribute(__MODULE__, :bsatn_fields, accumulate: true)
      Module.register_attribute(__MODULE__, :bsatn_variants, accumulate: true)
    end
  end

  # ---------------------------------------------------------------------------
  # bsatn_schema macro
  # ---------------------------------------------------------------------------

  defmacro bsatn_schema(do: block) do
    quote do
      import SpacetimeDB.BSATN.Schema, only: [field: 2]
      unquote(block)

      # Freeze accumulated fields into a non-accumulating attribute so they
      # are accessible inside def bodies (which need a stable module attribute).
      @bsatn_fields_ordered Enum.reverse(@bsatn_fields)

      defstruct Enum.map(@bsatn_fields_ordered, fn {name, _type} -> name end)

      @doc "Returns `[{field_name, bsatn_type}]` in declaration order."
      def fields, do: @bsatn_fields_ordered

      @doc "Decode a BSATN binary into a `#{__MODULE__}` struct."
      @spec decode(binary()) :: {:ok, struct(), binary()} | {:error, term()}
      def decode(bin) do
        SpacetimeDB.BSATN.Schema.__decode_product__(bin, @bsatn_fields_ordered, __MODULE__)
      end

      @doc "Encode a `#{__MODULE__}` struct into BSATN binary."
      @spec encode(struct()) :: binary()
      def encode(%__MODULE__{} = s) do
        SpacetimeDB.BSATN.Schema.__encode_product__(s, @bsatn_fields_ordered)
      end
    end
  end

  defmacro field(name, type) do
    quote do
      @bsatn_fields {unquote(name), unquote(type)}
    end
  end

  # ---------------------------------------------------------------------------
  # bsatn_sum macro
  # ---------------------------------------------------------------------------

  defmacro bsatn_sum(do: block) do
    quote do
      import SpacetimeDB.BSATN.Schema, only: [variant: 2]
      unquote(block)

      @bsatn_variants_ordered Enum.reverse(@bsatn_variants)

      @doc "Decode a BSATN sum discriminant byte into an atom."
      @spec decode(binary()) :: {:ok, atom(), binary()} | {:error, term()}
      def decode(<<tag::little-unsigned-8, rest::binary>>) do
        case Enum.find(@bsatn_variants_ordered, fn {_name, t} -> t == tag end) do
          {name, _} -> {:ok, name, rest}
          nil -> {:error, {:unknown_variant_tag, tag}}
        end
      end

      def decode(_), do: {:error, :not_enough_bytes}

      @doc "Encode a variant atom into its `u8` BSATN discriminant."
      @spec encode(atom()) :: binary()
      def encode(name) when is_atom(name) do
        case Enum.find(@bsatn_variants_ordered, fn {n, _} -> n == name end) do
          {_, tag} -> <<tag::little-unsigned-8>>
          nil -> raise ArgumentError, "unknown variant: #{inspect(name)}"
        end
      end
    end
  end

  defmacro variant(name, tag) do
    quote do
      @bsatn_variants {unquote(name), unquote(tag)}
    end
  end

  # ---------------------------------------------------------------------------
  # Runtime helpers (called from generated decode/encode)
  # ---------------------------------------------------------------------------

  @doc false
  def __decode_product__(bin, fields, module) do
    case do_decode_fields(bin, fields, %{}) do
      {:ok, map, rest} -> {:ok, struct!(module, map), rest}
      err -> err
    end
  end

  defp do_decode_fields(rest, [], acc), do: {:ok, acc, rest}

  defp do_decode_fields(bin, [{name, type} | tail], acc) do
    case BSATN.decode(type, bin) do
      {:ok, value, rest} -> do_decode_fields(rest, tail, Map.put(acc, name, value))
      err -> err
    end
  end

  @doc false
  def __encode_product__(struct, fields) do
    Enum.map_join(fields, fn {name, type} ->
      BSATN.encode(type, Map.fetch!(struct, name))
    end)
  end
end
