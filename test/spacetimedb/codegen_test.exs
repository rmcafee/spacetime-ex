defmodule SpacetimeDB.CodeGenTest do
  use ExUnit.Case, async: true

  alias SpacetimeDB.CodeGen

  # ---------------------------------------------------------------------------
  # Fixture schemas
  # ---------------------------------------------------------------------------

  # SpacetimeDB v1 format: typespace + product_type_ref
  defp v1_schema do
    %{
      "typespace" => %{
        "types" => [
          # index 0 — Player product type
          %{
            "Product" => %{
              "elements" => [
                %{"name" => "id", "algebraic_type" => "U32"},
                %{"name" => "name", "algebraic_type" => "String"},
                %{"name" => "health", "algebraic_type" => "U32"},
                %{"name" => "alive", "algebraic_type" => "Bool"}
              ]
            }
          },
          # index 1 — Message product type
          %{
            "Product" => %{
              "elements" => [
                %{"name" => "id", "algebraic_type" => "U64"},
                %{"name" => "sender", "algebraic_type" => "U32"},
                %{"name" => "text", "algebraic_type" => "String"}
              ]
            }
          }
        ]
      },
      "tables" => [
        %{
          "name" => "Player",
          "product_type_ref" => 0,
          "primary_key" => [0]
        },
        %{
          "name" => "Message",
          "product_type_ref" => 1,
          "primary_key" => [0]
        }
      ]
    }
  end

  # Older inline "schema" format
  defp legacy_schema do
    %{
      "tables" => [
        %{
          "name" => "Item",
          "schema" => %{
            "columns" => [
              %{"col_pos" => 0, "name" => "id", "col_type" => "U32"},
              %{"col_pos" => 1, "name" => "owner_id", "col_type" => "U32"},
              %{"col_pos" => 2, "name" => "kind", "col_type" => "String"}
            ]
          },
          "primary_key" => [0]
        }
      ]
    }
  end

  # Schema with nested object types: {"U32": {}}
  defp object_type_schema do
    %{
      "typespace" => %{
        "types" => [
          %{
            "Product" => %{
              "elements" => [
                %{"name" => "id", "algebraic_type" => %{"U32" => %{}}},
                %{"name" => "score", "algebraic_type" => %{"I64" => %{}}},
                %{"name" => "ratio", "algebraic_type" => %{"F64" => %{}}},
                %{"name" => "tags", "algebraic_type" => %{"Array" => %{"elem_ty" => "String"}}},
                %{"name" => "nickname", "algebraic_type" => %{"Option" => "String"}}
              ]
            }
          }
        ]
      },
      "tables" => [
        %{"name" => "Score", "product_type_ref" => 0, "primary_key" => [0]}
      ]
    }
  end

  # Schema with Builtin wrapper (some SpacetimeDB versions)
  defp builtin_schema do
    %{
      "typespace" => %{
        "types" => [
          %{
            "Product" => %{
              "elements" => [
                %{"name" => "id", "algebraic_type" => %{"Builtin" => %{"U32" => {}}}},
                %{"name" => "value", "algebraic_type" => %{"Builtin" => "F32"}}
              ]
            }
          }
        ]
      },
      "tables" => [
        %{"name" => "Sensor", "product_type_ref" => 0, "primary_key" => [0]}
      ]
    }
  end

  # ---------------------------------------------------------------------------
  # generate_modules/2
  # ---------------------------------------------------------------------------

  describe "generate_modules/2 — v1 format" do
    test "returns one module per table" do
      modules = CodeGen.generate_modules(v1_schema(), namespace: "MyApp")
      assert length(modules) == 2
    end

    test "filenames are snake_case.ex" do
      modules = CodeGen.generate_modules(v1_schema(), namespace: "MyApp")
      filenames = Enum.map(modules, &elem(&1, 0))
      assert "player.ex" in filenames
      assert "message.ex" in filenames
    end

    test "module name uses namespace and PascalCase table name" do
      [{_file, src} | _] = CodeGen.generate_modules(v1_schema(), namespace: "MyApp.SpacetimeDB")
      assert src =~ "defmodule MyApp.SpacetimeDB.Player do"
    end

    test "generates bsatn_schema block" do
      [{_file, src} | _] = CodeGen.generate_modules(v1_schema(), namespace: "MyApp")
      assert src =~ "use SpacetimeDB.BSATN.Schema"
      assert src =~ "bsatn_schema do"
    end

    test "generates correct field types" do
      [{_file, src} | _] = CodeGen.generate_modules(v1_schema(), namespace: "MyApp")
      assert src =~ "field :id, :u32"
      assert src =~ "field :name, :string"
      assert src =~ "field :health, :u32"
      assert src =~ "field :alive, :bool"
    end

    test "generates primary_key/0 from first primary key column" do
      [{_file, src} | _] = CodeGen.generate_modules(v1_schema(), namespace: "MyApp")
      assert src =~ ~s(def primary_key, do: :id)
    end

    test "generates table_name/0" do
      [{_file, src} | _] = CodeGen.generate_modules(v1_schema(), namespace: "MyApp")
      assert src =~ ~s(def table_name, do: "Player")
    end

    test "includes auto-generated header comment" do
      [{_file, src} | _] = CodeGen.generate_modules(v1_schema(), namespace: "MyApp")
      assert src =~ "Generated by SpacetimeDB.CodeGen"
    end
  end

  describe "generate_modules/2 — legacy inline schema" do
    test "parses inline column list" do
      [{_file, src}] = CodeGen.generate_modules(legacy_schema(), namespace: "MyApp")
      assert src =~ "defmodule MyApp.Item do"
      assert src =~ "field :id, :u32"
      assert src =~ "field :owner_id, :u32"
      assert src =~ "field :kind, :string"
    end
  end

  describe "generate_modules/2 — object type notation" do
    test "handles {\"U32\": {}} form" do
      [{_file, src}] = CodeGen.generate_modules(object_type_schema(), namespace: "MyApp")
      assert src =~ "field :id, :u32"
      assert src =~ "field :score, :i64"
      assert src =~ "field :ratio, :f64"
    end

    test "handles Array wrapper" do
      [{_file, src}] = CodeGen.generate_modules(object_type_schema(), namespace: "MyApp")
      assert src =~ "field :tags, {:array, :string}"
    end

    test "handles Option wrapper" do
      [{_file, src}] = CodeGen.generate_modules(object_type_schema(), namespace: "MyApp")
      assert src =~ "field :nickname, {:option, :string}"
    end
  end

  describe "generate_modules/2 — Builtin wrapper" do
    test "unwraps Builtin and maps inner type" do
      [{_file, src}] = CodeGen.generate_modules(builtin_schema(), namespace: "MyApp")
      assert src =~ "field :id, :u32"
      assert src =~ "field :value, :f32"
    end
  end

  describe "generate_modules/2 — all primitive types" do
    test "maps every BSATN primitive" do
      types = ~w(Bool U8 U16 U32 U64 U128 I8 I16 I32 I64 I128 F32 F64 String Bytes)

      elements =
        Enum.with_index(types, fn t, i ->
          %{"name" => "field_#{i}", "algebraic_type" => t}
        end)

      schema = %{
        "typespace" => %{"types" => [%{"Product" => %{"elements" => elements}}]},
        "tables" => [%{"name" => "All", "product_type_ref" => 0, "primary_key" => [0]}]
      }

      [{_file, src}] = CodeGen.generate_modules(schema, namespace: "T")

      assert src =~ ":bool"
      assert src =~ ":u8"
      assert src =~ ":u16"
      assert src =~ ":u32"
      assert src =~ ":u64"
      assert src =~ ":u128"
      assert src =~ ":i8"
      assert src =~ ":i16"
      assert src =~ ":i32"
      assert src =~ ":i64"
      assert src =~ ":i128"
      assert src =~ ":f32"
      assert src =~ ":f64"
      assert src =~ ":string"
      assert src =~ ":bytes"
    end
  end

  describe "generate_modules/2 — edge cases" do
    test "empty tables list returns []" do
      assert [] = CodeGen.generate_modules(%{"tables" => []})
    end

    test "missing tables key returns []" do
      assert [] = CodeGen.generate_modules(%{})
    end

    test "PascalCase conversion" do
      schema = %{
        "typespace" => %{
          "types" => [%{"Product" => %{"elements" => [%{"name" => "id", "algebraic_type" => "U32"}]}}]
        },
        "tables" => [%{"name" => "agent_run", "product_type_ref" => 0, "primary_key" => [0]}]
      }

      [{filename, src}] = CodeGen.generate_modules(schema, namespace: "MyApp")
      assert filename == "agent_run.ex"
      assert src =~ "defmodule MyApp.AgentRun do"
    end

    test "Ref type resolved via typespace" do
      schema = %{
        "typespace" => %{
          "types" => [
            # index 0 — outer table
            %{
              "Product" => %{
                "elements" => [
                  %{"name" => "id", "algebraic_type" => "U32"},
                  # Ref to index 1
                  %{"name" => "inner", "algebraic_type" => %{"Ref" => 1}}
                ]
              }
            },
            # index 1 — resolves to U64
            "U64"
          ]
        },
        "tables" => [%{"name" => "Outer", "product_type_ref" => 0, "primary_key" => [0]}]
      }

      [{_file, src}] = CodeGen.generate_modules(schema, namespace: "T")
      assert src =~ "field :inner, :u64"
    end
  end

  # ---------------------------------------------------------------------------
  # write_files/2
  # ---------------------------------------------------------------------------

  describe "write_files/2" do
    test "writes files to the output directory" do
      dir = System.tmp_dir!() |> Path.join("stdb_codegen_test_#{System.unique_integer([:positive])}")

      modules = [{"player.ex", "defmodule Test.Player do\nend\n"}]
      CodeGen.write_files(modules, dir)

      assert File.exists?(Path.join(dir, "player.ex"))
      assert File.read!(Path.join(dir, "player.ex")) =~ "defmodule Test.Player"

      File.rm_rf!(dir)
    end

    test "creates output directory if it does not exist" do
      dir = System.tmp_dir!() |> Path.join("stdb_new_dir_#{System.unique_integer([:positive])}")
      refute File.exists?(dir)

      CodeGen.write_files([{"x.ex", ""}], dir)
      assert File.exists?(dir)

      File.rm_rf!(dir)
    end
  end

  # ---------------------------------------------------------------------------
  # Generated source compiles
  # ---------------------------------------------------------------------------

  describe "generated source compiles" do
    test "Player module from v1 schema is valid Elixir" do
      [{_file, src} | _] = CodeGen.generate_modules(v1_schema(), namespace: "CompileTest")

      assert {{:module, CompileTest.Player, _, _}, _} =
               Code.eval_string(src, [], file: "generated_player.ex")
    end
  end
end
