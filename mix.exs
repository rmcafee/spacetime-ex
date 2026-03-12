defmodule SpacetimeDB.MixProject do
  use Mix.Project

  @version "0.3.0"
  @source_url "https://github.com/maco144/spacetime-ex"

  def project do
    [
      app: :spacetimedb_ex,
      version: @version,
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Elixir client for SpacetimeDB v2 — BSATN binary protocol, WebSocket subscriptions, reducer/procedure calls, live ETS table mirrors",
      package: package(),
      docs: docs(),
      name: "SpacetimeDB",
      source_url: @source_url,
      dialyzer: [plt_add_apps: [:mix]]
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto]]
  end

  defp deps do
    [
      {:mint_web_socket, "~> 1.0"},
      {:jason, "~> 1.4"},
      {:castore, "~> 1.0"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE CHANGELOG.md)
    ]
  end

  defp docs do
    [
      main: "SpacetimeDB",
      source_url: @source_url,
      extras: ["README.md", "CHANGELOG.md"]
    ]
  end
end
