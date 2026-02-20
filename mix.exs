defmodule Goblin.MixProject do
  use Mix.Project

  def project do
    [
      app: :goblin,
      version: "0.6.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      name: "Goblin",
      deps: deps(),
      consolidate_protocols: Mix.env() != :test,
      package: package(),
      description: description(),
      docs: &docs/0,
      source_url: "https://github.com/zteln/goblin"
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:benchee, "~> 1.5", only: :dev},
      {:mimic, "~> 2.2", only: :test},
      {:stream_data, "~> 1.2", only: :test},
      {:ex_doc, "~> 0.39.1", only: :dev, runtime: false, warn_if_outdated: true}
    ]
  end

  defp package do
    [
      name: "goblin",
      files: ~w(lib .formatter.exs mix.exs README.md LICENSE CHANGELOG.md),
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/zteln/goblin"}
    ]
  end

  defp description do
    "An embedded LSM-Tree database for Elixir."
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "CHANGELOG.md", "LICENSE"]
    ]
  end
end
