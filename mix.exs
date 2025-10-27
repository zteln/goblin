defmodule Goblin.MixProject do
  use Mix.Project

  def project do
    [
      app: :goblin,
      version: "0.1.1",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      name: "Goblin",
      deps: deps(),
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
      main: "Goblin",
      extras: ["README.md", "CHANGELOG.md", "LICENSE"]
    ]
  end
end
