defmodule SeaGoat.MixProject do
  use Mix.Project

  def project do
    [
      app: :sea_goat,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:recon, "~> 2.5", only: :dev},
      {:patch, "~> 0.16", only: :test}
    ]
  end
end
