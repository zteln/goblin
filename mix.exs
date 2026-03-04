defmodule Goblin.MixProject do
  use Mix.Project

  def project do
    [
      app: :goblin,
      version: "0.7.0",
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
      {:ex_doc, "~> 0.40.0", only: :dev, runtime: false, warn_if_outdated: true}
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
      extras: ["README.md", "CHANGELOG.md", "ARCHITECTURE.md", "LICENSE"],
      before_closing_body_tag: &before_closing_body_tag/1
    ]
  end

  defp before_closing_body_tag(:html) do
    """
    <script defer src="https://cdn.jsdelivr.net/npm/mermaid@10.2.3/dist/mermaid.min.js"></script>
    <script>
    let initialized = false;

    window.addEventListener("exdoc:loaded", () => {
      if (!initialized) {
        mermaid.initialize({
          startOnLoad: false,
          theme: document.body.className.includes("dark") ? "dark" : "default"
        });
        initialized = true;
      }

      let id = 0;
      for (const codeEl of document.querySelectorAll("pre code.mermaid")) {
        const preEl = codeEl.parentElement;
        const graphDefinition = codeEl.textContent;
        const graphEl = document.createElement("div");
        const graphId = "mermaid-graph-" + id++;
        mermaid.render(graphId, graphDefinition).then(({svg, bindFunctions}) => {
          graphEl.innerHTML = svg;
          bindFunctions?.(graphEl);
          preEl.insertAdjacentElement("afterend", graphEl);
          preEl.remove();
        });
      }
    });
    </script>
    """
  end

  defp before_closing_body_tag(:epub), do: ""
end
