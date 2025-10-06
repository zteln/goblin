defmodule SeaGoat.Recovery do
  use Task
  alias SeaGoat.Manifest

  def start_link(opts) do
    dir = opts[:dir]
    manifest = opts[:manifest]
    Task.start_link(__MODULE__, :run, [dir, manifest])
  end

  def run(dir, manifest) do
    :ok = rm_tmp_files(dir)
    :ok = rm_orphaned_files(dir, manifest)
  end

  defp rm_tmp_files(dir) do
    dir_listing(dir)
    |> Enum.filter(&String.ends_with?(&1, ".seagoat.tmp"))
    |> Enum.each(&File.rm!/1)
  end

  defp rm_orphaned_files(dir, manifest) do
    %{files: files} = Manifest.get_version(manifest, [:files])

    dir_listing(dir)
    |> Enum.reject(&String.starts_with?(Path.basename(&1), ["wal", "manifest"]))
    |> Enum.filter(&(&1 not in files))
    |> Enum.each(&File.rm!/1)
  end

  defp dir_listing(dir) do
    dir
    |> File.ls!()
    |> Enum.map(&Path.join(dir, &1))
  end
end
