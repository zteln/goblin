defmodule Goblin.Export do
  @moduledoc false
  alias Goblin.Manifest

  @spec export(Path.t(), Goblin.server()) :: {:ok, Path.t()} | {:error, term()}
  def export(dir, manifest_server) do
    %{
      disk_tables: disk_tables,
      wal: wal,
      wal_rotations: wal_rotations,
      manifest: {suffix, manifest}
    } =
      Manifest.snapshot(manifest_server, [
        :disk_tables,
        :wal,
        :wal_rotations,
        :manifest
      ])

    tar_name = tar_name(dir)

    tar_filelist =
      List.flatten([disk_tables, wal, wal_rotations, manifest])
      |> Enum.map(&tar_file(&1, suffix))

    case create_tar(tar_name, tar_filelist) do
      :ok ->
        File.rm!(manifest)
        {:ok, tar_name}

      error ->
        File.rm!(manifest)
        File.rm!(tar_name)
        error
    end
  end

  defp create_tar(name, filelist) do
    :erl_tar.create(name, filelist, [:compressed])
  end

  defp tar_name(dir) do
    now = DateTime.utc_now(:second) |> DateTime.to_iso8601(:basic)
    filename = "goblin_#{now}.tar.gz"
    Path.join(dir, filename)
  end

  defp tar_file(path, suffix) do
    export_name =
      path
      |> Path.basename()
      |> String.trim_trailing(".#{suffix}")

    {~c"#{export_name}", ~c"#{path}"}
  end
end
