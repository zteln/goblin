defmodule Goblin.Export do
  @moduledoc false

  @spec into_tar(Path.t(), list(Path.t())) :: {:ok, Path.t()} | {:error, term()}
  def into_tar(dir, filelist) do
    tar_name = tar_name(dir)
    tar_filelist = Enum.map(filelist, &tar_file/1)

    case create_tar(tar_name, tar_filelist) do
      :ok ->
        {:ok, tar_name}

      error ->
        File.rm(tar_name)
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

  defp tar_file(path) do
    {~c"#{Path.basename(path)}", ~c"#{path}"}
  end
end
