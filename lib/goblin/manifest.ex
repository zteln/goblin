defmodule Goblin.Manifest do
  @moduledoc false

  alias Goblin.FileIO

  @log_file "manifest.goblin"
  @max_size 10 * 1024 * 1024

  defstruct [
    :io,
    :data_dir,
    :path,
    size: 0,
    snapshot: {0, 0, [], []}
  ]

  @type t :: %__MODULE__{
          io: FileIO.t(),
          data_dir: Path.t(),
          path: Path.t(),
          size: non_neg_integer(),
          snapshot:
            {non_neg_integer(), non_neg_integer(), list({atom(), Path.t()}),
             list({atom(), Path.t()})}
        }

  @spec open(Path.t()) :: {:ok, t()} | {:error, term()}
  def open(data_dir) do
    path = Path.join(data_dir, @log_file)

    if File.exists?(tmp(path)),
      do: FileIO.rename(tmp(path), path)

    with {:ok, io} <- FileIO.open(path, write?: true) do
      size = FileIO.size_of(path)

      manifest =
        %__MODULE__{path: path, data_dir: data_dir, io: io, size: size}
        |> recover_manifest()

      {:ok, manifest}
    end
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(manifest), do: FileIO.close(manifest.io)

  @spec current_file(t()) :: Path.t()
  def current_file(manifest), do: Path.join(manifest.data_dir, @log_file)

  @spec update(t(), list({atom(), Path.t()}), list({atom(), Path.t()}), non_neg_integer()) ::
          {:ok, t()} | {:error, term()}
  def update(manifest, add, del, seq) do
    {_, no_files, files, dirt} = manifest.snapshot
    add = Enum.map(add, &trim_dir/1)
    del = Enum.map(del, &trim_dir/1)

    files =
      (files ++ add)
      |> Enum.reject(&(&1 in del))

    dirt = del ++ dirt
    snapshot = {seq, length(add) + no_files, files, dirt}
    manifest = %{manifest | snapshot: snapshot}
    write_snapshot(manifest)
  end

  @spec snapshot(t()) ::
          {non_neg_integer(), non_neg_integer(), list({atom(), Path.t()}), list(Path.t())}
  def snapshot(manifest) do
    {seq, no_files, files, dirt} = manifest.snapshot

    files =
      Enum.map(files, fn {type, name} ->
        {type, Path.join(manifest.data_dir, name)}
      end)

    dirt =
      Enum.map(dirt, fn {_type, name} ->
        Path.join(manifest.data_dir, name)
      end)

    {seq, no_files, files, dirt}
  end

  @spec sweep_dirt(t(), list(Path.t())) :: {:ok, t()} | {:error, term()}
  def sweep_dirt(manifest, paths) do
    {seq, no_files, files, dirt} = manifest.snapshot

    dirt =
      Enum.reject(dirt, fn {_, name} ->
        path = Path.join(manifest.data_dir, name)
        path in paths
      end)

    snapshot = {seq, no_files, files, dirt}
    manifest = %{manifest | snapshot: snapshot}
    write_snapshot(manifest)
  end

  defp write_snapshot(manifest) do
    with {:ok, size} <- FileIO.append(manifest.io, [manifest.snapshot]),
         :ok <- FileIO.sync(manifest.io) do
      manifest = %{manifest | size: manifest.size + size}
      maybe_rotate(manifest)
    end
  end

  defp recover_manifest(manifest) do
    snapshot =
      manifest.io
      |> FileIO.stream!(truncate?: true)
      |> Enum.reduce(manifest.snapshot, fn next, _acc -> next end)

    %{manifest | snapshot: snapshot}
  end

  defp maybe_rotate(%{size: size} = manifest) when size >= @max_size do
    tmp_path = tmp(manifest.path)

    with :ok <- FileIO.close(manifest.io),
         :ok <- FileIO.rename(manifest.path, tmp_path),
         {:ok, new_io} <- FileIO.open(manifest.path, write?: true),
         {:ok, size} <- FileIO.append(new_io, manifest.snapshot),
         :ok <- FileIO.sync(new_io),
         :ok <- FileIO.remove(tmp_path) do
      {:ok, %{manifest | io: new_io, size: size}}
    end
  end

  defp maybe_rotate(manifest), do: {:ok, manifest}

  defp trim_dir({type, path}), do: {type, Path.basename(path)}
  defp tmp(path), do: path <> ".tmp"
end
