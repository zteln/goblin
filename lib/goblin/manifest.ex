defmodule Goblin.Manifest do
  @moduledoc false

  alias Goblin.FileIO

  @log_file "manifest.goblin"
  @default_max_log_size 10 * 1024 * 1024

  defstruct [
    :io,
    :data_dir,
    :path,
    :max_log_size,
    size: 0,
    snapshot: {0, [], []}
  ]

  @type snapshot :: {non_neg_integer(), list({atom(), Path.t()}), list({atom(), Path.t()})}
  @type t :: %__MODULE__{
          io: FileIO.t(),
          data_dir: Path.t(),
          path: Path.t(),
          size: non_neg_integer(),
          snapshot: snapshot()
        }

  @spec open(Path.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def open(data_dir, opts \\ []) do
    path = Path.join(data_dir, @log_file)

    if File.exists?(tmp(path)),
      do: FileIO.rename(tmp(path), path)

    with {:ok, io} <- FileIO.open(path, write?: true) do
      size = FileIO.size_of(path)

      manifest = %__MODULE__{
        path: path,
        data_dir: data_dir,
        io: io,
        size: size,
        max_log_size: opts[:max_log_size] || @default_max_log_size
      }

      recover_manifest(manifest)
    end
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(manifest), do: FileIO.close(manifest.io)

  @spec current_file(t()) :: Path.t()
  def current_file(manifest), do: Path.join(manifest.data_dir, @log_file)

  @spec update(t(), list({atom(), Path.t()}), list({atom(), Path.t()}), non_neg_integer()) ::
          {:ok, t()} | {:error, term()}
  def update(manifest, add, del, seq) do
    {_, files, dirt} = manifest.snapshot
    add = Enum.map(add, &trim_dir/1)
    del = Enum.map(del, &trim_dir/1)

    files =
      (files ++ add)
      |> Enum.reject(&(&1 in del))

    dirt = del ++ dirt
    snapshot = {seq, files, dirt}
    manifest = %{manifest | snapshot: snapshot}
    write_snapshot(manifest)
  end

  @spec snapshot(t()) :: snapshot()
  def snapshot(manifest) do
    {seq, files, dirt} = manifest.snapshot

    files =
      Enum.map(files, fn {type, name} ->
        {type, Path.join(manifest.data_dir, name)}
      end)

    dirt =
      Enum.map(dirt, fn {_type, name} ->
        Path.join(manifest.data_dir, name)
      end)

    {seq, files, dirt}
  end

  @spec sweep_dirt(t(), list(Path.t())) :: {:ok, t()} | {:error, term()}
  def sweep_dirt(manifest, paths) do
    {seq, files, dirt} = manifest.snapshot

    dirt =
      Enum.reject(dirt, fn {_, name} ->
        path = Path.join(manifest.data_dir, name)
        path in paths
      end)

    snapshot = {seq, files, dirt}
    manifest = %{manifest | snapshot: snapshot}
    write_snapshot(manifest)
  end

  defp write_snapshot(manifest) do
    with {:ok, size} <- FileIO.append(manifest.io, manifest.snapshot),
         :ok <- FileIO.sync(manifest.io) do
      manifest = %{manifest | size: manifest.size + size}
      maybe_rotate(manifest)
    end
  end

  defp recover_manifest(manifest) do
    with {:ok, snapshot} <- recover_snapshot(manifest) do
      {:ok, %{manifest | snapshot: snapshot}}
    end
  end

  defp recover_snapshot(manifest) do
    manifest.io
    |> FileIO.stream()
    |> Enum.reduce_while({:ok, manifest.snapshot}, fn
      {:ok, snapshot}, _acc ->
        {:cont, {:ok, snapshot}}

      {:corrupt, pos}, {:ok, acc} ->
        if valid_snapshot?(acc, manifest.data_dir) do
          FileIO.truncate(manifest.io, pos)
          {:halt, {:ok, acc}}
        else
          {:halt, {:error, :corrupt_manifest}}
        end

      error, _acc ->
        {:halt, error}
    end)
  end

  defp maybe_rotate(%{size: size, max_log_size: max_log_size} = manifest)
       when size >= max_log_size do
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

  defp valid_snapshot?({_, files, _}, dir) do
    files
    |> Enum.map(&elem(&1, 1))
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.all?(&File.exists?/1)
  end

  defp trim_dir({type, path}), do: {type, Path.basename(path)}
  defp tmp(path), do: path <> ".tmp"
end
