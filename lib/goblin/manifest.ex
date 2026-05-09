defmodule Goblin.Manifest do
  @moduledoc false

  alias Goblin.FileIO

  @log_file "manifest.goblin"
  @max_size 10 * 1024 * 1024

  defstruct [
    :io,
    :data_dir,
    :path,
    :files,
    sequence: 0,
    size: 0
  ]

  @type t :: %__MODULE__{
          io: FileIO.t(),
          data_dir: Path.t(),
          path: Path.t(),
          size: non_neg_integer()
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

  # @spec current_files(t()) :: list(Path.t())
  # def current_files(manifest) do
  #   File.ls!(manifest.data_dir)
  #   |> Enum.filter(&String.starts_with?(&1, @log_file))
  #   |> Enum.map(&Path.join(manifest.data_dir, &1))
  # end

  @spec update(t(), list(Path.t()), list(Path.t()), nil | non_neg_integer()) ::
          {:ok, t()} | {:error, term()}
  def update(manifest, add, del, seq) do
    add = Enum.map(add, &trim_dir/1)
    del = Enum.map(del, &trim_dir/1)
    seq = if seq, do: seq, else: manifest.sequence

    files =
      (add ++ manifest.files)
      |> Enum.reject(&(&1 in del))

    with {:ok, size} <- FileIO.append(manifest.io, {seq, files}) do
      manifest = %{manifest | files: files, sequence: seq, size: manifest.size + size}
      maybe_rotate(manifest)
    end
  end

  # @spec add_wal(t(), Path.t()) :: {:ok, t()} | {:error, term()}
  # def add_wal(manifest, wal_path) do
  #   updates = [{:wal, trim_dir(wal_path)}]
  #   apply_updates(manifest, updates)
  # end
  #
  # @spec add_flush(t(), list(Path.t()), Path.t(), non_neg_integer()) ::
  #         {:ok, t()} | {:error, term()}
  # def add_flush(manifest, dt_paths, wal_path, seq) do
  #   updates = [
  #     {:wal_removed, trim_dir(wal_path)},
  #     {:sequence, seq}
  #     | Enum.map(dt_paths, &{:disk_table_added, trim_dir(&1)})
  #   ]
  #
  #   apply_updates(manifest, updates)
  # end
  #
  # @spec add_compaction(t(), list(Path.t()), list(Path.t())) :: {:ok, t()} | {:error, term()}
  # def add_compaction(manifest, new_dts, old_dts) do
  #   updates =
  #     Enum.map(new_dts, &{:disk_table_added, trim_dir(&1)}) ++
  #       Enum.map(old_dts, &{:disk_table_removed, trim_dir(&1)})
  #
  #   apply_updates(manifest, updates)
  # end

  # @spec clear_dirt(t()) :: t()
  # def clear_dirt(manifest) do
  #   snapshot = Snapshot.clear_dirt(manifest.snapshot)
  #   %{manifest | snapshot: snapshot}
  # end

  @spec snapshot(t(), list(atom())) :: map()
  def snapshot(manifest, keys) do
    manifest.snapshot
    |> Map.take(keys)
    |> Map.replace_lazy(:wal, &(&1 && Path.join(manifest.data_dir, &1)))
    |> Map.replace_lazy(:disk_tables, fn dts ->
      Enum.map(dts, &Path.join(manifest.data_dir, &1))
    end)
    |> Map.replace_lazy(:wals, fn wals ->
      wals
      |> Enum.map(&Path.join(manifest.data_dir, &1))
      |> Enum.reverse()
    end)
    |> Map.replace_lazy(:dirt, fn dirt ->
      Enum.map(dirt, &Path.join(manifest.data_dir, &1))
    end)
  end

  # defp apply_updates(manifest, updates) do
  #   snapshot = Snapshot.update(manifest.snapshot, updates)
  #
  #   with {:ok, size} <- FileIO.append(manifest.io, updates),
  #        :ok <- FileIO.sync(manifest.io) do
  #     %{manifest | snapshot: snapshot, size: manifest.size + size}
  #     |> maybe_rotate()
  #   end
  # end

  defp recover_manifest(manifest) do
    {seq, files} =
      manifest.io
      |> FileIO.stream!(truncate?: true)
      |> Enum.reduce({0, []}, fn next, _acc -> next end)

    %{manifest | sequence: seq, files: files}
  end

  # defp recover_snapshot(manifest) do
  #   FileIO.stream!(manifest.io, truncate?: true)
  #   |> Enum.reduce(%Snapshot{}, &Snapshot.update(&2, &1))
  # end

  defp maybe_rotate(%{size: size} = manifest) when size >= @max_size do
    tmp_path = tmp(manifest.path)

    with :ok <- FileIO.close(manifest.io),
         :ok <- FileIO.rename(manifest.path, tmp_path),
         {:ok, new_io} <- FileIO.open(manifest.path, write?: true),
         {:ok, size} <- FileIO.append(new_io, {:snapshot, manifest.snapshot}),
         :ok <- FileIO.sync(new_io),
         :ok <- FileIO.remove(tmp_path) do
      {:ok, %{manifest | io: new_io, size: size}}
    end
  end

  defp maybe_rotate(manifest), do: {:ok, manifest}

  defp trim_dir(name), do: Path.basename(name)
  defp tmp(path), do: path <> ".tmp"
end
