defmodule Goblin.Manifest do
  @moduledoc false

  alias Goblin.Log
  alias Goblin.Manifest.Snapshot

  @log_key :manifest
  @log_file "manifest.goblin"
  @default_max_log_size 10 * 1024 * 1024

  defstruct [
    :log,
    :log_file,
    :data_dir,
    :snapshot,
    log_size: 0
  ]

  @type t :: %__MODULE__{
          log: any(),
          log_file: Path.t(),
          data_dir: Path.t(),
          snapshot: Snapshot.t(),
          log_size: non_neg_integer()
        }

  @spec open(atom(), Path.t()) :: {:ok, t()} | {:error, term()}
  def open(name, data_dir) do
    log_file = Path.join(data_dir, @log_file)

    if File.exists?(rotation_file(log_file)),
      do: File.rename(rotation_file(log_file), log_file)

    with {:ok, log} <- Log.open({name, @log_key}, log_file) do
      %{size: log_size} = File.stat!(log_file)
      snapshot = recover_snapshot(log)

      {:ok,
       %__MODULE__{
         log: log,
         log_file: log_file,
         data_dir: data_dir,
         log_size: log_size,
         snapshot: snapshot
       }}
    end
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(manifest), do: Log.close(manifest.log)

  @spec update_sequence(t(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  def update_sequence(manifest, sequence) do
    updates = [{:sequence, sequence}]

    with {:ok, manifest} <- apply_updates(manifest, updates),
         {:ok, manifest} <- rotate(manifest) do
      {:ok, manifest}
    end
  end

  @spec add_wal(t(), Path.t()) :: {:ok, t()} | {:error, term()}
  def add_wal(manifest, wal_path) do
    updates = [{:wal, trim_dir(wal_path)}]

    with {:ok, manifest} <- apply_updates(manifest, updates),
         {:ok, manifest} <- rotate(manifest) do
      {:ok, manifest}
    end
  end

  @spec add_flush(t(), list(Path.t()), Path.t()) :: {:ok, t()} | {:error, term()}
  def add_flush(manifest, dt_paths, wal_path) do
    updates = [
      {:wal_removed, trim_dir(wal_path)}
      | Enum.map(dt_paths, &{:disk_table_added, trim_dir(&1)})
    ]

    with {:ok, manifest} <- apply_updates(manifest, updates),
         {:ok, manifest} <- rotate(manifest) do
      {:ok, manifest}
    end
  end

  @spec add_compaction(t(), list(Path.t()), list(Path.t())) :: {:ok, t()} | {:error, term()}
  def add_compaction(manifest, new_dts, old_dts) do
    updates =
      Enum.map(new_dts, &{:disk_table_added, trim_dir(&1)}) ++
        Enum.map(old_dts, &{:disk_table_removed, trim_dir(&1)})

    with {:ok, manifest} <- apply_updates(manifest, updates),
         {:ok, manifest} <- rotate(manifest) do
      {:ok, manifest}
    end
  end

  @spec clear_dirt(t()) :: t()
  def clear_dirt(manifest) do
    snapshot = Snapshot.update(manifest.snapshot, :clear_dirt)
    %{manifest | snapshot: snapshot}
  end

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

  defp apply_updates(manifest, updates) do
    with {:ok, size} <- Log.append(manifest.log, updates) do
      snapshot = Snapshot.update(manifest.snapshot, updates)
      log_size = manifest.log_size + size
      manifest = %{manifest | snapshot: snapshot, log_size: log_size}
      {:ok, manifest}
    end
  end

  defp rotate(%{log_size: log_size} = manifest) when log_size >= @default_max_log_size do
    %{
      log: log,
      log_file: log_file,
      snapshot: snapshot
    } = manifest

    with :ok <- Log.close(log),
         :ok <- File.rename(log_file, rotation_file(log_file)),
         {:ok, log} <- Log.open(log, log_file),
         {:ok, size} <- Log.append(log, {:snapshot, snapshot}),
         :ok <- File.rm(rotation_file(log_file)) do
      %{size: log_size} = File.stat!(log_file)
      {:ok, %{manifest | log_size: log_size + size, snapshot: snapshot}}
    end
  end

  defp rotate(manifest), do: {:ok, manifest}

  defp recover_snapshot(log) do
    log
    |> Log.stream_log!()
    |> Enum.reduce(%Snapshot{}, &Snapshot.update(&2, &1))
  end

  defp rotation_file(file), do: "#{file}.rotation"
  defp trim_dir(name), do: Path.basename(name)
end
