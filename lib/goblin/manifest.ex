defmodule Goblin.Manifest do
  @moduledoc false

  alias Goblin.Manifest.{
    Snapshot,
    Log
  }

  @log_key :manifest
  @log_file "manifest.goblin"

  defstruct [
    :log,
    :data_dir,
    :snapshot
  ]

  @type t :: %__MODULE__{
          log: any(),
          data_dir: Path.t(),
          snapshot: Snapshot.t()
        }

  @spec open(atom(), Path.t()) :: {:ok, t()} | {:error, term()}
  def open(name, data_dir) do
    log_file = Path.join(data_dir, @log_file)
    manifest = %__MODULE__{data_dir: data_dir}

    with {:ok, log} <- Log.open_log({name, @log_key}, log_file) do
      recover_manifest(%{manifest | log: log})
    end
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(manifest), do: Log.close_log(manifest.log)

  @spec current_files(t()) :: list(Path.t())
  def current_files(manifest) do
    File.ls!(manifest.data_dir)
    |> Enum.filter(&String.starts_with?(&1, @log_file))
    |> Enum.map(&Path.join(manifest.data_dir, &1))
  end

  @spec update_sequence(t(), non_neg_integer()) :: {:ok, t()} | {:error, term()}
  def update_sequence(manifest, sequence) do
    updates = [{:sequence, sequence}]
    apply_updates(manifest, updates)
  end

  @spec add_wal(t(), Path.t()) :: {:ok, t()} | {:error, term()}
  def add_wal(manifest, wal_path) do
    updates = [{:wal, trim_dir(wal_path)}]
    apply_updates(manifest, updates)
  end

  @spec add_flush(t(), list(Path.t()), Path.t()) :: {:ok, t()} | {:error, term()}
  def add_flush(manifest, dt_paths, wal_path) do
    updates = [
      {:wal_removed, trim_dir(wal_path)}
      | Enum.map(dt_paths, &{:disk_table_added, trim_dir(&1)})
    ]

    apply_updates(manifest, updates)
  end

  @spec add_compaction(t(), list(Path.t()), list(Path.t())) :: {:ok, t()} | {:error, term()}
  def add_compaction(manifest, new_dts, old_dts) do
    updates =
      Enum.map(new_dts, &{:disk_table_added, trim_dir(&1)}) ++
        Enum.map(old_dts, &{:disk_table_removed, trim_dir(&1)})

    apply_updates(manifest, updates)
  end

  @spec clear_dirt(t()) :: t()
  def clear_dirt(manifest) do
    snapshot = Snapshot.clear_dirt(manifest.snapshot)
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
    order = Snapshot.order(manifest.snapshot)

    updates =
      Enum.with_index(updates, fn {action, data}, idx ->
        {idx + order + 1, action, data}
      end)

    snapshot = Snapshot.update(manifest.snapshot, updates)

    with :ok <- Log.append(manifest.log, updates, {:snapshot, snapshot}) do
      {:ok, %{manifest | snapshot: snapshot}}
    end
  end

  defp recover_manifest(manifest) do
    snapshot = recover_snapshot(manifest)

    with :ok <- Log.set_header(manifest.log, {:snapshot, snapshot}) do
      {:ok, %{manifest | snapshot: snapshot}}
    end
  end

  defp recover_snapshot(manifest) do
    manifest.log
    |> Log.stream_log!()
    |> Enum.reduce(%Snapshot{}, &Snapshot.update(&2, &1))
  end

  defp trim_dir(name), do: Path.basename(name)
end
