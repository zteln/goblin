defmodule Goblin.Manifest.Snapshot do
  @moduledoc false

  defstruct [
    :wal,
    disk_tables: [],
    wals: [],
    sequence: 0,
    disk_table_count: 0,
    wal_count: 0,
    dirt: []
  ]

  @type t :: %__MODULE__{}
  @type update_term ::
          {:snapshot, t()}
          | {:disk_table_added, Path.t()}
          | {:disk_table_removed, Path.t()}
          | {:wal, Path.t()}
          | {:wal_removed, Path.t()}
          | {:sequence, non_neg_integer()}

  @spec update(t(), update_term() | [update_term()]) :: t()
  def update(snapshot, []), do: snapshot

  def update(snapshot, [update | updates]) do
    snapshot
    |> update(update)
    |> update(updates)
  end

  def update(_snapshot, {:snapshot, snapshot}) do
    snapshot
  end

  def update(snapshot, {:disk_table_added, dt_path}) do
    disk_tables = [dt_path | snapshot.disk_tables]

    %{
      snapshot
      | disk_tables: disk_tables,
        disk_table_count: snapshot.disk_table_count + 1
    }
  end

  def update(snapshot, {:disk_table_removed, dt_path}) do
    disk_tables = Enum.reject(snapshot.disk_tables, &(&1 == dt_path))

    %{
      snapshot
      | disk_tables: disk_tables,
        dirt: [dt_path | snapshot.dirt]
    }
  end

  def update(%{wal: nil} = snapshot, {:wal, wal_path}) do
    %{snapshot | wal: wal_path, wal_count: snapshot.wal_count + 1}
  end

  def update(snapshot, {:wal, wal_path}) do
    wals = [snapshot.wal | snapshot.wals]
    %{snapshot | wal: wal_path, wals: wals, wal_count: snapshot.wal_count + 1}
  end

  def update(snapshot, {:wal_removed, wal_path}) do
    wals = Enum.reject(snapshot.wals, &(&1 == wal_path))
    %{snapshot | wals: wals}
  end

  def update(snapshot, {:sequence, sequence}) do
    %{snapshot | sequence: sequence}
  end

  def clear_dirt(snapshot) do
    %{snapshot | dirt: []}
  end
end
