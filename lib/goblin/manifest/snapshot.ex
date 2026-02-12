defmodule Goblin.Manifest.Snapshot do
  @moduledoc false

  defstruct [
    :wal,
    wal_rotations: [],
    disk_tables: MapSet.new(),
    tracked: [],
    count: 0,
    seq: 0
  ]

  @type t :: %__MODULE__{}
  @type update_term ::
          {:disk_table_added, Path.t()}
          | {:disk_table_removed, Path.t()}
          | {:wal_added, Path.t()}
          | {:wal_removed, Path.t()}
          | {:wal_created, Path.t()}
          | {:seq, non_neg_integer()}

  @doc "Update the manifest snapshot with term(s)."
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

  def update(snapshot, {:disk_table_added, disk_table}) do
    disk_tables = MapSet.put(snapshot.disk_tables, disk_table)
    %{snapshot | disk_tables: disk_tables, count: snapshot.count + 1}
  end

  def update(snapshot, {:disk_table_removed, disk_table}) do
    disk_tables = MapSet.delete(snapshot.disk_tables, disk_table)
    tracked = [disk_table | snapshot.tracked]
    %{snapshot | disk_tables: disk_tables, tracked: tracked}
  end

  def update(snapshot, {:wal_added, wal}) do
    wal_rotations = [wal | snapshot.wal_rotations]
    %{snapshot | wal_rotations: wal_rotations}
  end

  def update(snapshot, {:wal_removed, wal}) do
    wal_rotations = Enum.reject(snapshot.wal_rotations, &(&1 == wal))
    tracked = [wal | snapshot.tracked]
    %{snapshot | wal_rotations: wal_rotations, tracked: tracked}
  end

  def update(snapshot, {:wal_created, wal}) do
    %{snapshot | wal: wal}
  end

  def update(snapshot, {:seq, seq}) do
    %{snapshot | seq: seq}
  end
end
