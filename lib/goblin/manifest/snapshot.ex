defmodule Goblin.Manifest.Snapshot do
  @moduledoc false

  defstruct [
    :active_wal,
    retired_wals: [],
    active_disk_tables: [],
    dirt: [],
    count: 0,
    seq: 0
  ]

  @type t :: %__MODULE__{}
  @type update_term ::
          {:disk_table_activated, Path.t()}
          | {:disk_table_removed, Path.t()}
          | {:wal_activated, Path.t()}
          | {:wal_retired, Path.t()}
          | {:wal_removed, Path.t()}
          | {:seq_set, non_neg_integer()}
          | :sweeped

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

  def update(snapshot, {:disk_table_activated, disk_table}) do
    disk_tables = [disk_table | snapshot.active_disk_tables]
    %{snapshot | active_disk_tables: disk_tables, count: snapshot.count + 1}
  end

  def update(snapshot, {:disk_table_removed, disk_table}) do
    disk_tables = Enum.reject(snapshot.active_disk_tables, &(&1 == disk_table))
    dirt = [disk_table | snapshot.dirt]
    %{snapshot | active_disk_tables: disk_tables, dirt: dirt}
  end

  def update(snapshot, {:wal_retired, wal}) do
    retired_wals = [wal | snapshot.retired_wals]
    %{snapshot | retired_wals: retired_wals}
  end

  def update(snapshot, {:wal_removed, wal}) do
    retired_wals = Enum.reject(snapshot.retired_wals, &(&1 == wal))
    dirt = [wal | snapshot.dirt]
    %{snapshot | retired_wals: retired_wals, dirt: dirt}
  end

  def update(snapshot, {:wal_activated, wal}) do
    %{snapshot | active_wal: wal}
  end

  def update(snapshot, {:seq_set, seq}) do
    %{snapshot | seq: seq}
  end

  def update(snapshot, :sweeped) do
    %{snapshot | dirt: []}
  end
end
