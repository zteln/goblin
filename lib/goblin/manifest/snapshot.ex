defmodule Goblin.Manifest.Snapshot do
  @moduledoc false

  defstruct [
    :wal,
    retired_wals: [],
    disk_tables: [],
    tracked: [],
    count: 0,
    seq: 0
  ]

  @type t :: %__MODULE__{}
  @type update_term ::
          {:disk_table_added, Path.t()}
          | {:disk_table_removed, Path.t()}
          | {:wal_retired, Path.t()}
          | {:wal_removed, Path.t()}
          | {:wal_set, Path.t()}
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
    disk_tables = [disk_table | snapshot.disk_tables]
    %{snapshot | disk_tables: disk_tables, count: snapshot.count + 1}
  end

  def update(snapshot, {:disk_table_removed, disk_table}) do
    disk_tables = Enum.reject(snapshot.disk_tables, &(&1 == disk_table))
    tracked = [disk_table | snapshot.tracked]
    %{snapshot | disk_tables: disk_tables, tracked: tracked}
  end

  def update(snapshot, {:wal_retired, wal}) do
    retired_wals = [wal | snapshot.retired_wals]
    %{snapshot | retired_wals: retired_wals}
  end

  def update(snapshot, {:wal_removed, wal}) do
    retired_wals = Enum.reject(snapshot.retired_wals, &(&1 == wal))
    tracked = [wal | snapshot.tracked]
    %{snapshot | retired_wals: retired_wals, tracked: tracked}
  end

  def update(snapshot, {:wal_set, wal}) do
    %{snapshot | wal: wal}
  end

  def update(snapshot, {:seq, seq}) do
    %{snapshot | seq: seq}
  end
end
