defmodule Goblin.Manifest.Snapshot do
  @moduledoc false

  defstruct [
    :wal,
    disk_tables: [],
    wals: [],
    sequence: 0,
    disk_table_count: 0,
    wal_count: 0,
    dirt: [],
    order: 0
  ]

  @type t :: %__MODULE__{}
  @type update_term ::
          {:snapshot, t()}
          | {non_neg_integer(), :disk_table_added, Path.t()}
          | {non_neg_integer(), :disk_table_removed, Path.t()}
          | {non_neg_integer(), :wal, Path.t()}
          | {non_neg_integer(), :wal_removed, Path.t()}
          | {non_neg_integer(), :sequence, non_neg_integer()}

  def order(snapshot), do: snapshot.order

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

  def update(%{order: order1} = snapshot, {order2, _, _}) when order1 >= order2,
    do: snapshot

  def update(snapshot, {order, :disk_table_added, dt_path}) do
    disk_tables = [dt_path | snapshot.disk_tables]

    %{
      snapshot
      | order: order,
        disk_tables: disk_tables,
        disk_table_count: snapshot.disk_table_count + 1
    }
  end

  def update(snapshot, {order, :disk_table_removed, dt_path}) do
    disk_tables = Enum.reject(snapshot.disk_tables, &(&1 == dt_path))

    %{
      snapshot
      | order: order,
        disk_tables: disk_tables,
        dirt: [dt_path | snapshot.dirt]
    }
  end

  def update(%{wal: nil} = snapshot, {order, :wal, wal_path}) do
    %{snapshot | order: order, wal: wal_path, wal_count: snapshot.wal_count + 1}
  end

  def update(snapshot, {order, :wal, wal_path}) do
    wals = [snapshot.wal | snapshot.wals]
    %{snapshot | order: order, wal: wal_path, wals: wals, wal_count: snapshot.wal_count + 1}
  end

  def update(snapshot, {order, :wal_removed, wal_path}) do
    wals = Enum.reject(snapshot.wals, &(&1 == wal_path))
    %{snapshot | order: order, wals: wals}
  end

  def update(snapshot, {order, :sequence, sequence}) do
    %{snapshot | order: order, sequence: sequence}
  end

  def clear_dirt(snapshot) do
    %{snapshot | dirt: []}
  end
end
