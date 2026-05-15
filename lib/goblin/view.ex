defmodule Goblin.View do
  @moduledoc false

  # How to handle MemTable rotations?

  # The tables is levels with -1 level_key (not yet flushed memtables)

  # Model:
  # {:snapshot, [ABC]}
  # {:reader1}
  # {:snapshot, [ BCD]} -> can delete, no readers referencing it 
  # {:snapshot, [ BC E]}
  # {:reader2}
  # {:snapshot, [ B  EF]}
  # can delete table D in second snapshot
  # diff tables between snapshot{n} and snapshot{n+1} to see which tables to delete

  @type t :: :ets.table()
  @type table :: Goblin.MemTable.t() | Goblin.DiskTable.t()
  @type level_key :: -1 | non_neg_integer()

  @spec new() :: t()
  def new() do
    :ets.new(:goblin_view, [
      :public,
      :ordered_set,
      write_concurrency: true,
      read_concurrency: true
    ])
  end

  def put_sequence(ref, seq) do
    case :ets.prev(ref, {:snapshot, nil}) do
      {:snapshot, _} = key -> :ets.update_element(ref, key, 2, {2, seq})
      _ -> :ets.insert(ref, {{:snapshot, 0}, seq, %{}})
    end

    :ok
  end

  def update(ref, levels) do
    {seq, version} =
      case :ets.prev(ref, {:snapshot, nil}) do
        {:snapshot, version} = key ->
          [{_, seq, _}] = :ets.lookup(ref, key)
          {seq, version + 1}

        _ ->
          {0, inc_get_version(ref)}
      end

    :ets.insert(ref, {{:snapshot, seq, version}, levels})
    :ok
  end

  # def filter(ref, version, lk \\ :_, filter \\ fn -> true end) do
  # end

  def acquire(ref, reader_key) do
    :ets.insert(ref, {{:reader, :pending, reader_key}})
    version = get_version(ref)
    :ets.insert(ref, {{:reader, version, reader_key}})
    :ets.delete(ref, {:reader, :pending, reader_key})
    :ok
  end

  def release(ref, reader_key) do
    :ets.match_delete(ref, {{:reader, :_, reader_key}})
    :ok
  end

  def sweep(ref) do
  end

  defp inc_get_version(ref), do: :ets.update_counter(ref, :version, 1, {:version, 0})
  defp get_version(ref), do: :ets.lookup_element(ref, :version, 2, 0)
end
