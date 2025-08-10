defmodule SeaGoat.Flusher do
  use Agent
  alias SeaGoat.LockManager
  alias SeaGoat.FileHandler
  alias SeaGoat.Block
  alias SeaGoat.MemTable
  alias SeaGoat.BloomFilter

  defstruct [:dir, :lock_manager, :retries]

  def start_link(opts) do
    Agent.start_link(
      fn ->
        %__MODULE__{
          dir: opts[:dir],
          lock_manager: opts[:lock_manager]
        }
      end,
      name: opts[:name]
    )
  end

  def flush(flusher, mem_table, level, block_count, ref) do
    caller = self()

    Agent.cast(flusher, fn state ->
      case run(mem_table, state.dir, block_count, level, ref, caller, state.lock_manager) do
        :ok ->
          state
      end
    end)
  end

  defp run(mem_table, dir, block_count, level, ref, caller, lock_manager) do
    Task.async(fn ->
      starting_offset = 0
      path = FileHandler.path(dir, block_count)
      tmp_path = FileHandler.path(dir, ".tmp.#{block_count}")

      with :ok <- LockManager.acquire_exclusive_lock(lock_manager, path),
           :ok <- LockManager.acquire_exclusive_lock(lock_manager, tmp_path),
           {:ok, io, ^starting_offset} <- FileHandler.open(tmp_path, write?: true),
           {:ok, bloom_filter} <- write_flush(io, starting_offset, mem_table, level),
           :ok <- FileHandler.sync(io),
           :ok <- FileHandler.close(io),
           :ok <- FileHandler.rename(tmp_path, path),
           :ok <- LockManager.release_lock(lock_manager, path),
           :ok <- LockManager.release_lock(lock_manager, tmp_path) do
        send(caller, {:flushed, bloom_filter, path, ref})
        :ok
      end
    end)
    |> Task.await(:infinity)
  end

  defp write_flush(io, offset, mem_table, level) do
    with {:ok, offset, index} <- write_data(io, offset, mem_table) do
      write_meta(io, offset, mem_table, index, level)
    end
  end

  defp write_data(io, offset, mem_table) do
    MemTable.reduce(mem_table, {offset, %{}}, fn k, v, {offset, index} ->
      encoded_block = Block.encode_block(k, v)
      size = byte_size(encoded_block)

      case FileHandler.write(io, offset, encoded_block) do
        {:ok, new_offset} ->
          {:ok, {new_offset, Map.put(index, k, {offset, size})}}

        {:error, _reason} = e ->
          {:halt, e}
      end
    end)
    |> case do
      {:error, _} = e ->
        e

      {offset, index} ->
        {:ok, offset, index}
    end
  end

  defp write_meta(io, offset, mem_table, index, level) do
    smallest = MemTable.smallest(mem_table)
    largest = MemTable.largest(mem_table)
    bloom_filter = BloomFilter.new(MemTable.keys(mem_table), MemTable.size(mem_table))
    meta = Block.encode_meta(level, smallest, largest, index, bloom_filter)

    with {:ok, _offset} <- FileHandler.write(io, offset, meta) do
      {:ok, bloom_filter}
    end
  end
end
