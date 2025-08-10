defmodule SeaGoat.Merger do
  use Agent
  alias SeaGoat.FileHandler
  alias SeaGoat.BloomFilter
  alias SeaGoat.Block
  alias SeaGoat.LockManager

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

  def remove_merged(merger, merged) do
    Agent.cast(merger, fn state ->
      for old_file <- merged do
        with :ok <- LockManager.acquire_exclusive_lock(state.lock_manager, old_file),
             :ok <- FileHandler.rm(old_file) do
          LockManager.release_lock(state.lock_manager, old_file)
        end
      end

      state
    end)
  end

  def merge(merger, to_merge, block_count, level, ref) do
    caller = self()

    Agent.cast(merger, fn state ->
      case run(to_merge, state.dir, block_count, level, ref, caller, state.lock_manager) do
        :ok ->
          state
      end
    end)
  end

  defp run(to_merge, dir, block_count, level, ref, caller, lock_manager) do
    Task.async(fn ->
      starting_offset = 0
      tmp_path = FileHandler.path(dir, ".tmp.#{block_count}")
      new_path = FileHandler.path(dir, block_count)

      with :ok <- LockManager.acquire_exclusive_lock(lock_manager, new_path),
           :ok <- LockManager.acquire_exclusive_lock(lock_manager, tmp_path),
           {:ok, tmp_io, ^starting_offset} <- FileHandler.open(tmp_path, write?: true),
           {:ok, merging} <- init_mergers(to_merge, lock_manager),
           {:ok, bloom_filter} <- write_merge(merging, level, tmp_io, starting_offset),
           :ok <- FileHandler.sync(tmp_io),
           :ok <- FileHandler.close(tmp_io),
           :ok <- FileHandler.rename(tmp_path, new_path),
           :ok <- deinit_mergers(to_merge, lock_manager),
           :ok <- LockManager.release_lock(lock_manager, new_path),
           :ok <- LockManager.release_lock(lock_manager, tmp_path) do
        send(caller, {:merged, to_merge, new_path, bloom_filter, level, ref})
        :ok
      end
    end)
    |> Task.await(:infinity)
  end

  defp write_merge(merging, level, io, offset, index \\ %{})

  defp write_merge([], level, io, offset, index) do
    keys = index |> Map.keys() |> Enum.sort()
    smallest = List.first(keys)
    largest = List.last(keys)
    bloom_filter = BloomFilter.new(keys, length(keys))
    meta = Block.encode_meta(level, smallest, largest, index, bloom_filter)

    with {:ok, _offset} <- FileHandler.write(io, offset, meta) do
      {:ok, bloom_filter}
    end
  end

  defp write_merge(merging, level, io, offset, index) do
    {smallest_key, value} = merging |> Enum.map(&elem(&1, 2)) |> take_smallest_kv()
    encoded_block = Block.encode_block(smallest_key, value)
    size = byte_size(encoded_block)
    index = Map.put(index, smallest_key, {offset, size})

    with {:ok, merging} = advance(merging, smallest_key),
         {:ok, offset} <- FileHandler.write(io, offset, encoded_block) do
      write_merge(merging, level, io, offset, index)
    end
  end

  defp take_smallest_kv(kvs) do
    kvs
    |> Enum.reduce(hd(kvs), fn {k, v}, {smallest_key, _} = smallest_kv ->
      cond do
        k < smallest_key ->
          {k, v}

        k > smallest_key ->
          smallest_kv

        k == smallest_key ->
          smallest_kv
      end
    end)
  end

  defp advance(merging, key, advanced? \\ false, acc \\ [])

  defp advance([], _key, _advanced?, acc), do: {:ok, Enum.reverse(acc)}

  defp advance([{io, offset, {key, _v}} | merging], key, _advanced?, acc) do
    with {:ok, io, offset, kv} <- next_kv(io, offset) do
      acc = if kv, do: [{io, offset, kv} | acc], else: acc
      advance(merging, key, true, acc)
    end
  end

  defp advance([merge | merging], key, advanced?, acc) do
    advance(merging, key, advanced?, [merge | acc])
  end

  defp init_mergers(files, lock_manager, acc \\ [])
  defp init_mergers([], _lock_manager, acc), do: {:ok, Enum.reverse(acc)}

  defp init_mergers([file | files], lock_manager, acc) do
    with :ok <- LockManager.acquire_shared_lock(lock_manager, file),
         {:ok, io, offset} <- FileHandler.open(file, start?: true),
         {:ok, io, offset, kv} <- next_kv(io, offset) do
      init_mergers(files, lock_manager, [{io, offset, kv} | acc])
    end
  end

  defp deinit_mergers([], _lock_manager), do: :ok

  defp deinit_mergers([file | files], lock_manager) do
    with :ok <- LockManager.release_lock(lock_manager, file) do
      deinit_mergers(files, lock_manager)
    end
  end

  defp next_kv(io, offset) do
    with {:ok, encoded_kv_size} <- FileHandler.read(io, offset, Block.kv_header_size()),
         {:ok, kv_size} <- size(io, offset, encoded_kv_size),
         {:ok, encoded_kv_block} <-
           FileHandler.read(io, offset, Block.kv_header_size() + kv_size),
         {:ok, kv} <- Block.decode_block(encoded_kv_block) do
      {:ok, io, offset + Block.kv_header_size() + kv_size, kv}
    end
  end

  defp size(io, offset, encoded_kv_size) do
    case Block.decode_kv_header(encoded_kv_size) do
      {:ok, :end_of_data} ->
        {:ok, io, offset, nil}

      {:ok, kv_size} ->
        {:ok, kv_size}

      e ->
        e
    end
  end
end
