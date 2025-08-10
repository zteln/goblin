defmodule SeaGoat.Server do
  use GenServer
  alias SeaGoat.MemTable
  alias SeaGoat.BloomFilter
  alias SeaGoat.WAL
  alias SeaGoat.Merger
  alias SeaGoat.Flusher
  alias SeaGoat.Reader

  @flush_level 0
  @default_mem_limit 20000
  @default_level_limit 10

  defstruct [
    :dir,
    :wal,
    :merger,
    :flusher,
    :lock_manager,
    :limit,
    :level_limit,
    :mem_table,
    block_count: 0,
    levels: %{},
    flushing: :queue.new()
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args =
      Keyword.take(opts, [:dir, :flusher, :merger, :lock_manager, :wal, :limit, :level_limit])

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  @spec put(server :: GenServer.server(), key :: term(), value :: term()) :: :ok
  def put(server, key, value) do
    GenServer.call(server, {:put, key, value})
  end

  @spec remove(server :: GenServer.server(), key :: term()) :: :ok
  def remove(server, key) do
    GenServer.call(server, {:remove, key})
  end

  @spec get(server :: GenServer.server(), key :: term()) :: {:ok, term()} | {:error, term()}
  def get(server, key) do
    GenServer.call(server, {:get, key})
  end

  @impl GenServer
  def init(opts) do
    dir = opts[:dir]

    with :ok <- sweep(dir),
         {:ok, block_count} <- Reader.fetch_latest_block_count(dir),
         {:ok, levels} <- Reader.fetch_levels(dir, opts[:lock_manager]) do
      levels =
        Enum.reduce(levels, %{}, fn {path, bloom_filter, level}, acc ->
          into_level(acc, path, bloom_filter, level)
        end)

      {:ok,
       %__MODULE__{
         dir: dir,
         wal: opts[:wal],
         merger: opts[:merger],
         flusher: opts[:flusher],
         lock_manager: opts[:lock_manager],
         mem_table: MemTable.new(),
         levels: levels,
         block_count: block_count,
         level_limit: opts[:level_limit] || @default_level_limit,
         limit: opts[:limit] || @default_mem_limit
       }, {:continue, :replay}}
    else
      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp sweep(dir) do
    with {:ok, files} <- File.ls(dir) do
      files
      |> Enum.filter(&(String.starts_with?(&1, ".tmp") and String.ends_with?(&1, ".seagoat")))
      |> Enum.each(&File.rm/1)
    end
  end

  @impl GenServer
  def handle_call({:put, key, value}, _from, state) do
    WAL.append(state.wal, {:put, key, value})
    mem_table = MemTable.upsert(state.mem_table, key, value)
    state = %{state | mem_table: mem_table}
    {:reply, :ok, state, {:continue, :flush}}
  end

  def handle_call({:remove, key}, _from, state) do
    WAL.append(state.wal, {:remove, key})
    mem_table = MemTable.delete(state.mem_table, key)
    state = %{state | mem_table: mem_table}
    {:reply, :ok, state, {:continue, :flush}}
  end

  def handle_call({:get, key}, from, state) do
    case read_in_memory(state, key) do
      {:ok, value} ->
        {:reply, {:ok, value}, state}

      {:error, :not_found} ->
        start_read(state.levels, key, from, state.lock_manager)
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_continue(:flush, state) do
    {:noreply, maybe_flush(state)}
  end

  def handle_continue(:replay, state) do
    state =
      state.wal
      |> WAL.replay()
      |> Enum.reduce(state, fn {chunk, ref}, acc ->
        mem_table =
          Enum.reduce(chunk, acc.mem_table, fn
            {:put, key, value}, acc -> MemTable.upsert(acc, key, value)
            {:remove, key}, acc -> MemTable.delete(acc, key)
          end)

        %{acc | mem_table: mem_table} |> maybe_flush(ref)
      end)

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:flushed, bloom_filter, path, ref}, state) do
    levels = into_level(state.levels, path, bloom_filter, @flush_level)

    flushing =
      :queue.filter(
        fn
          {_mem_table, ^ref} -> false
          _ -> true
        end,
        state.flushing
      )

    WAL.drop_archived(state.wal, ref)
    state = %{state | levels: levels, flushing: flushing} |> maybe_merge()
    {:noreply, state}
  end

  def handle_info({:merged, merged, new_path, bloom_filter, level, ref}, state) do
    levels =
      state.levels
      |> into_level(new_path, bloom_filter, level)
      |> remove_merged(merged, level - 1, ref)

    Merger.remove_merged(state.merger, merged)
    state = %{state | levels: levels} |> maybe_merge()
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  defp maybe_flush(state, ref \\ nil) do
    if MemTable.has_overflow(state.mem_table, state.limit) do
      ref = if ref, do: ref, else: WAL.rotate(state.wal)
      flushing = :queue.in({state.mem_table, ref}, state.flushing)

      Flusher.flush(state.flusher, state.mem_table, @flush_level, state.block_count, ref)

      %{
        state
        | mem_table: MemTable.new(),
          flushing: flushing,
          block_count: state.block_count + 1
      }
    else
      state
    end
  end

  defp maybe_merge(state) do
    Enum.reduce(state.levels, state, fn {level, tables}, acc ->
      non_merging_tables = Enum.reject(tables, &Map.get(&1, :merging?))

      if length(non_merging_tables) >= state.level_limit do
        tables = merge(tables, acc.merger, acc.block_count, level, acc.level_limit)
        levels = Map.put(acc.levels, level, tables)
        %{acc | levels: levels, block_count: acc.block_count + 1}
      else
        levels = Map.put(acc.levels, level, tables)
        %{acc | levels: levels}
      end
    end)
  end

  defp merge(tables, merger, block_count, level, level_limit) do
    ref = make_ref()

    {to_merge, tables} =
      tables
      |> Enum.reverse()
      |> Enum.reduce({[], []}, fn table, {to_merge, tables} ->
        cond do
          match?(%{merging?: {true, _}}, table) ->
            {to_merge, [table | tables]}

          length(to_merge) <= level_limit ->
            table = %{table | merging?: {true, ref}}
            {[table.path | to_merge], [table | tables]}

          true ->
            {to_merge, [table | tables]}
        end
      end)

    Merger.merge(merger, to_merge, block_count, level + 1, ref)
    tables
  end

  defp into_level(acc, path, bloom_filter, level) do
    table = %{
      path: path,
      bloom_filter: bloom_filter,
      merging?: false
    }

    Map.update(acc, level, [table], &[table | &1])
  end

  defp remove_merged(levels, merged, level, ref) do
    tables =
      levels
      |> Map.get(level)
      |> Enum.reject(fn
        %{merging?: {true, ^ref}} = table -> table.path in merged
        _ -> false
      end)

    Map.put(levels, level, tables)
  end

  defp read_in_memory(state, key) do
    :queue.in({state.mem_table, nil}, state.flushing)
    |> read_mem_tables(key)
  end

  defp read_mem_tables(mem_tables, key) do
    case :queue.out_r(mem_tables) do
      {:empty, _} ->
        {:error, :not_found}

      {{:value, {mem_table, _}}, mem_tables} ->
        case MemTable.read(mem_table, key) do
          {:value, value} -> {:ok, value}
          _ -> read_mem_tables(mem_tables, key)
        end
    end
  end

  defp start_read(levels, key, from, lock_manager) do
    files_to_read =
      levels
      |> Enum.sort_by(fn {level, _tables} -> level end)
      |> Enum.flat_map(fn {_level, tables} ->
        tables
      end)
      |> Enum.filter(fn table ->
        BloomFilter.is_member(table.bloom_filter, key)
      end)
      |> Enum.map(&Map.get(&1, :path))

    Task.async(fn ->
      reply = Reader.read_files(files_to_read, key, lock_manager)
      GenServer.reply(from, reply)
    end)
  end
end
