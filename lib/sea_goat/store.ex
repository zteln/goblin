defmodule SeaGoat.Store do
  use GenServer
  alias __MODULE__.Levels
  alias SeaGoat.Compactor
  alias SeaGoat.SSTables
  alias SeaGoat.WAL
  alias SeaGoat.RWLocks
  alias SeaGoat.BloomFilter

  require SeaGoat.Writer
  require SeaGoat.Compactor

  @file_suffix ".seagoat"
  @tmp_suffix ".tmp"
  @dump_suffix ".dump"

  @type path :: String.t()

  defstruct [
    :dir,
    :wal,
    :compactor,
    :rw_locks,
    :latest_wal,
    compacting_paths: %{},
    writes: [],
    file_count: 0,
    levels: Levels.new()
  ]

  def start_link(opts) do
    args = Keyword.take(opts, [:dir, :wal, :compactor, :rw_locks, :writer])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  def put(store, path, bloom_filter, level) do
    GenServer.call(store, {:put, path, bloom_filter, level})
  end

  def remove(store, paths, level) do
    GenServer.call(store, {:remove, paths, level})
  end

  def new_path(store) do
    GenServer.call(store, :new_path)
  end

  def reuse_path(store, paths) do
    GenServer.call(store, {:reuse_path, paths})
  end

  def get_ss_tables(store, key) do
    GenServer.call(store, {:get_ss_tables, key})
  end

  def tmp_path(path), do: path <> @tmp_suffix
  def dump_path(path), do: path <> @dump_suffix

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       dir: args[:dir],
       wal: args[:wal],
       rw_locks: args[:rw_locks],
       compactor: args[:compactor]
     }, {:continue, {:replay, args[:writer]}}}
  end

  @impl GenServer
  def handle_call({:put, path, bloom_filter, level}, _from, state) do
    levels = Levels.insert(state.levels, level, {path, bloom_filter})
    {:reply, :ok, %{state | levels: levels}, {:continue, {:put_in_compactor, level, path}}}
  end

  def handle_call({:remove, paths, level}, _from, state) do
    levels =
      Levels.remove(state.levels, level, fn {path, _} ->
        path in paths
      end)

    {:reply, :ok, %{state | levels: levels}}
  end

  def handle_call({:get_ss_tables, key}, {pid, _ref}, state) do
    ss_tables =
      state.levels
      |> Levels.levels()
      |> Enum.reduce([], fn level, acc ->
        acc ++
          Levels.get_all_entries(
            state.levels,
            level,
            fn {_path, bloom_filter} ->
              BloomFilter.is_member(bloom_filter, key)
            end,
            fn {path, _bloom_filter} ->
              RWLocks.rlock(state.rw_locks, path, pid)

              {fn -> SSTables.read(path, key) end,
               fn -> RWLocks.unlock(state.rw_locks, path, pid) end}
            end
          )
      end)

    {:reply, ss_tables, state}
  end

  def handle_call(:new_path, _from, state) do
    new_file_count = state.file_count + 1
    new_path = path(state.dir, new_file_count)
    {:reply, new_path, %{state | file_count: new_file_count}}
  end

  def handle_call({:reuse_path, paths}, _from, state) do
    {path, compacting_paths} = Map.pop(state.compacting_paths, paths)
    {:reply, path, %{state | compacting_paths: compacting_paths}}
  end

  @impl GenServer
  def handle_continue({:put_in_compactor, level, path}, state) do
    file_count = file_count(path)
    :ok = Compactor.put(state.compactor, level, file_count, path)
    {:noreply, state}
  end

  def handle_continue({:replay, writer}, state) do
    state =
      case File.ls!(state.dir) do
        [] ->
          file_count = 0
          path = path(state.dir, file_count)
          send(writer, {:store_ready, path, state.writes})
          %{state | file_count: file_count}

        files ->
          state = replay_files(state, Enum.map(files, &Path.join(state.dir, &1)))
          path = path(state.dir, state.latest_wal)
          send(writer, {:store_ready, path, Enum.reverse(state.writes)})
          %{state | writes: [], latest_wal: nil}
      end

    {:noreply, state}
  end

  defp replay_files(state, files) do
    files
    |> Enum.filter(&String.ends_with?(&1, [@file_suffix, @dump_suffix]))
    |> Enum.flat_map(fn file ->
      [block_count, _ext] =
        file
        |> Path.basename()
        |> String.split(".", parts: 2)

      case Integer.parse(block_count) do
        {int, _} -> [{int, file}]
        _ -> []
      end
    end)
    |> List.keysort(0)
    |> Enum.flat_map(fn {block, file} ->
      case wal_or_db(file, state.wal) do
        {:logs, logs} ->
          [{:logs, logs, block, file}]

        {:level, bloom_filter, level} ->
          [{:level, bloom_filter, level, block, file}]

        _ ->
          []
      end
    end)
    |> Enum.reduce(state, fn
      {:logs, [SeaGoat.Writer.writer_tag() | logs], file_count, path}, acc ->
        writes = [{path, logs} | acc.writes]
        %{acc | writes: writes, file_count: file_count, latest_wal: file_count}

      {:logs, [{SeaGoat.Compactor.compactor_tag(), paths} | logs], file_count, path}, acc ->
        compacting_paths = Map.put(acc.compacting_paths, paths, path)
        :ok = run_logs(logs)
        %{acc | file_count: file_count, compacting_paths: compacting_paths}

      {:logs, logs, file_count, _path}, acc ->
        :ok = run_logs(logs)
        %{acc | file_count: file_count}

      {:level, bloom_filter, level, file_count, path}, acc ->
        levels = Levels.insert(acc.levels, level, {path, bloom_filter})
        :ok = Compactor.put(state.compactor, level, file_count, path)
        %{acc | levels: levels, file_count: file_count}
    end)
  end

  defp run_logs([]), do: :ok

  defp run_logs([{:del, paths} | logs]) do
    with :ok <- SSTables.delete(paths) do
      run_logs(logs)
    end
  end

  defp run_logs([_ | logs]), do: run_logs(logs)

  defp wal_or_db(file, wal) do
    case WAL.get_logs(wal, file) do
      {:ok, logs} ->
        {:logs, logs}

      _ ->
        case SSTables.fetch_ss_table_info(file) do
          {:ok, bloom_filter, level} ->
            {:level, bloom_filter, level}

          _ ->
            {:error, :not_wal_or_db}
        end
    end
  end

  defp file_count(path) do
    Path.basename(path, @file_suffix)
    |> String.to_integer()
  end

  defp path(dir, count), do: Path.join(dir, to_string(count) <> @file_suffix)
end
