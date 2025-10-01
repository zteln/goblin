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

  @type file :: String.t()

  defstruct [
    :dir,
    :wal,
    :compactor,
    :rw_locks,
    :latest_wal,
    compacting_files: %{},
    writes: [],
    file_count: 0,
    levels: Levels.new()
  ]

  def start_link(opts) do
    args = Keyword.take(opts, [:dir, :wal, :compactor, :rw_locks, :writer])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  def put(store, file, bloom_filter, level) do
    GenServer.call(store, {:put, file, bloom_filter, level})
  end

  def remove(store, files, level) do
    GenServer.call(store, {:remove, files, level})
  end

  def new_file(store) do
    GenServer.call(store, :new_file)
  end

  def reuse_file(store, files) do
    GenServer.call(store, {:reuse_file, files})
  end

  def get_ss_tables(store, key) do
    GenServer.call(store, {:get_ss_tables, key})
  end

  def tmp_file(file), do: file <> @tmp_suffix
  def dump_file(file), do: file <> @dump_suffix

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
  def handle_call({:put, file, bloom_filter, level}, _from, state) do
    levels = Levels.insert(state.levels, level, {file, bloom_filter})
    {:reply, :ok, %{state | levels: levels}, {:continue, {:put_in_compactor, level, file}}}
  end

  def handle_call({:remove, files, level}, _from, state) do
    levels =
      Levels.remove(state.levels, level, fn {file, _} ->
        file in files
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
            fn {_file, bloom_filter} ->
              BloomFilter.is_member(bloom_filter, key)
            end,
            fn {file, _bloom_filter} ->
              RWLocks.rlock(state.rw_locks, file, pid)

              {fn -> SSTables.read(file, key) end,
               fn -> RWLocks.unlock(state.rw_locks, file, pid) end}
            end
          )
      end)

    {:reply, ss_tables, state}
  end

  def handle_call(:new_file, _from, state) do
    new_file_count = state.file_count + 1
    new_file = file(state.dir, new_file_count)
    {:reply, new_file, %{state | file_count: new_file_count}}
  end

  def handle_call({:reuse_file, files}, _from, state) do
    {file, compacting_files} = Map.pop(state.compacting_files, files)
    {:reply, file, %{state | compacting_files: compacting_files}}
  end

  @impl GenServer
  def handle_continue({:put_in_compactor, level, file}, state) do
    file_count = file_count(file)
    :ok = Compactor.put(state.compactor, level, file_count, file)
    {:noreply, state}
  end

  def handle_continue({:replay, writer}, state) do
    state =
      case File.ls!(state.dir) do
        [] ->
          file_count = 0
          file = file(state.dir, file_count)
          send(writer, {:store_ready, file, state.writes})
          %{state | file_count: file_count}

        files ->
          state = replay_files(state, Enum.map(files, &Path.join(state.dir, &1)))
          file = file(state.dir, state.latest_wal)
          send(writer, {:store_ready, file, Enum.reverse(state.writes)})
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
      {:logs, [SeaGoat.Writer.writer_tag() | logs], file_count, file}, acc ->
        writes = [{file, logs} | acc.writes]
        %{acc | writes: writes, file_count: file_count, latest_wal: file_count}

      {:logs, [{SeaGoat.Compactor.compactor_tag(), files} | logs], file_count, file}, acc ->
        compacting_files = Map.put(acc.compacting_files, files, file)
        :ok = run_logs(logs)
        %{acc | file_count: file_count, compacting_files: compacting_files}

      {:logs, logs, file_count, _file}, acc ->
        :ok = run_logs(logs)
        %{acc | file_count: file_count}

      {:level, bloom_filter, level, file_count, file}, acc ->
        levels = Levels.insert(acc.levels, level, {file, bloom_filter})
        :ok = Compactor.put(state.compactor, level, file_count, file)
        %{acc | levels: levels, file_count: file_count}
    end)
  end

  defp run_logs([]), do: :ok

  defp run_logs([{:del, files} | logs]) do
    with :ok <- SSTables.delete(files) do
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

  defp file_count(file) do
    Path.basename(file, @file_suffix)
    |> String.to_integer()
  end

  defp file(dir, count), do: Path.join(dir, to_string(count) <> @file_suffix)
end
