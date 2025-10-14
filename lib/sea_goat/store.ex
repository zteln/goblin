defmodule SeaGoat.Store do
  use GenServer
  alias SeaGoat.Compactor
  alias SeaGoat.Manifest
  alias SeaGoat.BloomFilter
  alias SeaGoat.RWLocks
  alias SeaGoat.SSTables

  @file_suffix ".seagoat"
  @tmp_suffix ".tmp"

  @type file :: String.t()
  @type level :: non_neg_integer()

  defstruct [
    :dir,
    :compactor,
    :rw_locks,
    :ss_tables,
    :manifest,
    max_file_count: 0
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:dir, :manifest, :compactor, :rw_locks])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  def put(store, file, level, data) do
    GenServer.call(store, {:put, file, level, data})
  end

  def remove(store, file) do
    GenServer.call(store, {:remove, file})
  end

  def new_file(store) do
    GenServer.call(store, :new_file)
  end

  @spec get_ss_tables(GenServer.server(), SeaGoat.db_key()) :: [
          {(-> SeaGoat.db_value()), (-> :ok)}
        ]
  def get_ss_tables(store, key) do
    GenServer.call(store, {:get_ss_tables, key})
  end

  def tmp_file(file), do: file <> @tmp_suffix

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       dir: args[:dir],
       manifest: args[:manifest],
       rw_locks: args[:rw_locks],
       compactor: args[:compactor]
     }, {:continue, :recover_state}}
  end

  @impl GenServer
  def handle_call({:put, file, level, {bloom_filter, seq, size, key_range}}, _from, state) do
    ss_tables = [
      %{file: file, bloom_filter: bloom_filter} | state.ss_tables
    ]

    {:reply, :ok, %{state | ss_tables: ss_tables},
     {:continue, {:put_in_compactor, level, file, {seq, size, key_range}}}}
  end

  def handle_call({:remove, file}, _from, state) do
    ss_tables = Enum.reject(state.ss_tables, &(&1.file == file))
    {:reply, :ok, %{state | ss_tables: ss_tables}}
  end

  def handle_call({:get_ss_tables, key}, {pid, _ref}, state) do
    rw_locks = state.rw_locks

    ss_tables =
      state.ss_tables
      |> Enum.filter(&BloomFilter.is_member(&1.bloom_filter, key))
      |> Enum.map(fn ss_table ->
        RWLocks.rlock(rw_locks, ss_table.file, pid)

        {
          fn -> SSTables.find(ss_table.file, key) end,
          fn -> RWLocks.unlock(rw_locks, ss_table.file, pid) end
        }
      end)

    {:reply, ss_tables, state}
  end

  def handle_call(:new_file, _from, state) do
    file = file_path(state.dir, state.max_file_count)
    {:reply, file, %{state | max_file_count: state.max_file_count + 1}}
  end

  @impl GenServer
  def handle_continue({:put_in_compactor, level, file, data}, state) do
    :ok = Compactor.put(state.compactor, level, file, data)
    {:noreply, state}
  end

  def handle_continue(:recover_state, state) do
    %{files: files, count: file_count} = Manifest.get_version(state.manifest, [:files, :count])

    case recover_ss_tables(files, state.compactor) do
      {:ok, ss_tables} ->
        state = %{state | ss_tables: ss_tables, max_file_count: file_count}
        {:noreply, state}

      {:error, _reason} = error ->
        {:stop, error, state}
    end
  end

  defp recover_ss_tables(files, compactor, acc \\ [])
  defp recover_ss_tables([], _compactor, acc), do: {:ok, acc}

  defp recover_ss_tables([file | files], compactor, acc) do
    with {:ok, bloom_filter, level_key, priority, size, key_range} <-
           SSTables.fetch_ss_table_info(file) do
      Compactor.put(compactor, level_key, file, {priority, size, key_range})
      acc = [%{file: file, bloom_filter: bloom_filter} | acc]
      recover_ss_tables(files, compactor, acc)
    end
  end

  defp file_path(dir, count), do: Path.join(dir, to_string(count) <> @file_suffix)
end
