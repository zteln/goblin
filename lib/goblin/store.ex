defmodule Goblin.Store do
  @moduledoc false
  use GenServer
  alias Goblin.Compactor
  alias Goblin.Manifest
  alias Goblin.BloomFilter
  alias Goblin.RWLocks
  alias Goblin.SSTs

  @file_suffix ".goblin"

  @type store :: GenServer.server()
  @type data ::
          {BloomFilter.t(), Goblin.db_sequence(), non_neg_integer(),
           {Goblin.db_key(), Goblin.db_key()}}

  defstruct [
    :dir,
    :compactor,
    :rw_locks,
    :ss_tables,
    :manifest,
    max_file_count: 0
  ]

  @spec put(
          store(),
          Goblin.db_file(),
          Goblin.db_level_key(),
          Goblin.BloomFilter.t(),
          Goblin.db_sequence(),
          non_neg_integer(),
          {Goblin.db_key(), Goblin.db_key()}
        ) :: :ok
  def put(store, file, level_key, bloom_filter, priority, size, key_range) do
    GenServer.call(store, {:put, file, level_key, bloom_filter, priority, size, key_range})
  end

  @spec remove(store(), Goblin.db_file()) :: :ok
  def remove(store, file) do
    GenServer.call(store, {:remove, file})
  end

  @spec get(store(), Goblin.db_key() | [Goblin.db_key()]) :: [
          {(-> Goblin.db_value()), (-> :ok)}
        ]
  def get(store, keys) when is_list(keys) do
    GenServer.call(store, {:get, keys})
  end

  def get(store, key), do: get(store, [key])

  @spec new_file(store()) :: Goblin.db_file()
  def new_file(store) do
    GenServer.call(store, :new_file)
  end

  # def get_ss_tables(store) do
  #   GenServer.call(store, :get_ss_tables)
  # end

  # def get_ss_tables(store, min, max) do
  #   GenServer.call(store, {:get_ss_tables, min, max})
  # end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:dir, :manifest, :compactor, :rw_locks])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

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
  def handle_call({:put, file, level_key, bloom_filter, priority, size, key_range}, _from, state) do
    ss_tables = [
      %{file: file, bloom_filter: bloom_filter} | state.ss_tables
    ]

    {:reply, :ok, %{state | ss_tables: ss_tables},
     {:continue, {:put_in_compactor, level_key, file, {priority, size, key_range}}}}
  end

  def handle_call({:remove, file}, _from, state) do
    ss_tables = Enum.reject(state.ss_tables, &(&1.file == file))
    {:reply, :ok, %{state | ss_tables: ss_tables}}
  end

  def handle_call({:get, keys}, {caller, _ref}, state) do
    %{
      ss_tables: ss_tables,
      rw_locks: rw_locks
    } = state

    ss_tables =
      Enum.map(keys, fn key ->
        {key, get_sst(key, caller, ss_tables, rw_locks)}
      end)

    {:reply, ss_tables, state}
  end

  def handle_call(:new_file, _from, state) do
    file = file_path(state.dir, state.max_file_count)
    {:reply, file, %{state | max_file_count: state.max_file_count + 1}}
  end

  @impl GenServer
  def handle_continue({:put_in_compactor, level_key, file, data}, state) do
    :ok = Compactor.put(state.compactor, level_key, file, data)
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

  defp get_sst(key, caller, ssts, rw_locks) do
    ssts
    |> Enum.filter(&BloomFilter.is_member(&1.bloom_filter, key))
    |> Enum.flat_map(fn sst ->
      case RWLocks.rlock(rw_locks, sst.file, caller) do
        :ok ->
          [
            {
              fn -> SSTs.find(sst.file, key) end,
              fn -> RWLocks.unlock(rw_locks, sst.file, caller) end
            }
          ]

        _ ->
          []
      end
    end)
  end

  defp recover_ss_tables(files, compactor, acc \\ [])
  defp recover_ss_tables([], _compactor, acc), do: {:ok, acc}

  defp recover_ss_tables([file | files], compactor, acc) do
    with {:ok, bloom_filter, level_key, priority, size, key_range} <-
           SSTs.fetch_sst_info(file) do
      Compactor.put(compactor, level_key, file, {priority, size, key_range})
      acc = [%{file: file, bloom_filter: bloom_filter} | acc]
      recover_ss_tables(files, compactor, acc)
    end
  end

  defp file_path(dir, count), do: Path.join(dir, to_string(count) <> @file_suffix)
end
