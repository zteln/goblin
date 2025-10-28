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
    :ssts,
    :manifest,
    max_file_count: 0
  ]

  @spec put(store(), SSTs.SST.t()) :: :ok
  def put(store, sst) do
    GenServer.call(store, {:put, sst})
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

  def get(store) do
    GenServer.call(store, :get)
  end

  @spec new_file(store()) :: Goblin.db_file()
  def new_file(store) do
    GenServer.call(store, :new_file)
  end

  # def get_ssts(store) do
  #   GenServer.call(store, :get_ssts)
  # end

  # def get_ssts(store, min, max) do
  #   GenServer.call(store, {:get_ssts, min, max})
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
  def handle_call({:put, sst}, _from, state) do
    ssts = [sst | state.ssts]
    {:reply, :ok, %{state | ssts: ssts}, {:continue, {:put_in_compactor, sst}}}
  end

  def handle_call({:remove, file}, _from, state) do
    ssts = Enum.reject(state.ssts, &(&1.file == file))
    {:reply, :ok, %{state | ssts: ssts}}
  end

  def handle_call(:get, {caller, _ref}, state) do
    %{rw_locks: rw_locks} = state

    ssts =
      state.ssts
      |> Enum.flat_map(fn sst ->
        %{file: file} = sst

        case read_pair(file, caller, rw_locks, fn -> file end) do
          {:ok, read_pair} -> [read_pair]
          _ -> []
        end
      end)

    {:reply, ssts, state}
  end

  def handle_call({:get, keys}, {caller, _ref}, state) do
    %{
      ssts: ssts,
      rw_locks: rw_locks
    } = state

    ssts =
      Enum.map(keys, fn key ->
        {key, get_sst(key, caller, ssts, rw_locks)}
      end)

    {:reply, ssts, state}
  end

  def handle_call(:new_file, _from, state) do
    file = file_path(state.dir, state.max_file_count)
    {:reply, file, %{state | max_file_count: state.max_file_count + 1}}
  end

  @impl GenServer
  def handle_continue({:put_in_compactor, sst}, state) do
    %{
      level_key: level_key,
      file: file,
      priority: priority,
      size: size,
      key_range: key_range
    } = sst

    :ok = Compactor.put(state.compactor, level_key, {file, priority, size, key_range})
    {:noreply, state}
  end

  def handle_continue(:recover_state, state) do
    %{files: files, count: file_count} = Manifest.get_version(state.manifest, [:files, :count])

    case recover_ssts(files, state.compactor) do
      {:ok, ssts} ->
        state = %{state | ssts: ssts, max_file_count: file_count}
        {:noreply, state}

      {:error, _reason} = error ->
        {:stop, error, state}
    end
  end

  defp get_sst(key, caller, ssts, rw_locks) do
    ssts
    |> Enum.filter(&BloomFilter.is_member(&1.bloom_filter, key))
    |> Enum.flat_map(fn sst ->
      case read_pair(sst.file, caller, rw_locks, fn -> SSTs.find(sst.file, key) end) do
        {:ok, read_pair} -> [read_pair]
        _ -> []
      end
    end)
  end

  defp read_pair(file, caller, rw_locks, reader) do
    with :ok <- RWLocks.rlock(rw_locks, file, caller) do
      {:ok, {reader, fn -> RWLocks.unlock(rw_locks, file, caller) end}}
    end
  end

  defp recover_ssts(files, compactor, acc \\ [])
  defp recover_ssts([], _compactor, acc), do: {:ok, acc}

  defp recover_ssts([file | files], compactor, acc) do
    with {:ok,
          %{
            level_key: level_key,
            file: file,
            priority: priority,
            size: size,
            key_range: key_range
          } = sst} <- SSTs.fetch_sst(file) do
      Compactor.put(compactor, level_key, {file, priority, size, key_range})
      acc = [sst | acc]
      recover_ssts(files, compactor, acc)
    end
  end

  defp file_path(dir, count), do: Path.join(dir, to_string(count) <> @file_suffix)
end
