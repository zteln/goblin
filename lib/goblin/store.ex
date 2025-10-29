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

  @spec get_iterators(store(), Goblin.db_key() | nil, Goblin.db_key() | nil) :: [
          {{
             (-> SSTs.iterator()),
             (SSTs.iterator() -> :ok | {Goblin.triple(), SSTs.iterator()})
           }, (-> :ok)}
        ]
  def get_iterators(store, min, max) do
    GenServer.call(store, {:get_iterators, min, max})
  end

  @spec new_file(store()) :: Goblin.db_file()
  def new_file(store) do
    GenServer.call(store, :new_file)
  end

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

  def handle_call({:get_iterators, min, max}, {caller, _ref}, state) do
    %{
      ssts: ssts,
      rw_locks: rw_locks
    } = state

    iterators =
      ssts
      |> Enum.filter(fn sst ->
        %{key_range: {sst_min, sst_max}} = sst

        cond do
          is_nil(min) and is_nil(max) -> true
          is_nil(min) and sst_min > max -> false
          is_nil(max) and sst_max < min -> false
          sst_max < min or sst_min > max -> false
          true -> true
        end
      end)
      |> Enum.flat_map(&into_iterator(&1, caller, rw_locks))

    {:reply, iterators, state}
  end

  def handle_call({:get, keys}, {caller, _ref}, state) do
    %{
      ssts: ssts,
      rw_locks: rw_locks
    } = state

    read_pairs =
      Enum.map(keys, fn key ->
        {key, filter_ssts(ssts, key, caller, rw_locks)}
      end)

    {:reply, read_pairs, state}
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

  defp into_iterator(sst, caller, rw_locks) do
    %{file: file} = sst
    reader = {fn -> SSTs.iterate(file) end, fn iter -> SSTs.iterate(iter) end}

    case read_pair(file, caller, rw_locks, reader) do
      {:ok, read_pair} -> [read_pair]
      _ -> []
    end
  end

  defp filter_ssts(ssts, key, caller, rw_locks) do
    ssts
    |> Enum.filter(&BloomFilter.is_member(&1.bloom_filter, key))
    |> Enum.flat_map(fn sst ->
      reader = fn -> SSTs.find(sst.file, key) end

      case read_pair(sst.file, caller, rw_locks, reader) do
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
