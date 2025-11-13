defmodule Goblin.Store do
  @moduledoc false
  use GenServer
  alias Goblin.Compactor
  alias Goblin.Manifest
  alias Goblin.BloomFilter
  alias Goblin.SSTs

  @file_suffix ".goblin"

  @type store :: module() | {:via, Registry, {module(), module()}}

  defstruct [
    :dir,
    :local_name,
    :compactor,
    :manifest,
    max_file_count: 0
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name]

    args = [
      local_name: opts[:local_name] || name,
      db_dir: opts[:db_dir],
      manifest: opts[:manifest],
      compactor: opts[:compactor]
    ]

    GenServer.start_link(__MODULE__, args, name: name)
  end

  @spec get(store(), Goblin.db_key()) :: {Goblin.db_key(), [Goblin.SSTs.SST.t()]}
  def get(store, key) do
    wait_until_store_ready(store)

    ssts =
      :ets.select(store, [
        {
          {:_, {:"$1", :"$2"}, :_},
          [{:andalso, {:"=<", :"$1", key}, {:"=<", key, :"$2"}}],
          [:"$_"]
        }
      ])
      |> Enum.filter(fn {_file, _key_range, sst} ->
        BloomFilter.is_member(sst.bloom_filter, key)
      end)
      |> Enum.map(fn {_file, _key_range, sst} -> sst end)

    {key, ssts}
  end

  @spec get_multi(store(), [Goblin.db_key()]) :: [{Goblin.db_key(), [Goblin.SSTs.SST.t()]}]
  def get_multi(store, keys) do
    wait_until_store_ready(store)
    Enum.map(keys, &get(store, &1))
  end

  @spec iterators(store(), Goblin.db_key() | nil, Goblin.db_key() | nil) :: [
          {Goblin.SSTs.iterator(),
           (Goblin.SSTs.iterator() -> {Goblin.triple(), Goblin.SSTs.iterator()})}
        ]
  def iterators(store, min, max) do
    wait_until_store_ready(store)

    guard =
      cond do
        is_nil(min) and is_nil(max) -> []
        is_nil(min) -> [{:"=<", :"$2", max}]
        is_nil(max) -> [{:"=<", min, :"$3"}]
        true -> [{:andalso, {:"=<", :"$2", max}, {:"=<", min, :"$3"}}]
      end

    ms = [{{:"$1", {:"$2", :"$3"}, :_}, guard, [:"$1"]}]

    :ets.select(store, ms)
    |> Enum.map(fn file ->
      iter_f = fn iter -> SSTs.iterate(iter) end
      {SSTs.iterate(file), iter_f}
    end)
  end

  @spec put(store(), SSTs.SST.t()) :: :ok
  def put(store, ssts) do
    GenServer.call(store, {:put, ssts})
  end

  @spec remove(store(), Goblin.db_file()) :: :ok
  def remove(store, files) do
    GenServer.call(store, {:remove, files})
  end

  @spec new_file(store()) :: Goblin.db_file()
  def new_file(store) do
    GenServer.call(store, :new_file)
  end

  @impl GenServer
  def init(args) do
    local_name = args[:local_name]
    :ets.new(local_name, [:named_table])

    {:ok,
     %__MODULE__{
       local_name: local_name,
       compactor: args[:compactor],
       manifest: args[:manifest],
       dir: args[:db_dir]
     }, {:continue, :recover_state}}
  end

  @impl GenServer
  def handle_call({:put, ssts}, _from, state) do
    Enum.each(ssts, fn sst ->
      :ets.insert(state.local_name, {sst.file, sst.key_range, sst})
    end)

    {:reply, :ok, state, {:continue, {:put_in_compactor, ssts}}}
  end

  def handle_call({:remove, files}, _from, state) do
    Enum.each(files, fn file ->
      :ets.delete(state.local_name, file)
    end)

    {:reply, :ok, state}
  end

  def handle_call(:new_file, _from, state) do
    file = file_path(state.dir, state.max_file_count)
    {:reply, file, %{state | max_file_count: state.max_file_count + 1}}
  end

  @impl GenServer
  def handle_continue({:put_in_compactor, ssts}, state) do
    Enum.each(ssts, &put_in_compactor(&1, state.compactor))
    {:noreply, state}
  end

  def handle_continue(:recover_state, state) do
    %{files: files, count: file_count} = Manifest.get_version(state.manifest, [:files, :count])

    case recover_ssts(files, state.local_name, state.compactor) do
      :ok -> {:noreply, %{state | max_file_count: file_count}}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  defp recover_ssts([], table_name, _compactor) do
    :ets.insert(table_name, {:ready})
    :ok
  end

  defp recover_ssts([file | files], table_name, compactor) do
    with {:ok, sst} <- SSTs.fetch_sst(file) do
      :ets.insert(table_name, {file, sst.key_range, sst})
      put_in_compactor(sst, compactor)
      recover_ssts(files, table_name, compactor)
    end
  end

  defp put_in_compactor(sst, compactor) do
    %{
      level_key: level_key,
      file: file,
      seq_range: {min_seq, _max_seq},
      size: size,
      key_range: key_range
    } = sst

    Compactor.put(compactor, level_key, file, min_seq, size, key_range)
  end

  defp wait_until_store_ready(store, timeout \\ 5000)
  defp wait_until_store_ready(_store, 0), do: raise("Store failed to get ready within timeout")

  defp wait_until_store_ready(store, timeout) do
    if :ets.member(store, :ready) do
      :ok
    else
      Process.sleep(50)
      wait_until_store_ready(store, timeout - 50)
    end
  end

  defp file_path(dir, count), do: Path.join(dir, to_string(count) <> @file_suffix)
end
