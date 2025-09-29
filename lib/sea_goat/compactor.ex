defmodule SeaGoat.Compactor do
  use GenServer
  alias SeaGoat.Store
  alias SeaGoat.SSTables
  alias SeaGoat.WAL
  alias SeaGoat.RWLocks

  @default_level_limit 10

  defstruct [
    :wal,
    :store,
    :rw_locks,
    :level_limit,
    levels: %{},
    merging: %{}
  ]

  defmacro compactor_tag do
    quote do
      :compactor
    end
  end

  def start_link(opts) do
    args = Keyword.take(opts, [:level_limit, :wal, :store, :rw_locks])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  def put(compactor, level, count, path) do
    GenServer.call(compactor, {:put, level, count, path})
  end

  @impl GenServer
  def init(args) do
    level_limit = args[:level_limit] || @default_level_limit

    {:ok,
     %__MODULE__{
       level_limit: level_limit,
       wal: args[:wal],
       store: args[:store],
       rw_locks: args[:rw_locks]
     }}
  end

  @impl GenServer
  def handle_call({:put, level, count, path}, _from, state) do
    {entries, new_merge} =
      state.levels
      |> Map.get(level, [])
      |> put_in_level(count, path)
      |> maybe_compact(level, state.level_limit, state.store, state.wal, state.rw_locks)

    levels = Map.put(state.levels, level, entries)
    merging = Map.merge(state.merging, new_merge)
    state = %{state | levels: levels, merging: merging}
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({ref, :merged}, state),
    do: {:noreply, %{state | merging: Map.delete(state.merging, ref)}}

  def handle_info(_msg, state) do
    # Handle error, retry compaction
    {:noreply, state}
  end

  defp put_in_level(level, count, path), do: [{count, path} | level] |> List.keysort(0)

  defp maybe_compact(entries, level, level_limit, store, wal, rw_locks) do
    if length(entries) >= level_limit do
      paths = Enum.map(entries, &elem(&1, 1))
      ref = compact(paths, level, store, wal, rw_locks)
      {[], %{ref => {level, paths}}}
    else
      {entries, %{}}
    end
  end

  defp compact(paths, level, store, wal, rw_locks) do
    %{ref: ref} =
      Task.async(fn ->
        path = Store.reuse_path(store, paths) || Store.new_path(store)
        tmp_path = Store.tmp_path(path)
        dump_path = Store.dump_path(path)
        WAL.dump(wal, path, [{@compactor_tag, paths}, {:del, [tmp_path, dump_path]}])

        with {:ok, bloom_filter, tmp_path, new_level} <-
               SSTables.write(
                 %SSTables.MergeIterator{},
                 Enum.reverse(paths),
                 tmp_path,
                 level + 1
               ),
             :ok <- WAL.dump(wal, dump_path, [{:del, paths}]),
             :ok <- SSTables.switch(tmp_path, path),
             :ok <- Store.put(store, path, bloom_filter, new_level),
             :ok <- Store.remove(store, paths, level),
             :ok <- lock_unlock_paths(rw_locks, paths, &RWLocks.wlock/2),
             :ok <- SSTables.delete(paths ++ [dump_path]),
             :ok <- lock_unlock_paths(rw_locks, paths, &RWLocks.unlock/2) do
          :merged
        end
      end)

    ref
  end

  defp lock_unlock_paths(_rw_locks, [], _lock_unlock), do: :ok

  defp lock_unlock_paths(rw_locks, [path | paths], lock_unlock) do
    with :ok <- lock_unlock.(rw_locks, path) do
      lock_unlock_paths(rw_locks, paths, lock_unlock)
    end
  end
end
