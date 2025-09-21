defmodule SeaGoat.Compactor do
  use GenServer
  alias SeaGoat.Store
  alias SeaGoat.SSTables
  alias SeaGoat.WAL
  alias SeaGoat.RWLocks

  @default_tier_limit 10

  defstruct [
    :wal,
    :store,
    :rw_locks,
    :tier_limit,
    tier_paths: %{},
    merging: %{}
  ]

  def start_link(opts) do
    args = Keyword.take(opts, [:tier_limit, :wal, :store, :rw_locks])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  def put(compactor, tier, path) do
    GenServer.call(compactor, {:put, tier, path})
  end

  @impl GenServer
  def init(args) do
    tier_limit = args[:tier_limit] || @default_tier_limit

    {:ok,
     %__MODULE__{
       tier_limit: tier_limit,
       wal: args[:wal],
       store: args[:store],
       rw_locks: args[:rw_locks]
     }}
  end

  @impl GenServer
  def handle_call({:put, tier, path}, _from, state) do
    {paths, new_merge} =
      state.tier_paths
      |> Map.get(tier, [])
      |> put_in_tier(path)
      |> maybe_compact(tier, state.tier_limit, state.store, state.wal, state.rw_locks)

    tier_paths = Map.put(state.tier_paths, tier, paths)
    merging = Map.merge(state.merging, new_merge)
    state = %{state | tier_paths: tier_paths, merging: merging}
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info({ref, :merged}, state),
    do: {:noreply, %{state | merging: Map.delete(state.merging, ref)}}

  def handle_info(_msg, state) do
    # Handle error, retry compaction
    {:noreply, state}
  end

  defp put_in_tier(queue, path), do: [path | queue]

  defp maybe_compact(paths, tier, tier_limit, store, wal, rw_locks) do
    if length(paths) >= tier_limit do
      ref = compact(paths, tier, store, wal, rw_locks)
      {[], %{ref => {tier, paths}}}
    else
      {paths, %{}}
    end
  end

  defp compact(paths, tier, store, wal, rw_locks) do
    %{ref: ref} =
      Task.async(fn ->
        path = Store.new_path(store)
        tmp_path = Store.tmp_path(path)
        dump_path = Store.dump_path(path)
        WAL.dump(wal, path, [{:del, [tmp_path]}])

        with {:ok, bloom_filter, tmp_path, new_tier} <-
               SSTables.write(
                 %SSTables.MergeIterator{},
                 Enum.reverse(paths),
                 tmp_path,
                 tier + 1
               ),
             :ok <-
               WAL.dump(wal, dump_path, [{:del, paths}]),
             :ok <- SSTables.switch(tmp_path, path),
             :ok <- Store.put(store, path, bloom_filter, new_tier),
             :ok <- Store.remove(store, paths, tier),
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
