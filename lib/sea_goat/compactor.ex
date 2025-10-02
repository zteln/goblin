defmodule SeaGoat.Compactor do
  use GenServer
  alias SeaGoat.Store
  alias SeaGoat.SSTables
  alias SeaGoat.WAL
  alias SeaGoat.RWLocks

  @compactor_tag :compactor
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
    tag = Macro.expand(@compactor_tag, __CALLER__)

    quote do
      unquote(tag)
    end
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:level_limit, :wal, :store, :rw_locks])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  @spec put(GenServer.server(), SeaGoat.level(), non_neg_integer(), SeaGoat.db_file()) :: :ok
  def put(compactor, level, count, file) do
    GenServer.call(compactor, {:put, level, count, file})
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
  def handle_call({:put, level, count, file}, _from, state) do
    {entries, new_merge} =
      state.levels
      |> Map.get(level, [])
      |> put_in_level(count, file)
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

  defp put_in_level(level, count, file), do: [{count, file} | level] |> List.keysort(0)

  defp maybe_compact(entries, level, level_limit, store, wal, rw_locks) do
    if length(entries) >= level_limit do
      files = Enum.map(entries, &elem(&1, 1))
      ref = compact(files, level, store, wal, rw_locks)
      {[], %{ref => {level, files}}}
    else
      {entries, %{}}
    end
  end

  defp compact(files, level, store, wal, rw_locks) do
    %{ref: ref} =
      Task.async(fn ->
        file = Store.reuse_file(store, files) || Store.new_file(store)
        tmp_file = Store.tmp_file(file)
        dump_file = Store.dump_file(file)
        WAL.dump(wal, file, [{@compactor_tag, files}, {:del, [tmp_file, dump_file]}])

        with {:ok, bloom_filter, tmp_file, new_level} <-
               SSTables.write(
                 %SSTables.SSTablesIterator{},
                 files,
                 tmp_file,
                 level + 1
               ),
             :ok <- WAL.dump(wal, dump_file, [{:del, files}]),
             :ok <- SSTables.switch(tmp_file, file),
             :ok <- Store.put(store, file, bloom_filter, new_level),
             :ok <- Store.remove(store, files),
             :ok <- lock_unlock_files(rw_locks, files, &RWLocks.wlock/2),
             :ok <- SSTables.delete(files ++ [dump_file]),
             :ok <- lock_unlock_files(rw_locks, files, &RWLocks.unlock/2) do
          :merged
        end
      end)

    ref
  end

  defp lock_unlock_files(_rw_locks, [], _lock_unlock), do: :ok

  defp lock_unlock_files(rw_locks, [file | files], lock_unlock) do
    with :ok <- lock_unlock.(rw_locks, file) do
      lock_unlock_files(rw_locks, files, lock_unlock)
    end
  end
end
