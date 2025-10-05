defmodule SeaGoat.Compactor do
  use GenServer
  alias SeaGoat.Store
  alias SeaGoat.SSTables
  alias SeaGoat.RWLocks
  alias SeaGoat.Manifest

  @default_level_limit 10

  defstruct [
    :store,
    :rw_locks,
    :manifest,
    :level_limit,
    is_compacting: false,
    compaction_queue: :queue.new(),
    levels: %{},
    merging: %{}
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args = Keyword.take(opts, [:level_limit, :store, :rw_locks, :manifest])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  @spec put(
          GenServer.server(),
          SeaGoat.db_file(),
          SeaGoat.level(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def put(compactor, file, level, min_seq, max_seq) do
    GenServer.call(compactor, {:put, file, level, {min_seq, max_seq}})
  end

  @impl GenServer
  def init(args) do
    level_limit = args[:level_limit] || @default_level_limit

    {:ok,
     %__MODULE__{
       level_limit: level_limit,
       store: args[:store],
       rw_locks: args[:rw_locks],
       manifest: args[:manifest]
     }}
  end

  @impl GenServer
  def handle_call({:put, file, level, seq_range}, _from, state) do
    {entries, new_merge} =
      state.levels
      |> Map.get(level, [])
      |> put_in_level(file, seq_range)
      |> maybe_compact(level, state.level_limit, state.store, state.rw_locks, state.manifest)

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

  defp put_in_level(level, file, seq_range), do: [{file, seq_range} | level] |> List.keysort(1)

  defp maybe_compact(entries, level, level_limit, store, rw_locks, manifest) do
    if length(entries) >= level_limit do
      ref = compact(entries, level, store, rw_locks, manifest)
      {[], %{ref => {level, entries}}}
    else
      {entries, %{}}
    end
  end

  defp compact(entries, level, store, rw_locks, manifest) do
    files = Enum.map(entries, &elem(&1, 0))
    min_seq = entries |> Enum.map(fn {_, {min_seq, _}} -> min_seq end) |> Enum.min()
    max_seq = entries |> Enum.map(fn {_, {_, max_seq}} -> max_seq end) |> Enum.max()

    %{ref: ref} =
      Task.async(fn ->
        file = Store.new_file(store)
        tmp_file = Store.tmp_file(file)

        with {:ok, bloom_filter} <-
               SSTables.write(
                 %SSTables.SSTablesIterator{files: files, min_seq: min_seq, max_seq: max_seq},
                 tmp_file,
                 level + 1
               ),
             :ok <- SSTables.switch(tmp_file, file),
             :ok <- Manifest.log_compaction(manifest, files, file),
             :ok <- Store.put(store, file, bloom_filter, level + 1, min_seq, max_seq),
             :ok <- Store.remove(store, files),
             :ok <- delete_files(files, rw_locks) do
          :merged
        end
      end)

    ref
  end

  defp delete_files([], _rw_locks), do: :ok

  defp delete_files([file | files], rw_locks) do
    with :ok <- RWLocks.wlock(rw_locks, file),
         :ok <- SSTables.delete(file),
         :ok <- RWLocks.unlock(rw_locks, file) do
      delete_files(files, rw_locks)
    end
  end
end
