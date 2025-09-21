defmodule SeaGoat.Store do
  use GenServer
  alias SeaGoat.Store.Tiers
  alias SeaGoat.Compactor
  alias SeaGoat.SSTables
  alias SeaGoat.WAL

  @file_suffix ".seagoat"
  @tmp_suffix ".tmp"
  @dump_suffix ".dump"

  defstruct [
    :dir,
    :wal,
    :compactor,
    replays: [],
    file_count: 0,
    tiers: Tiers.new()
  ]

  def start_link(opts) do
    args = Keyword.take(opts, [:dir, :wal, :compactor, :writer])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  def put(store, path, bloom_filter, tier) do
    GenServer.call(store, {:put, path, bloom_filter, tier})
  end

  def remove(store, paths, tier) do
    GenServer.call(store, {:remove, paths, tier})
  end

  def new_path(store) do
    GenServer.call(store, :new_path)
  end

  def read(store, key) do
    GenServer.call(store, {:read, key})
  end

  def tmp_path(path), do: path <> @tmp_suffix
  def dump_path(path), do: path <> @dump_suffix

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       dir: args[:dir],
       wal: args[:wal],
       compactor: args[:compactor]
     }, {:continue, {:replay, args[:writer]}}}
  end

  @impl GenServer
  def handle_call({:put, path, bloom_filter, tier}, _from, state) do
    tiers = Tiers.insert(state.tiers, tier, {path, nil, bloom_filter})
    {:reply, :ok, %{state | tiers: tiers}, {:continue, {:put_in_compactor, tier, path}}}
  end

  def handle_call({:remove, paths, tier}, _from, state) do
    tiers =
      Tiers.remove(state.tiers, tier, fn {path, _, _} ->
        path in paths
      end)

    {:reply, :ok, %{state | tiers: tiers}}
  end

  # def handle_call({:read, key}, _from, state) do
  #
  # end

  def handle_call(:new_path, _from, state) do
    new_file_count = state.file_count + 1
    new_path = path(state.dir, new_file_count)
    {:reply, new_path, %{state | file_count: new_file_count}}
  end

  @impl GenServer
  def handle_continue({:put_in_compactor, tier, path}, state) do
    :ok = Compactor.put(state.compactor, tier, path)
    {:noreply, state}
  end

  def handle_continue({:replay, writer}, state) do
    state =
      case File.ls!(state.dir) do
        [] ->
          file_count = 0
          path = path(state.dir, file_count)
          WAL.start_log(state.wal, path)
          send(writer, {:replay, state.replays})
          %{state | file_count: file_count}

        files ->
          state = replay_files(state, Enum.map(files, &Path.join(state.dir, &1)))
          send(writer, {:replay, state.replays})
          %{state | replays: []}
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

        {:level, bloom_filter, tier} ->
          [{:level, bloom_filter, tier, block, file}]

        _ ->
          []
      end
    end)
    |> Enum.reduce(state, fn
      {:logs, logs, file_count, path}, acc ->
        replays = [{path, logs} | acc.replays]
        %{acc | replays: replays, file_count: file_count}

      {:level, bloom_filter, tier, file_count, path}, acc ->
        tiers = Tiers.insert(state.tiers, tier, {path, nil, bloom_filter})
        :ok = Compactor.put(state.compactor, tier, path)
        %{acc | tiers: tiers, file_count: file_count}
    end)
  end

  defp wal_or_db(file, wal) do
    case WAL.get_logs(wal, file) do
      {:ok, logs} ->
        {:logs, logs}

      _ ->
        case SSTables.read_bloom_filter(file) do
          {:ok, {bloom_filter, tier}} ->
            {:level, bloom_filter, tier}

          _ ->
            {:error, :not_wal_or_db}
        end
    end
  end

  defp path(dir, count), do: Path.join(dir, to_string(count) <> @file_suffix)
end
