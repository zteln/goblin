defmodule Goblin.MemTable do
  @moduledoc false
  use GenServer
  alias Goblin.MemTable.{Store, Iterator}
  alias Goblin.WAL
  alias Goblin.Manifest
  alias Goblin.DiskTables

  @flush_level_key 0

  defstruct [
    :store,
    :mem_limit,
    :wal_server,
    :manifest_server,
    :disk_tables_server,
    :flushing,
    seq: 0,
    flush_queue: :queue.new()
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name]
    store_name = opts[:local_name] || name

    args =
      opts
      |> Keyword.take([
        :mem_limit,
        :wal_server,
        :manifest_server,
        :disk_tables_server
      ])
      |> Keyword.merge(store_name: store_name)

    GenServer.start_link(__MODULE__, args, name: name)
  end

  @spec insert(Goblin.server(), Enumerable.t(Goblin.write_term()), Goblin.seq_no()) ::
          :ok | {:error, term()}
  def insert(server, writes, seq) do
    GenServer.call(server, {:insert, writes, seq})
  end

  @spec get(Store.t(), Goblin.db_key(), Goblin.seq_no()) ::
          {:value, Goblin.triple()} | :not_found
  def get(store, key, seq) do
    Store.wait_until_ready(store)

    case Store.get_by_key(store, key, seq) do
      {key, seq, value} -> {:value, {key, seq, value}}
      _ -> :not_found
    end
  end

  @spec get_multi(Store.t(), [Goblin.db_key()], Goblin.seq_no()) :: [
          {:value, Goblin.triple()} | {:not_found, Goblin.db_key()}
        ]
  def get_multi(store, keys, seq) do
    Store.wait_until_ready(store)

    Enum.map(keys, fn key ->
      case Store.get_by_key(store, key, seq) do
        {key, seq, value} -> {:value, {key, seq, value}}
        _ -> {:not_found, key}
      end
    end)
  end

  @spec iterator(Store.t(), Goblin.seq_no()) :: Goblin.Iterable.t()
  def iterator(store, seq) do
    %Iterator{
      store: store,
      max_seq: seq
    }
  end

  @spec commit_seq(Store.t()) :: Goblin.seq_no()
  def commit_seq(store) do
    Store.wait_until_ready(store)
    Store.get_commit_seq(store)
  end

  @spec flushing?(Goblin.server()) :: boolean()
  def flushing?(server) do
    GenServer.call(server, :flushing?)
  end

  @impl GenServer
  def init(args) do
    store = Store.new(args[:store_name])

    {:ok,
     %__MODULE__{
       store: store,
       mem_limit: args[:mem_limit],
       wal_server: args[:wal_server],
       manifest_server: args[:manifest_server],
       disk_tables_server: args[:disk_tables_server]
     }, {:continue, :recover_state}}
  end

  @impl GenServer
  def handle_call({:insert, writes, new_seq}, _from, state) do
    case WAL.append(state.wal_server, writes) do
      :ok ->
        Enum.each(writes, &apply_write(state.store, &1))
        Store.insert_commit_seq(state.store, new_seq)
        {:reply, :ok, %{state | seq: new_seq}, {:continue, :maybe_flush}}

      {:error, reason} = error ->
        {:stop, reason, error, state}
    end
  end

  def handle_call(:flushing?, _from, state) do
    {:reply, state.flushing != nil, state}
  end

  @impl GenServer
  def handle_continue(:maybe_flush, %{flushing: nil} = state) do
    if Store.size(state.store) >= state.mem_limit do
      case coordinate_wal_rotation(state.wal_server, state.manifest_server) do
        {:ok, rotated_wal} ->
          state = enqueue_flush(state, rotated_wal, state.seq)
          {:noreply, state}

        {:error, reason} ->
          {:stop, reason, state}
      end
    else
      {:noreply, state}
    end
  end

  def handle_continue(:maybe_flush, state), do: {:noreply, state}

  def handle_continue(:dequeue_flush, state) do
    case :queue.out(state.flush_queue) do
      {:empty, _flush_queue} ->
        {:noreply, state, {:continue, :maybe_flush}}

      {{:value, {wal, seq}}, flush_queue} ->
        state =
          %{state | flush_queue: flush_queue}
          |> flush(wal, seq)

        {:noreply, state}
    end
  end

  def handle_continue(:recover_state, state) do
    {:noreply, recover_mem_table(state)}
  end

  @impl GenServer
  def handle_info({ref, {:ok, :flushed}}, %{flushing: {ref, _, _}} = state) do
    {_ref, flushed_seq, _rotated_wal} = state.flushing
    Store.delete_range(state.store, flushed_seq)
    {:noreply, %{state | flushing: nil}, {:continue, :dequeue_flush}}
  end

  def handle_info({ref, {:error, reason}}, %{flushing: {ref, _, _}} = state) do
    {:stop, reason, state}
  end

  def handle_info({:DOWN, ref, _, _, reason}, %{flushing: {ref, _, _}} = state) do
    {:stop, reason, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp coordinate_wal_rotation(wal_server, manifest_server) do
    with {:ok, rotated_wal, current_wal} <- WAL.rotate(wal_server),
         :ok <- Manifest.log_rotation(manifest_server, rotated_wal, current_wal) do
      {:ok, rotated_wal}
    end
  end

  defp enqueue_flush(%{flushing: nil} = state, wal, seq) do
    flush(state, wal, seq)
  end

  defp enqueue_flush(state, wal, seq) do
    flush_queue = :queue.in({wal, seq}, state.flush_queue)
    %{state | flush_queue: flush_queue}
  end

  defp flush(state, rotated_wal, seq) do
    %{
      store: store,
      disk_tables_server: disk_tables_server,
      manifest_server: manifest_server,
      wal_server: wal_server
    } = state

    %{ref: ref} =
      Task.async(fn ->
        opts = [
          level_key: @flush_level_key,
          compress?: false
        ]

        stream = Goblin.Iterator.linear_stream(iterator(store, seq))

        with {:ok, disk_tables} <- DiskTables.new(disk_tables_server, stream, opts),
             :ok <- Manifest.log_flush(manifest_server, disk_tables, rotated_wal),
             :ok <- Manifest.log_sequence(manifest_server, seq),
             :ok <- WAL.clean(wal_server, rotated_wal) do
          {:ok, :flushed}
        end
      end)

    %{state | flushing: {ref, seq, rotated_wal}}
  end

  defp recover_mem_table(state) do
    %{seq: flushed_seq} = Manifest.snapshot(state.manifest_server, [:seq])
    logs = WAL.get_log_streams(state.wal_server)
    state = handle_logs(%{state | seq: flushed_seq}, logs)
    Store.insert_commit_seq(state.store, state.seq)
    Store.set_ready(state.store)
    state
  end

  defp handle_logs(state, [{_current_wal, log_stream}]) do
    replay_logs(state, log_stream)
  end

  defp handle_logs(state, [{rotated_wal, log_stream} | logs]) do
    state
    |> replay_logs(log_stream)
    |> then(fn state ->
      enqueue_flush(state, state.seq, rotated_wal)
    end)
    |> handle_logs(logs)
  end

  defp replay_logs(state, log_stream) do
    Enum.each(log_stream, &apply_write(state.store, &1))
    %{state | seq: state.seq + Enum.count(log_stream)}
  end

  defp apply_write(store, {:put, seq, key, value}) do
    Store.insert(store, key, seq, value)
  end

  defp apply_write(store, {:remove, seq, key}) do
    Store.remove(store, key, seq)
  end
end
