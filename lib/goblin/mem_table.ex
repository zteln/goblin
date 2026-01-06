defmodule Goblin.MemTable do
  @moduledoc false
  use GenServer
  alias Goblin.WAL
  alias Goblin.Manifest
  alias Goblin.DiskTables

  @flush_level_key 0

  defstruct [
    :mem_table,
    :mem_limit,
    :wal_server,
    :manifest_server,
    :disk_tables_server,
    :flushing,
    seq: 0,
    flush_queue: :queue.new()
  ]

  defmodule Iterator do
    @moduledoc false
    defstruct [
      :idx,
      :mem_table,
      :max_seq
    ]

    defimpl Goblin.Iterable do
      def init(iterator), do: iterator

      def next(%{idx: nil} = iterator) do
        idx = :ets.first(iterator.mem_table)
        handle_iteration(iterator, idx)
      end

      def next(iterator) do
        idx = :ets.next(iterator.mem_table, iterator.idx)
        handle_iteration(iterator, idx)
      end

      def close(_iterator), do: :ok

      defp handle_iteration(_iterator, :"$end_of_table"), do: :ok

      defp handle_iteration(%{max_seq: max_seq} = iterator, {key, seq} = idx)
           when abs(seq) < max_seq do
        [{{_key, _seq}, value}] = :ets.lookup(iterator.mem_table, idx)
        {{key, abs(seq), value}, %{iterator | idx: idx}}
      end

      defp handle_iteration(iterator, idx) do
        next(%{iterator | idx: idx})
      end
    end
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name]
    table_name = opts[:local_name] || name

    args =
      opts
      |> Keyword.take([
        :mem_limit,
        :wal_server,
        :manifest_server,
        :disk_tables_server
      ])
      |> Keyword.merge(table_name: table_name)

    GenServer.start_link(__MODULE__, args, name: name)
  end

  @spec insert(Goblin.server(), Enumerable.t(Goblin.write_term()), Goblin.seq_no()) ::
          :ok | {:error, term()}
  def insert(server, writes, seq) do
    GenServer.call(server, {:insert, writes, seq})
  end

  @spec get(Goblin.table(), Goblin.db_key(), Goblin.seq_no()) ::
          {:value, Goblin.triple()} | :not_found
  def get(table, key, seq) do
    wait_until_ready(table)

    case get_by_key(table, key, seq) do
      {{key, seq}, value} -> {:value, {key, seq, value}}
      _ -> :not_found
    end
  end

  @spec get_multi(Goblin.table(), [Goblin.db_key()], Goblin.seq_no()) :: [
          {:value, Goblin.triple()} | {:not_found, Goblin.db_key()}
        ]
  def get_multi(table, keys, seq) do
    wait_until_ready(table)

    Enum.map(keys, fn key ->
      case get_by_key(table, key, seq) do
        {{key, seq}, value} -> {:value, {key, seq, value}}
        _ -> {:not_found, key}
      end
    end)
  end

  @spec iterator(Goblin.table(), Goblin.seq_no()) :: Goblin.Iterable.t()
  def iterator(table, seq) do
    %Iterator{
      mem_table: table,
      max_seq: seq
    }
  end

  @spec commit_seq(Goblin.table()) :: Goblin.seq_no()
  def commit_seq(table) do
    wait_until_ready(table)
    get_commit_seq(table)
  end

  @spec is_flushing?(Goblin.server()) :: boolean()
  def is_flushing?(server) do
    GenServer.call(server, :is_flushing?)
  end

  @impl GenServer
  def init(args) do
    table = :ets.new(args[:table_name], [:named_table, :ordered_set])

    {:ok,
     %__MODULE__{
       mem_table: table,
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
        Enum.each(writes, &apply_write(state.mem_table, &1))
        put_commit_seq(state.mem_table, new_seq)
        {:reply, :ok, %{state | seq: new_seq}, {:continue, :maybe_flush}}

      {:error, reason} = error ->
        {:stop, reason, error, state}
    end
  end

  def handle_call(:is_flushing?, _from, state) do
    {:reply, state.flushing != nil, state}
  end

  @impl GenServer
  def handle_continue(:maybe_flush, %{flushing: nil} = state) do
    if size(state.mem_table) >= state.mem_limit do
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
    delete_range(state.mem_table, flushed_seq)
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
      mem_table: mem_table,
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

        stream = Goblin.Iterator.linear_stream(iterator(mem_table, seq))

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
    put_commit_seq(state.mem_table, state.seq)
    ready(state.mem_table)
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
    Enum.each(log_stream, &apply_write(state.mem_table, &1))
    %{state | seq: state.seq + Enum.count(log_stream)}
  end

  defp apply_write(mem_table, {:put, seq, key, value}) do
    :ets.insert(mem_table, {{key, -seq}, value})
  end

  defp apply_write(mem_table, {:remove, seq, key}) do
    :ets.insert(mem_table, {{key, -seq}, :"$goblin_tombstone"})
  end

  defp put_commit_seq(mem_table, seq) do
    :ets.insert(mem_table, {:commit_seq, seq})
  end

  defp get_commit_seq(mem_table) do
    case :ets.lookup(mem_table, :commit_seq) do
      [] -> 0
      [{_, seq}] -> seq
    end
  end

  defp delete_range(mem_table, seq) do
    ms = [{{{:_, :"$1"}, :_}, [{:<, {:abs, :"$1"}, seq}], [true]}]
    :ets.select_delete(mem_table, ms)
  end

  defp get_by_key(mem_table, key, seq) do
    ms = [
      {
        {{:"$1", :"$2"}, :_},
        [{:andalso, {:"=:=", :"$1", key}, {:<, {:abs, :"$2"}, seq}}],
        [:"$_"]
      }
    ]

    :ets.select(mem_table, ms)
    |> Enum.map(fn {{key, seq}, value} -> {{key, abs(seq)}, value} end)
    |> Enum.max_by(fn {{_key, seq}, _value} -> seq end, fn -> :not_found end)
  end

  defp wait_until_ready(mem_table, timeout \\ 5000)

  defp wait_until_ready(_mem_table, timeout) when timeout <= 0,
    do: raise("MemTable failed to get ready within timeout")

  defp wait_until_ready(mem_table, timeout) do
    if :ets.member(mem_table, :ready) do
      :ok
    else
      Process.sleep(50)
      wait_until_ready(mem_table, timeout - 50)
    end
  end

  defp ready(mem_table) do
    :ets.insert(mem_table, {:ready})
  end

  defp size(mem_table) do
    :ets.info(mem_table, :memory) * :erlang.system_info(:wordsize)
  end
end
