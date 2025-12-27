defmodule Goblin.Writer do
  @moduledoc false
  use GenServer
  alias Goblin.Writer.MemTable
  alias Goblin.Writer.Transaction
  alias Goblin.Store
  alias Goblin.WAL
  alias Goblin.Manifest
  alias Goblin.DiskTable
  alias Goblin.PubSub
  alias Goblin.Iterator

  @flush_level 0

  @type writer :: module() | {:via, Registry, {module(), module()}}

  defstruct [
    :bf_fpp,
    :manifest,
    :store,
    :wal,
    :pub_sub,
    :task_sup,
    :task_mod,
    :mem_limit,
    :max_sst_size,
    :mem_table,
    :writer,
    :flushing,
    seq: 0,
    write_queue: :queue.new(),
    flush_queue: :queue.new()
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name]
    local_name = opts[:local_name] || name

    args =
      opts
      |> Keyword.take([
        :bf_fpp,
        :store,
        :wal,
        :manifest,
        :pub_sub,
        :task_sup,
        :task_mod,
        :mem_limit,
        :max_sst_size
      ])
      |> Keyword.merge(local_name: local_name)

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  @spec get(writer(), Goblin.db_key(), Goblin.seq_no() | nil) ::
          {:value, non_neg_integer(), Goblin.db_value()} | :not_found
  def get(mem_table, key, seq \\ nil) do
    MemTable.wait_until_memtable_ready(mem_table)
    seq = seq || MemTable.commit_seq(mem_table)

    case MemTable.read(mem_table, key, seq) do
      :not_found -> :not_found
      {_key, seq, value} -> {:value, seq, value}
    end
  end

  @spec get_multi(writer(), [Goblin.db_key()]) ::
          {[Goblin.triple()], [Goblin.db_key()]}
  def get_multi(mem_table, keys) do
    MemTable.wait_until_memtable_ready(mem_table)
    seq = MemTable.commit_seq(mem_table)

    Enum.reduce(keys, {[], []}, fn key, {found, not_found} ->
      case MemTable.read(mem_table, key, seq) do
        :not_found -> {found, [key | not_found]}
        result -> {[result | found], not_found}
      end
    end)
  end

  @spec iterators(writer()) :: Goblin.Iterable.t()
  def iterators(mem_table) do
    MemTable.wait_until_memtable_ready(mem_table)
    seq = MemTable.commit_seq(mem_table)
    MemTable.iterator(mem_table, seq)
  end

  @spec latest_commit_sequence(writer()) :: Goblin.seq_no()
  def latest_commit_sequence(mem_table) do
    MemTable.wait_until_memtable_ready(mem_table)
    MemTable.commit_seq(mem_table)
  end

  @spec is_flushing(writer()) :: boolean()
  def is_flushing(writer) do
    GenServer.call(writer, :is_flushing)
  end

  @spec flush_now(writer()) :: :ok | {:error, term()}
  def flush_now(writer) do
    GenServer.call(writer, :flush_now)
  end

  @spec transaction(writer(), (Goblin.Tx.t() -> Goblin.Tx.return())) ::
          term() | :ok | {:error, term()}
  def transaction(writer, f) do
    with {:ok, tx} <- start_transaction(writer),
         {:ok, tx, reply} <- run_transaction(writer, f, tx),
         :ok <- commit_transaction(writer, tx) do
      reply
    end
  end

  defp start_transaction(writer),
    do: GenServer.call(writer, :start_transaction)

  defp run_transaction(writer, f, tx) do
    case f.(tx) do
      {:commit, tx, reply} ->
        tx = Transaction.reverse_writes(tx)
        {:ok, tx, reply}

      :cancel ->
        cancel_transaction(writer)

      _ ->
        cancel_transaction(writer)
        raise "Invalid return type from transaction."
    end
  end

  defp commit_transaction(writer, tx) do
    GenServer.call(writer, {:commit_transaction, tx})
  end

  defp cancel_transaction(writer) do
    GenServer.call(writer, :cancel_transaction)
  end

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       bf_fpp: args[:bf_fpp],
       manifest: args[:manifest],
       store: args[:store],
       wal: args[:wal],
       pub_sub: args[:pub_sub],
       task_sup: args[:task_sup],
       task_mod: args[:task_mod] || Task.Supervisor,
       mem_limit: args[:mem_limit],
       max_sst_size: args[:max_sst_size],
       mem_table: MemTable.new(args[:local_name])
     }, {:continue, :recover_state}}
  end

  @impl GenServer
  def handle_call(:start_transaction, {pid, _ref}, %{writer: nil} = state) do
    monitor_ref = Process.monitor(pid)
    tx = new_tx(state)
    {:reply, {:ok, tx}, %{state | writer: {pid, monitor_ref}}}
  end

  def handle_call(:start_transaction, {pid, _ref}, %{writer: {pid, _monitor_ref}} = state) do
    {:reply, {:error, :already_in_tx}, state}
  end

  def handle_call(:start_transaction, from, state) do
    write_queue = :queue.in(from, state.write_queue)
    {:noreply, %{state | write_queue: write_queue}}
  end

  def handle_call({:commit_transaction, tx}, {pid, _ref}, %{writer: {pid, monitor_ref}} = state) do
    Process.demonitor(monitor_ref)

    %{
      wal: wal,
      mem_table: mem_table
    } = state

    %{
      writes: writes,
      seq: seq
    } = tx

    case WAL.append(wal, writes) do
      :ok ->
        Enum.each(writes, &apply_write(mem_table, &1))
        publish_commit(writes, state)
        MemTable.put_commit_seq(mem_table, seq)
        {:reply, :ok, %{state | seq: seq, writer: nil}, {:continue, :next_tx}}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  def handle_call({:commit_transaction, _tx}, _from, state) do
    {:reply, {:error, :not_tx_holder}, state}
  end

  def handle_call(:cancel_transaction, {pid, _ref}, %{writer: {pid, _monitor_ref}} = state) do
    {:reply, :ok, %{state | writer: nil}, {:continue, :next_tx}}
  end

  def handle_call(:cancel_transaction, _from, state) do
    {:reply, {:error, :not_tx_holder}, state}
  end

  def handle_call(:is_flushing, _from, state) do
    {:reply, state.flushing != nil, state}
  end

  def handle_call(:flush_now, _from, state) do
    %{
      seq: seq,
      wal: wal,
      manifest: manifest
    } = state

    case coordinate_wal_rotation(wal, manifest) do
      {:ok, rotated_wal} ->
        {:reply, :ok, enqueue_flush(state, seq, rotated_wal)}

      {:error, reason} = error ->
        {:stop, reason, error, state}
    end
  end

  @impl GenServer
  def handle_continue(:next_tx, state) do
    case :queue.out(state.write_queue) do
      {:empty, _write_queue} ->
        {:noreply, state, {:continue, :maybe_flush}}

      {{:value, {pid, _ref} = from}, write_queue} ->
        monitor_ref = Process.monitor(pid)
        tx = new_tx(state)
        GenServer.reply(from, {:ok, tx})
        state = %{state | write_queue: write_queue, writer: {pid, monitor_ref}}
        {:noreply, state}
    end
  end

  def handle_continue(:maybe_flush, state) do
    %{
      mem_table: mem_table,
      mem_limit: mem_limit,
      wal: wal,
      manifest: manifest,
      seq: seq
    } = state

    cond do
      not_flushing?(state) and MemTable.size(mem_table) >= mem_limit ->
        case coordinate_wal_rotation(wal, manifest) do
          {:ok, rotated_wal} ->
            {:noreply, enqueue_flush(state, seq, rotated_wal)}

          {:error, reason} = error ->
            {:stop, reason, error, state}
        end

      true ->
        {:noreply, state}
    end
  end

  def handle_continue(:next_flush, state) do
    case :queue.out(state.flush_queue) do
      {:empty, _flush_queue} ->
        {:noreply, state, {:continue, :maybe_flush}}

      {{:value, {seq, rotated_wal}}, flush_queue} ->
        state =
          %{state | flush_queue: flush_queue}
          |> flush(seq, rotated_wal)

        {:noreply, state}
    end
  end

  def handle_continue(:recover_state, state) do
    {:noreply, recover_writes(state)}
  end

  @impl GenServer
  def handle_info({ref, {:ok, :flushed}}, %{flushing: {ref, _, _}} = state) do
    %{
      flushing: {_ref, flushed_seq, _rotated_wal},
      mem_table: mem_table
    } = state

    MemTable.clean_seq_range(mem_table, flushed_seq)
    state = %{state | flushing: nil}
    {:noreply, state, {:continue, :next_flush}}
  end

  def handle_info({ref, {:error, _reason} = error}, %{flushing: {ref, _, _}} = state) do
    {:stop, error, state}
  end

  def handle_info({:DOWN, ref, _, _, reason}, %{flushing: {ref, _, _}} = state) do
    {:stop, {:error, reason}, state}
  end

  def handle_info({:DOWN, monitor_ref, _, pid, _}, %{writer: {pid, monitor_ref}} = state) do
    state = %{state | writer: nil}
    {:noreply, state, {:continue, :next_tx}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp enqueue_flush(state, seq, rotated_wal) do
    if not_flushing?(state) do
      flush(state, seq, rotated_wal)
    else
      flush_queue = :queue.in({seq, rotated_wal}, state.flush_queue)
      %{state | flush_queue: flush_queue}
    end
  end

  defp not_flushing?(state) do
    is_nil(state.flushing) and :queue.is_empty(state.flush_queue)
  end

  defp coordinate_wal_rotation(wal, manifest) do
    with {:ok, rotated_wal, current_wal} <- WAL.rotate(wal),
         :ok <- Manifest.log_rotation(manifest, rotated_wal, current_wal) do
      {:ok, rotated_wal}
    end
  end

  defp flush(state, seq, rotated_wal) do
    %{
      mem_table: mem_table,
      bf_fpp: bf_fpp,
      store: {_local_store_name, store},
      manifest: manifest,
      wal: wal,
      task_sup: task_sup,
      task_mod: task_mod,
      max_sst_size: max_sst_size
    } = state

    %{ref: ref} =
      task_mod.async(task_sup, fn ->
        opts = [
          level_key: @flush_level,
          file_getter: fn -> Store.new_file(store) end,
          bf_fpp: bf_fpp,
          compress?: false,
          max_sst_size: max_sst_size
        ]

        stream = Iterator.k_merge_stream([MemTable.iterator(mem_table, seq)])

        with {:ok, ssts} <- DiskTable.new(stream, opts),
             :ok <- Manifest.log_flush(manifest, Enum.map(ssts, & &1.file), rotated_wal),
             :ok <- Manifest.log_sequence(manifest, seq),
             :ok <- WAL.clean(wal, rotated_wal),
             :ok <- Store.put(store, ssts) do
          {:ok, :flushed}
        end
      end)

    %{state | flushing: {ref, seq, rotated_wal}}
  end

  defp publish_commit(writes, state) do
    %{
      task_sup: task_sup,
      task_mod: task_mod,
      pub_sub: pub_sub
    } = state

    task_mod.async(task_sup, fn ->
      PubSub.publish(pub_sub, writes)
    end)
  end

  defp new_tx(state) do
    %{
      store: {local_store_name, _},
      mem_table: mem_table,
      seq: seq
    } = state

    Transaction.new(seq, mem_table, local_store_name)
  end

  defp recover_writes(state) do
    %{
      manifest: manifest,
      wal: wal,
      mem_table: mem_table
    } = state

    %{seq: flushed_seq} = Manifest.get_version(manifest, [:seq])

    logs = WAL.get_log_streams(wal)
    state = recover_state(%{state | seq: flushed_seq}, logs)
    MemTable.put_commit_seq(mem_table, state.seq)
    MemTable.set_ready(mem_table)
    state
  end

  defp recover_state(state, [{_current_wal, log_stream}]) do
    replay_logs!(state, log_stream)
  end

  defp recover_state(state, [{rotated_wal, log_stream} | logs]) do
    state
    |> replay_logs!(log_stream)
    |> then(fn state ->
      enqueue_flush(state, state.seq, rotated_wal)
    end)
    |> recover_state(logs)
  end

  defp replay_logs!(state, log_stream) do
    %{mem_table: mem_table} = state
    seq = Enum.count(log_stream)
    Enum.each(log_stream, &apply_write(mem_table, &1))
    %{state | seq: state.seq + seq}
  end

  defp apply_write(mem_table, {:put, seq, key, value}),
    do: MemTable.upsert(mem_table, key, seq, value)

  defp apply_write(mem_table, {:remove, seq, key}),
    do: MemTable.delete(mem_table, key, seq)
end
