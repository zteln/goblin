defmodule Goblin.Writer do
  @moduledoc false
  use GenServer
  alias Goblin.Writer.MemTable
  alias Goblin.Writer.Transaction
  alias Goblin.Store
  alias Goblin.WAL
  alias Goblin.Manifest
  alias Goblin.SSTs
  alias Goblin.PubSub

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
    :key_limit,
    :mem_table,
    :writer,
    :flushing,
    seq: 0,
    write_queue: :queue.new()
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
        :key_limit
      ])
      |> Keyword.merge(local_name: local_name)

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  @spec get(writer(), Goblin.db_key(), Goblin.seq_no() | nil) ::
          {:value, non_neg_integer(), Goblin.db_value()} | :not_found
  def get(writer_name, key, seq \\ nil) do
    case MemTable.read(writer_name, key, seq) do
      :not_found -> :not_found
      {_key, seq, value} -> {:value, seq, value}
    end
  end

  @spec get_multi(writer(), [Goblin.db_key()]) ::
          {[Goblin.triple()], [Goblin.db_key()]}
  def get_multi(writer_name, keys) do
    Enum.reduce(keys, {[], []}, fn key, {found, not_found} ->
      case MemTable.read(writer_name, key, nil) do
        :not_found -> {found, [key | not_found]}
        result -> {[result | found], not_found}
      end
    end)
  end

  @spec iterators(writer(), Goblin.db_key() | nil, Goblin.db_key() | nil) ::
          {[Goblin.triple()], ([Goblin.triple()] -> {Goblin.triple(), [Goblin.triple()]})}
  def iterators(writer_name, min, max) do
    range = MemTable.get_range(writer_name, min, max)

    iter_f = fn
      [] -> :ok
      [next | range] -> {next, range}
    end

    {range, iter_f}
  end

  @spec latest_commit_sequence(writer()) :: Goblin.seq_no() | -1
  def latest_commit_sequence(writer_name) do
    MemTable.commit_seq(writer_name)
  end

  @spec is_flushing(writer()) :: boolean()
  def is_flushing(writer) do
    GenServer.call(writer, :is_flushing)
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
       key_limit: args[:key_limit],
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

    case WAL.append(wal, tx.writes) do
      :ok ->
        tx.writes
        |> Enum.reverse()
        |> tap(&publish_commit(&1, state))
        |> Enum.each(&apply_write(mem_table, &1))

        seq = tx.seq
        MemTable.put_commit_seq(mem_table, seq - 1)

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
    {:reply, not is_nil(state.flushing), state}
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
    case maybe_flush(state) do
      {:ok, state} -> {:noreply, state}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  def handle_continue(:recover_state, state) do
    case recover_writes(state) do
      {:ok, state} -> {:noreply, state}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  @impl GenServer
  def handle_info({ref, {:ok, :flushed}}, %{flushing: {ref, _, _}} = state) do
    %{
      flushing: {_ref, flushed_seq, _rotated_wal},
      mem_table: mem_table
    } = state

    MemTable.clean_seq_range(mem_table, flushed_seq)
    state = %{state | flushing: nil}
    {:noreply, state, {:continue, :maybe_flush}}
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

  defp maybe_flush(state, rotated_wal \\ nil)

  defp maybe_flush(%{writer: nil, flushing: nil} = state, rotated_wal) do
    %{
      seq: seq,
      mem_table: mem_table,
      key_limit: key_limit
    } = state

    if MemTable.size(mem_table) >= key_limit do
      with {:ok, rotated_wal} <- maybe_rotate(state, rotated_wal) do
        ref = flush(state, seq - 1, rotated_wal)
        state = %{state | flushing: {ref, seq - 1, rotated_wal}}
        {:ok, state}
      end
    else
      {:ok, state}
    end
  end

  defp maybe_flush(state, _rotated_wal) do
    {:ok, state}
  end

  defp maybe_rotate(state, nil) do
    %{
      wal: wal,
      manifest: manifest,
      seq: seq
    } = state

    with {:ok, rotation_wal, current_wal} <- WAL.rotate(wal),
         :ok <- Manifest.log_rotation(manifest, rotation_wal, current_wal),
         :ok <- Manifest.log_sequence(manifest, seq) do
      {:ok, rotation_wal}
    end
  end

  defp maybe_rotate(_state, rotated_wal), do: {:ok, rotated_wal}

  defp flush(state, seq, rotated_wal) do
    %{
      mem_table: mem_table,
      bf_fpp: bf_fpp,
      store: {_local_store_name, store},
      manifest: manifest,
      wal: wal,
      task_sup: task_sup,
      task_mod: task_mod,
      key_limit: key_limit
    } = state

    %{ref: ref} =
      task_mod.async(task_sup, fn ->
        data =
          MemTable.get_seq_range(mem_table, seq)
          |> Enum.chunk_every(key_limit)

        opts = [
          file_getter: fn -> Store.new_file(store) end,
          bf_fpp: bf_fpp,
          compress?: false
        ]

        with {:ok, flushed} <- SSTs.new([data], @flush_level, opts),
             :ok <- Manifest.log_flush(manifest, Enum.map(flushed, & &1.file), rotated_wal),
             :ok <- WAL.clean(wal, rotated_wal),
             :ok <- Store.put(store, flushed) do
          {:ok, :flushed}
        end
      end)

    ref
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

    with {:ok, logs} <- WAL.recover(wal),
         {:ok, state} <- recover_state(state, logs, flushed_seq) do
      MemTable.put_commit_seq(mem_table, state.seq)
      MemTable.set_ready(mem_table)
      {:ok, state}
    end
  end

  defp recover_state(state, [], _default_seq), do: {:ok, state}

  defp recover_state(state, [{_wal, []} | logs], default_seq) do
    state = %{state | seq: default_seq}
    recover_state(state, logs, default_seq)
  end

  defp recover_state(state, [{rotated_wal, writes} | logs], default_seq) do
    %{mem_table: mem_table} = state

    seq =
      writes
      |> Enum.max_by(&elem(&1, 1), fn -> default_seq end)
      |> elem(1)

    Enum.each(writes, &apply_write(mem_table, &1))
    state = %{state | seq: seq + 1}

    with {:ok, state} <- maybe_flush(state, rotated_wal) do
      recover_state(state, logs, seq)
    end
  end

  defp apply_write(mem_table, {:put, seq, key, value}),
    do: MemTable.upsert(mem_table, key, seq, value)

  defp apply_write(mem_table, {:remove, seq, key}), do: MemTable.delete(mem_table, key, seq)
end
