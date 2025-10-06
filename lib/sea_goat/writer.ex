defmodule SeaGoat.Writer do
  use GenServer
  alias SeaGoat.Writer.MemTable
  alias SeaGoat.Writer.Transaction
  alias SeaGoat.WAL
  alias SeaGoat.Manifest
  alias SeaGoat.SSTables
  alias SeaGoat.Store

  @flush_level 0
  @default_mem_limit 20000

  defstruct [
    :wal,
    :store,
    :manifest,
    :limit,
    :flushing,
    min_seq: 0,
    max_seq: 0,
    mem_table: MemTable.new(),
    transactions: %{},
    flush_queue: :queue.new(),
    subscribers: %{}
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args =
      Keyword.take(opts, [:wal, :store, :manifest, :limit])

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  # def subscribe(server, pid \\ self()) do
  #   GenServer.call(server, {:subscribe, pid})
  # end

  def read(writer, key) do
    GenServer.call(writer, {:read, key})
  end

  @spec put(writer :: GenServer.server(), key :: term(), value :: term()) :: :ok
  def put(writer, key, value) do
    transaction(writer, fn tx ->
      tx = Transaction.put(tx, key, value)
      {:commit, tx, :ok}
    end)
  end

  @spec remove(writer :: GenServer.server(), key :: term()) :: :ok
  def remove(writer, key) do
    transaction(writer, fn tx ->
      tx = Transaction.remove(tx, key)
      {:commit, tx, :ok}
    end)
  end

  def transaction(writer, f) do
    with {:ok, tx} <- start_transaction(writer, self()) do
      run_transaction(writer, f, tx)
    end
  end

  defp start_transaction(writer, pid), do: GenServer.call(writer, {:start_transaction, pid})

  defp run_transaction(writer, f, tx) do
    case f.(tx) do
      {:commit, tx, reply} ->
        :ok = commit_transaction(writer, tx, self())
        reply

      :cancel ->
        cancel_transaction(writer, self())

      _ ->
        raise "Invalid return type from transaction."
    end
  end

  defp commit_transaction(writer, tx, pid) do
    GenServer.call(writer, {:commit_transaction, tx, pid})
  end

  defp cancel_transaction(writer, pid) do
    GenServer.call(writer, {:cancel_transaction, pid})
  end

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       wal: args[:wal],
       store: args[:store],
       manifest: args[:manifest],
       mem_table: MemTable.new(),
       limit: args[:limit] || @default_mem_limit
     }, {:continue, :recover_state}}
  end

  @impl GenServer
  def handle_call({:read, key}, _from, state) do
    flushing_mem_table = if state.flushing, do: [elem(state.flushing, 1)], else: []

    flush_queue_mem_tables =
      state.flush_queue
      |> :queue.to_list()
      |> Enum.map(&elem(&1, 2))

    mem_tables = [state.mem_table | flush_queue_mem_tables ++ flushing_mem_table]

    case search_for_key(mem_tables, key) do
      {:ok, value} ->
        {:reply, {:ok, value}, state}

      :not_found ->
        {:reply, :error, state}
    end
  end

  def handle_call({:start_transaction, pid}, _from, state) do
    if not Map.has_key?(state.transactions, pid) do
      writer = self()
      store = state.store
      tx = Transaction.new(pid, &SeaGoat.Reader.get(writer, store, &1))
      transactions = Map.put(state.transactions, pid, [])
      {:reply, {:ok, tx}, %{state | transactions: transactions}}
    else
      {:reply, {:error, :already_in_transaction}, state}
    end
  end

  def handle_call({:commit_transaction, tx, pid}, _from, state) do
    case Map.get(state.transactions, pid) do
      nil ->
        {:reply, {:error, :no_tx_found}, state}

      commits ->
        if not Transaction.has_conflict(tx, commits) do
          transactions =
            state.transactions
            |> Map.delete(pid)
            |> Enum.into(%{}, fn {pid, commits} ->
              {pid, [tx.mem_table | commits]}
            end)

          WAL.append(state.wal, tx.writes)
          mem_table = MemTable.merge(state.mem_table, tx.mem_table)

          state = %{
            state
            | transactions: transactions,
              mem_table: mem_table,
              max_seq: state.max_seq + length(tx.writes)
          }

          {:reply, :ok, state, {:continue, :flush}}
        else
          transactions = Map.delete(state.transactions, pid)
          state = %{state | transactions: transactions}
          {:reply, {:error, :in_conflict}, state}
        end
    end
  end

  def handle_call({:cancel_transaction, pid}, _from, state) do
    transactions = Map.delete(state.transactions, pid)
    state = %{state | transactions: transactions}
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_continue(:recover_state, state) do
    case recover_writes(state) do
      {:ok, state} ->
        {:noreply, state}

      {:error, _reason} = error ->
        {:stop, error, state}
    end
  end

  def handle_continue(:flush, state) do
    {:noreply, maybe_flush(state)}
  end

  @impl GenServer
  def handle_info({_ref, :flushed}, state) do
    {:noreply, %{state | flushing: nil}, {:continue, :flush}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp maybe_flush(state) do
    has_overflow = MemTable.has_overflow(state.mem_table, state.limit)
    is_flush_queue_empty = :queue.len(state.flush_queue) == 0
    is_flushing = not is_nil(state.flushing)

    cond do
      has_overflow and is_flushing ->
        flush_queue =
          :queue.in({state.min_seq, state.max_seq, state.mem_table}, state.flush_queue)

        %{state | mem_table: MemTable.new(), flush_queue: flush_queue, min_seq: state.max_seq}

      has_overflow and is_flush_queue_empty ->
        ref =
          flush(
            state.mem_table,
            state.min_seq,
            state.max_seq,
            state.store,
            state.wal,
            state.manifest
          )

        flushing = {ref, state.mem_table}

        %{
          state
          | mem_table: MemTable.new(),
            flushing: flushing,
            min_seq: state.max_seq
        }

      has_overflow ->
        ref =
          flush(
            state.mem_table,
            state.min_seq,
            state.max_seq,
            state.store,
            state.wal,
            state.manifest
          )

        flushing = {ref, state.mem_table}

        %{
          state
          | mem_table: MemTable.new(),
            flushing: flushing,
            min_seq: state.max_seq
        }

      not is_flushing and not is_flush_queue_empty ->
        {{:value, {min_seq, max_seq, mem_table}}, flush_queue} = :queue.out(state.flush_queue)
        ref = flush(mem_table, min_seq, max_seq, state.store, state.wal, state.manifest)
        flushing = {ref, mem_table}

        %{
          state
          | flushing: flushing,
            flush_queue: flush_queue
        }

      true ->
        state
    end
  end

  defp flush(mem_table, min_seq, max_seq, store, wal, manifest) do
    Manifest.log_sequence(manifest, max_seq)
    WAL.rotate(wal)
    file = Store.new_file(store)
    tmp_file = Store.tmp_file(file)

    %{ref: ref} =
      Task.async(fn ->
        with {:ok, bloom_filter} <-
               SSTables.write(
                 %SSTables.MemTableIterator{
                   data: Enum.sort(mem_table),
                   min_seq: min_seq,
                   max_seq: max_seq
                 },
                 tmp_file,
                 @flush_level
               ),
             :ok <- SSTables.switch(tmp_file, file),
             :ok <- Manifest.log_file_added(manifest, file),
             :ok <- WAL.clean(wal),
             :ok <- Store.put(store, file, bloom_filter, @flush_level, min_seq, max_seq) do
          :flushed
        end
      end)

    ref
  end

  defp search_for_key([], _key), do: :not_found

  defp search_for_key([mem_table | mem_tables], key) do
    case MemTable.read(mem_table, key) do
      {:value, value} ->
        {:ok, value}

      :not_found ->
        search_for_key(mem_tables, key)
    end
  end

  defp recover_writes(state) do
    with {:ok, writes} <- WAL.recover(state.wal) do
      %{sequence: seq} = Manifest.get_version(state.manifest, [:sequence])
      state = Enum.reduce(writes, %{state | min_seq: seq, max_seq: seq}, &recover_state/2)
      {:ok, state}
    end
  end

  defp recover_state({:put, k, v}, state) do
    mem_table = MemTable.upsert(state.mem_table, k, v)

    %{state | mem_table: mem_table, max_seq: state.max_seq + 1}
    |> maybe_flush()
  end

  defp recover_state({:remove, k}, state) do
    mem_table = MemTable.delete(state.mem_table, k)

    %{state | mem_table: mem_table, max_seq: state.max_seq + 1}
    |> maybe_flush()
  end
end
