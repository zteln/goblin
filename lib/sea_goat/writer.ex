defmodule SeaGoat.Writer do
  use GenServer
  alias SeaGoat.Writer.MemTable
  alias SeaGoat.Writer.Transaction
  alias SeaGoat.WAL
  alias SeaGoat.SSTables
  alias SeaGoat.Store

  @writer_tag :writer
  @flush_level 0
  @default_mem_limit 20000

  defstruct [
    :wal,
    :store,
    :limit,
    mem_table: MemTable.new(),
    transactions: %{},
    flushing: [],
    subscribers: %{}
  ]

  defmacro writer_tag do
    quote do
      :writer
    end
  end

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    args =
      Keyword.take(opts, [:wal, :store, :limit])

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

      _ ->
        cancel_transaction(writer, self())
    end
  end

  defp commit_transaction(writer, tx, pid) do
    GenServer.call(writer, {:commit_transaction, tx, pid})
  end

  defp cancel_transaction(writer, pid) do
    GenServer.call(writer, {:cancel_transaction, pid})
  end

  @impl GenServer
  def init(opts) do
    {:ok,
     %__MODULE__{
       wal: opts[:wal],
       store: opts[:store],
       mem_table: MemTable.new(),
       limit: opts[:limit] || @default_mem_limit
     }, {:continue, :get_previous_writes}}
  end

  @impl GenServer
  def handle_call({:read, key}, _from, state) do
    mem_tables = [state.mem_table | Enum.map(state.flushing, &elem(&1, 1))]

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

          WAL.append_batch(state.wal, tx.writes)
          mem_table = MemTable.merge(state.mem_table, tx.mem_table)
          state = %{state | transactions: transactions, mem_table: mem_table}
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
  def handle_continue(:get_previous_writes, state) do
    {previous_writes, file} = Store.get_write_replays(state.store)
    WAL.open(state.wal, file)
    WAL.append_batch(state.wal, [@writer_tag, {:del, [Store.tmp_file(file)]}])
    state = replay_previous_writes(state, previous_writes)
    {:noreply, state}
  end

  def handle_continue(:flush, state) do
    {:noreply, maybe_flush(state)}
  end

  @impl GenServer
  def handle_info({ref, :flushed}, state) do
    flushing = Enum.reject(state.flushing, &(elem(&1, 0) == ref))
    state = %{state | flushing: flushing}
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp maybe_flush(state) do
    if MemTable.has_overflow(state.mem_table, state.limit) do
      file = WAL.current_file(state.wal)
      tmp_file = Store.tmp_file(file)
      new_file = Store.new_file(state.store)
      WAL.append(state.wal, :flush)
      WAL.rotate(state.wal, new_file)
      WAL.append_batch(state.wal, [{:del, [Store.tmp_file(new_file)]}, @writer_tag])

      ref = flush(state.mem_table, file, tmp_file, state.store)
      flushing = [{ref, state.mem_table} | state.flushing]
      %{state | mem_table: MemTable.new(), flushing: flushing}
    else
      state
    end
  end

  defp flush(mem_table, file, tmp_file, store) do
    %{ref: ref} =
      Task.async(fn ->
        with {:ok, bloom_filter, tmp_file, level} <-
               SSTables.write(
                 %SSTables.MemTableIterator{},
                 mem_table,
                 tmp_file,
                 @flush_level
               ),
             :ok <- SSTables.switch(tmp_file, file),
             :ok <- Store.put(store, file, bloom_filter, level) do
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

  defp replay_previous_writes(state, []), do: state

  defp replay_previous_writes(state, [{file, batch} | commands]) do
    batch
    |> Enum.reduce(state, &replay_previous_write(&2, &1, file))
    |> replay_previous_writes(commands)
  end

  defp replay_previous_write(state, :flush, file) do
    ref = flush(state.mem_table, file, Store.tmp_file(file), state.store)
    flushing = [{ref, state.mem_table} | state.flushing]
    %{state | mem_table: MemTable.new(), flushing: flushing}
  end

  defp replay_previous_write(state, {:del, files}, _file) do
    SSTables.delete(files)
    state
  end

  defp replay_previous_write(state, {:put, key, value}, _file) do
    mem_table = MemTable.upsert(state.mem_table, key, value)
    %{state | mem_table: mem_table}
  end

  defp replay_previous_write(state, {:remove, key}, _file) do
    mem_table = MemTable.delete(state.mem_table, key)
    %{state | mem_table: mem_table}
  end

  defp replay_previous_write(state, _command, _file), do: state
end
