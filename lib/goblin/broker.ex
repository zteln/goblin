defmodule Goblin.Broker do
  @moduledoc false
  use GenServer
  alias Goblin.Broker.{ReadTx, WriteTx}
  alias Goblin.MemTable
  alias Goblin.Cleaner
  alias Goblin.PubSub

  defstruct [
    :writer,
    :mem_table_server,
    :mem_table,
    :disk_tables,
    :cleaner_table,
    :cleaner_server,
    :pub_sub,
    write_queue: :queue.new()
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name]

    args = [
      mem_table_server: opts[:mem_table_server],
      mem_table: opts[:mem_table],
      disk_tables: opts[:disk_tables],
      cleaner_table: opts[:cleaner_table],
      cleaner_server: opts[:cleaner_server],
      pub_sub: opts[:pub_sub]
    ]

    GenServer.start_link(__MODULE__, args, name: name)
  end

  @spec write_transaction(Goblin.server(), (Goblin.Tx.t() -> Goblin.Tx.return())) ::
          any() | {:error, :aborted}
  def write_transaction(server, f) do
    with {:ok, tx} <- start_transaction(server),
         {:ok, tx, reply} <- run_transaction(server, tx, f),
         :ok <- commit_transaction(server, tx) do
      reply
    end
  end

  @spec read_transaction(
          Goblin.Cleaner.table(),
          Goblin.server(),
          Goblin.MemTable.Store.t(),
          Goblin.DiskTables.Store.t(),
          (Goblin.Tx.t() -> any())
        ) :: any()
  def read_transaction(cleaner_table, cleaner_server, mem_table, disk_tables, f) do
    Cleaner.inc(cleaner_table)
    tx = ReadTx.new(mem_table, disk_tables)
    f.(tx)
  after
    Cleaner.deinc(cleaner_table, cleaner_server)
  end

  @impl GenServer
  def init(args) do
    {:ok,
     %__MODULE__{
       mem_table_server: args[:mem_table_server],
       mem_table: args[:mem_table],
       disk_tables: args[:disk_tables],
       cleaner_table: args[:cleaner_table],
       cleaner_server: args[:cleaner_server],
       pub_sub: args[:pub_sub]
     }}
  end

  @impl GenServer
  def handle_call(:start_transaction, {pid, _ref}, %{writer: nil} = state) do
    monitor_ref = Process.monitor(pid)
    Cleaner.inc(state.cleaner_table)
    tx = WriteTx.new(state.mem_table, state.disk_tables)
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
    Cleaner.deinc(state.cleaner_table, state.cleaner_server)
    Process.demonitor(monitor_ref)

    %{writes: writes, seq: seq} = tx
    %{pub_sub: pub_sub, mem_table_server: mem_table_server} = state

    case MemTable.insert(mem_table_server, writes, seq) do
      :ok ->
        publish_commit(pub_sub, writes)
        {:reply, :ok, %{state | writer: nil}, {:continue, :dequeue_writer}}

      {:error, reason} = error ->
        {:stop, reason, error, state}
    end
  end

  def handle_call({:commit_transaction, _tx}, _from, state) do
    {:reply, {:error, :not_writer}, state}
  end

  def handle_call(:abort_transaction, {pid, _ref}, %{writer: {pid, _monitor_ref}} = state) do
    Cleaner.deinc(state.cleaner_table, state.cleaner_server)
    {:reply, :ok, %{state | writer: nil}, {:continue, :dequeue_writer}}
  end

  def handle_call(:abort_transaction, _from, state) do
    {:reply, {:error, :not_writer}, state}
  end

  @impl GenServer
  def handle_continue(:dequeue_writer, state) do
    case :queue.out(state.write_queue) do
      {:empty, _write_queue} ->
        {:noreply, state}

      {{:value, {pid, _ref} = from}, write_queue} ->
        monitor_ref = Process.monitor(pid)
        Cleaner.inc(state.cleaner_table)
        tx = WriteTx.new(state.mem_table, state.disk_tables)
        GenServer.reply(from, {:ok, tx})
        {:noreply, %{state | write_queue: write_queue, writer: {pid, monitor_ref}}}
    end
  end

  @impl GenServer
  def handle_info({:DOWN, monitor_ref, _, pid, _}, %{writer: {pid, monitor_ref}} = state) do
    {:noreply, %{state | writer: nil}, {:continue, :dequeue_writer}}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp start_transaction(broker) do
    GenServer.call(broker, :start_transaction)
  end

  defp run_transaction(broker, tx, f) do
    case f.(tx) do
      {:commit, tx, reply} ->
        {:ok, %{tx | writes: Enum.reverse(tx.writes)}, reply}

      :abort ->
        abort_transaction(broker)
        {:error, :aborted}

      _ ->
        abort_transaction(broker)
        raise "Invalid return from transaction"
    end
  end

  defp commit_transaction(broker, tx) do
    GenServer.call(broker, {:commit_transaction, tx})
  end

  defp abort_transaction(broker) do
    GenServer.call(broker, :abort_transaction)
  end

  defp publish_commit(pub_sub, writes) do
    Task.async(fn ->
      writes =
        writes
        |> Enum.map(&Tuple.delete_at(&1, 1))

      PubSub.publish(pub_sub, writes)
    end)
  end
end
