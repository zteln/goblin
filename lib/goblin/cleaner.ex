defmodule Goblin.Cleaner do
  @moduledoc false
  use GenServer
  alias Goblin.DiskTables

  defstruct [
    :table,
    :disk_tables_server,
    clean_buffer: []
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name]

    args = [
      table_name: opts[:local_name],
      disk_tables_server: opts[:disk_tables_server]
    ]

    GenServer.start_link(__MODULE__, args, name: name)
  end

  @spec clean(Goblin.server(), [Path.t()], boolean()) :: :ok | {:error, term()}
  def clean(server, files, disk_table? \\ false) do
    GenServer.call(server, {:clean, files, disk_table?})
  end

  @spec inc(Goblin.table()) :: :ok
  def inc(ets) do
    case :ets.lookup(ets, :transactions) do
      [] -> :ets.insert(ets, {:transactions, 1})
      [{_, n}] -> :ets.insert(ets, {:transactions, n + 1})
    end

    :ok
  end

  @spec deinc(Goblin.table(), Goblin.server()) :: :ok
  def deinc(ets, server) do
    case :ets.lookup(ets, :transactions) do
      [{_, 1}] ->
        :ets.delete(ets, :transactions)
        GenServer.cast(server, :clean_up)

      [{_, n}] ->
        :ets.insert(ets, {:transactions, n - 1})
    end

    :ok
  end

  @impl GenServer
  def init(args) do
    table = :ets.new(args[:table_name], [:named_table, :public])

    {:ok,
     %__MODULE__{
       table: table,
       disk_tables_server: args[:disk_tables_server]
     }}
  end

  @impl GenServer
  def handle_call({:clean, files, disk_table?}, _from, state) do
    clean_buffer = [{files, disk_table?} | state.clean_buffer]
    state = %{state | clean_buffer: clean_buffer}

    case handle_clean(state) do
      {:ok, state} -> {:reply, :ok, state}
      {:error, reason} = error -> {:stop, reason, error, state}
    end
  end

  @impl GenServer
  def handle_cast(:clean_up, state) do
    case handle_clean(state) do
      {:ok, state} -> {:noreply, state}
      {:error, reason} = error -> {:stop, reason, error, state}
    end
  end

  defp handle_clean(state) do
    case :ets.lookup(state.table, :transactions) do
      [] ->
        with :ok <- clean_buffer(state.clean_buffer, state.disk_tables_server) do
          {:ok, %{state | clean_buffer: []}}
        end

      _ ->
        {:ok, state}
    end
  end

  defp clean_buffer([], _disk_tables_server), do: :ok

  defp clean_buffer([{files, disk_table?} | buffer], disk_tables_server) do
    with :ok <- clean_files(files, disk_table?, disk_tables_server) do
      clean_buffer(buffer, disk_tables_server)
    end
  end

  defp clean_files([], _disk_table?, _disk_tables_server), do: :ok

  defp clean_files([file | files], disk_table?, disk_tables_server) do
    with :ok <- File.rm(file) do
      if disk_table?, do: DiskTables.remove(disk_tables_server, file)
      clean_files(files, disk_table?, disk_tables_server)
    end
  end
end
