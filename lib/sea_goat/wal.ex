defmodule SeaGoat.WAL do
  @moduledoc """
  Write-ahead log.
  Batches write operations within an interval.
  Default sync interval: 200.
  """
  use GenServer

  @default_sync_interval 200
  @wal_name :sea_goat_wal
  @tmp_wal_name :tmp_sea_goat_wal
  @wal_file "seagoat.wal"
  @tmp_wal_file ".tmp.wal"

  defstruct [
    :file,
    :log,
    :sync_interval,
    :last_sync,
    batch: [],
    archived_logs: [],
    waiting_for_sync?: false,
    seq: 0
  ]

  @spec start_link(opts :: keyword()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, Keyword.take(opts, [:dir, :sync_interval]),
      name: opts[:name]
    )
  end

  @spec append(wal :: GenServer.server(), term :: term()) :: :ok
  def append(wal, term) do
    GenServer.call(wal, {:append, term})
  end

  @spec rotate(wal :: GenServer.server()) :: :ok
  def rotate(wal) do
    GenServer.call(wal, :rotate)
  end

  @spec drop_archived(wal :: GenServer.server(), ref :: reference()) :: :ok
  def drop_archived(wal, ref) do
    GenServer.cast(wal, {:drop_archived, ref})
  end

  @spec replay(wal :: GenServer.server()) :: [term()]
  def replay(wal) do
    GenServer.call(wal, :replay)
  end

  @impl GenServer
  def init(opts) do
    file = Path.join(opts[:dir], @wal_file)

    case open_log(file, @wal_name) do
      {:ok, log} ->
        {:ok,
         %__MODULE__{
           log: log,
           file: file,
           sync_interval: opts[:sync_interval] || @default_sync_interval,
           archived_logs: archived_logs(opts[:dir]),
           last_sync: now()
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:append, term}, _from, state) do
    {:reply, :ok,
     %{
       state
       | batch: [{state.seq, term} | state.batch],
         seq: state.seq + 1
     }, {:continue, :sync}}
  end

  def handle_call(:rotate, _from, state) do
    dir = Path.dirname(state.file)
    tmp_file = Path.join(dir, @tmp_wal_file <> ".#{length(state.archived_logs)}")

    with :ok <- append_and_sync_log(state.log, Enum.reverse(state.batch)),
         :ok <- close_log(state.log),
         :ok <- rename_log(state.file, tmp_file),
         {:ok, log} <- open_log(state.file, @wal_name) do
      ref = make_ref()

      {:reply, ref,
       %{
         state
         | log: log,
           seq: 0,
           batch: [],
           archived_logs: [{tmp_file, ref} | state.archived_logs],
           last_sync: now()
       }}
    else
      {:error, reason} ->
        {:stop, reason}
    end
  end

  def handle_call(:replay, _from, state) do
    reply =
      []
      |> collect_archived_logs(state.archived_logs)
      |> collect_current_logs(state.log)
      |> Enum.reverse()

    {:reply, reply, state}
  end

  @impl GenServer
  def handle_cast({:drop_archived, ref}, state) do
    archived_logs =
      Enum.reject(state.archived_logs, fn
        {archived_log, ^ref} ->
          rm_log(archived_log)
          true

        _ ->
          false
      end)

    {:noreply, %{state | archived_logs: archived_logs}}
    # {:reply, :ok, %{state | archived_logs: archived_logs}}
  end

  @impl GenServer
  def handle_continue(:sync, state) do
    now = now()

    if now - state.last_sync >= state.sync_interval do
      append_and_sync_log(state.log, Enum.reverse(state.batch))
      {:noreply, %{state | last_sync: now, batch: []}}
    else
      state = send_sync(state)
      {:noreply, state}
    end
  end

  @impl GenServer
  def handle_info(:sync, state) do
    {:noreply, %{state | waiting_for_sync?: false}, {:continue, :sync}}
  end

  defp collect_archived_logs(acc, []), do: acc

  defp collect_archived_logs(acc, [{archived_log, ref} | archived_logs]) do
    {:ok, log} = open_log(archived_log, @tmp_wal_name)

    chunk =
      log
      |> collect_logged()
      |> Enum.map(&elem(&1, 1))

    close_log(log)
    collect_archived_logs([{chunk, ref} | acc], archived_logs)
  end

  defp collect_current_logs(acc, log) do
    chunk =
      log
      |> collect_logged()
      |> Enum.map(&elem(&1, 1))

    [{chunk, make_ref()} | acc]
  end

  defp collect_logged(log, acc \\ [], continuation \\ :start) do
    case :disk_log.chunk(log, continuation) do
      :eof ->
        acc

      {continuation, chunk} ->
        collect_logged(log, acc ++ chunk, continuation)
    end
  end

  defp open_log(file, name) do
    case :disk_log.open(name: name, file: ~c"#{file}") do
      {:ok, log} -> {:ok, log}
      {:repaired, log, _recovered, _bad_bytes} -> {:ok, log}
      {:error, _reason} = e -> e
    end
  end

  defp append_and_sync_log(log, batch) do
    :disk_log.log_terms(log, batch)
    :disk_log.sync(log)
  end

  defp close_log(log), do: :disk_log.close(log)

  defp rename_log(from, to), do: File.rename(from, to)

  defp rm_log(log), do: File.rm(log)

  defp send_sync(state) do
    if state.waiting_for_sync? do
      state
    else
      Process.send_after(self(), :sync, state.sync_interval)
      %{state | waiting_for_sync?: true}
    end
  end

  defp archived_logs(dir) do
    with {:ok, files} <- File.ls(dir) do
      files
      |> Enum.filter(&String.starts_with?(&1, @tmp_wal_file))
      |> Enum.map(fn "#{@tmp_wal_file}." <> n = file -> {String.to_integer(n), file} end)
      |> List.keysort(0)
      |> Enum.reduce([], fn {_, file}, acc -> [{Path.join(dir, file), make_ref()} | acc] end)
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
end
