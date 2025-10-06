defmodule SeaGoat.WAL do
  @moduledoc """
  Write-ahead log.
  Batches write operations within an interval.
  Default sync interval: 200.
  """
  use GenServer

  @default_sync_interval 200
  @wal_name :sea_goat_wal
  @wal_ro_name :sea_goat_wal_ro
  @wal_file "wal.seagoat"
  @old_wal_file_suffix ".old"

  defstruct [
    :log,
    :sync_interval,
    :last_sync,
    :file,
    :name,
    :old_file,
    watermarks: %{},
    batch: [],
    waiting_for_sync?: false
  ]

  def start_link(opts) do
    args = Keyword.take(opts, [:sync_interval, :wal_name, :wal_file, :dir])
    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  def sync(wal) do
    GenServer.call(wal, :sync_now)
  end

  def append(wal, batch) do
    GenServer.call(wal, {:append, batch})
  end

  def rotate(wal) do
    GenServer.call(wal, :rotate)
  end

  def clean(wal) do
    GenServer.call(wal, :clean)
  end

  def recover(wal) do
    GenServer.call(wal, :recover)
  end

  @impl GenServer
  def init(args) do
    name = args[:wal_name] || @wal_name
    file = Path.join(args[:dir], args[:wal_file] || @wal_file)

    case open_log(file, name) do
      {:ok, log} ->
        {:ok,
         %__MODULE__{
           name: name,
           file: file,
           log: log,
           sync_interval: args[:sync_interval] || @default_sync_interval,
           last_sync: now()
         }}

      {:error, _} = error ->
        {:stop, error}
    end
  end

  @impl GenServer
  def handle_call(:sync_now, _from, state) do
    append_and_sync_log(state.log, Enum.reverse(state.batch))
    {:reply, :ok, %{state | last_sync: now(), batch: []}}
  end

  def handle_call({:append, batch}, _from, state) do
    batch = batch ++ state.batch
    state = %{state | batch: batch}
    {:reply, :ok, state, {:continue, :sync}}
  end

  def handle_call(:rotate, _from, state) do
    case rotate_log(state.name, state.file, state.log, state.batch) do
      {:ok, log, old_file} ->
        state = %{state | log: log, old_file: old_file, batch: [], last_sync: now()}
        {:reply, :ok, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(:clean, _from, state) do
    case rm_log(state.old_file) do
      :ok ->
        state = %{state | old_file: nil}
        {:reply, :ok, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(:recover, _from, state) do
    {:reply, recover_writes(state.log, state.file), state}
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

  defp rotate_log(name, file, log, batch) do
    with :ok <- append_and_sync_log(log, Enum.reverse(batch)),
         :ok <- close_log(log),
         {:ok, old_file} <- mv_log(file),
         {:ok, log} <- open_log(file, name) do
      {:ok, log, old_file}
    end
  end

  defp open_log(file, name, opts \\ []) do
    opts = [name: name, file: ~c"#{file}"] |> Keyword.merge(opts)

    case :disk_log.open(opts) do
      {:ok, log} -> {:ok, log}
      {:repaired, log, _recovered, _bad_bytes} -> {:ok, log}
      {:error, _reason} = e -> e
    end
  end

  defp close_log(log), do: :disk_log.close(log)

  defp mv_log(file) do
    old_file = old_file(file)

    with :ok <- File.rename(file, old_file) do
      {:ok, old_file}
    end
  end

  defp rm_log(file) do
    File.rm(file)
  end

  defp append_and_sync_log(log, batch) do
    :disk_log.log_terms(log, batch)
    :disk_log.sync(log)
  end

  defp send_sync(state) do
    if state.waiting_for_sync? do
      state
    else
      Process.send_after(self(), :sync, state.sync_interval)
      %{state | waiting_for_sync?: true}
    end
  end

  defp recover_writes(log, file) do
    with {:ok, old_writes} <- recover_old_writes(file),
         {:ok, writes} <- recover_logs(log) do
      {:ok, old_writes ++ writes}
    end
  end

  defp recover_old_writes(file) do
    if File.exists?(old_file(file)) do
      with {:ok, log} <- open_log(old_file(file), @wal_ro_name, mode: :read_only),
           {:ok, writes} <- recover_logs(log),
           :ok <- close_log(log) do
        {:ok, writes}
      end
    else
      {:ok, []}
    end
  end

  defp recover_logs(log, continuation \\ :start, acc \\ []) do
    case :disk_log.chunk(log, continuation) do
      :eof ->
        {:ok, acc}

      {continuation, chunk} ->
        acc = acc ++ chunk
        recover_logs(log, continuation, acc)

      _ ->
        {:error, :failed_to_recover}
    end
  end

  defp now, do: System.monotonic_time(:millisecond)
  defp old_file(file), do: file <> @old_wal_file_suffix
end
