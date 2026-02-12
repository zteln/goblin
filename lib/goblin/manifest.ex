defmodule Goblin.Manifest do
  @moduledoc false
  use GenServer

  import Goblin.Registry, only: [via: 2]

  alias __MODULE__.{
    Log,
    Snapshot
  }

  @log_file "manifest.goblin"
  @default_max_log_size 10 * 1024 * 1024

  defstruct [
    :log,
    :log_file,
    :data_dir,
    :max_log_size,
    log_size: 0,
    snapshot: %Snapshot{}
  ]

  @type state :: %__MODULE__{}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    registry = opts[:registry]
    name = opts[:name] || __MODULE__
    max_log_size = opts[:max_log_size] || @default_max_log_size

    args = [
      name: name,
      max_log_size: max_log_size,
      data_dir: opts[:data_dir]
    ]

    GenServer.start_link(__MODULE__, args, name: via(registry, name))
  end

  @doc "Updates the manifest snapshot, appending any updates to the log."
  @spec update(Goblin.server(), keyword()) :: :ok | {:error, term()}
  def update(manifest, opts) do
    updates =
      opts
      |> Keyword.take([
        :add_disk_tables,
        :remove_disk_tables,
        :add_wals,
        :remove_wals,
        :create_wal,
        :seq
      ])
      |> Enum.reduce([], fn
        {:add_disk_tables, disk_tables}, acc ->
          Enum.map(disk_tables, &{:disk_table_added, &1}) ++ acc

        {:remove_disk_tables, disk_tables}, acc ->
          Enum.map(disk_tables, &{:disk_table_removed, &1}) ++ acc

        {:add_wals, wals}, acc ->
          Enum.map(wals, &{:wal_added, &1}) ++ acc

        {:remove_wals, wals}, acc ->
          Enum.map(wals, &{:wal_removed, &1}) ++ acc

        {:create_wal, wal}, acc ->
          [{:wal_created, wal} | acc]

        {:seq, seq}, acc ->
          [{:seq, seq} | acc]
      end)
      |> Enum.map(fn
        {:seq, seq} -> {:seq, seq}
        {action, file} -> {action, trim_dir(file)}
      end)

    GenServer.call(manifest, {:update, updates})
  end

  @doc "Fetches the current manifest snapshot."
  @spec snapshot(
          Goblin.server(),
          list(:disk_tables | :wal | :wal_rotations | :seq | :count | :copy)
        ) ::
          map()
  def snapshot(manifest, keys) do
    keys = Enum.filter(keys, &(&1 in [:disk_tables, :wal, :wal_rotations, :seq, :count, :copy]))
    GenServer.call(manifest, {:snapshot, keys})
  end

  @impl GenServer
  def init(args) do
    data_dir = args[:data_dir]
    name = args[:name]

    log_file = Path.join(data_dir, @log_file)

    if File.exists?(rotation_file(log_file)),
      do: File.rename(rotation_file(log_file), log_file)

    case Log.open(name, log_file) do
      {:ok, log} ->
        %{size: log_size} = File.stat!(log_file)

        {:ok,
         %__MODULE__{
           log: log,
           log_file: log_file,
           data_dir: data_dir,
           log_size: log_size,
           max_log_size: args[:max_log_size]
         }, {:continue, :recover_snapshot}}

      {:error, reason} ->
        {:stop, reason, %__MODULE__{}}
    end
  end

  @impl GenServer
  def handle_continue(:recover_snapshot, state) do
    snapshot =
      Log.stream_log!(state.log)
      |> Enum.reduce(state.snapshot, fn update, acc ->
        Snapshot.update(acc, update)
      end)

    {:noreply, %{state | snapshot: snapshot}}
  end

  def handle_continue(:maybe_rotate, %{log_size: log_size, max_log_size: max_log_size} = state)
      when log_size > max_log_size do
    case rotate(state) do
      {:ok, state} -> {:noreply, state}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  def handle_continue(:maybe_rotate, state), do: {:noreply, state}

  @impl GenServer
  def handle_call({:update, updates}, _from, state) do
    case Log.append(state.log, updates) do
      {:ok, size} ->
        snapshot = Snapshot.update(state.snapshot, updates)

        {:reply, :ok, %{state | snapshot: snapshot, log_size: state.log_size + size},
         {:continue, :maybe_rotate}}

      {:error, reason} = error ->
        {:stop, reason, error, state}
    end
  end

  def handle_call({:snapshot, keys}, _from, state) do
    %{snapshot: snapshot, data_dir: data_dir} = state

    copy =
      if :copy in keys do
        suffix = ".#{System.unique_integer([:positive])}"

        file_copies =
          Enum.map([@log_file, snapshot.wal | snapshot.wal_rotations], fn file ->
            src = Path.join(data_dir, file)
            dst = Path.join(data_dir, "#{file}#{suffix}")
            File.cp!(src, dst)
            dst
          end)

        {suffix, file_copies}
      end

    snapshot =
      snapshot
      |> Map.put(:copy, copy)
      |> Map.take(keys)
      |> Map.replace_lazy(:wal, &(&1 && Path.join(data_dir, &1)))
      |> Map.replace_lazy(:disk_tables, fn disk_tables ->
        disk_tables
        |> MapSet.to_list()
        |> Enum.map(&Path.join(data_dir, &1))
      end)
      |> Map.replace_lazy(:wal_rotations, fn wal_rotations ->
        wal_rotations
        |> Enum.map(&Path.join(data_dir, &1))
      end)

    {:reply, snapshot, state}
  end

  @spec rotate(state()) :: {:ok, state()} | {:error, term()}
  defp rotate(state) do
    %{log: log, log_file: file, snapshot: snapshot, data_dir: data_dir} = state
    snapshot = clean_tracked(snapshot, data_dir)

    with :ok <- Log.close(log),
         :ok <- File.rename(file, rotation_file(file)),
         {:ok, log} <- Log.open(log, file),
         {:ok, size} <- Log.append(log, {:snapshot, snapshot}),
         :ok <- File.rm(rotation_file(file)) do
      %{size: log_size} = File.stat!(file)
      {:ok, %{state | log_size: log_size + size, snapshot: snapshot}}
    end
  end

  @spec clean_tracked(Snapshot.t(), Path.t()) :: Snapshot.t()
  defp clean_tracked(snapshot, data_dir) do
    tracked = Enum.filter(snapshot.tracked, &File.exists?(Path.join(data_dir, &1)))
    %{snapshot | tracked: tracked}
  end

  @spec rotation_file(Path.t()) :: Path.t()
  defp rotation_file(file), do: "#{file}.rotation"

  @spec trim_dir(Path.t()) :: Path.t()
  defp trim_dir(name), do: Path.basename(name)
end
