defmodule Goblin.Manifest do
  @moduledoc false
  use GenServer

  @manifest_file "manifest.goblin"
  @manifest_max_size 1024 * 1024

  @type snapshot :: %{
          disk_tables: [Path.t()],
          wal_rotations: [Path.t()],
          wal: Path.t() | nil,
          count: non_neg_integer(),
          seq: Goblin.seq_no(),
          manifest: {String.t(), Path.t()}
        }

  defstruct [
    :name,
    :file,
    :dir,
    :max_size,
    :version,
    :waiting,
    size: 0
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name]

    args = [
      local_name: opts[:local_name] || name,
      db_dir: opts[:db_dir],
      manifest_max_size: opts[:manifest_max_size]
    ]

    GenServer.start_link(__MODULE__, args, name: name)
  end

  @spec log_wal(Goblin.server(), Path.t()) :: :ok | {:error, term()}
  def log_wal(manifest, wal) do
    GenServer.call(manifest, {:log_edit, {:current_wal, trim_dir(wal)}})
  end

  @spec log_rotation(Goblin.server(), Path.t(), Path.t()) ::
          :ok | {:error, term()}
  def log_rotation(manifest, wal_rotation, wal_current) do
    edits = [{:wal_added, trim_dir(wal_rotation)}, {:current_wal, trim_dir(wal_current)}]
    GenServer.call(manifest, {:log_edit, edits})
  end

  @spec log_flush(Goblin.server(), [Path.t()], Path.t()) ::
          :ok | {:error, term()}
  def log_flush(manifest, disk_tables, wal) do
    added_edits = Enum.map(disk_tables, &{:disk_table_added, trim_dir(&1)})
    removed_edit = {:wal_removed, trim_dir(wal)}
    GenServer.call(manifest, {:log_edit, [removed_edit | added_edits]})
  end

  @spec log_sequence(Goblin.server(), Goblin.seq_no()) :: :ok | {:error, term()}
  def log_sequence(manifest, seq) do
    GenServer.call(manifest, {:log_edit, {:seq, seq}})
  end

  @spec log_compaction(Goblin.server(), [Path.t()], [Path.t()]) ::
          :ok | {:error, term()}
  def log_compaction(manifest, removed_disk_tables, added_disk_tables) do
    removed_edits = Enum.map(removed_disk_tables, &{:disk_table_removed, trim_dir(&1)})
    added_edits = Enum.map(added_disk_tables, &{:disk_table_added, trim_dir(&1)})
    GenServer.call(manifest, {:log_edit, added_edits ++ removed_edits})
  end

  @spec snapshot(Goblin.server(), [
          :disk_tables | :wal_rotations | :wal | :count | :seq | :manifest
        ]) ::
          snapshot()
  def snapshot(manifest, keys \\ []) do
    GenServer.call(manifest, {:snapshot, keys})
  end

  @impl GenServer
  def init(args) do
    name = args[:local_name]
    db_dir = args[:db_dir]
    file = Path.join(db_dir, @manifest_file)
    max_size = args[:manifest_max_size] || @manifest_max_size

    if File.exists?(rotated_file(file)) do
      recover_rotated_manifest(file)
    end

    with :ok <- open_manifest(name, file),
         {:ok, version} <- fetch_version(name),
         :ok <- clean_up(db_dir, version) do
      {:ok,
       %__MODULE__{
         name: name,
         file: file,
         dir: db_dir,
         max_size: max_size,
         version: version
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:log_edit, edit}, _from, state) do
    case append_to_manifest(state.name, edit) do
      {:ok, size} ->
        version = apply_edit(state.version, edit)
        {:reply, :ok, %{state | size: state.size + size, version: version}, {:continue, :rotate}}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:snapshot, keys}, _from, state) do
    %{dir: dir} = state

    manifest_copy =
      if :manifest in keys do
        now = DateTime.utc_now(:second) |> DateTime.to_iso8601(:basic)
        suffix = "#{now}.copy"
        manifest_copy = "#{state.file}.#{suffix}"
        File.cp!(state.file, manifest_copy)
        {suffix, manifest_copy}
      end

    version =
      state.version
      |> Map.put(:manifest, manifest_copy)
      |> Map.take(keys)
      |> Map.replace_lazy(:wal, &(&1 && Path.join(dir, &1)))
      |> Map.replace_lazy(:disk_tables, fn disk_tables ->
        disk_tables
        |> MapSet.to_list()
        |> Enum.map(&Path.join(dir, &1))
      end)
      |> Map.replace_lazy(:wal_rotations, fn wal_rotations ->
        wal_rotations
        |> Enum.map(&Path.join(dir, &1))
      end)

    {:reply, version, state}
  end

  @impl GenServer
  def handle_continue(:rotate, %{size: size, max_size: max_size} = state) when size >= max_size do
    case rotate(state) do
      {:ok, state} -> {:noreply, state}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  def handle_continue(:rotate, state), do: {:noreply, state}

  defp clean_up(dir, version) do
    %{disk_tables: disk_tables, wal_rotations: wal_rotations, wal: wal} = version

    tracked_files = [wal | MapSet.to_list(disk_tables) ++ wal_rotations]

    File.ls!(dir)
    |> Enum.reject(&(&1 == @manifest_file))
    |> Enum.reject(&(&1 in tracked_files))
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.each(&File.rm!/1)
  end

  defp rotate(state) do
    with :ok <- close_manifest(state.name),
         :ok <- File.rename(state.file, rotated_file(state.file)),
         :ok <- open_manifest(state.name, state.file),
         {:ok, size} <- append_to_manifest(state.name, {:snapshot, state.version}),
         :ok <- File.rm(rotated_file(state.file)) do
      {:ok, %{state | size: size}}
    end
  end

  defp open_manifest(name, file) do
    opts = [name: name, file: ~c"#{file}"]

    case :disk_log.open(opts) do
      {:ok, _log} -> :ok
      {:repaired, _log, _recovered, _bad_bytes} -> :ok
      error -> error
    end
  end

  defp append_to_manifest(name, edits) when is_list(edits) do
    with :ok <- :disk_log.log_terms(name, edits),
         :ok <- :disk_log.sync(name) do
      {:ok, Enum.reduce(edits, 0, &(&2 + :erlang.external_size(&1)))}
    end
  end

  defp append_to_manifest(name, edit), do: append_to_manifest(name, [edit])

  defp close_manifest(name) do
    :disk_log.close(name)
  end

  defp recover_rotated_manifest(file) do
    File.rename(rotated_file(file), file)
  end

  defp fetch_version(name) do
    case :disk_log.chunk(name, :start, 1) do
      {:error, _reason} = error ->
        error

      :eof ->
        version = new_version()
        append_to_manifest(name, {:snapshot, version})
        {:ok, version}

      {continuation, [{:snapshot, version}]} ->
        update_version(name, continuation, version)
    end
  end

  defp update_version(name, continuation, version) do
    case :disk_log.chunk(name, continuation, 1) do
      :eof ->
        {:ok, version}

      {continuation, [edit]} ->
        version = apply_edit(version, edit)
        update_version(name, continuation, version)
    end
  end

  defp apply_edit(version, []), do: version

  defp apply_edit(version, [edit | edits]) do
    version
    |> apply_edit(edit)
    |> apply_edit(edits)
  end

  defp apply_edit(version, {:disk_table_added, disk_table}) do
    disk_tables = MapSet.put(version.disk_tables, disk_table)
    %{version | disk_tables: disk_tables, count: version.count + 1}
  end

  defp apply_edit(version, {:disk_table_removed, disk_table}) do
    disk_tables = MapSet.delete(version.disk_tables, disk_table)
    %{version | disk_tables: disk_tables}
  end

  defp apply_edit(version, {:wal_added, wal}) do
    wal_rotations = [wal | version.wal_rotations]
    %{version | wal_rotations: wal_rotations}
  end

  defp apply_edit(version, {:wal_removed, wal}) do
    wal_rotations = Enum.reject(version.wal_rotations, &(&1 == wal))
    %{version | wal_rotations: wal_rotations}
  end

  defp apply_edit(version, {:current_wal, wal}) do
    %{version | wal: wal}
  end

  defp apply_edit(version, {:seq, seq}) do
    %{version | seq: seq}
  end

  defp new_version,
    do: %{
      disk_tables: MapSet.new(),
      wal_rotations: [],
      wal: nil,
      count: 0,
      seq: 0
    }

  defp rotated_file(file), do: "#{file}.0"

  defp trim_dir(name), do: Path.basename(name)
end
