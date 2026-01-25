defmodule Goblin.Manifest do
  @moduledoc false
  use GenServer

  @manifest_file "goblin.manifest"
  @manifest_max_size 1024 * 1024

  @type snapshot :: %{
          disk_tables: [Path.t()],
          wal_rotations: [Path.t()],
          wal: Path.t() | nil,
          count: non_neg_integer(),
          seq: Goblin.seq_no(),
          manifest: {String.t(), Path.t()},
          trash: [Path.t()]
        }

  defstruct [
    :name,
    :file,
    :dir,
    :max_size,
    :snapshot,
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
         {:ok, snapshot} <- fetch_snapshot(name),
         :ok <- clean_up(db_dir, clean_snapshot_trash(snapshot, db_dir)) do
      {:ok,
       %__MODULE__{
         name: name,
         file: file,
         dir: db_dir,
         max_size: max_size,
         snapshot: snapshot,
         size: Map.get(File.stat!(file), :size)
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl GenServer
  def handle_call({:log_edit, edit}, _from, state) do
    case append_to_manifest(state.name, edit) do
      {:ok, size} ->
        snapshot = apply_edit(state.snapshot, edit)
        state = %{state | size: state.size + size, snapshot: snapshot}
        {:reply, :ok, state, {:continue, :maybe_rotate}}

      {:error, reason} = error ->
        {:stop, reason, error, state}
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

    snapshot =
      state.snapshot
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

    {:reply, snapshot, state}
  end

  @impl GenServer
  def handle_continue(:maybe_rotate, %{size: size, max_size: max_size} = state)
      when size >= max_size do
    case rotate(state) do
      {:ok, state} -> {:noreply, state}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  def handle_continue(:maybe_rotate, state), do: {:noreply, state}

  defp clean_up(dir, snapshot) do
    tracked_files =
      List.flatten([
        snapshot.wal,
        MapSet.to_list(snapshot.disk_tables),
        snapshot.wal_rotations
      ])

    File.ls!(dir)
    |> Enum.reject(&(&1 == @manifest_file))
    |> Enum.reject(&(&1 in tracked_files))
    |> Enum.filter(&(&1 in snapshot.trash))
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.each(&File.rm!/1)
  end

  defp rotate(state) do
    with :ok <- close_manifest(state.name),
         :ok <- File.rename(state.file, rotated_file(state.file)),
         :ok <- open_manifest(state.name, state.file),
         snapshot <- clean_snapshot_trash(state.snapshot, state.dir),
         {:ok, size} <- append_to_manifest(state.name, {:snapshot, snapshot}),
         :ok <- File.rm(rotated_file(state.file)) do
      size = Map.get(File.stat!(state.file), :size) + size
      {:ok, %{state | size: size, snapshot: snapshot}}
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

  defp fetch_snapshot(name) do
    case :disk_log.chunk(name, :start, 1) do
      {:error, _reason} = error ->
        error

      :eof ->
        snapshot = new_snapshot()

        with {:ok, _size} <- append_to_manifest(name, {:snapshot, snapshot}) do
          {:ok, snapshot}
        end

      {continuation, [{:snapshot, snapshot}]} ->
        update_snapshot(name, continuation, snapshot)
    end
  end

  defp update_snapshot(name, continuation, snapshot) do
    case :disk_log.chunk(name, continuation, 1) do
      :eof ->
        {:ok, snapshot}

      {continuation, [edit]} ->
        snapshot = apply_edit(snapshot, edit)
        update_snapshot(name, continuation, snapshot)
    end
  end

  defp clean_snapshot_trash(snapshot, dir) do
    trash = Enum.filter(snapshot.trash, &File.exists?(Path.join(dir, &1)))
    %{snapshot | trash: trash}
  end

  defp apply_edit(snapshot, []), do: snapshot

  defp apply_edit(snapshot, [edit | edits]) do
    snapshot
    |> apply_edit(edit)
    |> apply_edit(edits)
  end

  defp apply_edit(snapshot, {:disk_table_added, disk_table}) do
    disk_tables = MapSet.put(snapshot.disk_tables, disk_table)
    %{snapshot | disk_tables: disk_tables, count: snapshot.count + 1}
  end

  defp apply_edit(snapshot, {:disk_table_removed, disk_table}) do
    disk_tables = MapSet.delete(snapshot.disk_tables, disk_table)
    trash = [disk_table | snapshot.trash]
    %{snapshot | disk_tables: disk_tables, trash: trash}
  end

  defp apply_edit(snapshot, {:wal_added, wal}) do
    wal_rotations = [wal | snapshot.wal_rotations]
    %{snapshot | wal_rotations: wal_rotations}
  end

  defp apply_edit(snapshot, {:wal_removed, wal}) do
    wal_rotations = Enum.reject(snapshot.wal_rotations, &(&1 == wal))
    trash = [wal | snapshot.trash]
    %{snapshot | wal_rotations: wal_rotations, trash: trash}
  end

  defp apply_edit(snapshot, {:current_wal, wal}) do
    %{snapshot | wal: wal}
  end

  defp apply_edit(snapshot, {:seq, seq}) do
    %{snapshot | seq: seq}
  end

  defp new_snapshot,
    do: %{
      disk_tables: MapSet.new(),
      wal_rotations: [],
      wal: nil,
      count: 0,
      seq: 0,
      trash: []
    }

  defp rotated_file(file), do: "#{file}.0"

  defp trim_dir(name), do: Path.basename(name)
end
