defmodule Goblin.Manifest do
  @moduledoc false
  use GenServer
  import Goblin.ProcessRegistry, only: [via: 1]

  @manifest_file "manifest.goblin"
  @wal_file "wal.goblin"
  @tmp_suffix ".goblin.tmp"
  @rotated_manifest_suffix ".rot"
  @manifest_max_size 1024 * 1024

  @type version :: %{
          files: [Goblin.db_file()],
          wals: [Goblin.db_file()],
          count: non_neg_integer(),
          seq: Goblin.db_sequence()
        }

  defstruct [
    :log,
    :name,
    :file,
    :max_size,
    :version,
    size: 0
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    registry = opts[:registry]

    args =
      Keyword.take(opts, [
        :db_dir,
        :name,
        :manifest_file,
        :manifest_max_size
      ])

    GenServer.start_link(__MODULE__, args, name: via(registry))
  end

  @spec log_rotation(Goblin.registry(), Goblin.db_file()) :: :ok | {:error, term()}
  def log_rotation(registry, wal) do
    GenServer.call(via(registry), {:log_edit, {:wal_added, wal}})
  end

  @spec log_flush(Goblin.registry(), [Goblin.db_file()], Goblin.db_file()) ::
          :ok | {:error, term()}
  def log_flush(registry, files, wal) do
    added_edits = Enum.map(files, &{:file_added, &1})
    removed_edit = {:wal_removed, wal}
    GenServer.call(via(registry), {:log_edit, added_edits ++ [removed_edit]})
  end

  @spec log_sequence(Goblin.registry(), Goblin.db_sequence()) :: :ok | {:error, term()}
  def log_sequence(registry, seq) do
    GenServer.call(via(registry), {:log_edit, {:seq, seq}})
  end

  @spec log_compaction(Goblin.registry(), [Goblin.db_file()], [Goblin.db_file()]) ::
          :ok | {:error, term()}
  def log_compaction(registry, rotated_files, new_files) do
    added_edits = Enum.map(new_files, &{:file_added, &1})
    removed_edits = Enum.map(rotated_files, &{:file_removed, &1})
    GenServer.call(via(registry), {:log_edit, added_edits ++ removed_edits})
  end

  @spec get_version(Goblin.registry(), [:files | :wals | :count | :seq]) :: version()
  def get_version(registry, keys \\ []) do
    GenServer.call(via(registry), {:get_version, keys})
  end

  @impl GenServer
  def init(args) do
    name = Module.concat(args[:name], Manifest)
    file = args[:manifest_file] || @manifest_file
    db_dir = args[:db_dir]
    file = Path.join(db_dir, file)
    max_size = args[:manifest_max_size] || @manifest_max_size

    if File.exists?(rotated_file(file)) do
      recover_rotated_manifest(file)
    end

    with {:ok, log} <- open_manifest(name, file),
         {:ok, version} <- fetch_version(log),
         :ok <- clean_up(db_dir, version) do
      {:ok,
       %__MODULE__{
         name: name,
         file: file,
         log: log,
         max_size: max_size,
         version: version
       }}
    else
      error -> {:stop, error}
    end
  end

  @impl GenServer
  def handle_call({:log_edit, edit}, _from, state) do
    case append_to_manifest(state.log, edit) do
      {:ok, size} ->
        version = apply_edit(state.version, edit)
        {:reply, :ok, %{state | size: state.size + size, version: version}, {:continue, :rotate}}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:get_version, keys}, _from, state) do
    keys = if keys == [], do: Map.keys(state.version), else: keys

    version =
      state.version
      |> Map.replace(:files, MapSet.to_list(state.version.files))
      |> Map.replace(:wals, MapSet.to_list(state.version.wals))
      |> Map.take(keys)

    {:reply, version, state}
  end

  @impl GenServer
  def handle_continue(:rotate, %{size: size, max_size: max_size} = state) when size >= max_size do
    case rotate(state) do
      {:ok, state} ->
        {:noreply, state}

      error ->
        {:stop, error, state}
    end
  end

  def handle_continue(:rotate, state), do: {:noreply, state}

  defp clean_up(dir, version) do
    rm_tmp_files!(dir)
    rm_orphaned_files!(dir, version)
  end

  defp rm_tmp_files!(dir) do
    dir
    |> File.ls!()
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.filter(&String.ends_with?(&1, @tmp_suffix))
    |> Enum.each(&File.rm!/1)
  end

  defp rm_orphaned_files!(dir, version) do
    %{files: files, wals: wals} = version

    dir
    |> File.ls!()
    |> Enum.reject(&(&1 == @wal_file or &1 == @manifest_file))
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.reject(&MapSet.member?(files, &1))
    |> Enum.reject(&MapSet.member?(wals, &1))
    |> Enum.each(&File.rm!/1)
  end

  defp rotate(state) do
    with :ok <- close_manifest(state.log),
         :ok <- File.rename(state.file, rotated_file(state.file)),
         {:ok, log} <- open_manifest(state.name, state.file),
         {:ok, size} <- append_to_manifest(log, {:snapshot, state.version}),
         :ok <- File.rm(rotated_file(state.file)) do
      {:ok, %{state | log: log, size: size}}
    end
  end

  defp open_manifest(name, file) do
    opts = [name: name, file: ~c"#{file}"]

    case :disk_log.open(opts) do
      {:ok, log} -> {:ok, log}
      {:repaired, log, _recovered, _bad_bytes} -> {:ok, log}
      error -> error
    end
  end

  defp append_to_manifest(log, edits) when is_list(edits) do
    edits = Enum.map(edits, &:erlang.term_to_binary/1)

    with :ok <- :disk_log.blog_terms(log, edits),
         :ok <- :disk_log.sync(log) do
      {:ok, Enum.reduce(edits, 0, &(&2 + byte_size(&1)))}
    end
  end

  defp append_to_manifest(log, edit), do: append_to_manifest(log, [edit])

  defp close_manifest(log) do
    :disk_log.close(log)
  end

  defp recover_rotated_manifest(file) do
    File.rename(rotated_file(file), file)
  end

  defp fetch_version(log) do
    case :disk_log.chunk(log, :start, 1) do
      {:error, _reason} = error ->
        error

      :eof ->
        version = new_version()
        append_to_manifest(log, {:snapshot, version})
        {:ok, version}

      {continuation, [{:snapshot, version}]} ->
        update_version(log, continuation, version)
    end
  end

  defp update_version(log, continuation, version) do
    case :disk_log.chunk(log, continuation, 1) do
      :eof ->
        {:ok, version}

      {continuation, [edit]} ->
        version = apply_edit(version, edit)
        update_version(log, continuation, version)
    end
  end

  defp apply_edit(version, []), do: version

  defp apply_edit(version, [edit | edits]) do
    version
    |> apply_edit(edit)
    |> apply_edit(edits)
  end

  defp apply_edit(version, {:file_added, file}) do
    files = MapSet.put(version.files, file)
    %{version | files: files, count: version.count + 1}
  end

  defp apply_edit(version, {:file_removed, file}) do
    files = MapSet.delete(version.files, file)
    %{version | files: files}
  end

  defp apply_edit(version, {:wal_added, wal}) do
    wals = MapSet.put(version.wals, wal)
    %{version | wals: wals}
  end

  defp apply_edit(version, {:wal_removed, wal}) do
    wals = MapSet.delete(version.wals, wal)
    %{version | wals: wals}
  end

  defp apply_edit(version, {:seq, seq}) do
    %{version | seq: seq}
  end

  defp new_version, do: %{files: MapSet.new(), wals: MapSet.new(), count: 0, seq: 0}
  defp rotated_file(file), do: file <> @rotated_manifest_suffix
end
