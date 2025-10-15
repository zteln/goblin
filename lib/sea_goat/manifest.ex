defmodule SeaGoat.Manifest do
  @moduledoc """
  The manifest is the source-of-truth of DB changes.
  This module tracks the files added and removed from the database repo.
  Other modules get the database files from the manifest on start.

  The manifest is rotated when it becomes too large (1024 * 1024 bytes).
  On rotation, it appends the accumulated version (a snapshot) to the log file.
  Once this is done, then the previous log file is deleted and it starts logging to the new file.

  By default, it logs to a file called `manifest.seagoat`.

  All edits that are appended are synced immediately.
  """
  use GenServer

  @manifest_name :sea_goat_manifest
  @manifest_file "manifest.seagoat"
  @rotated_manifest_suffix ".rot"
  @manifest_max_size 1024 * 1024

  @type manifest :: GenServer.server()
  @type version :: %{
          files: MapSet.t(SeaGoat.db_file()),
          count: non_neg_integer(),
          seq: SeaGoat.db_sequence()
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
    args =
      Keyword.take(opts, [
        :db_dir,
        :manifest_file,
        :manifest_log_name,
        :manifest_max_size
      ])

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  @doc "Logs a file added in the DB repo to the manifest."
  @spec log_file_added(manifest(), SeaGoat.db_file()) :: :ok | {:error, term()}
  def log_file_added(manifest, file) do
    GenServer.call(manifest, {:log_edit, {:file_added, file}})
  end

  @doc "Logs a file removed in the DB repo to the manifest."
  @spec log_file_removed(manifest(), SeaGoat.db_file()) :: :ok | {:error, term()}
  def log_file_removed(manifest, file) do
    GenServer.call(manifest, {:log_edit, {:file_removed, file}})
  end

  @doc "Logs a new sequence number to the manifest."
  @spec log_sequence(manifest(), SeaGoat.db_sequence()) :: :ok | {:error, term()}
  def log_sequence(manifest, seq) do
    GenServer.call(manifest, {:log_edit, {:seq, seq}})
  end

  @doc "Logs a compaction to the manifest."
  @spec log_compaction(manifest(), [SeaGoat.db_file()], [SeaGoat.db_file()]) ::
          :ok | {:error, term()}
  def log_compaction(manifest, rotated_files, new_files) do
    added_edits = Enum.map(new_files, &{:file_added, &1})
    removed_edits = Enum.map(rotated_files, &{:file_removed, &1})
    GenServer.call(manifest, {:log_edit, added_edits ++ removed_edits})
  end

  @doc "Returns a snapshot of the DB repo."
  @spec get_version(manifest(), [atom()]) :: map()
  def get_version(manifest, keys \\ []) do
    GenServer.call(manifest, {:get_version, keys})
  end

  @impl GenServer
  def init(args) do
    name = args[:manifest_name] || @manifest_name
    file = Path.join(args[:db_dir], args[:manifest_file] || @manifest_file)
    max_size = args[:manifest_max_size] || @manifest_max_size

    if File.exists?(rotated_file(file)) do
      recover_rotated_manifest(file)
    end

    case open_manifest(name, file) do
      {:ok, log} ->
        {:ok, %__MODULE__{name: name, file: file, log: log, max_size: max_size},
         {:continue, :recover_version}}

      error ->
        {:stop, error}
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
      |> then(fn version ->
        if :files in keys do
          Map.replace(version, :files, MapSet.to_list(state.version.files))
        else
          version
        end
      end)
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

  def handle_continue(:recover_version, state) do
    case fetch_version(state.log) do
      {:ok, version} ->
        {:noreply, %{state | version: version}}

      error ->
        {:stop, error, state}
    end
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

  defp apply_edit(version, {:seq, seq}) do
    %{version | seq: seq}
  end

  defp new_version, do: %{files: MapSet.new(), count: 0, seq: 0}
  defp rotated_file(file), do: file <> @rotated_manifest_suffix
end
