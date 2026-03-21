defmodule Goblin.Manifest do
  @moduledoc false

  alias Goblin.Log
  alias Goblin.Manifest.Snapshot

  @log_key :manifest
  @log_file "manifest.goblin"
  @default_max_log_size 10 * 1024 * 1024

  defstruct [
    :log,
    :log_file,
    :data_dir,
    :snapshot,
    log_size: 0
  ]

  @type t :: %__MODULE__{}

  @doc "Opens the manifest log, recovering state from disk."
  @spec open(atom(), Path.t()) :: {:ok, t()} | {:error, term()}
  def open(name, data_dir) do
    log_file = Path.join(data_dir, @log_file)

    if File.exists?(rotation_file(log_file)),
      do: File.rename(rotation_file(log_file), log_file)

    with {:ok, log} <- Log.open({name, @log_key}, log_file) do
      %{size: log_size} = File.stat!(log_file)
      snapshot = recover_snapshot(log)

      {:ok,
       %__MODULE__{
         log: log,
         log_file: log_file,
         data_dir: data_dir,
         log_size: log_size,
         snapshot: snapshot
       }}
    end
  end

  @doc "Closes the manifest log."
  @spec close(t()) :: :ok | {:error, term()}
  def close(manifest), do: Log.close(manifest.log)

  @doc "Returns whether the manifest log has exceeded its size limit."
  @spec rotate?(t()) :: boolean()
  def rotate?(manifest) do
    manifest.log_size >= @default_max_log_size
  end

  @doc "Compacts the manifest log by rewriting it as a single snapshot."
  @spec rotate(t()) :: {:ok, t()} | {:error, term()}
  def rotate(manifest) do
    %{
      log: log,
      log_file: log_file,
      snapshot: snapshot
    } = manifest

    with :ok <- Log.close(log),
         :ok <- File.rename(log_file, rotation_file(log_file)),
         {:ok, log} <- Log.open(log, log_file),
         {:ok, size} <- Log.append(log, {:snapshot, snapshot}),
         :ok <- File.rm(rotation_file(log_file)) do
      %{size: log_size} = File.stat!(log_file)
      {:ok, %{manifest | log_size: log_size + size, snapshot: snapshot}}
    end
  end

  @doc "Appends actions to the manifest log and updates the in-memory snapshot."
  @spec update(t(), list(any())) :: {:ok, t()} | {:error, term()}
  def update(manifest, actions) do
    updates =
      actions
      |> Enum.flat_map(fn
        {:activate_disk_tables, disk_tables} ->
          Enum.map(disk_tables, &{:disk_table_activated, &1})

        {:remove_disk_tables, disk_tables} ->
          Enum.map(disk_tables, &{:disk_table_removed, &1})

        {:retire_wals, wals} ->
          Enum.map(wals, &{:wal_retired, &1})

        {:remove_wals, wals} ->
          Enum.map(wals, &{:wal_removed, &1})

        {:activate_wal, wal} ->
          [{:wal_activated, wal}]

        {:set_seq, seq} ->
          [{:seq_set, seq}]

        :sweep ->
          [:sweeped]

        _ ->
          []
      end)
      |> Enum.map(fn
        {:seq_set, seq} -> {:seq_set, seq}
        :sweeped -> :sweeped
        {update, file} -> {update, trim_dir(file)}
      end)

    with {:ok, size} <- Log.append(manifest.log, updates) do
      snapshot = Snapshot.update(manifest.snapshot, updates)
      {:ok, %{manifest | snapshot: snapshot, log_size: manifest.log_size + size}}
    end
  end

  @doc "Returns selected fields from the manifest snapshot with resolved paths."
  @spec snapshot(t(), list(atom())) :: map()
  def snapshot(manifest, keys) do
    manifest.snapshot
    |> Map.take(keys)
    |> Map.replace_lazy(:active_wal, &(&1 && Path.join(manifest.data_dir, &1)))
    |> Map.replace_lazy(:active_disk_tables, fn disk_tables ->
      Enum.map(disk_tables, &Path.join(manifest.data_dir, &1))
    end)
    |> Map.replace_lazy(:retired_wals, fn retired_wals ->
      retired_wals
      |> Enum.map(&Path.join(manifest.data_dir, &1))
      |> Enum.reverse()
    end)
    |> Map.replace_lazy(:dirt, fn dirt ->
      Enum.map(dirt, &Path.join(manifest.data_dir, &1))
    end)
  end

  defp recover_snapshot(log) do
    log
    |> Log.stream_log!()
    |> Enum.reduce(%Snapshot{}, &Snapshot.update(&2, &1))
  end

  defp rotation_file(file), do: "#{file}.rotation"
  defp trim_dir(name), do: Path.basename(name)
end
