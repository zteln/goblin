defmodule SeaGoat.Store.Recovery do
  alias SeaGoat.SSTables
  alias SeaGoat.Compactor
  alias SeaGoat.WAL
  alias SeaGoat.Store.FileManager
  alias SeaGoat.Store.Files

  require SeaGoat.Writer
  require SeaGoat.Compactor

  def recover_state(dir, wal, compactor) do
    recovered_state = %{
      max_file_count: 0,
      max_wal_count: 0,
      files: %{},
      recovered_writes: [],
      recovered_compacting_files: %{}
    }

    dir
    |> File.ls!()
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.filter(&FileManager.valid_db_file?/1)
    |> Enum.flat_map(&parse_file(&1, wal))
    |> Enum.sort_by(& &1.file_count)
    |> Enum.reduce(recovered_state, &process_file(&1, &2, compactor))
  end

  defp parse_file(file, wal) do
    case wal_or_ss_table(file, wal) do
      {:logs, logs} ->
        file_count = FileManager.file_count_from_path(file)
        [%{type: :log, logs: logs, file: file, file_count: file_count}]

      {:ss_table, bloom_filter, level} ->
        file_count = FileManager.file_count_from_path(file)

        [
          %{
            type: :ss_table,
            bloom_filter: bloom_filter,
            level: level,
            file: file,
            file_count: file_count
          }
        ]

      _ ->
        []
    end
  end

  defp process_file(%{type: :log} = file, recovered_state, _compactor) do
    case file.logs do
      [SeaGoat.Writer.writer_tag() | _logs] ->
        recovered_writes = [{file.file, file.logs} | recovered_state.recovered_writes]

        %{
          recovered_state
          | recovered_writes: recovered_writes,
            max_file_count: file.file_count,
            max_wal_count: file.file_count
        }

      [{SeaGoat.Compactor.compactor_tag(), files} | logs] ->
        recovered_compacting_files = Map.put(recovered_state.recovered_compacting_files, files, file.file)
        :ok = run_logs(logs)
        %{recovered_state | max_file_count: file.file_count, recovered_compacting_files: recovered_compacting_files}

      logs ->
        :ok = run_logs(logs)
        %{recovered_state | max_file_count: file.file_count}
    end
  end

  defp process_file(%{type: :ss_table} = file, recovered_state, compactor) do
    files = Files.insert(recovered_state.files, file.file, file.bloom_filter)
    :ok = Compactor.put(compactor, file.level, file.file_count, file.file)
    %{recovered_state | files: files, max_file_count: file.file_count}
  end

  defp run_logs([]), do: :ok

  defp run_logs([{:del, files} | logs]) do
    with :ok <- SSTables.delete(files) do
      run_logs(logs)
    end
  end

  defp run_logs([_ | logs]), do: run_logs(logs)

  defp wal_or_ss_table(file, wal) do
    case WAL.get_logs(wal, file) do
      {:ok, logs} ->
        {:logs, logs}

      _ ->
        case SSTables.fetch_ss_table_info(file) do
          {:ok, bloom_filter, level} ->
            {:ss_table, bloom_filter, level}

          _ ->
            {:error, :not_wal_or_db}
        end
    end
  end
end
