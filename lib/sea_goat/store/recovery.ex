defmodule SeaGoat.Store.Recovery do
  alias SeaGoat.SSTables
  alias SeaGoat.Compactor
  alias SeaGoat.WAL
  alias SeaGoat.Store.FileManager
  alias SeaGoat.Store.Files

  require SeaGoat.Writer
  require SeaGoat.Compactor

  def recover_state(dir, wal, compactor) do
    dir
    |> get_files()
    |> parse_files(wal)
    |> recover(compactor)
  end

  defp get_files(dir) do
    dir
    |> File.ls!()
    |> Enum.map(&Path.join(dir, &1))
    |> Enum.filter(&FileManager.valid_db_file?/1)
    |> Enum.sort_by(&FileManager.file_count_from_path/1)
  end

  defp parse_files(files, wal) do
    Enum.reduce(files, %{wal_files: [], ss_table_files: []}, fn file, acc ->
      case categorize_file(file, wal) do
        {:wal, logs} ->
          file_count = FileManager.file_count_from_path(file)
          wal_entry = {file, file_count, logs}
          %{acc | wal_files: [wal_entry | acc.wal_files]}

        {:ss_table, bloom_filter, level} ->
          file_count = FileManager.file_count_from_path(file)
          ss_table_entry = {file, file_count, bloom_filter, level}
          %{acc | ss_table_files: [ss_table_entry | acc.ss_table_files]}

        _ ->
          acc
      end
    end)
  end

  defp recover(%{wal_files: wal_files, ss_table_files: ss_table_files}, compactor) do
    wal_state =
      Enum.reduce(
        wal_files,
        %{
          max_file_count: 0,
          max_wal_count: 0,
          recovered_writes: [],
          recovered_compacting_files: %{}
        },
        &process_wal_file/2
      )

    ss_table_state =
      Enum.reduce(
        ss_table_files,
        %{max_file_count: 0, files: %{}},
        &process_ss_table_file(&1, &2, compactor)
      )

    %{
      max_file_count: max(wal_state.max_file_count, ss_table_state.max_file_count),
      max_wal_count: wal_state.max_wal_count,
      files: ss_table_state.files,
      recovered_writes: wal_state.recovered_writes,
      recovered_compacting_files: wal_state.recovered_compacting_files
    }
  end

  defp process_wal_file({file, file_count, [SeaGoat.Writer.writer_tag() | logs]}, state) do
    recovered_writes = [{file, logs} | state.recovered_writes]

    %{
      state
      | recovered_writes: recovered_writes,
        max_file_count: file_count,
        max_wal_count: file_count
    }
  end

  defp process_wal_file(
         {file, file_count, [{SeaGoat.Compactor.compactor_tag(), files} | logs]},
         state
       ) do
    recovered_compacting_files = Map.put(state.recovered_compacting_files, files, file)
    :ok = execute_logs(logs)
    %{state | max_file_count: file_count, recovered_compacting_files: recovered_compacting_files}
  end

  defp process_wal_file({_file, file_count, logs}, state) do
    :ok = execute_logs(logs)
    %{state | max_file_count: file_count}
  end

  defp process_ss_table_file({file, file_count, bloom_filter, level}, state, compactor) do
    files = Files.insert(state.files, file, bloom_filter)
    :ok = Compactor.put(compactor, level, file_count, file)
    %{state | files: files, max_file_count: file_count}
  end

  defp categorize_file(file, wal) do
    case WAL.get_logs(wal, file) do
      {:ok, logs} ->
        {:wal, logs}

      _ ->
        case SSTables.fetch_ss_table_info(file) do
          {:ok, bloom_filter, level} -> {:ss_table, bloom_filter, level}
          _ -> :no_file
        end
    end
  end

  defp execute_logs([]), do: :ok

  defp execute_logs([{:del, files} | logs]) do
    with :ok <- SSTables.delete(files) do
      execute_logs(logs)
    end
  end

  defp execute_logs([_ | logs]), do: execute_logs(logs)
end
