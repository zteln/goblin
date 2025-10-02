defmodule SeaGoat.Store.Files do
  alias SeaGoat.Store.FileManager
  alias SeaGoat.RWLocks
  alias SeaGoat.SSTables
  alias SeaGoat.BloomFilter

  def insert(files, file, bloom_filter) do
    file_count = FileManager.file_count_from_path(file)
    Map.put(files, file, {file_count, bloom_filter})
  end

  def remove(files, file) do
    Map.delete(files, file)
  end

  def get_all(files, key, pid, rw_locks) do
    files
    |> Enum.filter(fn {_file, {_file_count, bloom_filter}} ->
      BloomFilter.is_member(bloom_filter, key)
    end)
    |> List.keysort(1, :desc)
    |> Enum.map(fn {file, _} ->
      RWLocks.rlock(rw_locks, file, pid)

      {
        fn -> SSTables.read(file, key) end,
        fn -> RWLocks.unlock(rw_locks, file, pid) end
      }
    end)
  end
end
