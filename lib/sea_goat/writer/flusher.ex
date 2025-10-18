defmodule SeaGoat.Writer.Flusher do
  @moduledoc false
  alias SeaGoat.WAL
  alias SeaGoat.Manifest
  alias SeaGoat.SSTables
  alias SeaGoat.Store

  @flush_level 0

  @doc "Flushes a in-memory MemTable to an SST file on-disk."
  @spec flush(
          SeaGoat.Writer.MemTable.t(),
          WAL.rotated_file(),
          {Store.store(), WAL.wal(), Manifest.manifest()}
        ) :: :flushed | {:error, term()}
  def flush(mem_table, rotated_wal, {store, wal, manifest}) do
    file = Store.new_file(store)
    tmp_file = Store.tmp_file(file)
    stream = flush_stream(mem_table)

    with {:ok, bloom_filter, priority, size, key_range} <-
           SSTables.write(tmp_file, @flush_level, stream),
         :ok <- SSTables.switch(tmp_file, file),
         :ok <- Manifest.log_flush(manifest, file, rotated_wal),
         :ok <- WAL.clean(wal, rotated_wal),
         :ok <-
           Store.put(store, file, @flush_level, {bloom_filter, priority, size, key_range}) do
      :flushed
    end
  end

  defp flush_stream(mem_table) do
    Stream.resource(
      fn -> init_flush_data(mem_table) end,
      &iter_flush_data/1,
      &after_iter/1
    )
  end

  defp init_flush_data(mem_table) do
    mem_table
    |> Enum.sort()
    |> Enum.map(fn {key, {seq, value}} -> {seq, key, value} end)
  end

  defp iter_flush_data([]), do: {:halt, :ok}
  defp iter_flush_data([next | data]), do: {[next], data}

  defp after_iter(_), do: :ok
end
