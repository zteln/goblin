defmodule Goblin.Disk do
  @moduledoc false

  alias Goblin.FileIO
  alias Goblin.Disk.{Table, BinarySearch}

  @spec into_table(Enumerable.t(Goblin.triple()), keyword()) ::
          {:ok, list(Table.t())} | {:error, term()}
  def into_table(stream, opts) do
    max_sst_size = opts[:max_sst_size]

    stream
    |> Stream.transform(
      fn -> nil end,
      fn
        _triple, :halt ->
          {:halt, nil}

        triple, acc ->
          {table, io, tmp_file} = acc || open_new_table(opts)

          case append_data(table, io, triple, opts) do
            {:ok, %{size: size} = table, io} when size >= max_sst_size ->
              case finalize(table, io, tmp_file, opts) do
                {:ok, table} -> {[{:ok, table}], nil}
                error -> {[error], :halt}
              end

            {:ok, table, io} ->
              {[], {table, io, tmp_file}}

            error ->
              FileIO.close(io)
              {[error], :halt}
          end
      end,
      fn
        {table, io, tmp_file} ->
          case finalize(table, io, tmp_file, opts) do
            {:ok, table} -> {[{:ok, table}], nil}
            error -> {[error], nil}
          end

        _ ->
          {[], nil}
      end,
      fn
        {_table, io, _tmp_file} -> FileIO.close(io)
        _ -> :ok
      end
    )
    |> Enum.reduce_while({:ok, []}, fn
      {:ok, disk_table}, {:ok, acc} -> {:cont, {:ok, [disk_table | acc]}}
      error, {:ok, _acc} -> {:halt, error}
    end)
  end

  @spec from_file(Path.t()) :: {:ok, Table.t()} | {:error, term()}
  def from_file(path) do
    io = FileIO.open!(path, block_size: Table.block_size())

    try do
      case FileIO.pread(io, :eof) do
        {:ok, %Table{} = table} -> {:ok, table}
        {:ok, _} -> {:error, :invalid_disk_table}
        error -> error
      end
    after
      FileIO.close(io)
    end
  end

  def has_key?(table, key) do
    Table.has_key?(table, key)
  end

  def search(table, keys, seq) do
    Stream.resource(
      fn ->
        io = FileIO.open!(table.path, block_size: Table.block_size())
        BinarySearch.new(table, io, keys, seq)
      end,
      fn bs ->
        case BinarySearch.next(bs) do
          {:ok, triple, bs} ->
            {[triple], bs}

          {:ok, bs} ->
            {:halt, bs}

          error ->
            FileIO.close(bs.io)
            raise "iteration failed with error: #{inspect(error)}"
        end
      end,
      fn bs ->
        FileIO.close(bs.io)
      end
    )
  end

  def stream(table), do: stream_table(table, :infinity)

  def stream(table, min, max, seq) do
    if Table.within_bounds?(table, min, max) do
      stream_table(table, seq)
    else
      []
    end
  end

  defp stream_table(table, seq) do
    Stream.resource(
      fn -> FileIO.open!(table.path, block_size: Table.block_size()) end,
      fn io ->
        case FileIO.read(io) do
          {:ok, {_, s, _} = triple} when s <= seq ->
            {[triple], io}

          {:ok, _} ->
            {[], io}

          :eof ->
            {:halt, io}

          error ->
            FileIO.close(io)
            raise "iteration failed with error: #{inspect(error)}"
        end
      end,
      fn io -> FileIO.close(io) end
    )
  end

  defp open_new_table(opts) do
    {tmp_file, file} = opts[:next_file_f].()
    File.exists?(tmp_file) && File.rm!(tmp_file)
    io = FileIO.open!(tmp_file, write?: true, block_size: Table.block_size())
    bf_opts = Keyword.take(opts, [:bf_bit_array_size, :bf_fpp])
    table = Table.new(file, opts[:level_key], bf_opts)
    {table, io, tmp_file}
  end

  defp append_data(table, io, data, opts) do
    with {:ok, size} <- FileIO.append(io, data, opts) do
      table = Table.add_to_table(table, data, size)
      {:ok, table, io}
    end
  end

  defp finalize(table, io, tmp_file, opts) do
    with {:ok, _size} <- FileIO.append(io, table, opts),
         :ok <- FileIO.sync(io),
         :ok <- FileIO.close(io),
         :ok <- FileIO.rename(tmp_file, table.path) do
      {:ok, table}
    else
      error ->
        FileIO.close(io)
        error
    end
  end
end
