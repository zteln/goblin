defmodule Goblin.DiskTable do
  @moduledoc false
  alias Goblin.DiskTable.IOHandler
  alias Goblin.DiskTable.SST

  @tmp_suffix ".tmp"

  defmodule Iterator do
    @moduledoc false
    defstruct [
      :file,
      :disk
    ]

    defimpl Goblin.Iterable do
      def init(iterator) do
        disk = IOHandler.open!(iterator.file, start?: true)
        %{iterator | disk: disk}
      end

      def next(iterator) do
        case read_next_key(iterator.disk) do
          {:ok, triple, disk} ->
            {triple, %{iterator | disk: disk}}

          {:error, :eod} ->
            :ok

          error ->
            IOHandler.close(iterator.disk)
            raise "Iteration failed with error: #{inspect(error)}"
        end
      end

      def close(iterator) do
        IOHandler.close(iterator.disk)
      end

      defp read_next_key(disk) do
        with {:ok, enc_header} <- IOHandler.read(disk, SST.size(:block_header)),
             {:ok, span} <- SST.block_span(enc_header),
             {:ok, enc_block} <- IOHandler.read(disk, SST.size(:block) * span),
             {:ok, triple} <- SST.decode_block(enc_block) do
          {:ok, triple, IOHandler.advance_offset(disk, SST.size(:block) * span)}
        end
      end
    end
  end

  @spec new(Enumerable.t(Goblin.triple()), keyword()) :: {:ok, [SST.t()]} | {:error, term()}
  def new(data, opts) do
    with {:ok, moves, ssts} <- write_to_disk(data, opts),
         :ok <- switch(moves) do
      {:ok, ssts}
    end
  end

  @spec delete(Goblin.db_file()) :: :ok | {:error, term()}
  def delete(file), do: IOHandler.rm(file)

  @spec find(SST.t(), Goblin.db_key()) ::
          {:ok, {:value, Goblin.seq_no(), Goblin.db_value()}} | :not_found | {:error, term()}
  def find(sst, key) do
    with {:ok, disk} <- IOHandler.open(sst.file) do
      result = binary_search(disk, key, 0, sst.no_blocks)
      IOHandler.close(disk)
      result
    end
  end

  @spec fetch_sst(Goblin.db_file()) :: {:ok, SST.t()} | {:error, term()}
  def fetch_sst(file) do
    disk = IOHandler.open!(file)

    result =
      with {:ok, size} <- valid_sst(disk),
           {:ok,
            {
              level_key,
              bf_pos,
              bf_size,
              key_range_pos,
              key_range_size,
              seq_range_pos,
              seq_range_size,
              no_blocks
            }} <- read_metadata(disk),
           {:ok, bf} <- read_bloom_filter(disk, bf_pos, bf_size),
           {:ok, key_range} <- read_key_range(disk, key_range_pos, key_range_size),
           {:ok, seq_range} <- read_seq_range(disk, seq_range_pos, seq_range_size) do
        {:ok,
         %SST{
           file: file,
           bloom_filter: bf,
           level_key: level_key,
           seq_range: seq_range,
           key_range: key_range,
           no_blocks: no_blocks,
           size: size
         }}
      end

    IOHandler.close(disk)
    result
  end

  @spec iterator(Goblin.db_file()) :: Goblin.Iterable.t()
  def iterator(file) do
    %Iterator{file: file}
  end

  defp write_to_disk(data, opts) do
    file_getter = opts[:file_getter]
    max_sst_size = opts[:max_sst_size]
    level_key = opts[:level_key]

    file = file_getter.()
    tmp_file = tmp_file(file)
    sst = SST.new(file, level_key)
    disk = IOHandler.open!(tmp_file, write?: true)

    Enum.reduce_while(data, {sst, disk, [{tmp_file, file}], []}, fn
      triple, {%{size: size} = sst, disk, moves, ssts} when size >= max_sst_size ->
        file = file_getter.()
        tmp_file = tmp_file(file)
        new_sst = SST.new(file, level_key)

        with {:ok, sst, disk} <- append_metadata(sst, disk, opts),
             :ok <- IOHandler.close(disk),
             {:ok, new_disk} <- IOHandler.open(tmp_file, write?: true),
             {:ok, new_sst, disk} <- append_data(triple, new_sst, new_disk, opts) do
          {:cont, {new_sst, disk, [{tmp_file, file} | moves], [sst | ssts]}}
        else
          error -> {:halt, error}
        end

      triple, {sst, disk, moves, ssts} ->
        case append_data(triple, sst, disk, opts) do
          {:ok, sst, disk} -> {:cont, {sst, disk, moves, ssts}}
          error -> {:halt, error}
        end
    end)
    |> then(fn
      {:error, _} = error ->
        error

      {sst, disk, moves, ssts} ->
        with {:ok, sst, disk} <- append_metadata(sst, disk, opts),
             :ok <- IOHandler.close(disk) do
          {:ok, moves, Enum.reverse([sst | ssts])}
        end
    end)
  end

  defp append_data(triple, sst, disk, opts) do
    compress? = opts[:compress?] || false

    {block, sst} = SST.add_data(sst, triple, compress?)

    with {:ok, disk} <- IOHandler.write(disk, block) do
      {:ok, sst, disk}
    end
  end

  defp append_metadata(sst, disk, opts) do
    compress? = opts[:compress?] || false
    bf_fpp = opts[:bf_fpp]

    {metadata_block, sst} = SST.add_metadata(sst, disk.offset, bf_fpp, compress?)

    with {:ok, disk} <- IOHandler.write(disk, metadata_block) do
      {:ok, sst, disk}
    end
  end

  defp binary_search(_disk, _key, low, high) when high < low, do: :not_found

  defp binary_search(disk, key, low, high) do
    mid = div(low + high, 2)
    position = (mid - 1) * SST.size(:block)

    with {:ok, k, seq, v} <- read_block(disk, position) do
      cond do
        key == k ->
          {:ok, {:value, seq, v}}

        key < k ->
          binary_search(disk, key, low, mid - 1)

        key > k ->
          binary_search(disk, key, mid + 1, high)
      end
    end
  end

  defp read_block(disk, position) do
    position = max(0, position)

    with {:ok, encoded_header} <- IOHandler.read(disk, position, SST.size(:block_header)),
         {:ok, span} <- SST.block_span(encoded_header),
         {:ok, encoded} <- IOHandler.read(disk, position, SST.size(:block) * span),
         {:ok, {key, seq, value}} <- SST.decode_block(encoded) do
      {:ok, key, seq, value}
    else
      {:error, :not_block_start} ->
        read_block(disk, position - SST.size(:block))

      e ->
        e
    end
  end

  defp valid_sst(disk) do
    with {:ok, magic} <-
           IOHandler.read_from_end(
             disk,
             SST.size(:magic),
             SST.size(:magic)
           ),
         :ok <- SST.verify_magic(magic),
         {:ok, crc} <-
           IOHandler.read_from_end(
             disk,
             SST.size(:magic) + SST.size(:crc),
             SST.size(:crc)
           ),
         {:ok, crc} <- SST.decode_crc(crc),
         {:ok, size} <-
           IOHandler.read_from_end(
             disk,
             SST.size(:magic) + SST.size(:crc) + SST.size(:size),
             SST.size(:size)
           ),
         {:ok, size} <- SST.decode_size(size),
         :ok <- verify_crc(disk, 0, size, crc) do
      {:ok, size}
    end
  end

  defp read_metadata(disk) do
    with {:ok, encoded_metadata} <-
           IOHandler.read_from_end(
             disk,
             SST.size(:magic) + SST.size(:crc) + SST.size(:size) + SST.size(:metadata),
             SST.size(:metadata)
           ) do
      SST.decode_metadata(encoded_metadata)
    end
  end

  defp read_bloom_filter(disk, pos, size) do
    with {:ok, encoded_bf} <- IOHandler.read(disk, pos, size) do
      SST.decode_bloom_filter(encoded_bf)
    end
  end

  defp read_key_range(disk, pos, size) do
    with {:ok, encoded_key_range} <- IOHandler.read(disk, pos, size) do
      SST.decode_key_range(encoded_key_range)
    end
  end

  defp read_seq_range(disk, pos, size) do
    with {:ok, encoded_seq_range} <- IOHandler.read(disk, pos, size) do
      SST.decode_seq_range(encoded_seq_range)
    end
  end

  defp switch([]), do: :ok

  defp switch([{from, to} | to_switch]) do
    with :ok <- IOHandler.rename(from, to) do
      switch(to_switch)
    end
  end

  defp verify_crc(disk, start_pos, end_pos, target_crc, crc \\ :erlang.crc32(<<>>))

  defp verify_crc(_disk, end_pos, end_pos, target_crc, target_crc), do: :ok
  defp verify_crc(_disk, end_pos, end_pos, _target_crc, _crc), do: {:error, :corrupt_sst}

  defp verify_crc(disk, start_pos, end_pos, target_crc, crc) do
    inc = min(end_pos - start_pos, 4096)

    with {:ok, chunk} <- IOHandler.read(disk, start_pos, inc) do
      crc = :erlang.crc32(crc, chunk)
      verify_crc(disk, start_pos + inc, end_pos, target_crc, crc)
    end
  end

  defp tmp_file(file), do: file <> @tmp_suffix
end
