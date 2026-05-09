defmodule Goblin.FileIO do
  @moduledoc false

  @magic "GOBLIN00"
  @header_size byte_size(<<@magic::binary, 0::integer-32, 0::integer-32>>)
  @default_block_size 512

  @default_modes [
    :raw,
    :read,
    :binary,
    read_ahead: 64 * 1024
  ]

  defstruct [
    :path,
    :iodev,
    :block_size
  ]

  @type t :: %__MODULE__{
          path: Path.t(),
          iodev: :file.io_device(),
          block_size: non_neg_integer()
        }

  @spec open!(Path.t(), keyword()) :: t()
  def open!(path, opts \\ []) do
    case open(path, opts) do
      {:ok, file} -> file
      _ -> raise "failed to open file #{inspect(path)}"
    end
  end

  @spec open(Path.t(), keyword()) :: {:ok, t()} | {:error, term()}
  def open(path, opts \\ []) do
    modes =
      case opts[:write?] do
        true -> [:append | @default_modes]
        _ -> @default_modes
      end

    with {:ok, iodev} <- :file.open(path, modes) do
      {:ok,
       %__MODULE__{
         path: path,
         iodev: iodev,
         block_size: opts[:block_size] || @default_block_size
       }}
    end
  end

  @spec append(t(), term(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def append(file, term, opts \\ []) do
    iolist = encode_to_iolist(term, file.block_size, opts[:compress?] || false)

    with :ok <- :file.write(file.iodev, iolist) do
      {:ok, :erlang.iolist_size(iolist)}
    end
  end

  @spec stream_write(Enumerable.t(), any(), (any(), any() -> any()), keyword()) ::
          Enumerable.t({:ok, any(), non_neg_integer()} | {:error, term()})
  def stream_write(stream, init, reducer, opts) do
    max_size = opts[:max_size]
    block_size = opts[:block_size]
    compress? = opts[:compress?]
    filer = opts[:filer]

    stream
    |> Stream.transform(
      fn -> {nil, init, 0} end,
      fn
        _data, :halt ->
          {:halt, {nil, nil, 0}}

        data, {file, acc, size} when size + block_size >= max_size ->
          with {:ok, data_size} <- append(file, data, compress?: compress?),
               acc = reducer.(data, acc, size + data_size),
               {:ok, acc_size} <- append(file, acc, compress?: compress?),
               :ok <- close(file) do
            {[{:ok, acc, size + data_size + acc_size}], {nil, init, 0}}
          else
            error ->
              close(file)
              {[error], :halt}
          end

        data, {file, acc, size} ->
          file = file || open!(filer.(), write?: true, block_size: block_size)

          case append(file, data, compress?: compress?) do
            {:ok, data_size} ->
              size = size + data_size
              acc = reducer.(data, acc, size)
              {[], {file, acc, size}}

            error ->
              close(file)
              {[error], :halt}
          end
      end,
      fn
        {file, acc, _size} ->
          with {:ok, _acc_size} <- append(file, acc, compress?: compress?),
               :ok <- close(file) do
            {[{:ok, acc}], nil}
          end

        _ ->
          {[], nil}
      end,
      fn
        {file, _acc, _size} -> close(file)
        _ -> :ok
      end
    )
  end

  @spec pread(t(), non_neg_integer() | :eof) :: {:ok, term()} | {:error, term()} | :eof
  def pread(_file, pos) when pos < 0, do: {:error, :invalid_position}

  def pread(file, :eof) do
    case :filelib.file_size(file.path) do
      0 -> {:error, :empty}
      size -> pread(file, size - file.block_size)
    end
  end

  def pread(file, pos) do
    with {:ok, header} <- :file.pread(file.iodev, pos, @header_size),
         {:ok, no_blocks, block_size} <- decode_header(header),
         {:ok, payload} <-
           :file.pread(file.iodev, pos + @header_size, no_blocks * block_size - @header_size),
         :ok <- validate_size(byte_size(payload), no_blocks * block_size - @header_size) do
      {:ok, :erlang.binary_to_term(payload)}
    else
      {:error, :invalid_header} -> pread(file, pos - file.block_size)
      error -> error
    end
  end

  @spec read(t()) :: {:ok, term()} | {:error, term()} | :eof
  def read(file) do
    with {:ok, header} <- :file.read(file.iodev, @header_size),
         {:ok, no_blocks, block_size} <- decode_header(header),
         {:ok, payload} <- :file.read(file.iodev, no_blocks * block_size - @header_size),
         :ok <- validate_size(byte_size(payload), no_blocks * block_size - @header_size) do
      {:ok, :erlang.binary_to_term(payload)}
    end
  end

  @spec stream!(t(), keyword()) :: Enumerable.t(term())
  def stream!(file, opts \\ []) do
    truncate? = opts[:truncate?]

    Stream.resource(
      fn -> {file, 0} end,
      fn {file, pos} ->
        case read(file) do
          {:ok, terms} ->
            {:ok, pos} = :file.position(file.iodev, :cur)
            {terms, {file, pos}}

          :eof ->
            {:halt, :eof}

          {:error, :invalid_size} ->
            {:halt, {:truncate, pos}}

          {:error, :invalid_header} ->
            {:halt, {:truncate, pos}}

          {:error, reason} ->
            raise "stream! failed with reason: #{inspect(reason)}"
        end
      end,
      fn
        {:truncate, pos} when truncate? ->
          :file.position(file.iodev, pos)
          :file.truncate(file.iodev)

        _ ->
          :ok
      end
    )
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(file), do: :file.close(file.iodev)

  @spec sync(t()) :: :ok | {:error, term()}
  def sync(file), do: :file.datasync(file.iodev)

  @spec rename(Path.t(), Path.t()) :: :ok | {:error, term()}
  def rename(from, to), do: File.rename(from, to)

  @spec remove(Path.t()) :: :ok | {:error, term()}
  def remove(path), do: File.rm(path)

  @spec size_of(Path.t()) :: non_neg_integer()
  def size_of(path), do: :filelib.file_size(path)

  defp encode_to_iolist(terms, block_size, compress?) do
    opts = if compress?, do: [:compressed], else: []
    payload = :erlang.term_to_iovec(terms, opts)
    data_size = @header_size + :erlang.iolist_size(payload)
    no_blocks = div(data_size + block_size - 1, block_size)
    padding = no_blocks * block_size - data_size

    [
      <<@magic::binary>>,
      <<no_blocks::integer-32>>,
      <<block_size::integer-32>>,
      payload,
      <<0::size(padding)-unit(8)>>
    ]
  end

  defp decode_header(<<
         @magic::binary,
         no_blocks::integer-32,
         block_size::integer-32
       >>),
       do: {:ok, no_blocks, block_size}

  defp decode_header(_), do: {:error, :invalid_header}

  defp validate_size(size, size), do: :ok
  defp validate_size(_, _), do: {:error, :invalid_size}
end
