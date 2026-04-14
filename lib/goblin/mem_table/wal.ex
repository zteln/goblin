defmodule Goblin.MemTable.WAL do
  @moduledoc false
  require Logger

  @default_modes [:raw, :read, :binary]

  defstruct [
    :file,
    :iodev
  ]

  @type t :: %__MODULE__{
          file: Path.t(),
          iodev: :file.io_device()
        }

  @spec open(Path.t(), boolean()) :: {:ok, t()} | {:error, term()}
  def open(path, write? \\ true) do
    modes =
      case write? do
        true -> [:append | @default_modes]
        false -> @default_modes
      end

    with {:ok, iodev} <- :file.open(path, modes) do
      {:ok, %__MODULE__{iodev: iodev, file: path}}
    end
  end

  @spec append(t(), list(term())) :: :ok | {:error, term()}
  def append(wal, writes) do
    iolist = :erlang.term_to_iovec(writes)
    size = :erlang.iolist_size(iolist)
    iolist = [<<size::integer-32>> | iolist]

    with :ok <- size_within_limit(size),
         :ok <- :file.write(wal.iodev, iolist) do
      :file.datasync(wal.iodev)
    end
  end

  @spec replay(t()) :: Enumerable.t(term())
  def replay(wal) do
    Stream.resource(
      fn -> {wal.iodev, 0} end,
      fn {iodev, pos} ->
        case read(iodev, pos) do
          {:ok, terms, pos} -> {terms, {iodev, pos}}
          :eof -> {:halt, :eof}
          {:error, reason} -> raise "Failed to stream WAL file, reason: #{inspect(reason)}"
        end
      end,
      fn _ -> :ok end
    )
  end

  @spec close(t()) :: :ok | {:error, term()}
  def close(wal), do: :file.close(wal.iodev)

  @spec rm(t()) :: :ok | {:error, term()}
  def rm(wal), do: File.rm(wal.file)

  @spec filepath(t()) :: Path.t()
  def filepath(wal), do: wal.file

  defp read(iodev, pos) do
    with {:ok, <<size::integer-32>>} <- :file.pread(iodev, pos, 4),
         {:ok, bin} <- :file.pread(iodev, pos + 4, size),
         :ok <- validate_size(size, bin) do
      {:ok, :erlang.binary_to_term(bin), pos + 4 + size}
    end
  end

  defp validate_size(size, bin) when size == byte_size(bin), do: :ok

  defp validate_size(size, bin) do
    Logger.warning(fn ->
      "[#{inspect(__MODULE__)}] Unable to recover #{size - byte_size(bin)} from WAL. Possibly corrupt state..."
    end)

    :eof
  end

  defp size_within_limit(size) when size < 0xFFFFFFFF, do: :ok
  defp size_within_limit(_size), do: {:error, :commit_too_large}
end
