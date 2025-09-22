defmodule SeaGoat.SSTable.Disk do
  def open!(path, opts \\ []) do
    case open(path, opts) do
      {:ok, io, offset} -> {io, offset}
      _ -> raise "failed to open file #{inspect(path)}."
    end
  end

  def open(path, opts \\ []) do
    open_opts = [:binary, :read, :raw | List.wrap(if opts[:write?], do: :append)]
    position = if opts[:start?], do: 0, else: :eof

    with {:ok, io} <- :file.open(path, open_opts),
         {:ok, offset} <- :file.position(io, position) do
      {:ok, io, offset}
    end
  end

  def write(io, offset, content) do
    with :ok <- :file.pwrite(io, offset, content) do
      {:ok, offset + byte_size(content)}
    end
  end

  def read(io, offset, size) do
    case :file.pread(io, offset, size) do
      {:ok, data} ->
        {:ok, data}

      :eof ->
        {:error, :eof}

      {:error, _reason} = e ->
        e
    end
  end

  def rename(from, to) do
    case :file.rename(from, to) do
      {:error, :enoent} ->
        :ok

      :ok ->
        :ok

      e ->
        e
    end
  end

  def sync(io) do
    :file.datasync(io)
  end

  def close(io) do
    :file.close(io)
  end

  def rm(path) do
    case :file.delete(path) do
      :ok -> :ok
      {:error, :enoent} -> :ok
      e -> e
    end
  end
end
