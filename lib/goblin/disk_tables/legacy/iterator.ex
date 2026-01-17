defmodule Goblin.DiskTables.Legacy.Iterator do
  @moduledoc false
  alias Goblin.DiskTables.{Handler, Legacy}

  defstruct [
    :file,
    :handler
  ]

  @spec new(Path.t()) ::
          {:ok, Goblin.level_key(), boolean(), Goblin.Iterable.t()} | {:error, term()}
  def new(file) do
    handler = Handler.open!(file)

    with :ok <- verify_magic(handler),
         {:ok, level_key, compressed?} <- get_info(handler),
         :ok <- Handler.close(handler) do
      {:ok, level_key, compressed?, %__MODULE__{file: file}}
    else
      error ->
        Handler.close(handler)
        error
    end
  end

  defp verify_magic(handler) do
    with {:ok, magic_block} <-
           Handler.read_from_end(
             handler,
             Legacy.Encoder.magic_size(),
             Legacy.Encoder.magic_size()
           ) do
      Legacy.Encoder.validate_magic_block(magic_block)
    end
  end

  defp get_info(handler) do
    with {:ok, metadata_block} <-
           Handler.read_from_end(
             handler,
             Legacy.Encoder.magic_size() +
               Legacy.Encoder.size_block_size() +
               Legacy.Encoder.crc_block_size() +
               Legacy.Encoder.metadata_block_size(),
             Legacy.Encoder.metadata_block_size()
           ),
         {:ok, {level_key, _, _, _, _, compressed?}} <-
           Legacy.Encoder.decode_metadata_block(metadata_block) do
      {:ok, level_key, compressed?}
    end
  end

  defimpl Goblin.Iterable do
    def init(iterator) do
      handler = Handler.open!(iterator.file, start?: true)
      %{iterator | handler: handler}
    end

    def next(iterator) do
      case read_next_sst_block(iterator.handler) do
        {:ok, triple, handler} ->
          {triple, %{iterator | handler: handler}}

        {:error, :end_of_sst} ->
          :ok

        error ->
          Handler.close(iterator.handler)
          raise "migration iteration failed with error: #{inspect(error)}"
      end
    end

    def close(iterator) do
      Handler.close(iterator.handler)
    end

    defp read_next_sst_block(handler) do
      with {:ok, sst_header_block} <-
             Handler.read(handler, Legacy.Encoder.sst_header_size()),
           {:ok, no_blocks} <-
             Legacy.Encoder.decode_sst_header_block(sst_header_block),
           {:ok, sst_block} <-
             Handler.read(
               handler,
               no_blocks * Legacy.Encoder.sst_block_unit_size()
             ),
           {:ok, triple} <- Legacy.Encoder.decode_sst_block(sst_block) do
        {:ok, triple,
         Handler.advance_offset(
           handler,
           no_blocks * Legacy.Encoder.sst_block_unit_size()
         )}
      end
    end
  end
end
