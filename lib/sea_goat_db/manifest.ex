defprotocol SeaGoatDB.Manifest do
  @type t() :: t()

  @spec log_rotation(t(), SeaGoatDB.WAL.rotated_file()) :: :ok | {:error, term()}
  def log_rotation(manifest, rotated_wal)

  @spec log_flush(t(), [SeaGoatDB.db_file()], SeaGoatDB.WAL.rotated_file()) ::
          :ok | {:error, term()}
  def log_flush(manifest, files, rotated_wal)

  @spec log_sequence(t(), SeaGoatDB.db_sequence()) :: :ok | {:errot, term()}
  def log_sequence(manifest, seq)

  @spec log_compaction(t(), [SeaGoatDB.db_file()], [SeaGoatDB.db_file()]) ::
          :ok | {:error, term()}
  def log_compaction(manifest, removed_files, added_files)

  @spec get_version(t(), [atom()]) :: map()
  def get_version(manifest, keys)
end
