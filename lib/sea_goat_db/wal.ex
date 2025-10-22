defprotocol SeaGoatDB.WAL do
  @type t() :: t()
  @type write ::
          {SeaGoatDB.db_sequence(), :put, SeaGoatDB.db_key(), SeaGoatDB.db_value()}
          | {SeaGoatDB.db_sequence(), :remove, SeaGoatDB.db_key()}
  @type rotated_file :: String.t()

  @spec append(t(), [write()]) :: :ok | {:error, term()}
  def append(wal, buffer)

  @spec sync(t()) :: :ok | {:error, term()}
  def sync(wal)

  @spec rotate(t()) :: {:ok, rotated_file()} | {:error, term()}
  def rotate(wal)

  @spec clean(t(), rotated_file()) :: :ok | {:error, term()}
  def clean(wal, rotated_file)

  @spec recover(t()) :: {:ok, [{rotated_file(), [write()]}]} | {:error, term()}
  def recover(wal)
end
