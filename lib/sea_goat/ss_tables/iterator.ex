defprotocol SeaGoat.SSTables.Iterator do
  @type t() :: t()
  @type next() :: {SeaGoat.db_key(), SeaGoat.db_value()}

  @spec init(t()) :: {:ok, t()} | {:error, term()}
  def init(iterator)

  @spec next(t()) ::
          {:next, next(), t()} | {:end_iter, SeaGoat.db_sequence(), SeaGoat.db_sequence(), t()}
  def next(iterator)

  @spec deinit(t()) :: :ok | {:error, term()}
  def deinit(iterator)
end
