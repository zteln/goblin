defprotocol Goblin.Transactionable do
  @type t :: t()
  @type return :: {:commit, t(), term()} | :abort

  @spec put(t(), Goblin.db_key(), Goblin.db_value(), keyword()) :: t()
  def put(tx, key, value, opts)

  @spec put_multi(t(), list({Goblin.db_key(), Goblin.db_value()}), keyword()) :: t()
  def put_multi(tx, pairs, opts)

  @spec remove(t(), Goblin.db_key(), keyword()) :: t()
  def remove(tx, key, opts)

  @spec remove_multi(t(), list(Goblin.db_key()), keyword()) :: t()
  def remove_multi(tx, keys, opts)

  @spec get(t(), Goblin.db_key(), keyword()) :: Goblin.db_value()
  def get(tx, key, opts)

  @spec get_multi(t(), list(Goblin.db_key()), keyword()) ::
          list({Goblin.db_key(), Goblin.db_value()})
  def get_multi(tx, keys, opts)
end
