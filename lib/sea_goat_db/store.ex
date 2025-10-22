defprotocol SeaGoatDB.Store do
  @type t() :: t()

  @spec put(
          t(),
          SeaGoatDB.db_file(),
          SeaGoatDB.db_level_key(),
          SeaGoatDB.BloomFilter.t(),
          SeaGoatDB.db_sequence(),
          non_neg_integer(),
          {SeaGoatDB.db_key(), SeaGoatDB.db_key()}
        ) :: :ok
  def put(store, file, level_key, bloom_filter, priority, size, key_range)

  @spec remove(t(), SeaGoatDB.db_file()) :: :ok
  def remove(store, file)

  @spec new_file(t()) :: SeaGoatDB.db_file()
  def new_file(store)

  @spec get(t(), SeaGoatDB.db_key()) :: [
          {(-> SeaGoatDB.db_value()), (-> :ok)}
        ]
  def get(store, key)
end
