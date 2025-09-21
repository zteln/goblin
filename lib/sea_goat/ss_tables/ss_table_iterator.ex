defprotocol SeaGoat.SSTables.SSTableIterator do
  def init(iterator, data)
  def next(iterator)
  def deinit(iterator)
end
