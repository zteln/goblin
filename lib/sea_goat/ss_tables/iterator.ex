defprotocol SeaGoat.SSTables.Iterator do
  @type t() :: t()
  def init(iterator)
  def next(iterator)
  def deinit(iterator)
end
