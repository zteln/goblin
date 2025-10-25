# defmodule SeaGoatDB.Writer.FlushQueueTest do
#   use ExUnit.Case, async: true
#   alias SeaGoatDB.Writer.FlushQueue
#
#   test "pop retrieves oldest push" do
#     fq = FlushQueue.new()
#
#     fq = FlushQueue.push(fq, :foo, :bar)
#     fq = FlushQueue.push(fq, :bar, :baz)
#     fq = FlushQueue.push(fq, :baz, :foo)
#
#     assert {:foo, :bar, fq} = FlushQueue.pop(fq)
#     assert {:bar, :baz, fq} = FlushQueue.pop(fq)
#     assert {:baz, :foo, _fq} = FlushQueue.pop(fq)
#   end
#
#   test "empty?/1 and size/1 work as expected" do
#     fq = FlushQueue.new()
#
#     assert FlushQueue.empty?(fq)
#     assert FlushQueue.size(fq) == 0
#
#     fq = FlushQueue.push(fq, :foo, :bar)
#
#     refute FlushQueue.empty?(fq)
#     assert FlushQueue.size(fq) == 1
#
#     {:foo, :bar, fq} = FlushQueue.pop(fq)
#
#     assert FlushQueue.empty?(fq)
#     assert FlushQueue.size(fq) == 0
#   end
#
#   test "to_data_list/1 returns list of data" do
#     fq = FlushQueue.new()
#     fq = FlushQueue.push(fq, :data1, :extra1)
#     fq = FlushQueue.push(fq, :data2, :extra2)
#     fq = FlushQueue.push(fq, :data3, :extra3)
#
#     assert [:data3, :data2, :data1] == FlushQueue.to_data_list(fq)
#   end
# end
