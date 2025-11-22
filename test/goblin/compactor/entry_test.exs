# defmodule Goblin.Compactor.EntryTest do
#   use ExUnit.Case, async: true
#   alias Goblin.Compactor.Entry
#
#   test "in_range/2 returns true if key in range, false otherwise" do
#     entry = %Entry{key_range: {nil, nil}}
#     assert Entry.in_range(entry, :foo)
#
#     entry = %Entry{key_range: {1, nil}}
#     assert Entry.in_range(entry, 2)
#     refute Entry.in_range(entry, 0)
#
#     entry = %Entry{key_range: {nil, 5}}
#     assert Entry.in_range(entry, 2)
#     refute Entry.in_range(entry, 6)
#
#     entry = %Entry{key_range: {1, 5}}
#     assert Entry.in_range(entry, 2)
#     refute Entry.in_range(entry, 0)
#     refute Entry.in_range(entry, 6)
#   end
#
#   test "place_in_buffer/2 places data in entry's buffer" do
#     entry = %Entry{}
#     assert %{buffer: buffer} = entry
#     assert map_size(buffer) == 0
#     entry = Entry.place_in_buffer(entry, {:k, 0, :v})
#     assert %{buffer: %{:k => {0, :v}}} = entry
#     entry = Entry.place_in_buffer(entry, {:l, 1, :w})
#     assert %{buffer: %{:k => {0, :v}, :l => {1, :w}}} = entry
#     entry = Entry.place_in_buffer(entry, {:k, 2, :u})
#     assert %{buffer: %{:k => {2, :u}, :l => {1, :w}}} = entry
#   end
# end
