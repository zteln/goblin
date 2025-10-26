defmodule Goblin.Compactor.LevelTest do
  use ExUnit.Case, async: true
  alias Goblin.Compactor.Level
  alias Goblin.Compactor.Entry

  test "get_total_size/1 returns total size of entries" do
    level = %Level{}
    assert 0 == Level.get_total_size(level)

    level =
      level
      |> Level.put_entry(%Entry{id: :foo, size: 5})
      |> Level.put_entry(%Entry{id: :bar, size: 10})
      |> Level.put_entry(%Entry{id: :baz, size: 15})

    assert 30 == Level.get_total_size(level)
  end

  test "get_highest_prio_entries/1 returns all entries when level_key = 0" do
    level =
      %Level{level_key: 0}
      |> Level.put_entry(%Entry{id: :foo, priority: 1})
      |> Level.put_entry(%Entry{id: :bar, priority: 2})
      |> Level.put_entry(%Entry{id: :baz, priority: 3})

    assert [
             %Entry{id: :foo, priority: 1},
             %Entry{id: :bar, priority: 2},
             %Entry{id: :baz, priority: 3}
           ] = Level.get_highest_prio_entries(level)
  end

  test "get_highest_prio_entries/1 returns entry with highest priority (minimum)" do
    level =
      %Level{}
      |> Level.put_entry(%Entry{id: :foo, priority: 1})
      |> Level.put_entry(%Entry{id: :bar, priority: 2})
      |> Level.put_entry(%Entry{id: :baz, priority: 3})

    assert [%Entry{id: :foo, priority: 1}] = Level.get_highest_prio_entries(level)
  end

  test "put_entry/2 put new entry in entries, replacing if already existing" do
    level = %Level{}
    assert %{entries: entries} = level
    assert %{} == entries

    entry1 = %Entry{id: :foo, size: 5}
    level = Level.put_entry(level, entry1)
    assert %{entries: entries} = level
    assert %{foo: entry1} == entries

    entry2 = %Entry{id: :bar, size: 5}
    level = Level.put_entry(level, entry2)
    assert %{entries: entries} = level
    assert %{foo: entry1, bar: entry2} == entries

    entry3 = %Entry{id: :foo, size: 10}
    level = Level.put_entry(level, entry3)
    assert %{entries: entries} = level
    assert %{foo: entry3, bar: entry2} == entries
  end

  test "place_in_buffer/2 creates virtual entry if entries is empty" do
    level = Level.place_in_buffer(%Level{}, {0, 1, 2})
    assert %{entries: entries} = level
    assert %{nil: %Entry{is_virtual: true, key_range: {1, 1}, buffer: %{1 => {0, 2}}}} = entries
  end

  test "place_in_buffer/2 places data in the only entry that exists" do
    level = %Level{}
    entry = %Entry{id: :foo}

    level =
      level
      |> Level.put_entry(entry)
      |> Level.place_in_buffer({0, 1, 2})

    assert %{entries: entries} = level
    assert %{foo: %Entry{key_range: {1, 1}, buffer: %{1 => {0, 2}}}} = entries

    level = Level.place_in_buffer(level, {1, 2, 3})

    assert %{entries: entries} = level
    assert %{foo: %Entry{key_range: {1, 2}, buffer: %{1 => {0, 2}, 2 => {1, 3}}}} = entries
  end

  test "place_in_buffer/2 updates buffer with key in closest range" do
    level = %Level{}
    entry1 = %Entry{id: :foo, key_range: {0, 5}}
    entry2 = %Entry{id: :bar, key_range: {6, 10}}

    level =
      level
      |> Level.put_entry(entry1)
      |> Level.put_entry(entry2)

    level = Level.place_in_buffer(level, {1, 2, 3})
    assert %{entries: entries} = level

    assert %{
             foo: %Entry{key_range: {0, 5}, buffer: %{2 => {1, 3}}},
             bar: %Entry{key_range: {6, 10}, buffer: %{}}
           } = entries

    level = Level.place_in_buffer(level, {2, 7, 8})
    assert %{entries: entries} = level

    assert %{
             foo: %Entry{key_range: {0, 5}, buffer: %{2 => {1, 3}}},
             bar: %Entry{key_range: {6, 10}, buffer: %{7 => {2, 8}}}
           } = entries

    level = Level.place_in_buffer(level, {3, 11, 20})
    assert %{entries: entries} = level

    assert %{
             foo: %Entry{key_range: {0, 5}, buffer: %{2 => {1, 3}}},
             bar: %Entry{key_range: {6, 11}, buffer: %{7 => {2, 8}, 11 => {3, 20}}}
           } = entries
  end
end
