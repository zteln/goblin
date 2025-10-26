defmodule Talon.ReaderTest do
  use ExUnit.Case, async: true
  use TestHelper
  alias Talon.Reader

  @moduletag :tmp_dir
  @db_opts [
    id: :reader_test,
    name: :reader_test,
    key_limit: 10,
    sync_interval: 50,
    wal_name: :reader_test_wal,
    manifest_name: :reader_test_manifest
  ]

  setup_db(@db_opts)

  test "get/3/4 gets existing keys", c do
    assert :ok == Talon.Writer.put(c.writer, 1, "v-1")
    assert {0, "v-1"} == Reader.get(1, c.writer, c.store)

    for n <- 1..10 do
      assert :ok == Talon.Writer.put(c.writer, n, "v-#{n}")
    end

    for n <- 1..10 do
      assert {n, "v-#{n}"} == Reader.get(n, c.writer, c.store)
    end
  end

  test "get/3/4 returns :not_found for non-existing keys", c do
    assert :not_found == Reader.get(:non_existing_key, c.writer, c.store)
  end
end
