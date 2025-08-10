defmodule SeaGoatTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  setup c do
    {:ok, db} = SeaGoat.start_link(dir: c.tmp_dir, limit: 200)
    %{db: db}
  end

  test "put causes a flush", c do
    for n <- 1..1000 do
      SeaGoat.put(c.db, n, "v-#{n}")
    end

    Process.sleep(500)

    assert {:ok, "v-5"} == SeaGoat.get(c.db, 1)
  end
end
