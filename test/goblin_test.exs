defmodule GoblinTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  # setup c do
  #   {:ok, db} = Goblin.start_link(dir: c.tmp_dir, limit: 200)
  #   %{db: db}
  # end

  # test "put causes a flush", c do
  #   for n <- 1..1000 do
  #     Goblin.put(c.db, n, "v-#{n}")
  #   end
  #
  #   Process.sleep(500)
  #
  #   assert "v-1" == Goblin.get(c.db, 1)
  # end
end
