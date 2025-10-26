defmodule TalonTest do
  use ExUnit.Case, async: true

  @moduletag :tmp_dir

  # setup c do
  #   {:ok, db} = Talon.start_link(dir: c.tmp_dir, limit: 200)
  #   %{db: db}
  # end

  # test "put causes a flush", c do
  #   for n <- 1..1000 do
  #     Talon.put(c.db, n, "v-#{n}")
  #   end
  #
  #   Process.sleep(500)
  #
  #   assert "v-1" == Talon.get(c.db, 1)
  # end
end
