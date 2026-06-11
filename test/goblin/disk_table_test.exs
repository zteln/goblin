defmodule Goblin.DiskTableTest do
  use ExUnit.Case, async: true
  use ExUnitProperties
  alias Goblin.DiskTable

  @moduletag :tmp_dir

  setup ctx do
    counter = :counters.new(1, [])

    filer = fn ->
      n = :counters.get(counter, 1)
      :counters.add(counter, 1, 1)
      Path.join(ctx.tmp_dir, "#{n}.goblin")
    end

    opts = [
      level_key: 0,
      compress?: false,
      max_size: :infinity,
      fpp: 0.01,
      bit_array_size: 100,
      filer: filer
    ]

    %{opts: opts}
  end

  describe "build/1" do
    test "writes stream to file", ctx do
      data = for n <- 0..10, do: {n, n, "val-#{n}"}
      assert {:ok, [dt]} = DiskTable.build(data, ctx.opts)
      assert File.exists?(dt.id)
      assert {0, 10} == dt.key_range
      assert {0, 10} == dt.seq_range
    end

    test "wraps stream into multiple files if exceeding :max_size", ctx do
      opts = Keyword.put(ctx.opts, :max_size, 1024)
      data = for n <- 0..10, do: {n, n, :binary.copy("x", 150)}
      assert {:ok, dts} = DiskTable.build(data, opts)
      assert length(dts) > 1

      Enum.each(dts, fn dt ->
        assert File.exists?(dt.id)
      end)
    end

    test "empty stream writes no files", ctx do
      files = File.ls!(ctx.tmp_dir)
      assert {:ok, []} == DiskTable.build([], ctx.opts)
      assert files == File.ls!(ctx.tmp_dir)
    end
  end

  describe "from_file/1" do
    test "correctly generates DiskTable struct from disk", ctx do
      {:ok, [dt]} = DiskTable.build([{0, 0, :foo}], ctx.opts)
      assert {:ok, dt} == DiskTable.from_file(dt.id)
    end

    test "returns error if file is corrupt", ctx do
      {:ok, [dt]} = DiskTable.build([{0, 0, :foo}], ctx.opts)

      {:ok, f} = :file.open(dt.id, [:read, :raw, :binary, :write])
      {:ok, _} = :file.position(f, dt.size)
      :ok = :file.truncate(f)
      :ok = :file.close(f)

      assert {:error, _} = DiskTable.from_file(dt.id)
    end

    test "raises if file is missing", ctx do
      {:ok, [dt]} = DiskTable.build([{0, 0, :foo}], ctx.opts)
      File.rm!(dt.id)

      assert_raise RuntimeError, fn ->
        DiskTable.from_file(dt.id)
      end
    end
  end

  describe "has_key?/2, search/3" do
    test "round-trips", ctx do
      {:ok, [dt]} = DiskTable.build([{:foo, 0, :bar}], ctx.opts)
      assert [{:foo, 0, :bar}] == DiskTable.search(dt, [:foo], 1) |> Enum.to_list()
    end

    test "correctly shows key membership", ctx do
      {:ok, [dt]} = DiskTable.build([{:foo, 0, :bar}], ctx.opts)
      assert DiskTable.has_key?(dt, :foo)
      refute DiskTable.has_key?(dt, :not_foo)
    end

    test "provides latest version of key within provided sequence (exclusive)", ctx do
      data =
        [
          {:foo, 0, :bar},
          {:a, 1, :a},
          {:b, 2, :b},
          {:foo, 3, :bar2}
        ]
        |> Enum.sort_by(fn {key, seq, _} -> {key, -seq} end)

      {:ok, [dt]} = DiskTable.build(data, ctx.opts)

      assert [{:foo, 0, :bar}] == DiskTable.search(dt, [:foo], 3) |> Enum.to_list()
    end

    test "can search for keys spanning multiple blocks", ctx do
      val = :binary.copy("X", 2048)

      data =
        for(n <- 0..10, do: {n, n, val})
        |> Enum.sort_by(fn {k, s, _} -> {k, -s} end)

      {:ok, [dt]} = DiskTable.build(data, ctx.opts)

      for n <- 0..10 do
        assert [{n, n, val}] == DiskTable.search(dt, [n], 11) |> Enum.to_list()
      end
    end

    test "correctly returns highest matching sequence", ctx do
      data = for(n <- 10..0//-1, do: {:key, n, "v-#{n}"})
      {:ok, [dt]} = DiskTable.build(data, ctx.opts)

      for n <- 0..10 do
        assert [{:key, n, "v-#{n}"}] == DiskTable.search(dt, [:key], n + 1) |> Enum.to_list()
      end
    end

    test "is lazy", ctx do
      data =
        for(n <- 0..10, do: {n, n, "val-#{n}"})
        |> Enum.sort_by(fn {key, seq, _} -> {key, -seq} end)

      {:ok, [dt]} = DiskTable.build(data, ctx.opts)
      assert [{0, 0, "val-0"}] == DiskTable.search(dt, [0, 5, 10], 11) |> Enum.take(1)
    end

    test "error causes raise", ctx do
      {:ok, [dt]} = DiskTable.build([{0, 0, :foo}], ctx.opts)

      # Corrupt the first data block's payload (past the 20-byte header) so its
      # stored CRC no longer matches -> FileIO.pread returns {:error, :invalid_crc}.
      {:ok, f} = :file.open(dt.id, [:read, :write, :raw, :binary])
      :ok = :file.pwrite(f, 30, <<255, 255, 255, 255>>)
      :ok = :file.close(f)

      assert_raise RuntimeError, fn ->
        DiskTable.search(dt, [0], 1) |> Enum.to_list()
      end
    end
  end

  describe "stream/2" do
    test "streams entire disk-table", ctx do
      data =
        [
          {:foo, 0, :bar},
          {:a, 1, :a},
          {:b, 2, :b},
          {:foo, 3, :bar2}
        ]
        |> Enum.sort_by(fn {key, seq, _} -> {key, -seq} end)

      {:ok, [dt]} = DiskTable.build(data, ctx.opts)

      assert data == DiskTable.stream(dt) |> Enum.to_list()
    end

    test "returns stream only if within bounds", ctx do
      data =
        for(n <- 0..10, do: {n, n, "val-#{n}"})
        |> Enum.sort_by(fn {key, seq, _} -> {key, -seq} end)

      {:ok, [dt]} = DiskTable.build(data, ctx.opts)

      assert data == DiskTable.stream(dt, bounds: {0, 10}) |> Enum.to_list()
      assert data == DiskTable.stream(dt, bounds: {-1, 11}) |> Enum.to_list()
      assert data == DiskTable.stream(dt, bounds: {5, 7}) |> Enum.to_list()
      assert [] == DiskTable.stream(dt, bounds: {-10, -1}) |> Enum.to_list()
      assert [] == DiskTable.stream(dt, bounds: {11, 12}) |> Enum.to_list()
    end

    test "streams up to (exclusive) provided sequence", ctx do
      data =
        for(n <- 0..10, do: {n, n, "val-#{n}"})
        |> Enum.sort_by(fn {key, seq, _} -> {key, -seq} end)

      {:ok, [dt]} = DiskTable.build(data, ctx.opts)

      assert Enum.take(data, 5) == DiskTable.stream(dt, seq: 5) |> Enum.to_list()
    end

    test "is lazy", ctx do
      data =
        for(n <- 0..10, do: {n, n, "val-#{n}"})
        |> Enum.sort_by(fn {key, seq, _} -> {key, -seq} end)

      {:ok, [dt]} = DiskTable.build(data, ctx.opts)
      assert [{0, 0, "val-0"}] == DiskTable.stream(dt) |> Enum.take(1)
    end

    test "error causes raise", ctx do
      {:ok, [dt]} = DiskTable.build([{0, 0, :foo}], ctx.opts)

      # Corrupt the first data block's payload (past the 20-byte header) so its
      # stored CRC no longer matches -> FileIO.read returns {:error, :invalid_crc}.
      {:ok, f} = :file.open(dt.id, [:read, :write, :raw, :binary])
      :ok = :file.pwrite(f, 30, <<255, 255, 255, 255>>)
      :ok = :file.close(f)

      assert_raise RuntimeError, fn ->
        DiskTable.stream(dt) |> Enum.to_list()
      end
    end
  end

  @tag :property_tests
  property "anything written can be read again (round-trip)", ctx do
    check all(
            triples <-
              list_of(triple_generator(), length: 10)
              |> map(&Enum.sort_by(&1, fn {key, seq, _} -> {key, -seq} end))
          ) do
      assert {:ok, [dt]} = DiskTable.build(triples, ctx.opts)

      Enum.each(triples, fn {key, _, _} ->
        assert DiskTable.has_key?(dt, key)
      end)

      keys = Enum.map(triples, &elem(&1, 0))
      {_, max_seq, _} = Enum.max_by(triples, &elem(&1, 1))
      search_stream = DiskTable.search(dt, keys, max_seq + 1)
      assert Enum.count(search_stream) > 0

      Enum.each(search_stream, fn {key, _, _} = triple ->
        assert triple in triples

        assert triple ==
                 triples
                 |> Enum.filter(fn {k, _, _} -> k == key end)
                 |> Enum.max_by(fn {_, s, _} -> s end)
      end)
    end
  end

  @tag :property_tests
  property "input order is preserved when searching", ctx do
    check all(
            triples <-
              list_of(triple_generator(), length: 10)
              |> map(&Enum.sort_by(&1, fn {key, seq, _} -> {key, -seq} end))
          ) do
      assert {:ok, [dt]} = DiskTable.build(triples, ctx.opts)
      {_, max_seq, _} = Enum.max_by(triples, &elem(&1, 1))
      keys = Enum.map(triples, &elem(&1, 0))
      assert keys == DiskTable.search(dt, keys, max_seq + 1) |> Enum.map(&elem(&1, 0))
    end
  end

  defp triple_generator do
    gen all(
          key <- term(),
          seq <- repeatedly(fn -> System.unique_integer([:positive, :monotonic]) end),
          val <- term()
        ) do
      {key, seq, val}
    end
  end
end
