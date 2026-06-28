defmodule Goblin.DiskTable.MemIndex do
  @moduledoc false

  @type t :: {non_neg_integer(), non_neg_integer(), list(), list()} | binary()

  @spec new() :: t()
  def new(), do: {0, 0, [], []}

  @spec append(t(), term(), non_neg_integer()) :: t()
  def append({n, off, dir, recs}, key, pos) do
    rec = :erlang.term_to_iovec({key, pos})
    size = :erlang.iolist_size(rec)
    {n + 1, off + size, [dir, <<off::unsigned-32>>], [recs, rec]}
  end

  @spec finalize(t()) :: t()
  def finalize({n, total, dir, recs}) do
    IO.iodata_to_binary([<<n::unsigned-32>>, dir, <<total::unsigned-32>>, recs])
  end

  @spec lookup_offset(t(), term()) :: non_neg_integer()
  def lookup_offset(<<0::unsigned-32, _::binary>>, _target), do: 0

  def lookup_offset(<<n::unsigned-32, _::binary>> = blob, target) do
    point = partition_point(blob, n, target, 0, n)
    {_key, pos} = entry_at(blob, n, max(point - 1, 0))
    pos
  end

  defp partition_point(_, _, _, lo, lo), do: lo

  defp partition_point(blob, n, target, lo, hi) do
    mid = div(lo + hi, 2)
    {key, _pos} = entry_at(blob, n, mid)

    if key <= target,
      do: partition_point(blob, n, target, mid + 1, hi),
      else: partition_point(blob, n, target, lo, mid)
  end

  defp entry_at(blob, n, i) do
    base = 4 + (n + 1) * 4
    <<start::unsigned-32, stop::unsigned-32>> = binary_part(blob, 4 + i * 4, 8)
    :erlang.binary_to_term(binary_part(blob, base + start, stop - start))
  end
end
