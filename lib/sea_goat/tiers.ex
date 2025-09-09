defmodule SeaGoat.Tiers do
  defstruct [
    :path,
    :mem_table,
    :bloom_filter,
    :state
  ]

  def new do
    %{}
  end

  def insert(tiers, tier, path, mem_table, bloom_filter, state) do
    entry = %__MODULE__{
      path: path,
      mem_table: mem_table,
      bloom_filter: bloom_filter,
      state: state
    }

    Map.update(tiers, tier, [entry], &[entry | &1])
  end

  def update(tiers, tier, updater) do
    tier_entries =
      tiers
      |> Map.get(tier, [])
      |> Enum.map(&updater.(&1))

    Map.put(tiers, tier, tier_entries)
  end

  def remove(tiers, tier, predicate) do
    tier_entries =
      tiers
      |> Map.get(tier, [])
      |> Enum.flat_map(fn entry ->
        if predicate.(entry) do
          []
        else
          [entry]
        end
      end)

    Map.put(tiers, tier, tier_entries)
  end

  def tiers(tiers) do
    tiers
    |> Map.keys()
    |> Enum.sort()
  end

  def get(tiers, tier, getter) do
    tiers
    |> Map.get(tier, [])
    |> Enum.find(&getter.(&1))
  end

  def get_all(tiers, tier, predicate, mapper \\ & &1) do
    tiers
    |> Map.get(tier, [])
    |> Enum.flat_map(fn entry ->
      if predicate.(entry) do
        [mapper.(entry)]
      else
        []
      end
    end)
  end
end
