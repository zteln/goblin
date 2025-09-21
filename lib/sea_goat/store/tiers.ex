defmodule SeaGoat.Store.Tiers do
  def new, do: %{}

  def insert(tiers, tier, entry) do
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

  def get_entry(tiers, tier, getter) do
    tiers
    |> Map.get(tier, :queue.new())
    |> :queue.to_list()
    |> Enum.find(&getter.(&1))
  end

  def get_all_entries(tiers, tier, predicate, mapper \\ & &1) do
    tiers
    |> Map.get(tier, :queue.new())
    |> :queue.to_list()
    |> Enum.flat_map(fn entry ->
      if predicate.(entry) do
        [mapper.(entry)]
      else
        []
      end
    end)
  end
end
