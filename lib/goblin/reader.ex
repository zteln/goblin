defmodule Goblin.Reader do
  @moduledoc false
  use GenServer
  alias Goblin.SSTs
  alias Goblin.Writer
  alias Goblin.Store
  alias Goblin.Reader.Transaction

  @type reader :: module() | {:via, Registry, {module(), module()}}

  defstruct [
    :local_name,
    waiting: []
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = opts[:name]

    args = [
      local_name: opts[:local_name] || name
    ]

    GenServer.start_link(__MODULE__, args, name: opts[:name])
  end

  @spec empty?(reader()) :: :ok
  def empty?(reader) do
    GenServer.call(reader, :empty?)
  end

  @spec transaction(Writer.writer(), Store.store(), reader(), reader(), (Goblin.Tx.t() -> term())) ::
          term()
  def transaction(writer, store_table, reader_table, reader, f) do
    inc_reader(reader_table)
    seq = Writer.latest_commit_sequence(writer)
    tx = Transaction.new(seq, writer, store_table)
    f.(tx)
  after
    deinc_reader(reader_table, reader)
  end

  @spec get(Goblin.db_key(), Goblin.Writer.writer(), Goblin.Store.store(), Goblin.seq_no() | nil) ::
          {Goblin.seq_no(), Goblin.db_value()} | :not_found
  def get(key, writer, store, seq \\ nil) do
    case try_writer(writer, key, seq) do
      {:value, _seq, :"$goblin_tombstone"} ->
        :not_found

      {:value, seq, value} ->
        {seq, value}

      :not_found ->
        try_store(store, key, seq)
    end
  end

  @spec get_multi([Goblin.db_key()], Goblin.Writer.writer(), Goblin.Store.store()) :: [
          Goblin.triple()
        ]
  def get_multi(keys, writer, store) do
    {found, not_found} = try_writer(writer, keys)
    result = found ++ try_store(store, not_found)

    Enum.filter(result, fn
      {_seq, _key, :"$goblin_tombstone"} -> false
      :not_found -> false
      _ -> true
    end)
  end

  @spec select(
          Goblin.db_key() | nil,
          Goblin.db_key() | nil,
          Goblin.Writer.writer(),
          Goblin.Store.store()
        ) :: Enumerable.t(Goblin.pair())
  def select(min, max, writer, store) do
    Goblin.Iterator.stream_k_merge(
      fn ->
        mem_iterators = Writer.iterators(writer, min, max)
        sst_iterators = Store.iterators(store, min, max)
        [mem_iterators | sst_iterators]
      end,
      min: min,
      max: max
    )
  end

  @impl GenServer
  def init(args) do
    name = args[:local_name]
    :ets.new(name, [:named_table, :public])
    {:ok, %__MODULE__{local_name: name}}
  end

  @impl GenServer
  def handle_call(:empty?, from, state) do
    case :ets.lookup(state.local_name, :readers) do
      [] ->
        {:reply, :ok, state}

      _ ->
        waiting = [from | state.waiting]
        state = %{state | waiting: waiting}
        {:noreply, state}
    end
  end

  @impl GenServer
  def handle_cast(:empty_readers, state) do
    Enum.each(state.waiting, &GenServer.reply(&1, :ok))

    state = %{state | waiting: []}
    {:noreply, state}
  end

  defp inc_reader(table) do
    case :ets.lookup(table, :readers) do
      [] -> :ets.insert(table, {:readers, 1})
      [{_, n}] -> :ets.insert(table, {:readers, n + 1})
    end
  end

  defp deinc_reader(table, reader) do
    case :ets.lookup(table, :readers) do
      [] ->
        GenServer.cast(reader, :empty_readers)

      [{_, 1}] ->
        :ets.delete(table, :readers)
        GenServer.cast(reader, :empty_readers)

      [{_, n}] ->
        :ets.insert(table, {:readers, n - 1})
    end
  end

  defp try_writer(writer, keys, seq \\ nil)
  defp try_writer(writer, keys, _seq) when is_list(keys), do: Writer.get_multi(writer, keys)
  defp try_writer(writer, key, seq), do: Writer.get(writer, key, seq)

  defp try_store(store, keys, seq \\ nil)

  defp try_store(store, keys, seq) when is_list(keys) do
    Store.get_multi(store, keys)
    |> Task.async_stream(fn {key, ssts} ->
      case read_ssts(key, ssts, seq) do
        [] -> :not_found
        [{:value, _seq, :"$goblin_tombstone"}] -> :not_found
        [{:value, seq, value}] -> {key, seq, value}
      end
    end)
    |> Stream.filter(&match?({:ok, _}, &1))
    |> Stream.map(fn {:ok, res} -> res end)
    |> Enum.to_list()
  end

  defp try_store(store, key, seq) do
    {_key, ssts} = Store.get(store, key)

    case read_ssts(key, ssts, seq) do
      [] -> :not_found
      [{:value, _seq, :"$goblin_tombstone"}] -> :not_found
      [{:value, seq, value}] -> {seq, value}
    end
  end

  defp read_ssts(key, ssts, max_seq) do
    Task.async_stream(ssts, &SSTs.find(&1.file, key))
    |> Stream.map(fn {:ok, res} -> res end)
    |> Stream.filter(&match?({:ok, _}, &1))
    |> Stream.map(fn {:ok, res} -> res end)
    |> Stream.filter(fn
      {:value, _seq, _value} when is_nil(max_seq) -> true
      {:value, seq, _value} -> seq <= max_seq
      _ -> false
    end)
    |> Enum.sort_by(&elem(&1, 1), :desc)
    |> Enum.take(1)
  end
end
