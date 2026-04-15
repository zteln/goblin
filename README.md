# Goblin

A lightweight, embedded database for Elixir.

## Features

- **ACID transactions** with serial write execution
- **Concurrent readers** that do not block each other
- **Crash recovery** via write-ahead logging
- **Automatic background compaction**
- **Bloom filters** for fast negative lookups
- **Any Elixir term** as key or value

## Usage

Install by adding `:goblin` as a dependency:
```elixir
def deps do
  [
    {:goblin, "~> 0.9.0"}
  ]
end
```
Then run `mix deps.get`.

### Starting a database

```elixir
{:ok, db} = Goblin.start_link(
  name: MyApp.DB,
  data_dir: "/path/to/db"
)
```

Options:
- `:name` - Registered name for the database (optional, defaults to `Goblin`)
- `:data_dir` - Directory path for database files (required)
- `:mem_limit` - Bytes to buffer in memory before flushing to disk (default: 64 MB)
- `:bf_fpp` - Bloom filter false positive probability (default: 0.01)

### Basic operations

```elixir
Goblin.put(db, :alice, "Alice")
# => :ok

Goblin.get(db, :alice)
# => "Alice"

Goblin.get(db, :nonexistent)
# => nil

Goblin.get(db, :nonexistent, default: :not_found)
# => :not_found

Goblin.remove(db, :alice)
# => :ok
```

> #### Key types {: .warning}
>
> A key can be any Elixir term, but mixing floats and integers
> with the same numeric value (e.g. both `1` and `1.0`) is undefined behaviour.
> Goblin uses ETS `:ordered_set` internally, which considers `1` and `1.0` as
> equal (`1 == 1.0`), while the Bloom filters use exact equality (`1 === 1.0` is
> `false`). This mismatch can cause keys to not be found or old versions to not
> be garbage collected during compaction. To avoid issues, use a consistent
> numeric type for your keys (e.g. always integers or always floats).

### Batch operations

```elixir
Goblin.put_multi(db, [{:alice, "Alice"}, {:bob, "Bob"}, {:charlie, "Charlie"}])
# => :ok

Goblin.get_multi(db, [:alice, :bob, :charlie])
# => [{:alice, "Alice"}, {:bob, "Bob"}, {:charlie, "Charlie"}]

Goblin.remove_multi(db, [:bob, :charlie])
# => :ok
```

### Range queries

```elixir
Goblin.scan(db) |> Enum.to_list()
# => [{:alice, "Alice"}, {:bob, "Bob"}, {:charlie, "Charlie"}]

Goblin.scan(db, min: :bob) |> Enum.to_list()
# => [{:bob, "Bob"}, {:charlie, "Charlie"}]

Goblin.scan(db, min: :alice, max: :bob) |> Enum.to_list()
# => [{:alice, "Alice"}, {:bob, "Bob"}]
```

### Transactions

There are two types of transactions: **write** and **read**.

Write transactions (`Goblin.transaction/2`) are executed serially.
The function must return `{:commit, tx, reply}` to commit or `:abort` to abort.

```elixir
Goblin.transaction(db, fn tx ->
  counter = Goblin.Tx.get(tx, :counter, default: 0)

  tx
  |> Goblin.Tx.put(:counter, counter + 1)
  |> Goblin.Tx.commit()
end)
# => :ok

Goblin.transaction(db, fn tx ->
  counter = Goblin.Tx.get(tx, :counter, default: 0)

  if counter < 100 do
    tx
    |> Goblin.Tx.put(:counter, counter + 1)
    |> Goblin.Tx.commit()
  else
    Goblin.Tx.abort(tx)
  end
end)
# => 2 (if committed) or {:error, :aborted} (if aborted)
```

Read transactions (`Goblin.read/2`) take a snapshot and do not block each other.
Attempting to write within a read transaction raises.

```elixir
Goblin.read(db, fn tx ->
  alice = Goblin.Tx.get(tx, :alice)
  bob = Goblin.Tx.get(tx, :bob)
  {alice, bob}
end)
# => {"Alice", "Bob"}
```

### Tags

Keys can be namespaced under a tag. Tagged and untagged data are separate:
reading without a tag returns only untagged data, and reading with a tag
returns only data under that tag.

```elixir
Goblin.put(db, :alice, "Alice", tag: :admins)
# => :ok

Goblin.get(db, :alice)
# => nil

Goblin.get(db, :alice, tag: :admins)
# => "Alice"

Goblin.scan(db, tag: :admins) |> Enum.to_list()
# => [{:alice, "Alice"}]

# Removing without a tag does not affect tagged data
Goblin.remove(db, :alice)
Goblin.get(db, :alice, tag: :admins)
# => "Alice"

# Remove with the matching tag
Goblin.remove(db, :alice, tag: :admins)
Goblin.get(db, :alice, tag: :admins)
# => nil
```

### Backups

A snapshot of the database can be exported as a `.tar.gz` archive.
The archive can be unpacked and used as the `data_dir` for a new instance.

```elixir
Goblin.export(db, "/backups")
# => {:ok, "/backups/goblin_20260220T120000Z.tar.gz"}
```

### Supervision tree

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Goblin, name: MyApp.DB, data_dir: "/var/lib/myapp/db"}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end

Goblin.put(MyApp.DB, :alice, "Alice")
# => :ok

Goblin.get(MyApp.DB, :alice)
# => "Alice"
```

## Disk table format

Database files use the `<number>.goblin` naming convention.

| SST | SEPARATOR | FOOTER |
| --- | --- | --- |
| n * 1024 bytes | 16 bytes  | variable size |

### Sorted String Table (SST)

The SST consists of multiple blocks.
Each block is a multiple of 1024 bytes and contains:
- **Block ID** (16 bytes): `"GOBLINBLOCK00000"`
- **Span** (2 bytes): Number of blocks this entry spans
- **Data** (variable): Erlang term encoding of `{sequence, key, value}`
- **Padding** (variable): Zero-filled to reach n * 1024 bytes

### Footer

The footer contains metadata for efficient lookups:
- **Separator** (16 bytes): `"GOBLINSEP0000000"`
- **Bloom filter** (variable): Encoded bloom filter for membership testing
- **Key range** (variable): Min and max keys in the SST
- **Sequence range** (variable): Lowest and highest sequence numbers
- **Metadata** (57 bytes): Positions and sizes of footer components
- **CRC** (32-bit): Checksum over SST, bloom filter, key range, sequence range and metadata
- **Magic** (16 bytes): `"GOBLINFILE000000"`

### Migration

Goblin does not perform automatic data migration between versions.
To upgrade, stream entries from the old database into a new instance running the newer version.

## Benchmarks

See [BENCHMARKS.md](https://github.com/zteln/goblin/blob/main/BENCHMARKS.md) for benchmark results comparing Goblin against CubDB.

## Transactional consistency checks

Goblin is checked for transactional consistency via [Elle](https://github.com/jepsen-io/elle) in [GoblinKVStore](https://github.com/zteln/goblin_kv_store).

## Too many open files?

If you see `:emfile` errors, the file descriptor limit has been reached.
Increase it with `ulimit -n <limit>`.

## References

- [CubDB](https://github.com/lucaong/cubdb) - Embedded COW B+Tree database
- [RocksDB Documentation](https://github.com/facebook/rocksdb/wiki) - Facebook's LSM key-value store
