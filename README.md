# Goblin

An embedded LSM-Tree database for Elixir.

## What is an LSM-Tree?

A Log-Structured Merge-Tree (LSM-Tree) is a data structure optimized for write-heavy workloads. 
Instead of updating data in place, writes are first appended to a write-ahead log (WAL) for durability, then to an in-memory structure (MemTable).
When the MemTable fills up, it's flushed to disk as an immutable sorted file (SST).
Reads check the MemTable first, then search SST files from newest to oldest.
Background compaction merges SST files to reduce read amplification and reclaim space from deleted entries.
This design provides excellent write throughput while maintaining reasonable read performance through bloom filters and sorted data structures.

## Features

- **LSM-Tree architecture**: Write-optimized storage with efficient compaction
- **ACID transactions**: True serial execution of transactions
- **Concurrent reads**: Multiple readers can access data simultaneously
- **Write-ahead logging (WAL)**: Durability and crash recovery
- **Bloom filters**: Fast negative lookups to skip unnecessary SST scans
- **Automatic compaction**: Background merging of SST files across levels
- **Configurable limits**: Tune memory usage and compaction behavior

## Usage

Install by adding `:goblin` as a dependency:
```elixir
def deps do
  [
    {:goblin, "~> 0.1.0"}
  ]
end
```
Then run `mix deps.get`.

### Starting a database

```elixir
{:ok, db} = Goblin.start_link(
  name: MyApp.DB,
  db_dir: "/path/to/db",
  key_limit: 50_000,
  level_limit: 128 * 1024 * 1024,
  bf_fpp: 0.05
)
```

Options:
- `name` - Registered name for the database supervisor (optional)
- `db_dir` - Directory path for storing database files (required)
- `key_limit` - Maximum keys in MemTable before flushing (default: 50,000)
- `level_limit` - Size threshold in bytes for level 0 compaction (default: 128 MB)
- `bf_fpp` - Bloom filter false positive probability (default: 0.05)

### Basic operations

```elixir
Goblin.put(db, :user_123, %{name: "Alice", age: 30})
# => :ok

Goblin.get(db, :user_123)
# => %{name: "Alice", age: 30}

Goblin.get(db, :nonexistent)
# => nil

Goblin.get(db, :nonexistent, :not_found)
# => :not_found

Goblin.remove(db, :user_123)
# => :ok
```

### Batch operations

```elixir
Goblin.put_multi(db, [
  {:user_1, %{name: "Alice"}},
  {:user_2, %{name: "Bob"}},
  {:user_3, %{name: "Charlie"}}
])
# => :ok

Goblin.get_multi(db, [:user_1, :user_2, :user_3])
# => [{:user_1, %{name: "Alice"}}, {:user_2, %{name: "Bob"}}, {:user_3, %{name: "Charlie"}}]

Goblin.remove_multi(db, [:user_1, :user_2])
# => :ok
```

### Range queries

```elixir
Goblin.put_multi(db, [
  {1, "one"},
  {2, "two"},
  {3, "three"},
  {4, "four"},
  {5, "five"}
])

Goblin.select(db, min: 2, max: 4) |> Enum.to_list()
# => [{2, "two"}, {3, "three"}, {4, "four"}]

Goblin.select(db, min: 3) |> Enum.to_list()
# => [{3, "three"}, {4, "four"}, {5, "five"}]

Goblin.select(db, max: 2) |> Enum.to_list()
# => [{1, "one"}, {2, "two"}]
```

### Transactions

```elixir
alias Goblin.Tx

Goblin.transaction(db, fn tx ->
  count = Tx.get(tx, :counter, 0)
  tx = Tx.put(tx, :counter, count + 1)
  {:commit, tx, count + 1}
end)
# => 1

Goblin.transaction(db, fn tx ->
  balance = Tx.get(tx, :account_balance, 0)
  
  if balance >= 100 do
    tx = Tx.put(tx, :account_balance, balance - 100)
    {:commit, tx, :ok}
  else
    :cancel
  end
end)
# => :ok (if cancelled) or :ok (if committed)

Goblin.transaction(db, fn tx -> 
  root = Tx.get(tx, :root)
  next = Tx.get(tx, next) 
  Tx.get(tx, next)
end, read_only: true)
# => next
```

There are two types of transactions: read and write.

Read transactions do not block each other and are not allowed to write anything (causes a raise).

Write transactions are executed in serial order. 
Writers are blocked until the current writer completes its transaction.

### PubSub

Processes can subscribe to database writes.
When writes are committed to the database MemTable then subscribers receive either `{:put, key, value}` or `{:remove, key}`.

```elixir
Goblin.subscribe(db)
# => :ok

Goblin.unsubscribe(db)
# => :ok
```

### Back ups
A snapshot of the database can be exported in a `.tar.gz` file.
This file can be unpacked if the database needs to restored at a later time, acting as a backup.

```elixir
Goblin.export(db, "path/to/back_up_dir/")
# => {:ok, back_up}
```

### Using with a supervision tree

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Goblin, name: MyApp.DB, db_dir: "/var/lib/myapp/db"}
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

Goblin.put(MyApp.DB, :key, "value")
# => :ok

Goblin.get(MyApp.DB, :key)
# => "value"
```

## How it works

Goblin implements a Log-Structured Merge-Tree (LSM-Tree) with the following components:

- **Writer**: Manages in-memory MemTable and coordinates writes
- **Store**: Tracks SST files and provides read access
- **Compactor**: Merges SST files across levels to reduce read amplification
- **WAL**: Write-ahead log for durability
- **Manifest**: Tracks database state and file metadata

### Flushing

When the MemTable reaches the configured `key_limit`, the Writer initiates a flush:

1. **WAL rotation**: The current WAL file is rotated to preserve unflushed writes
2. **SST creation**: The frozen MemTable is sorted and written to one or more SST files at level 0
3. **Bloom filter generation**: A bloom filter is created for each SST to enable fast negative lookups
4. **Manifest update**: The new SST files are logged in the manifest
5. **WAL cleanup**: The rotated WAL file is deleted after successful flush
6. **Store registration**: SST files are registered in the Store for read access

Flushing happens asynchronously in a supervised task.
Only a single flush can occur at a given point in time.

### Compacting

Compaction merges SST files to reduce read amplification and reclaim space:

1. **Level monitoring**: Each level tracks its total size and triggers compaction when exceeding `level_limit * 10^level_key`
2. **Source selection**: The highest priority (oldest) entries from the source level are selected
3. **Target merging**: Selected SSTs are merged with overlapping SSTs in the target level (level + 1)
4. **Sorted merge**: Entries are merged in sorted order, with newer entries (higher sequence numbers) taking precedence
5. **Tombstone cleanup**: Deleted entries (tombstones) are removed at the deepest level
6. **New SST creation**: Merged data is written to new SST files in the target level
7. **Manifest update**: Old files are marked for deletion and new files are logged
8. **File cleanup**: Old SST files are deleted

Compaction runs asynchronously and retries up to 5 times on failure.
Data is compressed for levels larger than 1.

## SST file format

SST files use the `<no>.goblin` format and follow this binary structure:

| DATA BLOCKS | SEPARATOR | FOOTER |
| --- | --- | --- |
| n * 512 bytes | 16 bytes | variable size |


### Data blocks

Each block is a multiple of 512 bytes and contains:
- **Block ID** (16 bytes): `"GOBLINBLOCK00000"`
- **Span** (2 bytes): Number of blocks this entry spans
- **Data** (variable): Erlang term encoding of `{sequence, key, value}`
- **Padding** (variable): Zero-filled to reach n * 512 bytes

### Footer

The footer contains metadata for efficient lookups:
- **Separator** (16 bytes): `"GOBLINSEP0000000"`
- **Bloom filter** (variable): Encoded bloom filter for membership testing
- **Key range** (variable): Min and max keys in the SST
- **Priority** (variable): Sequence number for ordering
- **Metadata** (80 bytes): Positions and sizes of footer components
- **Magic** (16 bytes): `"GOBLINFILE000000"`

The metadata section stores:
- Level key (4 bytes)
- Bloom filter position and size (16 bytes)
- Key range position and size (16 bytes)
- Sequence range position and size (16 bytes)
- Number of blocks (8 bytes)
- Total file size (8 bytes)
- Data section size (8 bytes)
- CRC (4 bytes)

## References

- [CubDB](https://github.com/lucaong/cubdb) - Embedded COW B+Tree database
- [RocksDB Documentation](https://github.com/facebook/rocksdb/wiki) - Facebook's LSM key-value store
