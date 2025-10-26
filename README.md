# Talon

An embedded LSM-Tree database for Elixir.

## Features

- **LSM-Tree architecture**: Write-optimized storage with efficient compaction
- **ACID transactions**: Snapshot isolation with conflict detection
- **Concurrent reads**: Multiple readers can access data simultaneously using RW locks
- **Write-ahead logging (WAL)**: Durability and crash recovery
- **Bloom filters**: Fast negative lookups to skip unnecessary SST scans
- **Automatic compaction**: Background merging of SST files across levels
- **Configurable limits**: Tune memory usage and compaction behavior

## Usage

Install by adding `:talon` as a dependency:
```elixir
def deps do
  [
    {:talon, "~> 0.1.0"}
  ]
end
```
Then run `mix deps.get`.

### Starting a database

```elixir
{:ok, db} = Talon.start_link(
  name: MyApp.DB,
  db_dir: "/path/to/db",
  key_limit: 50_000,
  level_limit: 128 * 1024 * 1024
)
```

Options:
- `name` - Registered name for the database supervisor (optional)
- `db_dir` - Directory path for storing database files (required)
- `key_limit` - Maximum keys in MemTable before flushing (default: 50,000)
- `level_limit` - Size threshold in bytes for level 0 compaction (default: 128 MB)

### Basic operations

```elixir
Talon.put(db, :user_123, %{name: "Alice", age: 30})

Talon.get(db, :user_123)

Talon.remove(db, :user_123)
```

### Batch operations

```elixir
Talon.put_multi(db, [
  {:user_1, %{name: "Alice"}},
  {:user_2, %{name: "Bob"}},
  {:user_3, %{name: "Charlie"}}
])

Talon.remove_multi(db, [:user_1, :user_2])
```

### Transactions

```elixir
Talon.transaction(db, fn tx ->
  case Talon.Tx.get(tx, :counter) do
    nil ->
      tx = Talon.Tx.put(tx, :counter, 1)
      {:commit, tx, 1}
    
    count ->
      tx = Talon.Tx.put(tx, :counter, count + 1)
      {:commit, tx, count + 1}
  end
end)
```

Transactions provide snapshot isolation. If a conflict is detected (another transaction modified the same keys), the transaction returns `{:error, :in_conflict}`.

### Using with a supervision tree

```elixir
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    children = [
      {Talon, name: MyApp.DB, db_dir: "/var/lib/myapp/db"}
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

Talon.put(MyApp.DB, :key, "value")
```

## How it works

Talon implements a Log-Structured Merge-Tree (LSM-Tree) with the following components:

- **Writer**: Manages in-memory MemTable and coordinates writes
- **Store**: Tracks SST files and provides read access
- **Compactor**: Merges SST files across levels to reduce read amplification
- **WAL**: Write-ahead log for durability
- **Manifest**: Tracks database state and file metadata
- **RWLocks**: Coordinates concurrent access to SST files

### Flushing

When the MemTable reaches the configured `key_limit`, the Writer initiates a flush:

1. **WAL rotation**: The current WAL file is rotated to preserve unflushed writes
2. **MemTable snapshot**: The current MemTable is frozen and a new empty one is created
3. **SST creation**: The frozen MemTable is sorted and written to one or more SST files at level 0
4. **Bloom filter generation**: A bloom filter is created for each SST to enable fast negative lookups
5. **Manifest update**: The new SST files are logged in the manifest
6. **WAL cleanup**: The rotated WAL file is deleted after successful flush
7. **Store registration**: SST files are registered in the Store for read access

Flushing happens asynchronously in a supervised task. Multiple flushes can be in progress simultaneously, and reads continue to access the flushing MemTables until they complete.

### Compacting

Compaction merges SST files to reduce read amplification and reclaim space:

1. **Level monitoring**: Each level tracks its total size and triggers compaction when exceeding `level_limit * 10^level_key`
2. **Source selection**: The highest priority (oldest) entries from the source level are selected
3. **Target merging**: Selected SSTs are merged with overlapping SSTs in the target level (level + 1)
4. **Sorted merge**: Entries are merged in sorted order, with newer entries (higher sequence numbers) taking precedence
5. **Tombstone cleanup**: Deleted entries (tombstones) are removed at the deepest level
6. **New SST creation**: Merged data is written to new SST files in the target level
7. **Manifest update**: Old files are marked for deletion and new files are logged
8. **File cleanup**: Old SST files are deleted after acquiring write locks

Compaction runs asynchronously and retries up to 5 times on failure. The compactor uses RW locks to ensure files aren't deleted while being read.

## SST file format

SST files use the `.seagoat` extension and follow this binary structure:

```
┌─────────────────┬──────────────┬─────────────────┐
│ DATA BLOCKS     │ SEPARATOR    │ FOOTER          │
│ (n × 512 bytes) │ (16 bytes)   │ (variable size) │
└─────────────────┴──────────────┴─────────────────┘
```

### Data blocks

Each block is 512 bytes and contains:
- **Block ID** (16 bytes): `"TALONBLOCK000000"`
- **Span** (2 bytes): Number of blocks this entry spans
- **Data** (variable): Erlang term encoding of `{sequence, key, value}`
- **Padding** (variable): Zero-filled to reach 512 bytes

### Footer

The footer contains metadata for efficient lookups:
- **Separator** (16 bytes): `"TALONSEP00000000"`
- **Bloom filter** (variable): Encoded bloom filter for membership testing
- **Key range** (variable): Min and max keys in the SST
- **Priority** (variable): Sequence number for ordering
- **Metadata** (56 bytes): Positions and sizes of footer components
- **Magic** (16 bytes): `"TALONFILE0000000"`

The metadata section stores:
- Level key (4 bytes)
- Bloom filter position and size (16 bytes)
- Key range position and size (16 bytes)
- Priority position and size (16 bytes)
- Number of blocks (8 bytes)
- Total file size (8 bytes)
- Data section size (8 bytes)

## References

- [RocksDB Documentation](https://github.com/facebook/rocksdb/wiki) - Facebook's fork of LevelDB with extensive optimizations
