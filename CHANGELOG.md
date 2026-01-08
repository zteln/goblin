# Changelog

## v0.4.1 (2026-01-08)
* Bug fixes
    * Fix recursion bug in binary search iterator algorithm for disk tables.

## v0.4.0 (2026-01-06)
* Breaking changes
    * Changed disk table encoding format (not compatible with previous versions)
    * `Goblin.is_flushing/1`, `Goblin.is_compacting/1` is changed to `Goblin.is_flushing?/1`, `Goblin.is_compacting?/1`, respectively.
    * `Goblin.transaction/3` is split into `Goblin.transaction/2` and `Goblin.read/2`.
* Improvements
    * A lot more focus on consistency guarantees for transaction operations.
    * `Goblin.{put_multi/2, remove_multi/2, get_multi/2, select/2}` are now available.

## v0.3.2 (2025-12-04)
* Enhancements
    * Flush and compaction are now triggered via byte sizes instead of amount of keys
* Bug fixes
    * Add functionality for iterators to clean up after iteration ends
    * Fix key range computation when compacting from flush level to higher level

## v0.3.1 (2025-11-23)
* Bug fixes
    * Writes are published on a database specific topic instead of a global topic
    * Compaction algorithm rewritten, fixes overlapping key ranges in higher SST levels

## v0.3.0 (2025-11-21)
* Breaking changes
    * Changed the format in the manifest file
* Improvements
    * Make WAL follow the manifest as the source of truth
* Enhancements
    * Added `Goblin.export/2` which exports a `.tar.gz` of a snapshot of the database to a specified directory

## v0.2.2 (2025-11-16)
* Enhancements
    * Added `Goblin.stop/3` function, allows one to stop the database supervisor
    * Added read-only transactions in `Goblin.transaction/2`
* Bug fixes
    * Fix registry name in `Goblin.is_flushing/1` and `Goblin.is_compacting/1`
    * Fix sequence counting bug when write-transaction commits

## v0.2.1 (2025-11-13)
* Enhancements
    * Add checksum verification to SST files
    * Data and metadata are compressed in SSTs for levels > 1

## v0.2.0 (2025-11-12)
* Enhancements
    * The transaction model is changed to true serial execution. 
    * The MemTable and Store utilize ETS tables to allow concurrent reads.
    * Bloom filter false positive probability is configurable
* Bug fixes
    * Change how hashes are stored in BloomFilter structs. Anonymous functions are no longer stored directly in the struct, instead the salt and range for the hash is stored instead. This prevents `BadFunctionError` if the module is updated.

## v0.1.5 (2025-11-02)
* Improvements
    * Improved SST filtering from store
* Enhancements
    * Add benchmarking scripts

## v0.1.4 (2025-11-01)
* Enhancements
    * Add registry for process discovery within the database server
    * Add registry for pubsub functionality within the database server
    * Add new supervisor for processes separate from registry processes
* Bug fixes
    * Provide name scopes to `WAL` and `Manifest`. This fixes the bug that multiple database servers couldn't start simultaneously.

## v0.1.3 (2025-10-29)
* Enhancements
    * Add `is_flushing/1` and `is_compacting/1` functions
    * Refactor `Reader.select/4` stream

## v0.1.2 (2025-10-29)
* Enhancements
    * Add `select/1/2` for range queries

## v0.1.1 (2025-10-27)
* Enhancements
    * Add `get_multi/2` functionality

## v0.1.0 (2025-10-26)
* Enhancements
    * First public release
