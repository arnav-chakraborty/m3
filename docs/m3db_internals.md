# M3DB Internals

## Write Path

The M3DB write path is designed for high throughput and durability. It involves several key components working together to ingest, process, and store time-series data efficiently. This section outlines the major stages of the write path, from receiving data to making it queryable.

### Commit Log
The commit log in M3DB serves as a write-ahead log (WAL) to ensure data durability and enable recovery from crashes or restarts. All incoming writes are first appended to the commit log before being acknowledged to the client (if using the `StrategyWriteWait` strategy) or processed further. This guarantees that even if a node crashes before data is fully persisted to long-term storage (fileset files), acknowledged writes can be recovered by replaying the commit log.

**Recording Writes:**
Incoming writes, whether single data points (`Write` method) or batches (`WriteBatch` method), are sent to an internal queue (`writes` channel within the `commitLog` struct). A dedicated background goroutine (`commitLog.write()`) consumes from this queue. For each write operation, it serializes essential information to the active commit log file. This is performed by the `commitLogWriter.Write()` method, which is part of the `primary` writer in the `writerState`. The choice of `StrategyWriteWait` or `StrategyWriteBehind` (configured via `Options.Strategy()`) determines if the client call blocks until the commit log write is flushed (`writeWait` method) or returns after enqueuing (`writeBehind` method). A `maxQueueSize` limits the backlog.

**Information Stored:**
For each data point, the commit log stores the following (as passed to `commitLogWriter.Write()`):
-   **Series (`ts.Series`):** Contains the unique ID and tags identifying the time series.
-   **Datapoint (`ts.Datapoint`):** Includes the timestamp and the value of the data point.
-   **Unit (`xtime.Unit`):** Specifies the time unit of the datapoint's timestamp (e.g., nanoseconds, milliseconds).
-   **Annotation (`ts.Annotation`):** Any associated metadata or annotations for the datapoint.

**Bootstrapping and Recovery:**
During node startup (bootstrapping) or when recovering from a crash, M3DB replays entries from its commit log files. This process reconstructs the in-memory state (primarily data in buffers that hadn't yet been flushed to filesets) and ensures that no acknowledged writes are lost. The `commitLog.Open()` method initializes the commit log system, including setting up the writers and preparing for operations. While `Open()` itself doesn't perform the full replay, it makes the commit log ready for the bootstrap process to read existing log files (identified using `ActiveLogs()`) using a `commitLogReader`.

**Rotation, Flushing, and Retention:**
M3DB manages commit log files to control disk space usage and limit recovery times:
-   **Primary and Secondary Writers:** The `commitLog.writerState` maintains two `asyncResettableWriter` instances: `primary` and `secondary`. The `primary` writer is used for all current write operations.
-   **Rotation (`RotateLogs`, `openWriters`):**
    -   Rotation is initiated by calling `RotateLogs()`. The core logic resides in `openWriters()`.
    -   The `primary` and `secondary` writers are swapped. The previous `primary` (now `secondary`) is closed, ensuring all its buffered data is flushed to its file.
    -   The new `primary` (previously `secondary`) either uses its pre-opened file or opens a new one. New commit log files are named with an incrementing index (e.g., `CommitLogFilePath(fsPrefix, int(nextIndex))`).
    -   The new `secondary` (the old `primary`) is then reset asynchronously (`startSecondaryWriterAsyncReset`): its file is closed, and a new file is opened and prepared, making it ready to become the `primary` in the next rotation. This ensures there's always a "hot standby" writer.
-   **Active Files (`ActiveLogs`):** This method returns a list of `persist.CommitLogFile` structures representing the current commit log files being managed (`writerState.activeFiles`).
-   **Flushing:**
    -   Data is explicitly flushed to disk when a commit log file is closed (e.g., during rotation).
    -   If `FlushInterval` is set in `Options`, a background goroutine (`flushEvery`) periodically requests a flush by sending a `flushEventType` to the `writes` channel, which then calls `primary.writer.Flush(false)`.
    -   For `StrategyWriteWait`, writes are flushed before acknowledging to the client, ensuring durability. The `onFlush` callback (invoked by the writer) is crucial for signaling completion of these synchronous flushes.
-   **Retention:** The `commitlog` package itself focuses on file creation, rotation, and providing access to active logs. The actual deletion of old commit log files (retention) is typically handled by a higher-level component within M3DB. This component monitors which commit logs contain data that has been successfully flushed to persistent fileset files and are therefore no longer needed for recovery.

The commit log is a fundamental component for ensuring data integrity and recoverability in M3DB.

### In-Memory Buffers
After being written to the commit log (for durability), incoming data points are directed to in-memory buffers. These buffers are crucial for aggregating writes, serving recent data quickly, and organizing data before it's flushed to disk. Each time series (`dbSeries` in `src/dbnode/storage/series/series.go`) in M3DB maintains its own in-memory buffer. The `dbSeries` struct holds a `buffer` field of type `databaseBuffer`.

**Structure of In-Memory Buffers (`databaseBuffer` and `BufferBucket`s):**
The core structure for in-memory data is the `databaseBuffer` interface, implemented by `dbBuffer` (in `src/dbnode/storage/series/buffer.go`). This buffer organizes data by time windows, specifically by block start times determined by the datapoint's timestamp and the configured block size (from `RetentionOptions`).
-   **`bucketsMap`**: The `dbBuffer` uses a map called `bucketsMap` where keys are `xtime.UnixNano` (block start timestamps) and values are `*BufferBucketVersions`.
-   **`BufferBucketVersions`**: This structure holds potentially multiple versions of data for the *same* block start time. It contains a slice of `*BufferBucket`. Versioning is key to:
    -   Handling out-of-order writes.
    -   Merging data during reads or flushes.
    -   Differentiating between data that's actively being written (`WarmWrite`) versus older data or backfills (`ColdWrite`).
    -   Managing the lifecycle of data as it gets flushed (e.g., a flushed warm write bucket gets version 1).
-   **`BufferBucket`**: Each `BufferBucket` represents a specific version of data for a particular block start and `WriteType` (Warm or Cold). It contains:
    -   `encoders`: A slice of `inOrderEncoder`. Each `inOrderEncoder` wraps an actual `encoding.Encoder` (typically an M3TSZ encoder) and stores the `lastWriteAt` timestamp for that encoder. New writes for a series within a block are generally appended to the current writable encoder. Multiple encoders can exist within a single `BufferBucket` due to:
        -   Upserts: If a datapoint arrives for an existing timestamp with a different value, a new encoder might be created to store this new value, preserving the immutability of prior encoder data.
        -   Out-of-order data: Data arriving out of chronological sequence might also lead to new encoders.
    -   `loadedBlocks`: A slice of `block.DatabaseBlock`. These are blocks of data that were previously flushed to disk filesets and have been read back into memory. This can happen during bootstrapping (to populate recent data not yet in filesets but covered by commit logs) or if a series is queried for data that was previously evicted from memory. The `dbSeries.LoadBlock()` method handles adding these blocks into the buffer.

**Applying Writes (`dbSeries.Write` and `dbBuffer.Write`):**
1.  **Write Type Determination:** When `dbSeries.Write()` is called, it first determines if the write is a `WarmWrite` or a `ColdWrite` based on the timestamp relative to the `BufferPast` and `BufferFuture` windows (from `RetentionOptions`). If `ColdWritesEnabled` is false, out-of-window writes are rejected. Bootstrap writes (`WriteOptions.BootstrapWrite`) have special initial handling via `dbSeries.bootstrapWrite`, which uses a separate `dbSeriesBootstrap.buffer`. This temporary buffer is later merged into the main `dbSeries.buffer` during the `dbSeries.Bootstrap()` call.
2.  **Block Start Calculation:** The target block start time is calculated by truncating the datapoint's timestamp to the namespace's block size.
3.  **Accessing/Creating Buckets:** `dbBuffer.Write()` then calls `bucketVersionsAtCreate(blockStart)` to get or create the `BufferBucketVersions` for that block. If new, it's added to `bucketsMap` and `inOrderBlockStarts` (a sorted slice for efficient iteration).
4.  **Writing to `BufferBucket`:** Within `BufferBucketVersions`, `writableBucketCreate(writeType)` gets or creates a `BufferBucket` for the specific `WriteType` with `version == writableBucketVersion` (0).
5.  **Encoding:** The `BufferBucket.write()` method then appends the datapoint (`timestamp`, `value`, `unit`, `annotation`) to an `inOrderEncoder`. It finds an appropriate encoder or creates a new one if necessary (e.g., for an upsert on an existing timestamp or if too many encoders already exist, respecting `EncodersPerBlockLimit`). The `firstWrite` field in `BufferBucket` records when the bucket first received a write.

**Mutable and Immutable Buffers/Versions:**
-   **Mutable (Writable):** A `BufferBucket` with `version == writableBucketVersion` (typically 0) is considered mutable and accepts new incoming writes for its specific `WriteType`.
-   **Immutable (Post-Flush):**
    -   When a `WarmFlush` is successful for a `BufferBucket` containing warm writes, its `version` is updated to `1`.
    -   For cold writes, when `FetchBlocksForColdFlush` is called, the `version` of the cold write `BufferBucket` is updated to the `version` parameter provided by the flush process.
    -   These versioned buckets become immutable for new writes of that type for that flush cycle.
-   The `Tick` process in `dbBuffer` (`dbBuffer.Tick()`) is responsible for cleaning up these buckets. It iterates through `bucketsMap` and calls `buckets.removeBucketsUpToVersion(writeType, version)` based on the `ShardBlockStateSnapshot` which indicates what data is retrievable from disk. If all versions/data within a `BufferBucketVersions` are removed (i.e., `buckets.streamsLen() == 0`), the entire entry for that block start is removed from `bucketsMap`.

**Data Versioning (`BufferBucketVersions`):**
`BufferBucketVersions` is central to managing different states of data for the same block start:
-   It holds separate `BufferBucket` instances for `WarmWrite` and `ColdWrite` data.
-   It can hold multiple `BufferBucket`s of the same `WriteType` but different `version` numbers, representing data at different stages of its lifecycle (e.g., a version 0 warm write bucket being actively written to, and a version 1 warm write bucket that has been flushed).
-   The `Tick` process merges encoders within a bucket (`bucket.merge()`) to consolidate data and reclaims space. When data is read or prepared for flushing (`Snapshot` or `WarmFlush`), `mergeToStreams` or `mergeToStream` methods are used to combine data from different encoders and `loadedBlocks` within and across buckets.

**Duration in Memory and Flushing Triggers:**
Data for a specific block window is held in the `databaseBuffer` until:
-   **Warm Writes:** A block becomes eligible for a `WarmFlush` (via `dbSeries.WarmFlush`) when its time window has passed (i.e., it's no longer within `BufferFuture` and is older than `BufferPast` relative to current time). The `flushManager` typically triggers this.
-   **Cold Writes:** Cold writes are flushed via `dbSeries.FetchBlocksForColdFlush` (which calls `dbBuffer.FetchBlocksForColdFlush`) or as part of a `dbSeries.Snapshot`. These are often part of a broader compaction or snapshotting cycle.
-   **Snapshot:** `dbSeries.Snapshot` (calling `dbBuffer.Snapshot`) creates a merged view of all data (warm, cold, loaded blocks) for a block start, typically used after commit log rotation.
-   Once data is successfully persisted to disk and the `ShardBlockStateSnapshot` reflects this, the `dbBuffer.Tick()` process evicts the corresponding in-memory data by removing or cleaning out the relevant `BufferBucket`s or `BufferBucketVersions`. If a `BufferBucketVersions` becomes empty, it's removed from `bucketsMap`.

### Encoding and Compression
M3DB employs specialized encoding techniques for time series data stored in memory to optimize for space and access speed. This is crucial for handling high-throughput writes and efficient querying of recent data.

**Primary Encoding Scheme: M3TSZ**
The primary encoding scheme used for time series data in M3DB is M3TSZ. It's a highly optimized, lossless encoding algorithm specifically designed for time series data. When data points are written to an in-memory buffer, they are encoded using an M3TSZ encoder (`m3tsz.encoder`).

**How M3TSZ Works (High-Level):**
M3TSZ achieves high compression ratios by leveraging the typical characteristics of time series data:
-   **Timestamps:** Timestamps often have regular intervals. M3TSZ uses a delta-of-delta encoding scheme combined with XOR operations. Instead of storing full timestamps, it stores the difference (delta) from the previous timestamp. Then, it calculates the delta of these deltas and XORs it with the previous delta-of-delta. This often results in many leading zeros, which can be efficiently bit-packed. The `TimestampEncoder` within `m3tsz/encoder.go` manages this.
-   **Values (Floats and Integers):**
    -   **Integer Optimization:** M3TSZ attempts to convert float values that are whole numbers (or very close to whole numbers within a certain precision) into integers. These integers, along with a multiplier (e.g., to handle `46.0` as `46` with multiplier `1`, or `4.6` as `46` with multiplier `10`), are then encoded. This is handled by `convertToIntFloat` and related logic. It uses opcodes like `opcodeIntMode` and `opcodeFloatMode` to switch between integer and float encoding.
    -   **Float Encoding:** For true float values (or when integer optimization isn't applicable), M3TSZ uses XOR-based compression similar to the approach used in Facebook's Gorilla system. The XOR of the current float's bits with the previous float's bits often results in many leading and trailing zeros, which can be compressed by storing only the significant bits. The `FloatEncoderAndIterator` handles this.
    -   **Value Changes:** It uses opcodes to signify if a value is repeated (`opcodeRepeat`), or if the difference from the previous value is zero (`opcodeZeroValueXOR`), can be stored in the existing bit-width (`opcodeContainedValueXOR`), or requires a new bit-width (`opcodeUncontainedValueXOR`).

**Benefits of M3TSZ:**
-   **High Compression Ratios:** Specifically tailored for time series patterns, leading to significant memory savings.
-   **Efficient Encoding/Decoding:** Designed for speed, crucial for high-throughput write and read paths.
-   **Lossless:** No data precision is lost during the encoding/decoding process.

**Schema Utilization:**
The M3TSZ encoder (`m3tsz.encoder`) has a `SetSchema(descr namespace.SchemaDescr)` method. While the current core M3TSZ implementation primarily focuses on timestamp and float/integer values, the passing of schema information (defined per namespace via `namespace.Options` and `SchemaHistory`) allows for future extensions or different encoding strategies based on data types defined in a schema (e.g., for typed metrics). For standard time series data, the schema's primary role might be less about influencing the core M3TSZ algorithm and more about metadata management, but the hook is there.

**Other Encoding Schemes:**
While M3TSZ is the workhorse for time series values, M3DB also handles annotations. The `TimestampEncoder` within M3TSZ handles annotations alongside timestamps, implying they are interleaved in the encoded stream but not necessarily M3TSZ encoded themselves in the same way as primary data values. The `MarkerEncodingScheme` mentioned in `encoding/options.go` is used for stream markers rather than the primary time series data. For the context of time series data in buffers, M3TSZ is the dominant scheme.

**Encoding as Compression:**
M3TSZ itself is a compression technique. Its design inherently reduces the amount of memory required to store time series data by removing redundancy in timestamp and value representations. For in-memory data, this encoding provides the primary means of compression. Further generic compression algorithms (like zstd) are typically applied when data is flushed to disk, rather than on the hot data in memory.

### Flushing to Disk
Data accumulated in memory buffers is periodically flushed to disk to ensure persistence beyond the life of a node and to free up memory. This process involves creating a set of immutable files on disk known as a "fileset".

**Triggers for Flushing:**
Flushing is primarily time-driven, managed by the `flushManager` within the database.
-   **Block Sealing:** When a time block (defined by `blockSize` in retention options, e.g., a 2-hour window) is considered "sealed" (i.e., its time window has passed and it's no longer actively receiving new writes for the warm path according to retention options), it becomes eligible for flushing. The `flushManager.Flush` method, called periodically by the database's ticking mechanism, determines which blocks need flushing based on their start times and the retention options of their namespace (via `databaseNamespace.NeedsFlush`).
-   **Snapshotting:** Snapshots are a comprehensive form of flush. The `flushManager.dataSnapshot` process is triggered after a commit log rotation. This ensures all data in memory for given blocks—potentially including both warm (recent) and cold (older/backfilled) writes—is persisted.

**Distinction Between Warm Flush and Snapshot:**
-   **Warm Flush (`flushManager.dataWarmFlush`):** This targets data from the "warm" write path, which is typically the most recent data actively buffered in memory. For each series, `databaseSeries.WarmFlush` prepares data from the mutable (writable) portion of its in-memory buffer for a specific block.
-   **Snapshot (`flushManager.dataSnapshot`):** This is a more encompassing persistence operation. `databaseSeries.Snapshot` creates a point-in-time, merged view of *all* data within a series' buffer for a given block start. This includes warm writes, any pending cold writes, and data from previously loaded blocks that are still in memory. Snapshots are explicitly linked to commit log rotation via a `rotatedCommitlogID`.

**Process of Preparing Data for Flush/Snapshot:**
1.  **Identification:** The `flushManager` identifies which namespaces and block starts require flushing or snapshotting.
2.  **Data Preparation (per series):**
    *   `databaseSeries.WarmFlush` (for warm flushes) or `databaseSeries.Snapshot` (for snapshots) is called.
    *   These methods operate on the `databaseBuffer`, merging relevant in-memory encoders and any loaded blocks into a single, encoded `ts.Segment` for that series and block start. This segment represents all the data for that series in that time window.
3.  **Persistence Operation:** The prepared `ts.Segment`, its associated metadata (series ID, tags), and a checksum are passed to the `persist.Manager` (implemented by `fs.persistManager`).
    *   `persistManager.PrepareData` then sets up the necessary file writers (e.g., `fs.streamingWriter` or a similar component) for the specific namespace, shard, block start, and a volume index (which helps differentiate multiple filesets for the same block, especially for cold data resharding or repairs).
    *   The `persistManager.persist` method takes the `ts.Segment` and orchestrates its writing to the data file.

**How Data is Written to Disk (Fileset Files):**
The `fs.persistManager`, using components like `fs.streamingWriter`, writes data into a set of files known as a fileset. Each fileset pertains to a specific block of time for a shard within a namespace. The key files in a data fileset are:
-   **Info File (`info.db`):** Stores metadata about the fileset, such as the number of series, block start time, bloom filter parameters, and summary information.
-   **Data File (`data.db`):** Contains the actual M3TSZ-encoded time series data segments.
-   **Index File (`index.db`):** A sorted index mapping series IDs (and potentially tags) to their data offsets in the data file, enabling quick lookups.
-   **Summaries File (`summaries.db`):** A downsampled version of the index, allowing for faster scanning over ranges of series IDs during queries.
-   **Bloom Filter File (`bloomfilter.db`):** A probabilistic data structure containing series IDs, used to rapidly check if a series might exist in the fileset, avoiding unnecessary disk reads.
-   **Digest File (`digest.db`):** Stores a checksum (digest) of the content of the other files in the fileset (excluding the checkpoint file). This is used to verify the integrity of the entire fileset.
-   **Checkpoint File (`checkpoint.db`):** This is the final file written for a fileset. Its presence signals that all other files in the set were written successfully and are consistent. It typically contains the digest from the digest file. A missing or invalid checkpoint file indicates a corrupted or incomplete fileset.

**Ensuring Data Consistency:**
-   **Checksums:** Individual `ts.Segment` data comes with a checksum. This is used by the `fs.streamingWriter` when writing data. The overall fileset integrity is ensured by the digest stored in the checkpoint file.
-   **Atomic Fileset Creation:** The checkpoint file acts as an atomic marker. A fileset is only considered valid and ready for use if its checkpoint file exists and the digest matches. This atomicity prevents the system from using partially written or corrupted filesets.
-   **Durability:** Standard OS-level mechanisms like `fsync` are typically used by the file writing components (though not explicitly detailed in the provided high-level code) before closing critical files to ensure data is durably persisted to the storage medium.

**Role of Commit Log in Flushing:**
-   The commit log provides write-ahead durability, ensuring that writes are safe even before they are flushed from memory to fileset files.
-   When a snapshot is taken via `flushManager.dataSnapshot`, the ID of the most recent commit log file (`rotatedCommitlogID`) is recorded as part of the snapshot metadata (`persistManager.DoneSnapshot` writes this via `snapshotMetadataWriter`).
-   This links the on-disk snapshot fileset to a specific point in the commit log history.
-   Once a snapshot is successfully written and checkpointed, M3DB knows that all data up to that commit log identifier (for the snapshotted blocks) is now durably stored in filesets.
-   This information is vital for the commit log cleanup process. Older commit log files, whose data is now covered by these persistent filesets, can be safely truncated or deleted, reclaiming disk space and ensuring the commit log doesn't grow indefinitely.

## Read Path

The M3DB read path is designed to be flexible and efficient, capable of serving queries for time series data by accessing information from both in-memory buffers and on-disk filesets. It intelligently merges data from these sources to provide a complete and accurate view of the requested series over a given time range.

### Handling Read Requests
M3DB handles read requests through a multi-layered approach, starting from client-facing APIs (like Prometheus remote read endpoints) down to individual database nodes querying their local storage.

**Types of Read Operations:**
The `dbnode/storage/database.go` file defines several core read operations that a storage node can perform:
-   **`ReadEncoded(ctx context.Context, namespace ident.ID, id ident.ID, start, end xtime.UnixNano)`**: Fetches raw, M3TSZ-encoded time series data for a specific series ID within a given time range. This is often used when clients need to process the encoded data directly or apply their own decoding logic.
-   **`FetchBlocks(ctx context.Context, namespace ident.ID, shardID uint32, id ident.ID, starts []xtime.UnixNano)`**: Retrieves specific data blocks for a series ID based on a list of block start times. This is useful for targeted data fetching, such as during repair or specific analytical queries.
-   **`FetchBlocksMetadataV2(ctx context.Context, namespace ident.ID, shardID uint32, start, end xtime.UnixNano, limit int64, pageToken PageToken, opts block.FetchBlocksMetadataOptions)`**: Provides metadata about the data blocks stored for series within a shard. This includes block size, checksums, last read times, and number of series, useful for debugging, auditing, or optimized data access strategies.
-   **`QueryIDs(ctx context.Context, namespace ident.ID, query index.Query, opts index.QueryOptions)`**: Executes an indexed query (based on tags and their values) to return a list of series IDs that match the query criteria within a time range. This is a common entry point for tag-based lookups.
-   **`AggregateQuery(ctx context.Context, namespace ident.ID, query index.Query, opts index.AggregationOptions)`**: Similar to `QueryIDs`, but performs aggregation on the indexed data directly at the storage node. This is particularly useful for optimizing queries that involve large-scale aggregations over indexed tags, reducing the amount of data that needs to be sent back to the querier/coordinator.

**Request Reception and Initial Processing:**
1.  **Entry Point & Parsing:**
    *   Read requests typically originate from a client application or an M3DB coordinator (which acts as a client to the dbnodes).
    *   For example, the Prometheus integration (`src/query/api/v1/handler/prometheus/native/read.go`) exposes HTTP handlers like `PromReadHandler` (for range queries at `/prometheus/api/v1/query_range`) and `NewPromReadInstantHandler` (for instant queries at `/prometheus/api/v1/query`).
    *   These handlers parse the incoming HTTP request (parameters, headers, body) to extract the query (e.g., PromQL), time range, step, timeout, and other options. The `ParseRequest` function in `native/read.go` is responsible for this.
2.  **Context and Tracing:** A `context.Context` is established, often embedding tracing information (e.g., `ctx.StartSampledTraceSpan` in `database.go` methods), and is propagated throughout the read path for timeout management, cancellation, and observability.
3.  **Client-Side (Coordinator/Gateway) Dispatch:**
    *   The M3DB client (defined in `src/dbnode/client/client.go`), often embedded within a coordinator, is responsible for interacting with the storage nodes. It maintains sessions (`clientSession`) with dbnodes.
    *   The client determines which storage nodes are likely to hold the data for the query based on its knowledge of the cluster topology and sharding scheme.
    *   For ID-based queries (`ReadEncoded`, `FetchBlocks`), the client can route the request directly to the node(s) owning the relevant shard(s).
    *   For tag-based queries (`QueryIDs`, `AggregateQuery`), the request might be fanned out to multiple nodes/shards.
    *   The client uses methods like `c.NewSession()` to get a session for communication.

**At the DBNode (`database.go`):**
When a read request (like `ReadEncoded`, `FetchBlocks`, etc.) reaches an M3DB storage node (`db` struct):
1.  **Namespace Identification:** The target namespace ID is extracted from the request. The `db.namespaceFor(namespaceID)` method is called to retrieve the corresponding `databaseNamespace` object. If the namespace doesn't exist, an `UnknownNamespaceError` is returned, and a metric like `metrics.unknownNamespaceRead` is incremented.
2.  **Shard Targeting & Validation:**
    *   For operations like `FetchBlocks` and `FetchBlocksMetadataV2`, the specific `shardID` is usually part of the request from the coordinator. The `databaseNamespace` then routes the request to that specific `dbShard`.
    *   For `ReadEncoded` (which operates on a single series ID) and `QueryIDs`/`AggregateQuery` (which are tag-based), the `databaseNamespace` consults its `ShardSet` to identify all local shards that could potentially contain data for the query. The operation is then performed across these relevant local shards.
3.  **Query Limits Enforcement:** Before extensive processing, system-wide query limits are checked. For instance, `d.queryLimits.AnyFetchExceeded()` (defined in `src/dbnode/storage/limits/query_limits.go`) is checked at the beginning of `QueryIDs` to quickly reject queries if the system is overloaded or if limits would be immediately breached.
4.  **Delegation to Namespace/Shard:** The `db` object delegates the read operation to the appropriate `databaseNamespace` object. The `databaseNamespace` further routes the request to the relevant `dbShard` instances it owns. For example, `databaseNamespace.ReadEncoded` will find the correct shard for the given ID and then call that shard's `ReadEncoded` method.

This initial handling ensures that the request is valid, targets the correct data partitions (namespaces and shards), and is executed within the operational boundaries (query limits) of the system before proceeding to actual data retrieval from memory or disk begins.

### Retrieving Data from In-Memory Buffers
When a read request targets a time range that includes data still in memory, M3DB efficiently retrieves this data from the series' active buffers. This is typically the path for querying very recent data.

**Accessing the `databaseBuffer`:**
-   When a `dbSeries` object processes a read request (e.g., via its `ReadEncoded` method, which internally uses a `series.Reader`), it first attempts to fetch data from its `buffer` field. This field holds an instance of `databaseBuffer` (the `dbBuffer` implementation from `src/dbnode/storage/series/buffer.go`).
-   The `databaseBuffer` organizes data into time-based buckets (`BufferBucketVersions` stored in `bucketsMap`) corresponding to block start times. The read operation identifies which of these buckets fall within the queried time range.

**Reading from `BufferBucketVersions` and `BufferBucket`s:**
-   For each relevant block start time within the query range, the `dbBuffer.ReadEncoded` method is invoked. This method iterates through its `inOrderBlockStarts` (a sorted list of active block start times) to find matching `BufferBucketVersions` in its `bucketsMap`.
-   Each found `BufferBucketVersions.streams()` method is then called. This method iterates through all the `*BufferBucket` instances it holds. These buckets can represent different versions of data for the same block time due to flushing or cold writes:
    -   The currently writable bucket (`version == writableBucketVersion`, usually 0) for ongoing warm writes.
    -   The writable bucket for cold writes, if applicable.
    -   Older, immutable (versioned) buckets that might have been flushed but could still be in memory or contain data not yet merged from a different write type (warm/cold).
-   Each relevant `BufferBucket` contributes its data to the read operation by providing streams from its encoders and loaded blocks.

**Decoding from In-Memory `inOrderEncoder`s:**
-   Within each `BufferBucket`, data resides in one or more `inOrderEncoder` instances (a slice named `encoders`). Each of these encoders contains a stream of M3TSZ-encoded data points.
-   To retrieve data, the `encoder.Stream()` method is called on each `inOrderEncoder` within the bucket. This method returns an `xio.SegmentReader` which can decode the M3TSZ data on the fly. The `xio.BlockReader` wraps this, adding block context.
-   If a `BufferBucket` contains multiple `inOrderEncoder`s (e.g., due to out-of-order writes, or updates to existing data points which create new encoders to maintain immutability), streams from all these encoders are collected.

**Retrieving Data from `loadedBlocks`:**
-   A `BufferBucket` can also contain `loadedBlocks`. These are `block.DatabaseBlock` instances that were previously persisted to disk filesets and have been read back into memory (e.g., during bootstrapping or if a series' disk data was loaded into the buffer via `dbBuffer.Load()`).
-   If `loadedBlocks` are present in a relevant `BufferBucket`, their data is also streamed out using the `block.Stream()` method, which provides an `xio.BlockReader` for each.

**Order of Precedence and Merging In-Memory Data:**
-   A single read operation for a time range might need to access data from multiple `inOrderEncoder`s across several `BufferBucket`s (and potentially `BufferBucketVersions`), as well as any `loadedBlocks`.
-   The `dbBuffer.ReadEncoded` method collects all these `xio.BlockReader` streams.
-   The `series.Reader` (used by `dbSeries.ReadEncoded`) then takes these streams, along with streams from `cachedBlocks` (disk data cached in the series itself, discussed later), and uses a merging iterator, typically an `encoding.MultiReaderIterator`.
-   This iterator ensures that:
    -   Data points are yielded in strict chronological order.
    -   If multiple data points exist for the exact same timestamp (e.g., an original write and a subsequent update, which would reside in different encoders or buckets), the merging logic guarantees that the latest write takes precedence. This is often achieved by the order in which encoders/buckets are processed or by the inherent properties of the merge algorithm (e.g., data from `version 0` buckets or more recent encoders overriding older ones).

This process ensures that queries accessing recent data primarily hit memory and receive a consistent, merged view of all available data points for the requested series and time range, reflecting the latest state of the data before considering disk reads.

### Loading Data from Disk Filesets
When a query requires data that is not in memory (or only partially in memory), M3DB reads it from persistent storage, specifically from the fileset files on disk. This process is managed by the `fs.reader` (an implementation of `persist.DataFileSetReader` from `src/dbnode/persist/fs/read.go`).

**Identifying and Locating Fileset Files:**
-   M3DB determines which filesets to read based on the query's time range, the target namespace, shard, and block start time.
-   File paths are constructed using a standard convention: `FilePathPrefix/<namespace>/<shard>/<block-start>-<volumeIndex>/<file-type>.db`. Functions like `dataFilesetPathFromTimeAndIndex` (in `src/dbnode/persist/fs/fs.go`) generate these paths. `volumeIndex` helps differentiate between multiple filesets for the same block, particularly for flushed data versus snapshot data or repaired data.
-   The first step in accessing a fileset is to locate and validate its `checkpoint.db` file. The `ReadCheckpointFile` function reads this file and its digest, which is a checksum of all other data files in the fileset. A valid checkpoint file indicates the fileset was successfully written.

**Reading Core Fileset Components (`fs.reader`):**
The `fs.reader` handles the reading and validation of fileset components after `Open()` is called with `DataReaderOpenOptions` specifying the target fileset:
1.  **Digest File (`digest.db`):** After validating the checkpoint, `reader.readDigest()` reads the main digest file (`digest.db`). This file contains the expected digests for the info, index, bloom filter, and data files. These expected digests are stored in the `reader` to validate each component as it's read.
2.  **Info File (`info.db`):** `reader.readInfo()` reads this file, which contains metadata such as the block start time (`info.BlockStart`), block size (`info.BlockSize`), total number of series entries (`info.Entries`), and parameters for the Bloom filter (`info.BloomFilter`). The content is validated against its expected digest (`r.expectedInfoDigest`).
3.  **Bloom Filter File (`bloomfilter.db`):** `reader.ReadBloomFilter()` reads the Bloom filter data. This allows M3DB to quickly perform a probabilistic check (`bloomFilter.Test(seriesIDBytes)`) to see if a series ID might exist in this fileset before attempting more expensive index lookups. The content is validated against its expected digest (`r.expectedBloomFilterDigest`).
4.  **Index File (`index.db`):**
    *   The `reader.decoder` (a `msgpack.Decoder`) is reset with the index file's byte stream (`r.indexDecoderStream`).
    *   For non-streaming reads (where `streamingEnabled` is false, e.g., for `reader.Read()`), `reader.readIndexAndSortByOffsetAsc()` decodes all `schema.IndexEntry` objects from the index file. Each entry contains the series ID, encoded tags, offset in the data file, size of the data, and a checksum for that data. These entries are stored in `r.indexEntriesByOffsetAsc` and sorted by their data file offset to optimize sequential reads from the data file.
    *   For streaming reads (`reader.StreamingRead()` or `reader.StreamingReadMetadata()`), index entries are decoded one by one as needed directly from `r.indexDecoderStream`.
    *   The entire index file's integrity is checked against its expected digest (`r.expectedIndexDigest`) via `r.indexDecoderStream.reader().Validate()`.
5.  **Data File (`data.db`):**
    *   The data file is typically memory-mapped (mmap'd) for efficient access via `reader.dataMmap` (initialized in `reader.Open`).
    *   When a specific series' data is needed (its `IndexEntry` having been retrieved from the index file):
        *   For `reader.Read()`, the `reader.dataReader` (which wraps the mmap'd data) is used to read `entry.Size` bytes from `entry.Offset`.
        *   For `reader.StreamingRead()`, a slice of the `reader.dataMmap.Bytes` from `entry.Offset` to `entry.Offset + entry.Size` is used.
    *   The `DataChecksum` from the `IndexEntry` is used to verify the integrity of the data read for that specific series.
    *   The entire data file's integrity is eventually validated against its expected digest (`r.expectedDataDigest`) via `r.dataReader.Validate()`.

**Decoding and Decompression:**
-   The data read from the `data.db` file for a series is a `ts.Segment`. This segment contains M3TSZ-encoded data points.
-   Decoding of the M3TSZ format happens when the `ts.Segment` is actually iterated over (e.g., when a `block.DatabaseBlock` created from this segment is streamed via its `Stream()` method).
-   M3DB filesets store data in its M3TSZ encoded form directly. There isn't an additional layer of generic compression (like zstd) applied on top of M3TSZ for data within the filesets themselves; M3TSZ provides both encoding and compression.

**Verification Mechanisms:**
-   **Checkpoint Digest:** The `checkpoint.db` file's digest provides an overall integrity check for the entire fileset.
-   **Individual File Digests:** Each major component (info, index, data, bloom filter) has its digest stored in the main `digest.db` file, and these are verified when the component is read or fully processed by `fs.reader`.
-   **Data Segment Checksums:** Each `schema.IndexEntry` stores a `DataChecksum` for the corresponding series data segment in the data file. This checksum is verified when the specific segment is read (e.g., implicitly by `reader.Read()` or explicitly in `reader.StreamingRead()`).

**Role of `block.DatabaseBlockRetriever`:**
-   The `block.DatabaseBlockRetriever` (an interface defined in `src/dbnode/storage/block/block.go`) abstracts the process of fetching block data from disk. The `dbSeries` (in `src/dbnode/storage/series/series.go`) holds an instance of this retriever (`s.blockRetriever`).
-   When a `dbSeries` needs data for a specific block start time that isn't in its in-memory buffers or `cachedBlocks`, it uses its `blockRetriever`.
-   The retriever implementation (not detailed in the provided files but implied to use `fs.reader` or similar) interacts with the persistence layer to:
    1.  Open the appropriate fileset using the namespace, shard, block start, and volume index.
    2.  Optionally, use the Bloom filter from the fileset to quickly check if the series ID is likely present.
    3.  Search the fileset's index file for the series ID to get its data offset and size.
    4.  Read the `ts.Segment` from the data file.
    5.  Return this segment to the caller, typically by invoking the `onRetrieveBlock` callback.

**Creation of `block.DatabaseBlock` from Disk Data:**
-   The `onRetrieveBlock` callback for a `dbSeries` is `dbSeries.OnRetrieveBlock`.
-   When the `blockRetriever` successfully fetches a `ts.Segment` from disk for a series, it calls `s.OnRetrieveBlock(id, tags, startTime, segment, nsCtx)`.
-   Inside `dbSeries.OnRetrieveBlock`:
    1.  A new `block.DatabaseBlock` is obtained from a pool (`s.opts.DatabaseBlockOptions().DatabaseBlockPool().Get()`).
    2.  `block.ResetFromDisk(startTime, blockSize, segment, s.id, nsCtx)` is called. This initializes the block with the retrieved segment, its start time, block size, the series ID, and marks it as `wasRetrievedFromDisk = true`. The segment's checksum is also calculated and stored in the block.
    3.  The block's last read time is updated (`b.SetLastReadTime(s.now())`).
    4.  This newly loaded `DatabaseBlock` is then added to the `dbSeries.cachedBlocks` map (via `s.addBlockWithLock(b)`) for potential future reuse, subject to caching policies.
    5.  If an LRU cache (`WiredList`) is configured, the block is also updated in the list (`list.BlockingUpdate(b)`).

This entire process allows M3DB to efficiently load only the necessary data from disk, verify its integrity, and make it available for query processing, integrating it seamlessly with data held in memory.

### Merging Data from Memory and Disk
To provide a complete and accurate answer to a query, M3DB often needs to combine data from its in-memory buffers (recent writes, cached blocks) with data stored in on-disk filesets. This merging process is critical for presenting a unified view of a time series across its entire lifecycle.

**Necessity of Merging:**
Time series data in M3DB can reside in multiple locations depending on its age and access patterns:
-   **Active In-Memory Buffers (`dbSeries.buffer` via `dbBuffer`):** Contains the most recent writes, including data not yet flushed to disk. This includes various `BufferBucketVersions` and `BufferBucket`s with their `inOrderEncoder`s and potentially `loadedBlocks` (blocks read from disk and then loaded *into the buffer structure*).
-   **Cached Blocks (`dbSeries.cachedBlocks`):** These are `block.DatabaseBlock` instances that were previously read from disk filesets (via `OnRetrieveBlock`) and are now held in memory directly within the `dbSeries` struct for faster subsequent access, managed by a cache policy (e.g., LRU).
-   **Disk Filesets:** Older data that has been flushed and potentially evicted from memory caches resides in immutable fileset files on disk.

A query spanning a time range might require data points from any or all of these sources. Merging ensures that all relevant data points are considered and presented chronologically, with appropriate handling for duplicate or updated data points (upserts).

**Orchestration by `series.Reader`:**
The primary component responsible for orchestrating data retrieval and merging is the `series.Reader` (from `src/dbnode/storage/series/reader.go`). When a read operation like `dbSeries.ReadEncoded` is called, it utilizes a `series.Reader` (specifically, `NewReaderUsingRetriever` initializes one).
1.  **Gathering Data Streams (`readersWithBlocksMapAndBuffer`):**
    *   The `series.Reader.readersWithBlocksMapAndBuffer` method (and its aligned variant) is key. It identifies all potential sources of data for the requested time range (`start`, `end`):
        *   **In-Memory Buffer Data:** It calls `seriesBuffer.ReadEncoded()` (on the `dbSeries.buffer`, which is a `dbBuffer`). This method itself handles iterating through the `bucketsMap`, `BufferBucketVersions`, and `BufferBucket`s, collecting `xio.BlockReader` streams from all relevant `inOrderEncoder`s and any `loadedBlocks` within the buffer structure. These streams are added to the `buffer` variable (a `[][]xio.BlockReader`) within `readersWithBlocksMapAndBufferAligned`.
        *   **Cached Blocks (`dbSeries.cachedBlocks`):** It iterates through `dbSeries.cachedBlocks`. For each block whose start time falls within the query range, it obtains a data stream by calling `block.Stream()`. These are added to the `cached` variable (a `[]xio.BlockReader`).
        *   **Disk Filesets:** If the query range extends beyond what's available in memory (active buffer and `cachedBlocks`), the `series.Reader` uses the `block.DatabaseBlockRetriever` (the `s.blockRetriever` field in `dbSeries`) to fetch the required blocks from disk. This happens iteratively in `blockReaderIter.Next()` if a block for a specific `blockAt` timestamp is not found in the `cached` or `buffer` streams. The retriever, upon fetching a `ts.Segment` from disk, invokes the `onRetrieveBlock` callback (`dbSeries.OnRetrieveBlock`), which creates new `block.DatabaseBlock` instances. These new blocks are added to `dbSeries.cachedBlocks` and their data is also streamed for the current query.
2.  **Iteration and Merging:** The collected streams (from buffer, cache, and newly fetched disk blocks) are not immediately merged into one giant list. Instead, `readersWithBlocksMapAndBufferAligned` returns a `BlockReaderIter`. This iterator, when `Next()` is called, processes one block start time at a time, gathering all streams (buffer, cached, disk-fetched) for that specific block start. These streams for a single block start are then supplied to an `encoding.MultiReaderIterator`.

**Order of Precedence:**
M3DB's merging logic ensures that the most recent version of a data point takes precedence. This is critical for handling updates to existing data points (upserts). The `MultiReaderIterator` inherently handles this by processing data points chronologically. If multiple points have the exact same timestamp, the order in which iterators (representing different data sources or versions) are added to the `MultiReaderIterator` or the iterator's internal logic ensures the correct precedence (typically, more recent data, like that from writable buffers, would override older data).

**Role of `MultiReaderIterator`:**
The `encoding.MultiReaderIterator` (from `src/dbnode/encoding/multi_reader_iterator.go`) is the workhorse for the merging process:
-   **Input:** For each block start time, the `BlockReaderIter` provides a set of `xio.BlockReader` instances to a `MultiReaderIterator` (usually via `Reset()` with a `singleSlicesOfSlicesIterator`). These readers represent data from the buffer, `cachedBlocks`, or freshly read disk data for that specific block.
-   **Chronological Ordering:** The `MultiReaderIterator` uses a min-heap (`it.iters`) to manage the "next" data point from all its input iterators (which are `encoding.ReaderIterator` created from the `xio.BlockReader`s). It always yields data points in strict chronological order.
-   **Deduplication and Precedence:**
    *   As the `MultiReaderIterator.Next()` method advances, if it encounters multiple data points for the exact same timestamp from different underlying iterators, its heap-based selection (always picking the smallest timestamp) combined with how it processes subsequent identical timestamps ensures that only one value for a given timestamp is yielded.
    *   The comment "Dedupe by continuing" in `multiReaderIterator.moveIteratorsToNext()` implies that if it processes an iterator and the next point from that *same* iterator is a duplicate (or if another iterator yields an identical timestamp that should be overridden due to source precedence rules, though this is more implicitly handled by the overall flow), it will continue until a distinct, valid point is found. The latest write semantics are generally upheld by the combined logic of buffer versioning, `series.Reader` stream collection order, and `MultiReaderIterator`'s processing.

**Handling Overlap:**
When data for the same time window exists in multiple locations (e.g., a data point is in the active buffer, an older version is in a `cachedBlock`, and an even older version on disk), the `series.Reader` and `MultiReaderIterator` work together:
1.  The `series.Reader` gathers streams from all these sources for the relevant block start.
2.  The `MultiReaderIterator` then processes these streams. Due to its chronological processing and internal logic for handling identical timestamps, it ensures that the data from the most "current" source (e.g., active buffer over cached block, cached block over fresh disk read if versions differ) is what's ultimately presented for a given timestamp.

This merging strategy allows M3DB to provide fast access to recent data from memory while also seamlessly incorporating historical data from disk, giving users a consistent and complete view of their time series.

### Data Eviction from Memory
M3DB employs several mechanisms to manage memory by evicting data that is no longer needed or has been safely persisted to disk. This process is crucial for long-term stability and performance.

**Primary Eviction Mechanism: The `Tick` Process**
The primary driver for data eviction is the periodic `Tick` process, which operates at the `dbSeries` level and within its `databaseBuffer`.
-   **`dbSeries.Tick(blockStates ShardBlockStateSnapshot, ...)`:** This method is called regularly. It orchestrates the cleanup of both the series' active buffer and its cache of disk-loaded blocks (`cachedBlocks`).
    -   **Buffer Eviction:** `dbSeries.Tick` first calls `s.buffer.Tick(blockStates, ...)`.
        -   The `databaseBuffer.Tick` method (in `src/dbnode/storage/series/buffer.go`) receives a `ShardBlockStateSnapshot`. This snapshot indicates which time blocks have been successfully flushed to disk and are retrievable.
        -   The buffer iterates through its `bucketsMap`. For each `BufferBucketVersions` (representing a block start time):
            -   If `blockState.WarmRetrievable` is true for that block start, it means warm writes for that block are on disk. The buffer then calls `buckets.removeBucketsUpToVersion(WarmWrite, 1)` to remove the in-memory encoders for these successfully flushed warm writes.
            -   Similarly, if `blockState.ColdVersion > 0`, it calls `buckets.removeBucketsUpToVersion(ColdWrite, coldVersion)` to remove older cold write data that has also been persisted.
            -   If all versions within a `BufferBucketVersions` are removed (i.e., `buckets.streamsLen() == 0`), the entire entry for that block start is removed from the `bucketsMap` (`b.removeBucketVersionsAt(tNano)`). This frees the memory occupied by the encoders for that block.
    -   **`cachedBlocks` Management (`dbSeries.updateBlocksWithLock`):** After the buffer tick, this method manages blocks in `s.cachedBlocks`.

**Block Expiry (Retention Policy):**
-   During `dbSeries.updateBlocksWithLock`, any block in `cachedBlocks` whose start time (`start`) is older than the retention period (`expireCutoff = now.Add(-ropts.RetentionPeriod())`) is unconditionally marked for removal.
-   If the `CachePolicy` is not `CacheLRU` or if the block was not originally retrieved from disk, `currBlock.Close()` is called, and it's removed from `cachedBlocks`. Closing the block returns its resources to pools if applicable.

**Cache Policy Based Eviction for `cachedBlocks`:**
For blocks within the retention period, the `CachePolicy` (defined in `src/dbnode/storage/options.go`) dictates eviction from `cachedBlocks` if they have been flushed (`blockState.WarmRetrievable`):
-   **`CacheAll`:** Blocks are generally kept in `cachedBlocks` until they expire by retention.
-   **`CacheNone`:** If a block is flushed, it's marked to `shouldUnwire = true` and subsequently removed from `cachedBlocks` and closed.
-   **`CacheRecentlyRead`:** A block is marked `shouldUnwire = true` if it's flushed and the time since its `currBlock.LastReadTime()` exceeds the `blockDataExpiryAfterNotAccessedPeriod`. If unwired, it's removed and closed.
-   **`CacheLRU`:**
    -   **Blocks from Buffer Rotations:** If a block in `cachedBlocks` originated from an in-memory buffer rotation (i.e., `!currBlock.WasRetrievedFromDisk()`) and is flushed, it's marked `shouldUnwire = true`, then removed and closed by the series tick.
    -   **Blocks from Disk (WiredList):** If a block `WasRetrievedFromDisk()`, its lifecycle in `cachedBlocks` is primarily managed by a global or shard-level `WiredList` (defined in `src/dbnode/storage/block/wired_list.go`, though its detailed implementation is not read here). The `dbSeries.Tick` does *not* directly evict these.
        -   The `WiredList` employs LRU (Least Recently Used) logic. When the list reaches its memory capacity, it evicts the least recently used blocks.
        -   When the `WiredList` evicts a block, it calls the `OnEvictedFromWiredList` callback that was registered by the `dbSeries` when the block was first loaded (`dbSeries.OnRetrieveBlock` sets `b.SetOnEvictedFromWiredList(s.blockOnEvictedFromWiredList)`).
        -   The `dbSeries.OnEvictedFromWiredList` method then removes the specified block from its `cachedBlocks` map. The `WiredList` is responsible for actually closing the block instance.

**Series Expiry:**
-   After the `dbSeries.Tick` process completes the buffer and `cachedBlocks` cleanup, it checks if the series still holds any active data (`update.ActiveBlocks == 0`) and has no pending bootstrap data.
-   If the series is effectively empty across its entire retention period, `dbSeries.Tick` returns `ErrSeriesAllDatapointsExpired`.
-   The calling entity (typically the `dbShard`) receives this error and can then remove the `dbSeries` object from its collection of active series. This makes the `dbSeries` object eligible for garbage collection, freeing the memory associated with the series metadata and its (now empty) internal structures.
-   When a `dbBlock` is closed (either by the series tick or by the `WiredList`), if it was pooled, `pool.Put(b)` is called, returning the block object to its pool for reuse, further reducing allocations.

These mechanisms ensure that M3DB manages its memory footprint by evicting data from buffers once flushed, adhering to cache policies for disk-loaded data, and ultimately removing entire series objects when they no longer contain any data within the configured retention period.

## Indexing

M3DB incorporates a powerful inverted index to allow for fast, tag-based searching of time series. Instead of scanning every series to find those matching specific criteria, the index enables quick lookups based on tag names and values. This capability is crucial for ad-hoc querying, dashboards, and alerting where users need to dynamically filter and aggregate series. M3DB utilizes M3ninx as its core indexing engine.

### Purpose of Indexing
The primary purpose of indexing in M3DB is to provide an efficient way to locate time series based on their descriptive tags (metadata), rather than just their unique IDs. In a system with potentially millions or billions of unique time series, sequentially scanning through all series data to find those matching specific tag queries (e.g., `SELECT mean(value) WHERE service='api' AND env='prod'`) would be prohibitively slow and resource-intensive.

An inverted index solves this by creating a data structure where:
-   Tag names and their values (terms) are keys.
-   For each term (e.g., `service=api`), a list of series (more accurately, "documents" representing series) that contain that term is stored. This list is called a postings list.

This allows M3DB to:
-   Quickly retrieve a list of series IDs that match a given set of tag predicates (e.g., `service='api'`, `status_code=~'5.*'`).
-   Support complex queries involving multiple tag filters (AND, OR, NOT) and regular expressions by performing set operations (intersection, union, difference) on postings lists.
-   Greatly reduce query latency for tag-based searches, as it avoids scanning non-matching series.
-   Enable features like tag-based aggregations directly on the index.

Without indexing, such ad-hoc, tag-based querying would be impractical at scale. The indexing component in M3DB is managed by `nsIndex` (defined in `src/dbnode/storage/index.go`) for each namespace.

### Index Data Structures and Algorithms
M3DB's indexing capabilities are powered by **M3ninx**, a purpose-built inverted indexing engine (code primarily in `src/m3ninx/`). M3ninx employs several key data structures and algorithms:

**1. Documents:**
-   In M3ninx, each time series that is indexed is represented as a **document**.
-   A document (`doc.Metadata` from `src/m3ninx/doc/document.go`) primarily consists of:
    -   An **ID**: This is typically the unique M3DB series ID (`ident.ID`). M3ninx internally uses a reserved field name, `doc.IDReservedFieldName` (which is `_m3ninx_id`), to store this ID within the index.
    -   A list of **Fields** (`doc.Field`): Each field corresponds to a tag associated with the time series. A field has a `Name` (tag name, e.g., `service`) and a `Value` (tag value, e.g., `api`). Both are byte slices.
-   This document representation allows M3ninx to treat time series metadata (tags) like documents in traditional search engines. Documents are validated via `doc.Metadata.Validate()` to ensure fields and IDs are valid UTF-8 and don't use reserved names.

**2. Index Segments:**
-   The index is broken down into multiple **segments**. An index segment (`segment.Segment` interface from `src/m3ninx/index/segment/types.go`) represents an immutable, searchable portion of the inverted index.
-   In M3DB, these segments are typically time-bound, meaning each `index.Block` (which wraps one or more M3ninx segments) covers a specific time window (e.g., 2 hours, configurable by `index.Options().BlockSize()`).
-   Each segment contains its own data structures for the documents indexed within that segment's time window:
    -   **Term Dictionary:** A mapping from terms (tag name-value pairs) to their postings lists. M3ninx often uses **Finite State Transducers (FSTs)** for this. `fst.Segment` (in `src/m3ninx/index/segment/fst/segment.go`) is an FST-based segment implementation. There's a main FST for fields (`fieldsFST`) which maps field names to metadata about that field (including an offset to another FST for that field's terms). Each field then has its own terms FST that maps term values to postings list offsets.
    -   **Postings Lists:** Collections of document IDs.
    -   **Stored Fields/Documents:** A way to retrieve the original fields of a document given its ID within the segment, handled by `docs.DataReader` and `docs.IndexReader`.

**3. Postings Lists:**
-   At the heart of the inverted index are **postings lists** (`postings.List` interface from `src/m3ninx/postings/types.go`).
-   A postings list is associated with a specific term (tag value) within a specific field (tag name).
-   It contains a sorted list of document IDs (internal M3ninx IDs, which map to M3DB series IDs) that have that exact field-term combination.
-   M3ninx primarily uses **Roaring Bitmaps** (`roaring.Bitmap` from `github.com/m3dbx/pilosa/roaring`) as its postings list implementation (see `src/m3ninx/postings/roaring/roaring.go`). Roaring Bitmaps are highly efficient for storing and performing operations on sorted integer sets, offering:
    -   Good compression ratios.
    -   Fast set operations: union (`UnionInPlace`), intersection (`Intersect`), difference (`Difference`). These are fundamental for resolving complex queries with multiple predicates.
-   The `postings.Pool` provides pooling for these mutable lists.

**High-Level Search Process (within a segment):**
When a tag-based query (e.g., `service='api' AND region='us-east-1'`) is processed for a segment:
1.  The query is parsed into constituent field/term lookups (e.g., `service=api`, `region=us-east-1`).
2.  For each field/term pair:
    *   The `fieldsFST` is consulted to find the field (e.g., `service`).
    *   The associated terms FST for that field is then used to find the term (e.g., `api`).
    *   This lookup yields an offset pointing to the start of the serialized postings list (Roaring Bitmap) in the postings data file/memory region.
3.  The postings lists for each condition are retrieved (deserialized).
4.  Set operations are performed on these postings lists (e.g., an intersection for an AND query).
5.  The resulting postings list contains the document IDs that satisfy all conditions within that segment.
6.  These document IDs can then be used to retrieve the full document metadata (all tags) if needed, using the stored fields mechanism (e.g., `docsDataReader.Read(offset)` where offset is found via `docsIndexReader.Read(postings.ID)`).

This combination of FST-based term dictionaries and Roaring Bitmap postings lists allows M3ninx (and thus M3DB) to perform fast and scalable tag-based searches.

### Querying the Index
When M3DB needs to find series based on tags (e.g., as part of a `QueryIDs` or `AggregateQuery` operation initiated by `database.QueryIDs` or `database.AggregateQuery`), it queries its M3ninx-based inverted index.

**Query Execution Flow:**
1.  **Query Reception:** The `nsIndex` component (in `src/dbnode/storage/index.go`) receives an `index.Query` object. This object wraps an `idx.Query` (the M3ninx native query model from `src/m3ninx/idx/query.go`), which represents the tag-based search criteria. The request is also accompanied by `index.QueryOptions` or `index.AggregationOptions` that control aspects of the query execution.
2.  **Identifying Target Index Blocks:**
    *   The `nsIndex.queryWithSpan` method is responsible for orchestrating the query. It first identifies all `index.Block` instances that cover the time range specified in the `QueryOptions` (`opts.StartInclusive`, `opts.EndExclusive`). It iterates through its current `activeBlock` and the historical `blocksDescOrderImmutable`.
3.  **Per-Block Query Execution:**
    *   For each relevant `index.Block`, the `nsIndex` calls either `block.QueryIter(...)` (for retrieving document IDs and their metadata) or `block.AggregateIter(...)` (for performing aggregations directly on the index data). These methods are defined in `src/dbnode/storage/index/block.go`.
    *   Inside the `index.Block`, these methods obtain readable M3ninx segments. An `index.Block` can comprise multiple M3ninx segments: mutable segments (for recently indexed data) and immutable segments (from bootstrapped or flushed index data).
    *   An `m3ninx/search.Executor` is then created using readers obtained from these segments (`b.segmentReadersWithRLock()`).
    *   The M3ninx executor runs the actual search query (`idx.Query.SearchQuery()`) against the collected segment readers. M3ninx supports various query types:
        *   `idx.TermQuery`: For exact matches of a tag name and value (e.g., `service=api`).
        *   `idx.RegexpQuery`: For matching tag values against a regular expression.
        *   `idx.ConjunctionQuery`: For AND operations between multiple query conditions.
        *   `idx.DisjunctionQuery`: For OR operations.
        *   `idx.NegationQuery`: For NOT operations.
        *   `idx.AllQuery`: To match all documents within the segment (often used in aggregations without specific filters).
    *   The M3ninx execution yields an iterator over matching document IDs.
4.  **Combining Results from Multiple Blocks:**
    *   The `nsIndex.queryWithSpan` method manages the execution across the different time-sharded index blocks. Queries against these blocks can be run concurrently, with parallelism managed by a `permits.Manager` (from `src/dbnode/storage/limits/permits/types.go`).
    *   The results (document IDs or aggregated terms) from each `index.Block`'s iterator are collected into a global result object: `index.QueryResults` for ID queries or `index.AggregateResults` for aggregation queries.
    *   These result objects handle deduplication. A series might be present in multiple index blocks if it spans across their time windows. The result map (e.g., `results.Map()` for `QueryResults`) ensures each series ID is represented once with its associated document metadata.
5.  **Applying Options and Limits:**
    *   **`index.QueryOptions` / `index.AggregationOptions`**: These structures, passed into `nsIndex.Query` or `nsIndex.AggregateQuery`, control the query:
        *   `StartInclusive`, `EndExclusive`: Define the overall time window, used to select relevant index blocks.
        *   `SeriesLimit`, `DocsLimit`: Impose limits on the number of unique series IDs or total documents that can be returned or processed. The `nsIndex.queryWithSpan` checks these limits (`opts.LimitsExceeded`) and can terminate early if `RequireExhaustive` is `false`.
        *   Namespace runtime options (`i.state.runtimeOpts.maxQuerySeriesLimit`, `maxQueryDocsLimit`) can override user-provided limits if they are deemed too high.
        *   For aggregations, `AggregationOptions` also include `FieldFilter` (to specify which tag names to aggregate over) and `Type` (e.g., `AggregateTagNamesAndValues`).
    *   **Query Concurrency and Resource Control (`permits.Manager`, `limits.QueryLimits`):**
        *   The `permits.Manager` (`i.permitsManager` in `nsIndex`) is used to control the concurrency of queries executing against different blocks. Before processing a block's results, a `Permit` is acquired via `perms.Acquire(ctx)`.
        *   Global query limits, defined by `limits.QueryLimits` (from `src/dbnode/storage/limits/types.go` and implemented in `query_limits.go`), such as `FetchDocsLimit()` or `AggregateDocsLimit()`, provide overarching control to prevent queries from overwhelming the system. These are checked at various points, including at the beginning of a query in `database.go` and potentially during result collection.
6.  **Returning Results:**
    *   **For ID Queries (`nsIndex.Query`):** The method returns an `index.QueryResult`. This contains an `index.QueryResults` object, which typically provides a map where keys are the `ident.ID` of matching series and values are their `doc.Metadata` (tags).
    *   **For Aggregation Queries (`nsIndex.AggregateQuery`):** The method returns an `index.AggregateQueryResult`. This contains an `index.AggregateResults` object, which holds the aggregated data (e.g., a map of tag names to a map of tag values to their counts or other aggregated views).
    *   Both result types also include a flag indicating whether the query was exhaustive or if it was terminated early due to hitting a limit.

This multi-stage process, from block selection to M3ninx execution and result aggregation, allows M3DB to efficiently query its distributed, time-sharded inverted index while respecting system limits.

### Determining Data Presence for a Series ID
M3DB provides several ways to determine if data for a specific series ID might exist, ranging from probabilistic checks to definitive lookups. The method used often depends on whether the check is for data on disk or in memory, and the desired trade-off between speed and accuracy.

**1. Checking for Data on Disk (Filesets):**

*   **Bloom Filters (Probabilistic, Fast):**
    *   Each disk fileset has an associated Bloom filter file (`bloomfilter.db`). This filter is loaded into memory when a fileset is opened by a `DataFileSetReader` (as seen in `src/dbnode/persist/fs/read.go` via `reader.ReadBloomFilter()`).
    *   The Bloom filter stores a compact, probabilistic representation of all series IDs present in that specific fileset (for that time block and shard).
    *   To check if a series ID *might* be in a fileset, M3DB performs a `bloomFilter.Test(seriesIDBytes)` lookup.
        *   If `Test()` returns `false`, the series ID is definitively **not** in that fileset. This is a very fast way to rule out searching in a fileset.
        *   If `Test()` returns `true`, the series ID **may be** in the fileset (false positives are possible). A further, more definitive check (like an index lookup) is then required if precise knowledge is needed.
    *   This is primarily an optimization to reduce I/O by avoiding unnecessary access to index or data files for series that are not present in a given fileset.

*   **Inverted Index Query (Definitive for Indexed Data):**
    *   To definitively check if a series ID is known to the index within a specific time range (and thus likely has data associated with it in corresponding data filesets), a query against the inverted index is performed.
    *   M3ninx, the indexing engine, stores the series ID as a special field in its documents, typically using `doc.IDReservedFieldName` (which is `_m3ninx_id`).
    *   A targeted query, such as `idx.NewTermQuery(doc.IDReservedFieldName, seriesIDBytes)`, can be executed via `nsIndex.QueryIDs(...)`.
    *   If the query returns the series ID, it confirms that the series' metadata (tags and ID) is present in the index for the queried time range. This strongly implies data exists or existed for that series in blocks covered by those index segments.
    *   This method is more resource-intensive than a Bloom filter check as it involves FST lookups and potentially postings list traversals within M3ninx segments.

**2. Checking for Data in Memory:**

*   **Series Map (`dbShard.series`):**
    *   Each `dbShard` maintains an in-memory map (often `series.Map`) of active `dbSeries` objects that it currently owns.
    *   A direct lookup in this map using the series ID is the quickest way to determine if a series is active in memory (i.e., has data in its `databaseBuffer` or `cachedBlocks`).
    *   If a `dbSeries` object exists in this map, it means the series is known and likely has data in memory for recent time blocks, or has blocks cached from disk.
    *   This check is primarily for the dbnode's internal operations to manage active series rather than a direct client API for "existence."

**Relative Efficiency and Trade-offs:**
*   **In-Memory `series.Map` Lookup:** Fastest, definitive for data currently hot in memory.
*   **Bloom Filter Check (Disk Filesets):** Very fast, probabilistic (can have false positives, meaning it might say an ID exists when it doesn't, but will never say an ID doesn't exist if it does). Excellent for quickly skipping filesets that definitely don't contain a series.
*   **Inverted Index Query (Disk/Memory):** Slower than Bloom filters but provides a definitive answer for whether a series is indexed within the queried time range. It's the standard way to find series by tags. Querying specifically for `_m3ninx_id=<seriesID>` is a direct way to check ID presence in the index.

**Nature of M3DB's Index:**
It's important to remember that M3DB's primary index, powered by M3ninx, is an **inverted index**. It is optimized for finding series based on arbitrary tag combinations (e.g., `service=api AND env=prod`). While it *can* be queried by the internal series ID (as it's stored as just another field like `_m3ninx_id`), this is not the same as a primary key lookup in a traditional relational database or a simple key-value store lookup for the series ID itself to get its data. The path to data always involves either a direct memory lookup (if recent) or identifying relevant filesets (potentially aided by Bloom filters) and then looking up the series within those filesets (via their internal indexes) to get to the actual data blocks.

## Memory-mapped Files (mmap)

Memory-mapping files (commonly known as mmap) is a mechanism where a file's contents are mapped to a process's virtual address space. Instead of using traditional read/write syscalls, the program can access the file's data directly in memory as if it were an array of bytes. The operating system handles the loading of file pages into physical memory when they are accessed and the writing of modified pages back to disk.

In database systems, mmap can offer significant performance benefits by reducing the overhead of syscalls and copying data between kernel and user space. It also allows the OS to intelligently manage page caching, potentially keeping frequently accessed parts of files in memory.

### Purpose of using mmap in M3DB
M3DB utilizes memory-mapped files for several key reasons, primarily centered around performance and efficient resource utilization for read-heavy workloads:

-   **Reduced Syscall Overhead:** Accessing data via pointers in memory is generally faster than repeated `read()` syscalls, especially for random access patterns. By mapping a file into memory, M3DB can avoid the context switching costs associated with syscalls for many read operations.
-   **Leveraging OS Page Cache:** When a file is mmaped, the operating system's page cache manages the loading and eviction of file pages. This is often highly optimized and can lead to better overall memory utilization and I/O performance, as frequently accessed data is likely to be kept in RAM by the OS.
-   **Direct Memory Access:** For read operations, data can be accessed directly from the mmaped region without needing to copy it into separate user-space buffers first. This can save CPU cycles and memory bandwidth.
-   **Simplified Code for Certain Access Patterns:** For some read patterns, accessing file data as a simple byte slice in memory can simplify the application code compared to manual buffer management and file seeking.
-   **Shared Memory Access (Potentially):** While M3DB primarily uses `MAP_PRIVATE` (which creates a copy-on-write mapping, so modifications are not propagated to the original file or other processes), mmap provides the foundation for shared memory if needed, though this is not the primary use case for fileset data in M3DB.

M3DB typically uses mmap for read-only access to its persistent data files, allowing the OS to handle the complexities of caching and I/O optimization. The `src/x/mmap/mmap.go` wrapper in M3DB provides utilities for these operations, including options for HugeTLB for potentially further performance gains on suitable systems.

### Files mmaped by M3DB
M3DB memory-maps several types of files, almost exclusively for read-only purposes, to accelerate query performance:

-   **Fileset Data Files (`data.db`):** As processed by `src/dbnode/persist/fs/read.go`, the files containing the actual time series data (M3TSZ encoded segments) are mmaped. This allows for quick random access to different series' data blocks when serving queries.
-   **Fileset Index Files (`index.db`):** Also handled by `src/dbnode/persist/fs/read.go`, the index files that map series IDs to their data offsets within the `data.db` files are mmaped. This speeds up the lookup of series locations.
-   **Fileset Bloom Filter Files (`bloomfilter.db`):** Managed by `src/dbnode/persist/fs/bloom_filter.go`, these files are mmaped to allow for rapid probabilistic checks of series ID existence within a fileset, avoiding unnecessary disk I/O.
-   **Fileset Index Summaries Files (`summaries.db`):** Managed by `src/dbnode/persist/fs/index_lookup.go`, these files, which provide a downsampled view of the index for faster range scans, are mmaped.
-   **M3ninx Index Segment Files:** The M3ninx library, used for M3DB's inverted index, also uses mmap extensively. As seen in `src/m3ninx/persist/reader.go`, various components of its index segments are mmaped for reading. This includes:
    -   Document Data files
    -   Document Index files
    -   Postings files
    -   FST (Finite State Transducer) Fields files
    -   FST (Finite State Transducer) Terms files

In contrast, **Commit Log files** are generally not mmaped; they use standard file I/O operations for appending writes and sequential reads during recovery, as their access pattern is primarily sequential.

The use of mmap is a strategic choice in M3DB to optimize read performance for its on-disk storage components by leveraging direct memory access and the OS's page caching capabilities. Configuration options exist to influence mmap behavior, such as forcing bloom filters or index summaries into anonymous memory regions rather than file-backed mmaps under certain conditions.

### Usage and Benefits per File Type

Below is a breakdown of how M3DB utilizes mmap for specific file types:

**Fileset Data Files (`data.db`)**
    - **Mmap Mode:** Read-only.
    - **Usage:** The entire `data.db` file, containing M3TSZ-encoded time series segments, is mmaped.
        - For non-streaming reads, M3DB creates an `io.Reader` (specifically `bytes.NewReader`) over the mmaped byte slice (`reader.dataMmap.Bytes`). Data for a specific series is then read from this region into buffers. This allows for random access to different series' data blocks within the file, guided by offsets from the index file.
        - For streaming reads (`reader.StreamingRead()`), M3DB directly slices the mmaped region (`reader.dataMmap.Bytes[offset:offset+size]`) to get the data for a series. This avoids intermediate copying.
    - **Specific Benefits:**
        - Enables efficient random access to time series data blocks, crucial for query performance, as the OS can page in only the required portions of the file.
        - Direct memory access for streaming reads minimizes data copying.
        - Leverages the OS page cache for frequently accessed data segments.
    - **Relevant Code Pointers:** `src/dbnode/persist/fs/read.go` (struct `reader`, methods `Open()`, `Read()`, `StreamingRead()`). The mmap itself is initiated via `mmap.Files()`.

**Fileset Index Files (`index.db`)**
    - **Mmap Mode:** Read-only.
    - **Usage:** The `index.db` file, which maps series IDs to their data offsets in `data.db`, is fully mmaped (`reader.indexMmap.Bytes`).
        - This mmaped region is wrapped by a `dataFileSetReaderDecoderStream` (which uses a `bytes.NewReader`).
        - A `msgpack.Decoder` then reads from this stream to decode `schema.IndexEntry` objects.
        - Depending on the read mode (streaming or not), index entries are either decoded all at once upfront or on-demand as the reader progresses.
    - **Specific Benefits:**
        - Accelerates the lookup of series locations by having the entire index structure directly accessible in memory.
        - Supports both full index scans and on-demand entry decoding from the mmaped region.
        - OS page caching for frequently accessed index portions.
    - **Relevant Code Pointers:** `src/dbnode/persist/fs/read.go` (struct `reader`, methods `Open()`, `readIndexAndSortByOffsetAsc()`, `StreamingReadMetadata()`). `reader.indexMmap.Bytes` holds the mmaped data.

**Fileset Bloom Filter Files (`bloomfilter.db`)**
    - **Mmap Mode:** Read-only.
    - **Usage:** The bloom filter file is mmaped into memory. The `github.com/m3db/bloom/v4` library's `ConcurrentReadOnlyBloomFilter` is then initialized directly with the mmaped byte slice (`bloomFilterMmap.Bytes`).
        - Operations like `Test(value)` are performed directly on this memory region.
    - **Specific Benefits:**
        - Extremely fast probabilistic checks for series ID existence, as there's no disk I/O per check after initial mapping.
        - Reduces unnecessary lookups in the more expensive index and data files.
        - Efficient OS-level caching of the bloom filter.
    - **Relevant Code Pointers:** `src/dbnode/persist/fs/bloom_filter.go` (func `newManagedConcurrentBloomFilterFromFile()`). The `mmap.Descriptor.Bytes` is passed to `bloom.NewConcurrentReadOnlyBloomFilter()`.

**Fileset Index Summaries Files (`summaries.db`)**
    - **Mmap Mode:** Read-only.
    - **Usage:** The `summaries.db` file, providing a sparse index over the main `index.db`, is mmaped.
        - The `nearestIndexOffsetLookup` structure holds this mmaped region (`summariesMmap.Bytes`).
        - During lookups (`getNearestIndexFileOffset`), methods on `xmsgpack.IndexSummaryToken` (like `ID()` and `IndexOffset()`) directly access byte slices from this mmaped region to perform binary searches and retrieve offsets.
        - `msgpack.Decoder` also reads from this mmaped region during the initial loading of summary tokens.
    - **Specific Benefits:**
        - Enables very fast binary searches over the summarized index entries directly in memory, speeding up the process of finding a starting point for scans in the main `index.db` file.
        - OS-managed caching.
    - **Relevant Code Pointers:** `src/dbnode/persist/fs/index_lookup.go` (struct `nearestIndexOffsetLookup`, funcs `newNearestIndexOffsetLookupFromSummariesFile()`, `getNearestIndexFileOffset()`).

**M3ninx Index Segment Files**
    - **File Types:** Document Data, Document Index, Postings Data, FST Fields Data, FST Terms Data.
    - **Mmap Mode:** Read-only.
    - **Usage:** Each component file of an M3ninx FST segment is mmaped.
        - `src/m3ninx/persist/reader.go` (func `filesetToSegmentData`) calls `IndexSegmentFile.Mmap()` for each file, storing the `mmap.Descriptor` (and thus the byte slice) in an `fst.SegmentData` struct.
        - This `fst.SegmentData` is used by `src/m3ninx/index/segment/fst/segment.go` (func `NewSegment`) to construct the in-memory representation of the FST segment.
        - For example, `vellum.Load()` is called directly on mmaped byte slices for FST structures (`FSTFieldsData.Bytes`, `FSTTermsData.Bytes`).
        - Document data (`DocsData.Bytes`, `DocsIdxData.Bytes`) and postings lists data (`PostingsData.Bytes`) are also accessed as byte slices from these mmaped regions for lookups and unmarshaling. Methods like `retrieveBytesWithRLock` and `retrieveTermsBytesWithRLock` in `fst/segment.go` directly operate on these mmaped slices.
    - **Specific Benefits:**
        - High-performance FST operations (term/field lookups, regex searches) by having FST structures directly in memory.
        - Efficient retrieval and unmarshaling of postings lists and document metadata.
        - Avoids significant disk I/O during complex index queries.
        - OS handles caching of frequently accessed index parts (e.g., popular FST nodes or postings lists).
    - **Relevant Code Pointers:** `src/m3ninx/persist/reader.go` (func `filesetToSegmentData`), `src/m3ninx/index/segment/fst/segment.go` (struct `fsSegment`, func `NewSegment`, and various internal methods like `retrieveBytesWithRLock`).

### Potential Drawbacks and Considerations

While mmap offers significant performance advantages for read-heavy workloads, its use also comes with potential drawbacks and operational considerations that are relevant for a system like M3DB:

-   **Memory Pressure and Address Space:**
    -   Each mmaped file consumes a portion of the process's virtual address space. While 64-bit systems offer a vast address space, an extremely large number of small mmaped files (as can occur with many time series segments or index files) could theoretically still lead to pressure on virtual memory resources or hit per-process limits for memory maps.
    -   More critically, mmap relies on the OS page cache. If the total working set of mmaped data actively being accessed significantly exceeds available physical RAM, the system can experience "page cache thrashing." This occurs when the OS is forced to continuously swap pages between RAM and disk, leading to degraded performance. M3DB's architecture, with many individual files for different blocks and segments, means the number of mmaped regions can be substantial. Historical changelog entries (e.g., around v0.8.0) indicated a temporary switch away from mmap for data/index files specifically to reduce the number of mmaps, highlighting this as a past concern.

-   **I/O Stalls (Page Faults):**
    -   When a process accesses a part of an mmaped file that is not currently resident in physical RAM (a "cold" page), a page fault occurs. The OS then reads the required page from disk.
    -   For the faulting process/thread, this disk I/O is synchronous and blocking. This means that if M3DB needs to access a cold part of an mmaped data or index file, the query processing for that request can stall until the data is loaded. This can introduce variability in query latency, especially if the page cache is cold or under pressure.

-   **Error Handling:**
    -   Traditional I/O operations (`read()`, `write()`) typically return error codes that can be checked and handled by the application.
    -   With mmap, I/O errors (e.g., disk hardware failure, file corruption after mapping, or file truncation) occurring during access to a mapped region are often delivered as signals to the process, such as `SIGBUS` (bus error) or `SIGSEGV` (segmentation fault).
    -   Handling these signals gracefully can be more complex than handling typical I/O errors. An unhandled `SIGBUS` will terminate the process, which could be disruptive for a database node. M3DB, like any application using mmap extensively, needs to be robust to such scenarios, though explicit signal handling for mmap errors is not always straightforward.

-   **Kernel Parameter Tuning:**
    -   Effective use of mmap, especially at scale, often requires careful tuning of OS kernel parameters.
    -   **`vm.max_map_count`**: This parameter (mentioned in M3DB's operational guide) defines the maximum number of memory map areas a process can have. M3DB's design, which can involve many filesets and index segments, can lead to a large number of mmap regions. If this limit is too low, M3DB nodes may fail to start or map new files. The M3DB documentation recommends a significantly increased value (e.g., `3000000`).
    -   Other parameters like swappiness (`vm.swappiness`, also mentioned in the guide), page cache behavior, and dirty page writeback thresholds can also influence mmap performance and overall system stability.

-   **Debugging Complexity:**
    -   Issues related to mmap can sometimes be more challenging to debug than problems with explicit file I/O. This is because mmap involves a deeper interaction with the operating system's virtual memory (VM) subsystem.
    -   Problems might manifest as unexpected crashes (e.g., `SIGBUS`), subtle performance degradations due to page faulting, or complex interactions with the page cache. Tools like `perf`, `strace` (for observing `mmap` and page fault syscalls), and system memory profiling tools become essential.

-   **Portability/Platform Differences:**
    -   While `mmap` is a POSIX standard, there can be subtle differences in its behavior, performance characteristics, or available flags across different operating systems or kernel versions.
    -   M3DB's mmap wrapper (`src/x/mmap/`) has platform-specific files (e.g., `mmap_linux.go`, `mmap_darwin.go`, `mmap_other.go`), indicating that some level of platform-specific handling is already necessary. These differences could impact aspects like error reporting or the exact semantics of certain flags.

Despite these considerations, M3DB's choice to use mmap for read-intensive access to its data and index files is a common and often effective strategy for achieving high performance in database systems, offloading much of the I/O and caching complexity to the operating system. Careful monitoring and appropriate kernel tuning are key to successful deployments.

## Bootstrapping

Bootstrapping is the process by which an M3DB node initializes its state, primarily by loading existing data from disk or peers when it starts up or when its shard ownership changes. It's a critical phase to ensure a node can serve queries accurately and participate correctly in the cluster. The commit log plays a role in recovery during bootstrapping by replaying acknowledged writes that might not have been flushed to disk filesets yet.

### Purpose of Bootstrapping
M3DB needs to bootstrap for several key reasons:

-   **Node Startup/Restart:** When a node starts or restarts (e.g., after a crash or planned maintenance), it needs to load its assigned shards' data into memory to serve queries and accept new writes. This includes data previously persisted to disk (filesets) and any recent writes still only in the commit log.
-   **Integrating New Nodes:** When a new node is added to the cluster, it gets assigned a set of shards. It must bootstrap the data for these shards from other replicas (peers) in the cluster.
-   **Shard Ownership Changes:** If the cluster topology changes (e.g., due to scaling operations or node failures) and a node becomes responsible for new shards, it must bootstrap the data for these newly assigned shards.
-   **Catching Up on Missed Writes:** If a node was down for a period, upon restarting, it needs to catch up on writes it missed. This involves replaying its commit log and potentially fetching more recent data from peers or shared storage if applicable.
-   **Data Repair:** While not strictly part of the initial bootstrap, the mechanisms used for bootstrapping (like streaming data from peers) can also be leveraged for repairing data inconsistencies.

The goal is to bring the node's in-memory state (series data in buffers, index segments) up-to-date for the shards it owns, ensuring data consistency and availability.

### Bootstrapping Process, Strategies, and Data Sources

The bootstrapping process in M3DB is a sophisticated operation designed to efficiently load data from various sources and bring a node to a consistent state. A bootstrapping strategy defines the sequence and methods used to acquire this data.

**Main Goal:**
The primary objective is to initialize a node so that it correctly owns a set of shards and has loaded all necessary data (time series data and index information) into its in-memory structures. This allows the node to serve queries and accept new writes.

**Typical Triggers:**
-   **Node Startup/Restart:** When a node starts or restarts.
-   **Topology Changes:** When shard ownership changes.

**Key Components Involved:**
-   **`databaseBootstrapper` (managed by `databaseBootstrapManager`):** Central orchestrator for the entire database bootstrap.
-   **`bootstrap.ProcessProvider`:** Provides a `bootstrap.Process` instance.
-   **`bootstrap.Process` (implemented by `bootstrapProcess`):** Executes a bootstrap run, determining target time ranges.
-   **`Bootstrapper` (interface):** Defines a specific strategy, often a sequence of sources. Each `Bootstrapper` uses one or more `Source` implementations.
-   **`NamespaceDataAccumulator` (interface):** Used to load data into the correct namespace/shard.
-   **`databaseNamespace` and `databaseShard`:** Target structures for the loaded data.

**High-Level Sequence of Operations:**
1.  **Initialization & Trigger:** The `databaseBootstrapManager` initiates the process.
2.  **Determine Ranges and Targets:** The `bootstrapProcess` calculates required time ranges for data and index information, often in multiple "passes" or "runs":
    -   A "first pass" (`firstRangeWithPersistTrue`): Usually covers older, historical data. Data from peers might be configured with `PersistConfig` to be flushed to disk immediately (`persist.FileSetFlushType`).
    -   A "second pass" (`secondRange`): Covers more recent data. Data might be persisted as snapshots (`persist.FileSetSnapshotType`) to minimize commit log replay on the next restart.
3.  **Iterate Through Bootstrappers/Sources:** The system iterates through a defined sequence of `Bootstrapper` sources (detailed below). Each `Source` reports available data (`Source.AvailableData()`, `Source.AvailableIndex()`) and then `Source.Read()` fetches it.
4.  **Data Accumulation & Loading:** Fetched series data is loaded into `dbSeries` via `NamespaceDataAccumulator` (using `SeriesRef.LoadBlock()`). Index segments are loaded into `nsIndex` (via `nsIndex.Bootstrap()`). A `Cache` assists with fileset metadata.
5.  **Marking Completion:** Once all required data for assigned shards in a namespace is loaded, the namespace (and eventually the node) is marked as bootstrapped.

**Default Bootstrapping Strategy and Data Sources:**
M3DB typically employs a sequential, multi-stage strategy using sources in the following order. Each source is represented by an implementation of the `bootstrap.Source` interface:

1.  **Filesystem Bootstrapper (`src/dbnode/storage/bootstrap/bootstrapper/fs/source.go`):**
    -   **How it works:** Reads data directly from existing fileset files (info, index, data, bloom filter, summaries) on local disk.
    -   **Data Restored:** Historical data (M3TSZ segments and M3ninx FSTs).
    -   **Typical Use:** Usually the first source tried; fastest for existing persistent data.

2.  **Commit Log Bootstrapper (`src/dbnode/storage/bootstrap/bootstrapper/commitlog/source.go`):**
    -   **Role and Invocation:** Typically run after the fileset bootstrapper to replay data points from commit log files more recent than what filesets restored.
    -   **Data Restored:** Recent data points and series metadata not yet flushed. Essential for durability.
    -   **Detailed Recovery Process:**
        -   **Snapshot Consideration:** First attempts to load from existing snapshot files (`fs.SnapshotFiles`), which are compacted commit log data, to reduce processing.
        -   **File Filtering:** Uses `readCommitLogFilePredicate` to consider only commit log files existing *before* current node startup.
        -   **Time Range:** Focuses on unfulfilled time ranges, typically more recent than fileset data.
        -   **Reading and Applying Entries:** An `commitlog.Iterator` reads `commitlog.LogEntry` items. Data is written to series via `NamespaceDataAccumulator` using `series.WriteOptions{BootstrapWrite: true}`. This uses a temporary side buffer (`dbSeriesBootstrap.buffer`) in `dbSeries`, which is later merged by `dbSeries.Bootstrap()`. `SkipOutOfRetention: true` is also set.
        -   **Avoiding Replay of Flushed Data:** The fileset bootstrapper runs first. The commit log bootstrapper, along with deduplication in series buffers, ensures only newer or missing data is incorporated. Snapshots also help skip covered log portions.
        -   **Importance for Durability:** Recovers acknowledged writes not yet in filesets. Handles corrupt logs by logging errors, allowing subsequent bootstrappers (peers) to attempt recovery.

3.  **Peers Bootstrapper (`src/dbnode/storage/bootstrap/bootstrapper/peers/source.go`):**
    -   **How it works:** Streams data (series blocks and index segments) from replica peers in the cluster using `AdminClient`.
    -   **Data Restored:** Both time series data blocks and index segments.
    -   **Typical Use:** For new nodes, data repair/catch-up if local data is incomplete/corrupted, or if a node was down for an extended period. Ensures consistency.

4.  **Uninitialized Topology Bootstrapper (`src/dbnode/storage/bootstrap/bootstrapper/uninitialized/source.go`):**
    -   **How it works:** Marks shards as bootstrapped if no other source provided data. Does not fetch data.
    -   **Data Restored:** None directly.
    -   **Typical Use:** Often last in the chain; finalizes new/empty shards so they can accept writes.

**Configuration and Control:**

-   **Influence of `BootstrapOptions`:**
    Behavior (especially for Peers bootstrapper) is influenced by `BootstrapOptions` (in `src/dbnode/namespace/options.go`):
    -   `DefaultBootstrapConsistencyLevel`: (e.g., `topology.ReadConsistencyLevelMajority`) dictates peer agreement requirements.
    -   `Bootstrappers` field in `namespace.Options`: Allows custom list/order of bootstrappers.
    -   `RunOptions` (e.g., `PersistConfig`): Can dictate if peer-bootstrapped data is immediately persisted.

-   **Execution Management:**
    -   The `databaseBootstrapManager` uses a `bootstrap.ProcessProvider` (configured in `StorageOptions`) to get a `bootstrap.Process`.
    -   The `BootstrapperProvider` (via `StorageOptions.SetBootstrapProcessProvider()`) creates the `Bootstrapper` instance defining the strategy (e.g., `sequentialBootstrapper`).
    -   The `bootstrap.Process` calls the `Bootstrapper.Bootstrap()` method.
    -   The `Bootstrapper` iterates its `Source`s. Fulfilled ranges are not re-requested from later sources in a sequence.

The bootstrapping process for a namespace/shard is "done" when all required time ranges are populated. The `databaseBootstrapManager` tracks this, transitioning node state from `Bootstrapping` to `Bootstrapped`.

M3DB doesn't use operator-selectable "named strategies" (e.g., "repair_only") via simple configuration strings. The strategy is implicitly defined by the bootstrapper sequence and `BootstrapOptions`. Repair operations might use peer bootstrapping components with custom parameters.

### Bootstrapping Strategies

A bootstrapping strategy in M3DB defines the sequence and methods used to acquire data from various sources to bring a node's shards to an up-to-date and consistent state. The strategy aims to be efficient by prioritizing faster local sources before resorting to potentially slower network-based sources.

**Default Bootstrapping Strategy:**
M3DB typically employs a sequential, multi-stage strategy orchestrated by the `databaseBootstrapManager` (in `src/dbnode/storage/bootstrap.go`) which uses a `bootstrap.ProcessProvider`. The provider supplies a `bootstrap.Process` (implemented by `bootstrapProcess` in `src/dbnode/storage/bootstrap/process.go`), which in turn uses a configured `Bootstrapper`. The most common `Bootstrapper` is one that tries sources in the following order:

1.  **Filesystem Bootstrapper:** This is generally the first source. It attempts to load data from existing fileset files (data, index, summaries, bloom filters) already present on the node's local disk. This is the quickest way to restore the bulk of historical data.
2.  **Commit Log Bootstrapper:** If the Filesystem bootstrapper doesn't cover the most recent time ranges (up to the present), the Commit Log bootstrapper runs next. It replays commit log files to recover data that was written and acknowledged but not yet flushed to filesets. This ensures durability for recent writes.
3.  **Peers Bootstrapper:** If data is still missing after the Filesystem and Commit Log bootstrappers (e.g., for a new node, a node that was down for an extended period, or if data corruption is detected/suspected), the Peers bootstrapper attempts to stream the necessary data (both time series blocks and index segments) from replica peers in the cluster.
4.  **Uninitialized Topology Bootstrapper:** This is usually the final step. If a shard is entirely new to the cluster or has no data available from any of the prior sources, this bootstrapper marks the shard as "bootstrapped," allowing it to start accepting new writes.

**Influence of `BootstrapOptions`:**
The behavior of these sources, particularly the Peers bootstrapper, can be influenced by `BootstrapOptions` (defined in `src/dbnode/namespace/options.go` and part of a namespace's configuration):
-   `DefaultBootstrapConsistencyLevel`: This option (e.g., `topology.ReadConsistencyLevelMajority`) dictates how many peers must agree/provide data for a shard/block during peer bootstrapping for it to be considered consistent and sufficient. If this level isn't met, the peer bootstrap for that specific time range might be considered unfulfilled, potentially leading to data gaps if no other source can provide it.
-   Other options within `BootstrapOptions` can enable or disable specific bootstrappers or alter their parameters for a given namespace. For example, the `Bootstrappers` field in `namespace.Options` allows specifying a custom list of bootstrappers, overriding the default sequence.

**Execution Management:**
-   The `databaseBootstrapManager` (`src/dbnode/storage/bootstrap.go`) initiates the overall process. It calls `processProvider.Provide()` to get a `bootstrap.Process`. The `ProcessProvider` is configured in `StorageOptions` and holds the `BootstrapperProvider`.
-   The `BootstrapperProvider` (configured via `StorageOptions.SetBootstrapProcessProvider()`) is responsible for creating the specific `Bootstrapper` instance that defines the strategy (e.g., a `sequentialBootstrapper` which runs a list of sources in order).
-   The `bootstrap.Process` (`src/dbnode/storage/bootstrap/process.go`) then takes this `Bootstrapper` and calls its `Bootstrap` method.
-   The `Bootstrapper` iterates through its configured `Source`s. Each `Source` (e.g., filesystem, commitlog, peers) attempts to fulfill the data requirements for the specified time ranges (`ShardTimeRanges`). If a source fulfills a range, it's typically not requested from subsequent sources in a sequential strategy.

**"Runs" or "Passes" in Bootstrapping:**
The `bootstrapProcess.Run` method divides the total bootstrapping time window into logical "passes" or target ranges. Typically, this involves:
1.  A "first pass" (`firstRangeWithPersistTrue` in `bootstrapProcess.targetRanges()`): This usually covers older, historical data. Data bootstrapped in this pass, especially from peers, might be configured with a `PersistConfig` to be flushed to disk immediately (`persist.FileSetFlushType`) to avoid holding large amounts of historical data in memory.
2.  A "second pass" (`secondRange`): This covers more recent data, closer to the present time. Data bootstrapped here might also be persisted, potentially as snapshots (`persist.FileSetSnapshotType`), to ensure that post-bootstrap, the node has a recent on-disk state that minimizes commit log replay on the next restart.

The bootstrapping process for a given namespace and its shards is considered "done" when all its required time ranges (for both data and index, if applicable) have been successfully populated by the sequence of bootstrappers according to the defined strategy and consistency requirements. The `databaseBootstrapManager` tracks this, transitioning the node's state from `Bootstrapping` to `Bootstrapped`. Individual shards also track their bootstrap state via `databaseShard.IsBootstrapped()`.

M3DB does not typically use distinct, operator-selectable "named strategies" like "repair_only" exposed directly via a simple string name in configuration. Instead, the strategy is implicitly defined by the configured sequence of bootstrappers (via the `BootstrapperProvider` in `StorageOptions`) and the per-namespace `BootstrapOptions`. However, specific operational needs like repair might leverage the peer bootstrapping components with different parameters or target ranges, effectively creating a custom strategy for that operation.

### Data Sources for Bootstrapping
M3DB can bootstrap data from several sources, typically trying them in a specific order to efficiently reconstruct a node's state. The choice and order of these sources are determined by the node's `BootstrapOptions` (which can be configured per namespace) and the overall bootstrapping strategy. Each source is represented by an implementation of the `bootstrap.Source` interface.

-   **Fileset Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/fs/source.go`):**
    -   **How it works:** This source reads data directly from the fileset files (info, index, data, bloom filter, summaries) that are already persisted on the node's local disk. It identifies available fileset volumes for each shard and time block within the bootstrapping range.
    -   **Data Restored:** It primarily restores historical data that has been flushed from memory to disk in the past. This includes both the actual time series data (M3TSZ segments) and the corresponding index segments (M3ninx FSTs).
    -   **Typical Use:** This is usually the first source attempted, as it's the fastest way to recover the bulk of the data for a node that has existing persistent data.

-   **Commit Log Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/commitlog/source.go`):**
    -   **How it works:** This source reads through the commit log files to recover writes that were acknowledged to clients but might not have been flushed to fileset files before a node shutdown or crash. It iterates through commit log entries, reconstructs series data and metadata, and applies them to the in-memory representations. It also considers existing snapshot files (which are compact representations of commit log data up to a point) to potentially reduce the amount of raw commit log data it needs to process.
    -   **Data Restored:** It restores the most recent data points and series metadata that were written since the last successful flush/snapshot covered by the fileset bootstrapper. This ensures durability and prevents data loss for acknowledged writes.
    -   **Typical Use:** This source is critical for data recovery after an unexpected shutdown. It's typically run after the fileset bootstrapper to fill in the most recent data. The time range it covers is usually from the end of the last complete fileset block up to the current time (or the end of the commit log).

-   **Peer Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/peers/source.go`):**
    -   **How it works:** This source allows a node to stream data from its replica peers in the cluster. The node sends requests to its peers for specific shards and time ranges. Peers respond with the series data (blocks) and index segments they own. The client (`AdminClient`) handles fetching this data.
    -   **Data Restored:** It can fetch both time series data blocks and index segments.
    -   **Typical Use:**
        -   **New Nodes:** Essential for new nodes joining the cluster that have no local data.
        -   **Data Repair/Catch-up:** Used if a node's local data (filesets or commit log) is incomplete, corrupted, or if the node has been down for an extended period and missed many writes that peers have.
        -   **Consistency:** Helps ensure consistency across replicas. If a node determines its local data is insufficient (e.g., based on checksums or versioning, though this is more related to repair than basic bootstrap), it might prefer peer data.

-   **Uninitialized Topology Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/uninitialized/source.go`):**
    -   **How it works:** This is a special bootstrapper that doesn't fetch data from an external source. Instead, it marks shards for which no other bootstrapper provided data as bootstrapped.
    -   **Data Restored:** None directly. It essentially finalizes the state for shards that are genuinely new or empty.
    -   **Typical Use:** It's often the last bootstrapper in the chain. If a shard is newly added to a node's ownership and has no historical data on disk or on peers (e.g., a completely new shard in the cluster), this bootstrapper ensures it's marked as "bootstrapped" so the node can begin accepting writes for it.

**Order/Preference of Sources:**
The M3DB node's `Options` configure a `BootstrapProcessProvider`, which in turn provides a `bootstrap.Process`. This process (typically `bootstrapProcess` from `src/dbnode/storage/bootstrap/process.go`) orchestrates the overall flow. The default M3DB `BootstrapperProvider` (often configured in `db.NewDatabase`) usually defines a sequence of bootstrappers:
1.  **Filesystem Bootstrapper:** Prioritized to quickly load data already on local disk.
2.  **Commit Log Bootstrapper:** To recover the most recent writes not yet persisted to filesets.
3.  **Peers Bootstrapper:** Used if local data is insufficient or for new nodes.
4.  **Uninitialized Topology Bootstrapper:** To finalize shards that have no data from other sources.

The `bootstrap.Process` iterates through these configured bootstrappers. Each `Bootstrapper` (via its `Source`) reports the time ranges it can fulfill (`AvailableData` and `AvailableIndex`). The process then attempts to `Read` data from the chosen source for the required ranges. If a source cannot fulfill certain ranges, those ranges are passed to the next bootstrapper in the sequence.

The `BootstrapOptions` within each `namespace.Options` can enable or disable specific bootstrappers for that namespace (e.g., `fsOpts.SetDefaultBootstrapToolVersion(FilesystemBootstrapperVersion)` can enable/disable filesystem bootstrapper implicitly). The specific `RunOptions` passed during a bootstrap run (e.g., `PersistConfig` indicating whether to persist data immediately during peer bootstrap) can also influence how a source behaves. For instance, data bootstrapped from peers might be configured to be written to disk (flushed) immediately or held in memory to be flushed later by the standard flushing mechanism.

### Commit Log Recovery during Bootstrapping

Commit log recovery is a crucial part of the bootstrapping strategy, ensuring data durability by restoring writes that were acknowledged but not yet persisted to fileset files before a node shutdown or crash.

**Role and Invocation:**
-   The commit log bootstrapper (`commitlog.Source` in `src/dbnode/storage/bootstrap/bootstrapper/commitlog/source.go`) is typically invoked after the fileset bootstrapper.
-   Its primary role is to replay data points from commit log files that are more recent than the data already restored from filesets (snapshots or flushed data files).

**Determining Which Commit Logs to Read:**
-   **Snapshot Consideration:** Before reading raw commit log entries, the `commitlog.Source` first attempts to load data from any existing snapshot files (`fs.SnapshotFiles`) relevant to the bootstrapping shards and time ranges. Snapshots are compacted representations of commit log data and can significantly reduce the amount of raw commit log data it needs to process. The source identifies the most recent complete snapshot for each shard/block combination.
-   **File Filtering:** When iterating through commit log files, a `FileFilterPredicate` (`readCommitLogFilePredicate`) is used. This predicate ensures that only commit log files that existed *before* the current node startup are considered. This prevents re-reading data from the active commit log segment that might contain writes already processed in the current session or data that is still in memory.
-   **Time Range:** The commit log bootstrapper aims to fill in data for the time ranges requested by the overall `bootstrap.Process`, typically focusing on periods more recent than what filesets cover, up to the current time. The exact time ranges are determined by what remains unfulfilled after previous bootstrappers (like the fileset bootstrapper) have run.

**Reading and Applying Entries:**
1.  **Iteration:** An iterator (`commitlog.Iterator` from `src/dbnode/persist/fs/commitlog/reader.go`) reads entries sequentially from the selected commit log files. Each entry (`commitlog.LogEntry`) contains the series metadata (ID, namespace, shard, encoded tags), the datapoint (timestamp, value), unit, and any annotation.
2.  **Data Accumulation:**
    -   For each entry, the `commitlog.Source` resolves the namespace and series.
    -   It uses a `NamespaceDataAccumulator` to get a `SeriesRef` for the specific series.
    -   The recovered datapoint (timestamp, value, unit, annotation) is then written to the series.
3.  **BootstrapWrite Flag:** When these recovered data points are written back into the series, it's done using a special `series.WriteOptions{BootstrapWrite: true}` flag (as seen in `src/dbnode/storage/series/series.go`).
    -   This flag causes the series to initially write these bootstrapped data points into a temporary side buffer (`dbSeriesBootstrap.buffer`).
    -   After the commit log source (or any bootstrap source) finishes processing, the `dbSeries.Bootstrap()` method is called, which merges the data from this side buffer into the main series buffer, making it visible for queries and further processing. This approach minimizes lock contention on the main series buffer during the potentially intensive bootstrapping phase.
4.  **Skip Out-of-Retention:** The `BootstrapWrite` option also sets `SkipOutOfRetention: true`, ensuring that data older than the namespace's retention policy is not re-inserted from the commit log.

**Avoiding Replay of Flushed Data:**
-   M3DB's bootstrapping process is designed to avoid replaying data that has already been successfully persisted in filesets.
-   The fileset bootstrapper runs first and populates the series buffers with data from disk.
-   The commit log bootstrapper primarily focuses on time ranges *after* the data covered by those filesets. While the commit log reader itself might read older entries, the `SeriesRef.Write()` mechanism, when loading into series buffers, handles deduplication and ensures that only newer or missing data points are actually incorporated. The snapshot files also help in quickly skipping over large portions of the commit log that are already covered by on-disk snapshots.

**Importance for Durability:**
Commit log recovery is fundamental to M3DB's durability guarantees. It ensures that any write acknowledged by M3DB (when using `StrategyWriteWait` or after a successful commit log flush for `StrategyWriteBehind`) can be recovered even if the node crashes before that data is flushed to a fileset. This minimizes data loss and helps maintain data consistency upon node restarts. The process also handles corrupt commit log files by logging errors and potentially marking ranges as unfulfilled, allowing subsequent bootstrappers (like peers) to attempt recovery.

### Bootstrapping Strategies

A bootstrapping strategy in M3DB defines the sequence and methods used to acquire data from various sources to bring a node's shards to an up-to-date and consistent state. The strategy aims to be efficient by prioritizing faster local sources before resorting to potentially slower network-based sources.

**Default Bootstrapping Strategy:**
M3DB typically employs a sequential, multi-stage strategy orchestrated by the `databaseBootstrapManager` (in `src/dbnode/storage/bootstrap.go`) which uses a `bootstrap.ProcessProvider`. The provider supplies a `bootstrap.Process` (implemented by `bootstrapProcess` in `src/dbnode/storage/bootstrap/process.go`), which in turn uses a configured `Bootstrapper`. The most common `Bootstrapper` is one that tries sources in the following order:

1.  **Filesystem Bootstrapper:** This is generally the first source. It attempts to load data from existing fileset files (data, index, summaries, bloom filters) already present on the node's local disk. This is the quickest way to restore the bulk of historical data.
2.  **Commit Log Bootstrapper:** If the Filesystem bootstrapper doesn't cover the most recent time ranges (up to the present), the Commit Log bootstrapper runs next. It replays commit log files to recover data that was written and acknowledged but not yet flushed to filesets. This ensures durability for recent writes.
3.  **Peers Bootstrapper:** If data is still missing after the Filesystem and Commit Log bootstrappers (e.g., for a new node, a node that was down for an extended period, or if data corruption is detected/suspected), the Peers bootstrapper attempts to stream the necessary data (both time series blocks and index segments) from replica peers in the cluster.
4.  **Uninitialized Topology Bootstrapper:** This is usually the final step. If a shard is entirely new to the cluster or has no data available from any of the prior sources, this bootstrapper marks the shard as "bootstrapped," allowing it to start accepting new writes.

**Influence of `BootstrapOptions`:**
The behavior of these sources, particularly the Peers bootstrapper, can be influenced by `BootstrapOptions` (defined in `src/dbnode/namespace/options.go` and part of a namespace's configuration):
-   `DefaultBootstrapConsistencyLevel`: This option (e.g., `topology.ReadConsistencyLevelMajority`) dictates how many peers must agree/provide data for a shard/block during peer bootstrapping for it to be considered consistent and sufficient. If this level isn't met, the peer bootstrap for that specific time range might be considered unfulfilled, potentially leading to data gaps if no other source can provide it.
-   Other options within `BootstrapOptions` can enable or disable specific bootstrappers or alter their parameters for a given namespace. For example, the `Bootstrappers` field in `namespace.Options` allows specifying a custom list of bootstrappers, overriding the default sequence.

**Execution Management:**
-   The `databaseBootstrapManager` (`src/dbnode/storage/bootstrap.go`) initiates the overall process. It calls `processProvider.Provide()` to get a `bootstrap.Process`. The `ProcessProvider` is configured in `StorageOptions` and holds the `BootstrapperProvider`.
-   The `BootstrapperProvider` (configured via `StorageOptions.SetBootstrapProcessProvider()`) is responsible for creating the specific `Bootstrapper` instance that defines the strategy (e.g., a `sequentialBootstrapper` which runs a list of sources in order).
-   The `bootstrap.Process` (`src/dbnode/storage/bootstrap/process.go`) then takes this `Bootstrapper` and calls its `Bootstrap` method.
-   The `Bootstrapper` iterates through its configured `Source`s. Each `Source` (e.g., filesystem, commitlog, peers) attempts to fulfill the data requirements for the specified time ranges (`ShardTimeRanges`). If a source fulfills a range, it's typically not requested from subsequent sources in a sequential strategy.

**"Runs" or "Passes" in Bootstrapping:**
The `bootstrapProcess.Run` method divides the total bootstrapping time window into logical "passes" or target ranges. Typically, this involves:
1.  A "first pass" (`firstRangeWithPersistTrue` in `bootstrapProcess.targetRanges()`): This usually covers older, historical data. Data bootstrapped in this pass, especially from peers, might be configured with a `PersistConfig` to be flushed to disk immediately (`persist.FileSetFlushType`) to avoid holding large amounts of historical data in memory.
2.  A "second pass" (`secondRange`): This covers more recent data, closer to the present time. Data bootstrapped here might also be persisted, potentially as snapshots (`persist.FileSetSnapshotType`), to ensure that post-bootstrap, the node has a recent on-disk state that minimizes commit log replay on the next restart.

The bootstrapping process for a given namespace and its shards is considered "done" when all its required time ranges (for both data and index, if applicable) have been successfully populated by the sequence of bootstrappers according to the defined strategy and consistency requirements. The `databaseBootstrapManager` tracks this, transitioning the node's state from `Bootstrapping` to `Bootstrapped`. Individual shards also track their bootstrap state via `databaseShard.IsBootstrapped()`.

M3DB does not typically use distinct, operator-selectable "named strategies" like "repair_only" exposed directly via a simple string name in configuration. Instead, the strategy is implicitly defined by the configured sequence of bootstrappers (via the `BootstrapperProvider` in `StorageOptions`) and the per-namespace `BootstrapOptions`. However, specific operational needs like repair might leverage the peer bootstrapping components with different parameters or target ranges, effectively creating a custom strategy for that operation.

### Data Sources for Bootstrapping
M3DB can bootstrap data from several sources, typically trying them in a specific order to efficiently reconstruct a node's state. The choice and order of these sources are determined by the node's `BootstrapOptions` (which can be configured per namespace) and the overall bootstrapping strategy. Each source is represented by an implementation of the `bootstrap.Source` interface.

-   **Fileset Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/fs/source.go`):**
    -   **How it works:** This source reads data directly from the fileset files (info, index, data, bloom filter, summaries) that are already persisted on the node's local disk. It identifies available fileset volumes for each shard and time block within the bootstrapping range.
    -   **Data Restored:** It primarily restores historical data that has been flushed from memory to disk in the past. This includes both the actual time series data (M3TSZ segments) and the corresponding index segments (M3ninx FSTs).
    -   **Typical Use:** This is usually the first source attempted, as it's the fastest way to recover the bulk of the data for a node that has existing persistent data.

-   **Commit Log Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/commitlog/source.go`):**
    -   **How it works:** This source reads through the commit log files to recover writes that were acknowledged to clients but might not have been flushed to fileset files before a node shutdown or crash. It iterates through commit log entries, reconstructs series data and metadata, and applies them to the in-memory representations. It also considers existing snapshot files (which are compact representations of commit log data up to a point) to potentially reduce the amount of raw commit log data it needs to process.
    -   **Data Restored:** It restores the most recent data points and series metadata that were written since the last successful flush/snapshot covered by the fileset bootstrapper. This ensures durability and prevents data loss for acknowledged writes.
    -   **Typical Use:** This source is critical for data recovery after an unexpected shutdown. It's typically run after the fileset bootstrapper to fill in the most recent data. The time range it covers is usually from the end of the last complete fileset block up to the current time (or the end of the commit log).

-   **Peer Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/peers/source.go`):**
    -   **How it works:** This source allows a node to stream data from its replica peers in the cluster. The node sends requests to its peers for specific shards and time ranges. Peers respond with the series data (blocks) and index segments they own. The client (`AdminClient`) handles fetching this data.
    -   **Data Restored:** It can fetch both time series data blocks and index segments.
    -   **Typical Use:**
        -   **New Nodes:** Essential for new nodes joining the cluster that have no local data.
        -   **Data Repair/Catch-up:** Used if a node's local data (filesets or commit log) is incomplete, corrupted, or if the node has been down for an extended period and missed many writes that peers have.
        -   **Consistency:** Helps ensure consistency across replicas. If a node determines its local data is insufficient (e.g., based on checksums or versioning, though this is more related to repair than basic bootstrap), it might prefer peer data.

-   **Uninitialized Topology Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/uninitialized/source.go`):**
    -   **How it works:** This is a special bootstrapper that doesn't fetch data from an external source. Instead, it marks shards for which no other bootstrapper provided data as bootstrapped.
    -   **Data Restored:** None directly. It essentially finalizes the state for shards that are genuinely new or empty.
    -   **Typical Use:** It's often the last bootstrapper in the chain. If a shard is newly added to a node's ownership and has no historical data on disk or on peers (e.g., a completely new shard in the cluster), this bootstrapper ensures it's marked as "bootstrapped" so the node can begin accepting writes for it.

**Order/Preference of Sources:**
The M3DB node's `Options` configure a `BootstrapProcessProvider`, which in turn provides a `bootstrap.Process`. This process (typically `bootstrapProcess` from `src/dbnode/storage/bootstrap/process.go`) orchestrates the overall flow. The default M3DB `BootstrapperProvider` (often configured in `db.NewDatabase`) usually defines a sequence of bootstrappers:
1.  **Filesystem Bootstrapper:** Prioritized to quickly load data already on local disk.
2.  **Commit Log Bootstrapper:** To recover the most recent writes not yet persisted to filesets.
3.  **Peers Bootstrapper:** Used if local data is insufficient or for new nodes.
4.  **Uninitialized Topology Bootstrapper:** To finalize shards that have no data from other sources.

The `bootstrap.Process` iterates through these configured bootstrappers. Each `Bootstrapper` (via its `Source`) reports the time ranges it can fulfill (`AvailableData` and `AvailableIndex`). The process then attempts to `Read` data from the chosen source for the required ranges. If a source cannot fulfill certain ranges, those ranges are passed to the next bootstrapper in the sequence.

The `BootstrapOptions` within each `namespace.Options` can enable or disable specific bootstrappers for that namespace (e.g., `fsOpts.SetDefaultBootstrapToolVersion(FilesystemBootstrapperVersion)` can enable/disable filesystem bootstrapper implicitly). The specific `RunOptions` passed during a bootstrap run (e.g., `PersistConfig` indicating whether to persist data immediately during peer bootstrap) can also influence how a source behaves. For instance, data bootstrapped from peers might be configured to be written to disk (flushed) immediately or held in memory to be flushed later by the standard flushing mechanism.

### Commit Log Recovery during Bootstrapping

Commit log recovery is a crucial part of the bootstrapping strategy, ensuring data durability by restoring writes that were acknowledged but not yet persisted to fileset files before a node shutdown or crash.

**Role and Invocation:**
-   The commit log bootstrapper (`commitlog.Source` in `src/dbnode/storage/bootstrap/bootstrapper/commitlog/source.go`) is typically invoked after the fileset bootstrapper.
-   Its primary role is to replay data points from commit log files that are more recent than the data already restored from filesets (snapshots or flushed data files).

**Determining Which Commit Logs to Read:**
-   **Snapshot Consideration:** Before reading raw commit log entries, the `commitlog.Source` first attempts to load data from any existing snapshot files (`fs.SnapshotFiles`) relevant to the bootstrapping shards and time ranges. Snapshots are compacted representations of commit log data and can significantly reduce the amount of raw commit log data it needs to process. The source identifies the most recent complete snapshot for each shard/block combination.
-   **File Filtering:** When iterating through commit log files, a `FileFilterPredicate` (`readCommitLogFilePredicate`) is used. This predicate ensures that only commit log files that existed *before* the current node startup are considered. This prevents re-reading data from the active commit log segment that might contain writes already processed in the current session or data that is still in memory.
-   **Time Range:** The commit log bootstrapper aims to fill in data for the time ranges requested by the overall `bootstrap.Process`, typically focusing on periods more recent than what filesets cover, up to the current time. The exact time ranges are determined by what remains unfulfilled after previous bootstrappers (like the fileset bootstrapper) have run.

**Reading and Applying Entries:**
1.  **Iteration:** An iterator (`commitlog.Iterator` from `src/dbnode/persist/fs/commitlog/reader.go`) reads entries sequentially from the selected commit log files. Each entry (`commitlog.LogEntry`) contains the series metadata (ID, namespace, shard, encoded tags), the datapoint (timestamp, value), unit, and any annotation.
2.  **Data Accumulation:**
    -   For each entry, the `commitlog.Source` resolves the namespace and series.
    -   It uses a `NamespaceDataAccumulator` to get a `SeriesRef` for the specific series.
    -   The recovered datapoint (timestamp, value, unit, annotation) is then written to the series.
3.  **BootstrapWrite Flag:** When these recovered data points are written back into the series, it's done using a special `series.WriteOptions{BootstrapWrite: true}` flag (as seen in `src/dbnode/storage/series/series.go`).
    -   This flag causes the series to initially write these bootstrapped data points into a temporary side buffer (`dbSeriesBootstrap.buffer`).
    -   After the commit log source (or any bootstrap source) finishes processing, the `dbSeries.Bootstrap()` method is called, which merges the data from this side buffer into the main series buffer, making it visible for queries and further processing. This approach minimizes lock contention on the main series buffer during the potentially intensive bootstrapping phase.
4.  **Skip Out-of-Retention:** The `BootstrapWrite` option also sets `SkipOutOfRetention: true`, ensuring that data older than the namespace's retention policy is not re-inserted from the commit log.

**Avoiding Replay of Flushed Data:**
-   M3DB's bootstrapping process is designed to avoid replaying data that has already been successfully persisted in filesets.
-   The fileset bootstrapper runs first and populates the series buffers with data from disk.
-   The commit log bootstrapper primarily focuses on time ranges *after* the data covered by those filesets. While the commit log reader itself might read older entries, the `SeriesRef.Write()` mechanism, when loading into series buffers, handles deduplication and ensures that only newer or missing data points are actually incorporated. The snapshot files also help in quickly skipping over large portions of the commit log that are already covered by on-disk snapshots.

**Importance for Durability:**
Commit log recovery is fundamental to M3DB's durability guarantees. It ensures that any write acknowledged by M3DB (when using `StrategyWriteWait` or after a successful commit log flush for `StrategyWriteBehind`) can be recovered even if the node crashes before that data is flushed to a fileset. This minimizes data loss and helps maintain data consistency upon node restarts. The process also handles corrupt commit log files by logging errors and potentially marking ranges as unfulfilled, allowing subsequent bootstrappers (like peers) to attempt recovery.

### Bootstrapping Strategies

A bootstrapping strategy in M3DB defines the sequence and methods used to acquire data from various sources to bring a node's shards to an up-to-date and consistent state. The strategy aims to be efficient by prioritizing faster local sources before resorting to potentially slower network-based sources.

**Default Bootstrapping Strategy:**
M3DB typically employs a sequential, multi-stage strategy orchestrated by the `databaseBootstrapManager` (in `src/dbnode/storage/bootstrap.go`) which uses a `bootstrap.ProcessProvider`. The provider supplies a `bootstrap.Process` (implemented by `bootstrapProcess` in `src/dbnode/storage/bootstrap/process.go`), which in turn uses a configured `Bootstrapper`. The most common `Bootstrapper` is one that tries sources in the following order:

1.  **Filesystem Bootstrapper:** This is generally the first source. It attempts to load data from existing fileset files (data, index, summaries, bloom filters) already present on the node's local disk. This is the quickest way to restore the bulk of historical data.
2.  **Commit Log Bootstrapper:** If the Filesystem bootstrapper doesn't cover the most recent time ranges (up to the present), the Commit Log bootstrapper runs next. It replays commit log files to recover data that was written and acknowledged but not yet flushed to filesets. This ensures durability for recent writes.
3.  **Peers Bootstrapper:** If data is still missing after the Filesystem and Commit Log bootstrappers (e.g., for a new node, a node that was down for an extended period, or if data corruption is detected/suspected), the Peers bootstrapper attempts to stream the necessary data (both time series blocks and index segments) from replica peers in the cluster.
4.  **Uninitialized Topology Bootstrapper:** This is usually the final step. If a shard is entirely new to the cluster or has no data available from any of the prior sources, this bootstrapper marks the shard as "bootstrapped," allowing it to start accepting new writes.

**Influence of `BootstrapOptions`:**
The behavior of these sources, particularly the Peers bootstrapper, can be influenced by `BootstrapOptions` (defined in `src/dbnode/namespace/options.go` and part of a namespace's configuration):
-   `DefaultBootstrapConsistencyLevel`: This option (e.g., `topology.ReadConsistencyLevelMajority`) dictates how many peers must agree/provide data for a shard/block during peer bootstrapping for it to be considered consistent and sufficient. If this level isn't met, the peer bootstrap for that specific time range might be considered unfulfilled, potentially leading to data gaps if no other source can provide it.
-   Other options within `BootstrapOptions` can enable or disable specific bootstrappers or alter their parameters for a given namespace. For example, the `Bootstrappers` field in `namespace.Options` allows specifying a custom list of bootstrappers, overriding the default sequence.

**Execution Management:**
-   The `databaseBootstrapManager` (`src/dbnode/storage/bootstrap.go`) initiates the overall process. It calls `processProvider.Provide()` to get a `bootstrap.Process`. The `ProcessProvider` is configured in `StorageOptions` and holds the `BootstrapperProvider`.
-   The `BootstrapperProvider` (configured via `StorageOptions.SetBootstrapProcessProvider()`) is responsible for creating the specific `Bootstrapper` instance that defines the strategy (e.g., a `sequentialBootstrapper` which runs a list of sources in order).
-   The `bootstrap.Process` (`src/dbnode/storage/bootstrap/process.go`) then takes this `Bootstrapper` and calls its `Bootstrap` method.
-   The `Bootstrapper` iterates through its configured `Source`s. Each `Source` (e.g., filesystem, commitlog, peers) attempts to fulfill the data requirements for the specified time ranges (`ShardTimeRanges`). If a source fulfills a range, it's typically not requested from subsequent sources in a sequential strategy.

**"Runs" or "Passes" in Bootstrapping:**
The `bootstrapProcess.Run` method divides the total bootstrapping time window into logical "passes" or target ranges. Typically, this involves:
1.  A "first pass" (`firstRangeWithPersistTrue` in `bootstrapProcess.targetRanges()`): This usually covers older, historical data. Data bootstrapped in this pass, especially from peers, might be configured with a `PersistConfig` to be flushed to disk immediately (`persist.FileSetFlushType`) to avoid holding large amounts of historical data in memory.
2.  A "second pass" (`secondRange`): This covers more recent data, closer to the present time. Data bootstrapped here might also be persisted, potentially as snapshots (`persist.FileSetSnapshotType`), to ensure that post-bootstrap, the node has a recent on-disk state that minimizes commit log replay on the next restart.

The bootstrapping process for a given namespace and its shards is considered "done" when all its required time ranges (for both data and index, if applicable) have been successfully populated by the sequence of bootstrappers according to the defined strategy and consistency requirements. The `databaseBootstrapManager` tracks this, transitioning the node's state from `Bootstrapping` to `Bootstrapped`. Individual shards also track their bootstrap state via `databaseShard.IsBootstrapped()`.

M3DB does not typically use distinct, operator-selectable "named strategies" like "repair_only" exposed directly via a simple string name in configuration. Instead, the strategy is implicitly defined by the configured sequence of bootstrappers (via the `BootstrapperProvider` in `StorageOptions`) and the per-namespace `BootstrapOptions`. However, specific operational needs like repair might leverage the peer bootstrapping components with different parameters or target ranges, effectively creating a custom strategy for that operation.

### Data Sources for Bootstrapping
M3DB can bootstrap data from several sources, typically trying them in a specific order to efficiently reconstruct a node's state. The choice and order of these sources are determined by the node's `BootstrapOptions` (which can be configured per namespace) and the overall bootstrapping strategy. Each source is represented by an implementation of the `bootstrap.Source` interface.

-   **Fileset Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/fs/source.go`):**
    -   **How it works:** This source reads data directly from the fileset files (info, index, data, bloom filter, summaries) that are already persisted on the node's local disk. It identifies available fileset volumes for each shard and time block within the bootstrapping range.
    -   **Data Restored:** It primarily restores historical data that has been flushed from memory to disk in the past. This includes both the actual time series data (M3TSZ segments) and the corresponding index segments (M3ninx FSTs).
    -   **Typical Use:** This is usually the first source attempted, as it's the fastest way to recover the bulk of the data for a node that has existing persistent data.

-   **Commit Log Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/commitlog/source.go`):**
    -   **How it works:** This source reads through the commit log files to recover writes that were acknowledged to clients but might not have been flushed to fileset files before a node shutdown or crash. It iterates through commit log entries, reconstructs series data and metadata, and applies them to the in-memory representations. It also considers existing snapshot files (which are compact representations of commit log data up to a point) to potentially reduce the amount of raw commit log data it needs to process.
    -   **Data Restored:** It restores the most recent data points and series metadata that were written since the last successful flush/snapshot covered by the fileset bootstrapper. This ensures durability and prevents data loss for acknowledged writes.
    -   **Typical Use:** This source is critical for data recovery after an unexpected shutdown. It's typically run after the fileset bootstrapper to fill in the most recent data. The time range it covers is usually from the end of the last complete fileset block up to the current time (or the end of the commit log).

-   **Peer Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/peers/source.go`):**
    -   **How it works:** This source allows a node to stream data from its replica peers in the cluster. The node sends requests to its peers for specific shards and time ranges. Peers respond with the series data (blocks) and index segments they own. The client (`AdminClient`) handles fetching this data.
    -   **Data Restored:** It can fetch both time series data blocks and index segments.
    -   **Typical Use:**
        -   **New Nodes:** Essential for new nodes joining the cluster that have no local data.
        -   **Data Repair/Catch-up:** Used if a node's local data (filesets or commit log) is incomplete, corrupted, or if the node has been down for an extended period and missed many writes that peers have.
        -   **Consistency:** Helps ensure consistency across replicas. If a node determines its local data is insufficient (e.g., based on checksums or versioning, though this is more related to repair than basic bootstrap), it might prefer peer data.

-   **Uninitialized Topology Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/uninitialized/source.go`):**
    -   **How it works:** This is a special bootstrapper that doesn't fetch data from an external source. Instead, it marks shards for which no other bootstrapper provided data as bootstrapped.
    -   **Data Restored:** None directly. It essentially finalizes the state for shards that are genuinely new or empty.
    -   **Typical Use:** It's often the last bootstrapper in the chain. If a shard is newly added to a node's ownership and has no historical data on disk or on peers (e.g., a completely new shard in the cluster), this bootstrapper ensures it's marked as "bootstrapped" so the node can begin accepting writes for it.

**Order/Preference of Sources:**
The M3DB node's `Options` configure a `BootstrapProcessProvider`, which in turn provides a `bootstrap.Process`. This process (typically `bootstrapProcess` from `src/dbnode/storage/bootstrap/process.go`) orchestrates the overall flow. The default M3DB `BootstrapperProvider` (often configured in `db.NewDatabase`) usually defines a sequence of bootstrappers:
1.  **Filesystem Bootstrapper:** Prioritized to quickly load data already on local disk.
2.  **Commit Log Bootstrapper:** To recover the most recent writes not yet persisted to filesets.
3.  **Peers Bootstrapper:** Used if local data is insufficient or for new nodes.
4.  **Uninitialized Topology Bootstrapper:** To finalize shards that have no data from other sources.

The `bootstrap.Process` iterates through these configured bootstrappers. Each `Bootstrapper` (via its `Source`) reports the time ranges it can fulfill (`AvailableData` and `AvailableIndex`). The process then attempts to `Read` data from the chosen source for the required ranges. If a source cannot fulfill certain ranges, those ranges are passed to the next bootstrapper in the sequence.

The `BootstrapOptions` within each `namespace.Options` can enable or disable specific bootstrappers for that namespace (e.g., `fsOpts.SetDefaultBootstrapToolVersion(FilesystemBootstrapperVersion)` can enable/disable filesystem bootstrapper implicitly). The specific `RunOptions` passed during a bootstrap run (e.g., `PersistConfig` indicating whether to persist data immediately during peer bootstrap) can also influence how a source behaves. For instance, data bootstrapped from peers might be configured to be written to disk (flushed) immediately or held in memory to be flushed later by the standard flushing mechanism.

### Commit Log Recovery during Bootstrapping

Commit log recovery is a crucial part of the bootstrapping strategy, ensuring data durability by restoring writes that were acknowledged but not yet persisted to fileset files before a node shutdown or crash.

**Role and Invocation:**
-   The commit log bootstrapper (`commitlog.Source` in `src/dbnode/storage/bootstrap/bootstrapper/commitlog/source.go`) is typically invoked after the fileset bootstrapper.
-   Its primary role is to replay data points from commit log files that are more recent than the data already restored from filesets (snapshots or flushed data files).

**Determining Which Commit Logs to Read:**
-   **Snapshot Consideration:** Before reading raw commit log entries, the `commitlog.Source` first attempts to load data from any existing snapshot files (`fs.SnapshotFiles`) relevant to the bootstrapping shards and time ranges. Snapshots are compacted representations of commit log data and can significantly reduce the amount of raw commit log data that needs to be processed. The source identifies the most recent complete snapshot for each shard/block combination.
-   **File Filtering:** When iterating through commit log files, a `FileFilterPredicate` (`readCommitLogFilePredicate`) is used. This predicate ensures that only commit log files that existed *before* the current node startup are considered. This prevents re-reading data from the active commit log segment that might contain writes already processed in the current session or data that is still in memory.
-   **Time Range:** The commit log bootstrapper aims to fill in data for the time ranges requested by the overall `bootstrap.Process`, typically focusing on periods more recent than what filesets cover, up to the current time. The exact time ranges are determined by what remains unfulfilled after previous bootstrappers (like the fileset bootstrapper) have run.

**Reading and Applying Entries:**
1.  **Iteration:** An iterator (`commitlog.Iterator` from `src/dbnode/persist/fs/commitlog/reader.go`) reads entries sequentially from the selected commit log files. Each entry (`commitlog.LogEntry`) contains the series metadata (ID, namespace, shard, encoded tags), the datapoint (timestamp, value), unit, and any annotation.
2.  **Data Accumulation:**
    -   For each entry, the `commitlog.Source` resolves the namespace and series.
    -   It uses a `NamespaceDataAccumulator` to get a `SeriesRef` for the specific series.
    -   The recovered datapoint (timestamp, value, unit, annotation) is then written to the series.
3.  **BootstrapWrite Flag:** When these recovered data points are written back into the series, it's done using a special `series.WriteOptions{BootstrapWrite: true}` flag (as seen in `src/dbnode/storage/series/series.go`).
    -   This flag causes the series to initially write these bootstrapped data points into a temporary side buffer (`dbSeriesBootstrap.buffer`).
    -   After the commit log source (or any bootstrap source) finishes processing, the `dbSeries.Bootstrap()` method is called, which merges the data from this side buffer into the main series buffer, making it visible for queries and further processing. This approach minimizes lock contention on the main series buffer during the potentially intensive bootstrapping phase.
4.  **Skip Out-of-Retention:** The `BootstrapWrite` option also sets `SkipOutOfRetention: true`, ensuring that data older than the namespace's retention policy is not re-inserted from the commit log.

**Avoiding Replay of Flushed Data:**
-   M3DB's bootstrapping process is designed to avoid replaying data that has already been successfully persisted in filesets.
-   The fileset bootstrapper runs first and populates the series buffers with data from disk.
-   The commit log bootstrapper primarily focuses on time ranges *after* the data covered by those filesets. While the commit log reader itself might read older entries, the `SeriesRef.Write()` mechanism, when loading into series buffers, handles deduplication and ensures that only newer or missing data points are actually incorporated. The snapshot files also help in quickly skipping over large portions of the commit log that are already covered by on-disk snapshots.

**Importance for Durability:**
Commit log recovery is fundamental to M3DB's durability guarantees. It ensures that any write acknowledged by M3DB (when using `StrategyWriteWait` or after a successful commit log flush for `StrategyWriteBehind`) can be recovered even if the node crashes before that data is flushed to a fileset. This minimizes data loss and helps maintain data consistency upon node restarts. The process also handles corrupt commit log files by logging errors and potentially marking ranges as unfulfilled, allowing subsequent bootstrappers (like peers) to attempt recovery.

### Bootstrapping Strategies

A bootstrapping strategy in M3DB defines the sequence and methods used to acquire data from various sources to bring a node's shards to an up-to-date and consistent state. The strategy aims to be efficient by prioritizing faster local sources before resorting to potentially slower network-based sources.

**Default Bootstrapping Strategy:**
M3DB typically employs a sequential, multi-stage strategy orchestrated by the `databaseBootstrapManager` (in `src/dbnode/storage/bootstrap.go`) which uses a `bootstrap.ProcessProvider`. The provider supplies a `bootstrap.Process` (implemented by `bootstrapProcess` in `src/dbnode/storage/bootstrap/process.go`), which in turn uses a configured `Bootstrapper`. The most common `Bootstrapper` is one that tries sources in the following order:

1.  **Filesystem Bootstrapper:** This is generally the first source. It attempts to load data from existing fileset files (data, index, summaries, bloom filters) already present on the node's local disk. This is the quickest way to restore the bulk of historical data.
2.  **Commit Log Bootstrapper:** If the Filesystem bootstrapper doesn't cover the most recent time ranges (up to the present), the Commit Log bootstrapper runs next. It replays commit log files to recover data that was written and acknowledged but not yet flushed to filesets. This ensures durability for recent writes.
3.  **Peers Bootstrapper:** If data is still missing after the Filesystem and Commit Log bootstrappers (e.g., for a new node, a node that was down for an extended period, or if data corruption is detected/suspected), the Peers bootstrapper attempts to stream the necessary data (both time series blocks and index segments) from replica peers in the cluster.
4.  **Uninitialized Topology Bootstrapper:** This is usually the final step. If a shard is entirely new to the cluster or has no data available from any of the prior sources, this bootstrapper marks the shard as "bootstrapped," allowing it to start accepting new writes.

**Influence of `BootstrapOptions`:**
The behavior of these sources, particularly the Peers bootstrapper, can be influenced by `BootstrapOptions` (defined in `src/dbnode/namespace/options.go` and part of a namespace's configuration):
-   `DefaultBootstrapConsistencyLevel`: This option (e.g., `topology.ReadConsistencyLevelMajority`) dictates how many peers must agree/provide data for a shard/block during peer bootstrapping for it to be considered consistent and sufficient. If this level isn't met, the peer bootstrap for that specific time range might be considered unfulfilled, potentially leading to data gaps if no other source can provide it.
-   Other options within `BootstrapOptions` can enable or disable specific bootstrappers or alter their parameters for a given namespace. For example, the `Bootstrappers` field in `namespace.Options` allows specifying a custom list of bootstrappers, overriding the default sequence.

**Execution Management:**
-   The `databaseBootstrapManager` (`src/dbnode/storage/bootstrap.go`) initiates the overall process. It calls `processProvider.Provide()` to get a `bootstrap.Process`. The `ProcessProvider` is configured in `StorageOptions` and holds the `BootstrapperProvider`.
-   The `BootstrapperProvider` (configured via `StorageOptions.SetBootstrapProcessProvider()`) is responsible for creating the specific `Bootstrapper` instance that defines the strategy (e.g., a `sequentialBootstrapper` which runs a list of sources in order).
-   The `bootstrap.Process` (`src/dbnode/storage/bootstrap/process.go`) then takes this `Bootstrapper` and calls its `Bootstrap` method.
-   The `Bootstrapper` iterates through its configured `Source`s. Each `Source` (e.g., filesystem, commitlog, peers) attempts to fulfill the data requirements for the specified time ranges (`ShardTimeRanges`). If a source fulfills a range, it's typically not requested from subsequent sources in a sequential strategy.

**"Runs" or "Passes" in Bootstrapping:**
The `bootstrapProcess.Run` method divides the total bootstrapping time window into logical "passes" or target ranges. Typically, this involves:
1.  A "first pass" (`firstRangeWithPersistTrue` in `bootstrapProcess.targetRanges()`): This usually covers older, historical data. Data bootstrapped in this pass, especially from peers, might be configured with a `PersistConfig` to be flushed to disk immediately (`persist.FileSetFlushType`) to avoid holding large amounts of historical data in memory.
2.  A "second pass" (`secondRange`): This covers more recent data, closer to the present time. Data bootstrapped here might also be persisted, potentially as snapshots (`persist.FileSetSnapshotType`), to ensure that post-bootstrap, the node has a recent on-disk state that minimizes commit log replay on the next restart.

The bootstrapping process for a given namespace and its shards is considered "done" when all its required time ranges (for both data and index, if applicable) have been successfully populated by the sequence of bootstrappers according to the defined strategy and consistency requirements. The `databaseBootstrapManager` tracks this, transitioning the node's state from `Bootstrapping` to `Bootstrapped`. Individual shards also track their bootstrap state via `databaseShard.IsBootstrapped()`.

M3DB does not typically use distinct, operator-selectable "named strategies" like "repair_only" exposed directly via a simple string name in configuration. Instead, the strategy is implicitly defined by the configured sequence of bootstrappers (via the `BootstrapperProvider` in `StorageOptions`) and the per-namespace `BootstrapOptions`. However, specific operational needs like repair might leverage the peer bootstrapping components with different parameters or target ranges, effectively creating a custom strategy for that operation.

### Data Sources for Bootstrapping
M3DB can bootstrap data from several sources, typically trying them in a specific order to efficiently reconstruct a node's state. The choice and order of these sources are determined by the node's `BootstrapOptions` (which can be configured per namespace) and the overall bootstrapping strategy. Each source is represented by an implementation of the `bootstrap.Source` interface.

-   **Fileset Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/fs/source.go`):**
    -   **How it works:** This source reads data directly from the fileset files (info, index, data, bloom filter, summaries) that are already persisted on the node's local disk. It identifies available fileset volumes for each shard and time block within the bootstrapping range.
    -   **Data Restored:** It primarily restores historical data that has been flushed from memory to disk in the past. This includes both the actual time series data (M3TSZ segments) and the corresponding index segments (M3ninx FSTs).
    -   **Typical Use:** This is usually the first source attempted, as it's the fastest way to recover the bulk of the data for a node that has existing persistent data.

-   **Commit Log Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/commitlog/source.go`):**
    -   **How it works:** This source reads through the commit log files to recover writes that were acknowledged to clients but might not have been flushed to fileset files before a node shutdown or crash. It iterates through commit log entries, reconstructs series data and metadata, and applies them to the in-memory representations. It also considers existing snapshot files (which are compact representations of commit log data up to a point) to potentially reduce the amount of raw commit log data it needs to process.
    -   **Data Restored:** It restores the most recent data points and series metadata that were written since the last successful flush/snapshot covered by the fileset bootstrapper. This ensures durability and prevents data loss for acknowledged writes.
    -   **Typical Use:** This source is critical for data recovery after an unexpected shutdown. It's typically run after the fileset bootstrapper to fill in the most recent data. The time range it covers is usually from the end of the last complete fileset block up to the current time (or the end of the commit log).

-   **Peer Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/peers/source.go`):**
    -   **How it works:** This source allows a node to stream data from its replica peers in the cluster. The node sends requests to its peers for specific shards and time ranges. Peers respond with the series data (blocks) and index segments they own. The client (`AdminClient`) handles fetching this data.
    -   **Data Restored:** It can fetch both time series data blocks and index segments.
    -   **Typical Use:**
        -   **New Nodes:** Essential for new nodes joining the cluster that have no local data.
        -   **Data Repair/Catch-up:** Used if a node's local data (filesets or commit log) is incomplete, corrupted, or if the node has been down for an extended period and missed many writes that peers have.
        -   **Consistency:** Helps ensure consistency across replicas. If a node determines its local data is insufficient (e.g., based on checksums or versioning, though this is more related to repair than basic bootstrap), it might prefer peer data.

-   **Uninitialized Topology Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/uninitialized/source.go`):**
    -   **How it works:** This is a special bootstrapper that doesn't fetch data from an external source. Instead, it marks shards for which no other bootstrapper provided data as bootstrapped.
    -   **Data Restored:** None directly. It essentially finalizes the state for shards that are genuinely new or empty.
    -   **Typical Use:** It's often the last bootstrapper in the chain. If a shard is newly added to a node's ownership and has no historical data on disk or on peers (e.g., a completely new shard in the cluster), this bootstrapper ensures it's marked as "bootstrapped" so the node can begin accepting writes for it.

**Order/Preference of Sources:**
The M3DB node's `Options` configure a `BootstrapProcessProvider`, which in turn provides a `bootstrap.Process`. This process (typically `bootstrapProcess` from `src/dbnode/storage/bootstrap/process.go`) orchestrates the overall flow. The default M3DB `BootstrapperProvider` (often configured in `db.NewDatabase`) usually defines a sequence of bootstrappers:
1.  **Filesystem Bootstrapper:** Prioritized to quickly load data already on local disk.
2.  **Commit Log Bootstrapper:** To recover the most recent writes not yet persisted to filesets.
3.  **Peers Bootstrapper:** Used if local data is insufficient or for new nodes.
4.  **Uninitialized Topology Bootstrapper:** To finalize shards that have no data from other sources.

The `bootstrap.Process` iterates through these configured bootstrappers. Each `Bootstrapper` (via its `Source`) reports the time ranges it can fulfill (`AvailableData` and `AvailableIndex`). The process then attempts to `Read` data from the chosen source for the required ranges. If a source cannot fulfill certain ranges, those ranges are passed to the next bootstrapper in the sequence.

The `BootstrapOptions` within each `namespace.Options` can enable or disable specific bootstrappers for that namespace (e.g., `fsOpts.SetDefaultBootstrapToolVersion(FilesystemBootstrapperVersion)` can enable/disable filesystem bootstrapper implicitly). The specific `RunOptions` passed during a bootstrap run (e.g., `PersistConfig` indicating whether to persist data immediately during peer bootstrap) can also influence how a source behaves. For instance, data bootstrapped from peers might be configured to be written to disk (flushed) immediately or held in memory to be flushed later by the standard flushing mechanism.

### Commit Log Recovery during Bootstrapping

Commit log recovery is a crucial part of the bootstrapping strategy, ensuring data durability by restoring writes that were acknowledged but not yet persisted to fileset files before a node shutdown or crash.

**Role and Invocation:**
-   The commit log bootstrapper (`commitlog.Source` in `src/dbnode/storage/bootstrap/bootstrapper/commitlog/source.go`) is typically invoked after the fileset bootstrapper.
-   Its primary role is to replay data points from commit log files that are more recent than the data already restored from filesets (snapshots or flushed data files).

**Determining Which Commit Logs to Read:**
-   **Snapshot Consideration:** Before reading raw commit log entries, the `commitlog.Source` first attempts to load data from any existing snapshot files (`fs.SnapshotFiles`) relevant to the bootstrapping shards and time ranges. Snapshots are compacted representations of commit log data and can significantly reduce the amount of raw commit log data that needs to be processed. The source identifies the most recent complete snapshot for each shard/block combination.
-   **File Filtering:** When iterating through commit log files, a `FileFilterPredicate` (`readCommitLogFilePredicate`) is used. This predicate ensures that only commit log files that existed *before* the current node startup are considered. This prevents re-reading data from the active commit log segment that might contain writes already processed in the current session or data that is still in memory.
-   **Time Range:** The commit log bootstrapper aims to fill in data for the time ranges requested by the overall `bootstrap.Process`, typically focusing on periods more recent than what filesets cover, up to the current time. The exact time ranges are determined by what remains unfulfilled after previous bootstrappers (like the fileset bootstrapper) have run.

**Reading and Applying Entries:**
1.  **Iteration:** An iterator (`commitlog.Iterator` from `src/dbnode/persist/fs/commitlog/reader.go`) reads entries sequentially from the selected commit log files. Each entry (`commitlog.LogEntry`) contains the series metadata (ID, namespace, shard, encoded tags), the datapoint (timestamp, value), unit, and any annotation.
2.  **Data Accumulation:**
    -   For each entry, the `commitlog.Source` resolves the namespace and series.
    -   It uses a `NamespaceDataAccumulator` to get a `SeriesRef` for the specific series.
    -   The recovered datapoint (timestamp, value, unit, annotation) is then written to the series.
3.  **BootstrapWrite Flag:** When these recovered data points are written back into the series, it's done using a special `series.WriteOptions{BootstrapWrite: true}` flag (as seen in `src/dbnode/storage/series/series.go`).
    -   This flag causes the series to initially write these bootstrapped data points into a temporary side buffer (`dbSeriesBootstrap.buffer`).
    -   After the commit log source (or any bootstrap source) finishes processing, the `dbSeries.Bootstrap()` method is called, which merges the data from this side buffer into the main series buffer, making it visible for queries and further processing. This approach minimizes lock contention on the main series buffer during the potentially intensive bootstrapping phase.
4.  **Skip Out-of-Retention:** The `BootstrapWrite` option also sets `SkipOutOfRetention: true`, ensuring that data older than the namespace's retention policy is not re-inserted from the commit log.

**Avoiding Replay of Flushed Data:**
-   M3DB's bootstrapping process is designed to avoid replaying data that has already been successfully persisted in filesets.
-   The fileset bootstrapper runs first and populates the series buffers with data from disk.
-   The commit log bootstrapper primarily focuses on time ranges *after* the data covered by those filesets. While the commit log reader itself might read older entries, the `SeriesRef.Write()` mechanism, when loading into series buffers, handles deduplication and ensures that only newer or missing data points are actually incorporated. The snapshot files also help in quickly skipping over large portions of the commit log that are already covered by on-disk snapshots.

**Importance for Durability:**
Commit log recovery is fundamental to M3DB's durability guarantees. It ensures that any write acknowledged by M3DB (when using `StrategyWriteWait` or after a successful commit log flush for `StrategyWriteBehind`) can be recovered even if the node crashes before that data is flushed to a fileset. This minimizes data loss and helps maintain data consistency upon node restarts. The process also handles corrupt commit log files by logging errors and potentially marking ranges as unfulfilled, allowing subsequent bootstrappers (like peers) to attempt recovery.

### Bootstrapping Strategies

A bootstrapping strategy in M3DB defines the sequence and methods used to acquire data from various sources to bring a node's shards to an up-to-date and consistent state. The strategy aims to be efficient by prioritizing faster local sources before resorting to potentially slower network-based sources.

**Default Bootstrapping Strategy:**
M3DB typically employs a sequential, multi-stage strategy orchestrated by the `databaseBootstrapManager` (in `src/dbnode/storage/bootstrap.go`) which uses a `bootstrap.ProcessProvider`. The provider supplies a `bootstrap.Process` (implemented by `bootstrapProcess` in `src/dbnode/storage/bootstrap/process.go`), which in turn uses a configured `Bootstrapper`. The most common `Bootstrapper` is one that tries sources in the following order:

1.  **Filesystem Bootstrapper:** This is generally the first source. It attempts to load data from existing fileset files (data, index, summaries, bloom filters) already present on the node's local disk. This is the quickest way to restore the bulk of historical data.
2.  **Commit Log Bootstrapper:** If the Filesystem bootstrapper doesn't cover the most recent time ranges (up to the present), the Commit Log bootstrapper runs next. It replays commit log files to recover data that was written and acknowledged but not yet flushed to filesets. This ensures durability for recent writes.
3.  **Peers Bootstrapper:** If data is still missing after the Filesystem and Commit Log bootstrappers (e.g., for a new node, a node that was down for an extended period, or if data corruption is detected/suspected), the Peers bootstrapper attempts to stream the necessary data (both time series blocks and index segments) from replica peers in the cluster.
4.  **Uninitialized Topology Bootstrapper:** This is usually the final step. If a shard is entirely new to the cluster or has no data available from any of the prior sources, this bootstrapper marks the shard as "bootstrapped," allowing it to start accepting new writes.

**Influence of `BootstrapOptions`:**
The behavior of these sources, particularly the Peers bootstrapper, can be influenced by `BootstrapOptions` (defined in `src/dbnode/namespace/options.go` and part of a namespace's configuration):
-   `DefaultBootstrapConsistencyLevel`: This option (e.g., `topology.ReadConsistencyLevelMajority`) dictates how many peers must agree/provide data for a shard/block during peer bootstrapping for it to be considered consistent and sufficient. If this level isn't met, the peer bootstrap for that specific time range might be considered unfulfilled, potentially leading to data gaps if no other source can provide it.
-   Other options within `BootstrapOptions` can enable or disable specific bootstrappers or alter their parameters for a given namespace. For example, the `Bootstrappers` field in `namespace.Options` allows specifying a custom list of bootstrappers, overriding the default sequence.

**Execution Management:**
-   The `databaseBootstrapManager` (`src/dbnode/storage/bootstrap.go`) initiates the overall process. It calls `processProvider.Provide()` to get a `bootstrap.Process`. The `ProcessProvider` is configured in `StorageOptions` and holds the `BootstrapperProvider`.
-   The `BootstrapperProvider` (configured via `StorageOptions.SetBootstrapProcessProvider()`) is responsible for creating the specific `Bootstrapper` instance that defines the strategy (e.g., a `sequentialBootstrapper` which runs a list of sources in order).
-   The `bootstrap.Process` (`src/dbnode/storage/bootstrap/process.go`) then takes this `Bootstrapper` and calls its `Bootstrap` method.
-   The `Bootstrapper` iterates through its configured `Source`s. Each `Source` (e.g., filesystem, commitlog, peers) attempts to fulfill the data requirements for the specified time ranges (`ShardTimeRanges`). If a source fulfills a range, it's typically not requested from subsequent sources in a sequential strategy.

**"Runs" or "Passes" in Bootstrapping:**
The `bootstrapProcess.Run` method divides the total bootstrapping time window into logical "passes" or target ranges. Typically, this involves:
1.  A "first pass" (`firstRangeWithPersistTrue` in `bootstrapProcess.targetRanges()`): This usually covers older, historical data. Data bootstrapped in this pass, especially from peers, might be configured with a `PersistConfig` to be flushed to disk immediately (`persist.FileSetFlushType`) to avoid holding large amounts of historical data in memory.
2.  A "second pass" (`secondRange`): This covers more recent data, closer to the present time. Data bootstrapped here might also be persisted, potentially as snapshots (`persist.FileSetSnapshotType`), to ensure that post-bootstrap, the node has a recent on-disk state that minimizes commit log replay on the next restart.

The bootstrapping process for a given namespace and its shards is considered "done" when all its required time ranges (for both data and index, if applicable) have been successfully populated by the sequence of bootstrappers according to the defined strategy and consistency requirements. The `databaseBootstrapManager` tracks this, transitioning the node's state from `Bootstrapping` to `Bootstrapped`. Individual shards also track their bootstrap state via `databaseShard.IsBootstrapped()`.

M3DB does not typically use distinct, operator-selectable "named strategies" like "repair_only" exposed directly via a simple string name in configuration. Instead, the strategy is implicitly defined by the configured sequence of bootstrappers (via the `BootstrapperProvider` in `StorageOptions`) and the per-namespace `BootstrapOptions`. However, specific operational needs like repair might leverage the peer bootstrapping components with different parameters or target ranges, effectively creating a custom strategy for that operation.

### Data Sources for Bootstrapping
M3DB can bootstrap data from several sources, typically trying them in a specific order to efficiently reconstruct a node's state. The choice and order of these sources are determined by the node's `BootstrapOptions` (which can be configured per namespace) and the overall bootstrapping strategy. Each source is represented by an implementation of the `bootstrap.Source` interface.

-   **Fileset Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/fs/source.go`):**
    -   **How it works:** This source reads data directly from the fileset files (info, index, data, bloom filter, summaries) that are already persisted on the node's local disk. It identifies available fileset volumes for each shard and time block within the bootstrapping range.
    -   **Data Restored:** It primarily restores historical data that has been flushed from memory to disk in the past. This includes both the actual time series data (M3TSZ segments) and the corresponding index segments (M3ninx FSTs).
    -   **Typical Use:** This is usually the first source attempted, as it's the fastest way to recover the bulk of the data for a node that has existing persistent data.

-   **Commit Log Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/commitlog/source.go`):**
    -   **How it works:** This source reads through the commit log files to recover writes that were acknowledged to clients but might not have been flushed to fileset files before a node shutdown or crash. It iterates through commit log entries, reconstructs series data and metadata, and applies them to the in-memory representations. It also considers existing snapshot files (which are compact representations of commit log data up to a point) to potentially reduce the amount of raw commit log data it needs to process.
    -   **Data Restored:** It restores the most recent data points and series metadata that were written since the last successful flush/snapshot covered by the fileset bootstrapper. This ensures durability and prevents data loss for acknowledged writes.
    -   **Typical Use:** This source is critical for data recovery after an unexpected shutdown. It's typically run after the fileset bootstrapper to fill in the most recent data. The time range it covers is usually from the end of the last complete fileset block up to the current time (or the end of the commit log).

-   **Peer Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/peers/source.go`):**
    -   **How it works:** This source allows a node to stream data from its replica peers in the cluster. The node sends requests to its peers for specific shards and time ranges. Peers respond with the series data (blocks) and index segments they own. The client (`AdminClient`) handles fetching this data.
    -   **Data Restored:** It can fetch both time series data blocks and index segments.
    -   **Typical Use:**
        -   **New Nodes:** Essential for new nodes joining the cluster that have no local data.
        -   **Data Repair/Catch-up:** Used if a node's local data (filesets or commit log) is incomplete, corrupted, or if the node has been down for an extended period and missed many writes that peers have.
        -   **Consistency:** Helps ensure consistency across replicas. If a node determines its local data is insufficient (e.g., based on checksums or versioning, though this is more related to repair than basic bootstrap), it might prefer peer data.

-   **Uninitialized Topology Bootstrapping (`src/dbnode/storage/bootstrap/bootstrapper/uninitialized/source.go`):**
    -   **How it works:** This is a special bootstrapper that doesn't fetch data from an external source. Instead, it marks shards for which no other bootstrapper provided data as bootstrapped.
    -   **Data Restored:** None directly. It essentially finalizes the state for shards that are genuinely new or empty.
    -   **Typical Use:** It's often the last bootstrapper in the chain. If a shard is newly added to a node's ownership and has no historical data on disk or on peers (e.g., a completely new shard in the cluster), this bootstrapper ensures it's marked as "bootstrapped" so the node can begin accepting writes for it.

**Order/Preference of Sources:**
The M3DB node's `Options` configure a `BootstrapProcessProvider`, which in turn provides a `bootstrap.Process`. This process (typically `bootstrapProcess` from `src/dbnode/storage/bootstrap/process.go`) orchestrates the overall flow. The default M3DB `BootstrapperProvider` (often configured in `db.NewDatabase`) usually defines a sequence of bootstrappers:
1.  **Filesystem Bootstrapper:** Prioritized to quickly load data already on local disk.
2.  **Commit Log Bootstrapper:** To recover the most recent writes not yet persisted to filesets.
3.  **Peers Bootstrapper:** Used if local data is insufficient or for new nodes.
4.  **Uninitialized Topology Bootstrapper:** To finalize shards that have no data from other sources.

The `bootstrap.Process` iterates through these configured bootstrappers. Each `Bootstrapper` (via its `Source`) reports the time ranges it can fulfill (`AvailableData` and `AvailableIndex`). The process then attempts to `Read` data from the chosen source for the required ranges. If a source cannot fulfill certain ranges, those ranges are passed to the next bootstrapper in the sequence.

The `BootstrapOptions` within each `namespace.Options` can enable or disable specific bootstrappers for that namespace (e.g., `fsOpts.SetDefaultBootstrapToolVersion(FilesystemBootstrapperVersion)` can enable/disable filesystem bootstrapper implicitly). The specific `RunOptions` passed during a bootstrap run (e.g., `PersistConfig` indicating whether to persist data immediately during peer bootstrap) can also influence how a source behaves. For instance, data bootstrapped from peers might be configured to be written to disk (flushed) immediately or held in memory to be flushed later by the standard flushing mechanism.

### Commit Log Recovery during Bootstrapping

Commit log recovery is a crucial part of the bootstrapping strategy, ensuring data durability by restoring writes that were acknowledged but not yet persisted to fileset files before a node shutdown or crash.

**Role and Invocation:**
-   The commit log bootstrapper (`commitlog.Source` in `src/dbnode/storage/bootstrap/bootstrapper/commitlog/source.go`) is typically invoked after the fileset bootstrapper.
-   Its primary role is to replay data points from commit log files that are more recent than the data already restored from filesets (snapshots or flushed data files).

**Determining Which Commit Logs to Read:**
-   **Snapshot Consideration:** Before reading raw commit log entries, the `commitlog.Source` first attempts to load data from any existing snapshot files (`fs.SnapshotFiles`) relevant to the bootstrapping shards and time ranges. Snapshots are compacted representations of commit log data and can significantly reduce the amount of raw log data that needs to be processed. The source identifies the most recent complete snapshot for each shard/block combination.
-   **File Filtering:** When iterating through commit log files, a `FileFilterPredicate` (`readCommitLogFilePredicate`) is used. This predicate ensures that only commit log files that existed *before* the current node startup are considered. This prevents re-reading data from the active commit log segment that might contain writes already processed in the current session or data that is still in memory.
-   **Time Range:** The commit log bootstrapper aims to fill in data for the time ranges requested by the overall `bootstrap.Process`, typically focusing on periods more recent than what filesets cover, up to the current time. The exact time ranges are determined by what remains unfulfilled after previous bootstrappers (like the fileset bootstrapper) have run.

**Reading and Applying Entries:**
1.  **Iteration:** An iterator (`commitlog.Iterator` from `src/dbnode/persist/fs/commitlog/reader.go`) reads entries sequentially from the selected commit log files. Each entry (`commitlog.LogEntry`) contains the series metadata (ID, namespace, shard, encoded tags), the datapoint (timestamp, value), unit, and any annotation.
2.  **Data Accumulation:**
    -   For each entry, the `commitlog.Source` resolves the namespace and series.
    -   It uses a `NamespaceDataAccumulator` to get a `SeriesRef` for the specific series.
    -   The recovered datapoint (timestamp, value, unit, annotation) is then written to the series.
3.  **BootstrapWrite Flag:** When these recovered data points are written back into the series, it's done using a special `series.WriteOptions{BootstrapWrite: true}` flag (as seen in `src/dbnode/storage/series/series.go`).
    -   This flag causes the series to initially write these bootstrapped data points into a temporary side buffer (`dbSeriesBootstrap.buffer`).
    -   After the commit log source (or any bootstrap source) finishes processing, the `dbSeries.Bootstrap()` method is called, which merges the data from this side buffer into the main series buffer, making it visible for queries and further processing. This approach minimizes lock contention on the main series buffer during the potentially intensive bootstrapping phase.
4.  **Skip Out-of-Retention:** The `BootstrapWrite` option also sets `SkipOutOfRetention: true`, ensuring that data older than the namespace's retention policy is not re-inserted from the commit log.

**Avoiding Replay of Flushed Data:**
-   M3DB's bootstrapping process is designed to avoid replaying data that has already been successfully persisted in filesets.
-   The fileset bootstrapper runs first and populates the series buffers with data from disk.
-   The commit log bootstrapper primarily focuses on time ranges *after* the data covered by those filesets. While the commit log reader itself might read older entries, the `SeriesRef.Write()` mechanism, when loading into series buffers, handles deduplication and ensures that only newer or missing data points are actually incorporated. The snapshot files also help in quickly skipping over large portions of the commit log that are already covered by on-disk snapshots.

**Importance for Durability:**
Commit log recovery is fundamental to M3DB's durability guarantees. It ensures that any write acknowledged by M3DB (when using `StrategyWriteWait` or after a successful commit log flush for `StrategyWriteBehind`) can be recovered even if the node crashes before that data is flushed to a fileset. This minimizes data loss and helps maintain data consistency upon node restarts. The process also handles corrupt commit log files by logging errors and potentially marking ranges as unfulfilled, allowing subsequent bootstrappers (like peers) to attempt recovery.

[end of docs/m3db_internals.md]
