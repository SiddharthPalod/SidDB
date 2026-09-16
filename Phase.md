
# 2. Distributed KV Store
https://github.com/dineshgowda24/bitcask-rb
This should probably be your **flagship systems project**.

## Goal

Build something conceptually similar to:

* etcd
* TiKV
* CockroachDB

but obviously much smaller.

Architecture:

```text
             Client
               |
          +----+----+
          |         |
        Node 1    Node 2    Node 3
          |         |         |
       Raft       Raft      Raft
          |         |         |
       Storage    Storage   Storage
          |         |         |
       LSM Tree   LSM Tree  LSM Tree
```

---

## Phase 1 — KV store

**Week 1**

Implement:

```text
PUT key value
GET key
DELETE key
```

Start with an in-memory map.

Then introduce:

```text
WAL
 |
MemTable
 |
SSTable
```

---

## Phase 2 — WAL

**Week 2**

Every mutation:

```text
Client
  ↓
PUT
  ↓
WAL
  ↓
MemTable
```

Implement:

* append-only log
* checksums
* record framing
* crash recovery
* replay

Then deliberately kill the process and recover state.

---

# Phase 3 — LSM Tree

**Week 3–5**

This is where the project gets serious.

Implement:

### MemTable

Use:

```text
SkipList
```

or another ordered structure.

```text
PUT(a, 10)
PUT(c, 30)
PUT(b, 20)

MemTable:

a → 10
b → 20
c → 30
```

---

### SSTables

Flush MemTable:

```text
MemTable
    ↓
SSTable
```

SSTable:

```text
Data blocks
Index
Bloom filter
Metadata
Checksum
```

---

### Reads

Implement:

```text
MemTable
    ↓
SSTable 0
    ↓
SSTable 1
    ↓
SSTable 2
```

Use Bloom filters to avoid unnecessary disk reads.

---

### Compaction

Implement:

```text
SSTable A
SSTable B
SSTable C
     ↓
 Compaction
     ↓
SSTable D
```

Eventually implement:

* size-tiered compaction
* leveled compaction

This alone is a very good systems project.

---

<!-- UNDERSTANDING SIZE MATHS -->
Ran command: `Get-CimInstance Win32_OperatingSystem | Select-Object TotalVisibleMemorySize, FreePhysicalMemory`
Ran command: `Get-CimInstance Win32_Processor | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors`

Based on your system specs:

* **Total RAM:** 16 GB
* **Available Free RAM:** ~6.3 GB
* **CPU:** Intel Core i7 (4 Cores / 8 Threads)

Here is the exact engineering breakdown to calculate the optimal production threshold:

---

### 1. The Production Memory Budget Formula

In production LSM engines (RocksDB, LevelDB, Cassandra), memory is divided across 3 main pools:

```
Total Database Memory Budget (~1 GB - 2 GB on a 16GB Machine)
 ├── 25% Active + Frozen MemTable (Buffer new writes)   ➜  128 MB – 256 MB
 ├── 50% LRU Block Cache (Buffer hot 4KB disk blocks)    ➜  512 MB – 1 GB
 └── 25% In-Memory Bloom Filters & Sparse Indexes        ➜  128 MB – 256 MB
```

---

### 2. Calculating Key Threshold by Payload Size

$$\text{Flush Threshold (keys)} = \frac{\text{Target MemTable Buffer Size (e.g. 64 MB)}}{\text{Average Key-Value Record Size (bytes)}}$$

| Your Workload Type | Average Record Size | Recommended MemTable Threshold | SSTable Flush Size |
| :--- | :--- | :--- | :--- |
| **Small Keys / IDs / Counters** | ~64 – 128 bytes | **$250,000$ to $500,000$ keys** | ~32 MB – 64 MB |
| **General Purpose (JSON/User profiles)** *(Standard)* | ~512 bytes – 1 KB | **$64,000$ to $100,000$ keys** | ~64 MB |
| **Large Blobs / Payloads** | ~4 KB – 8 KB | **$10,000$ to $20,000$ keys** | ~64 MB – 80 MB |

---

### 3. Recommended Production Sweet Spot for Your Machine

For a general-purpose production deployment on your 16 GB laptop:

> **Recommended Production Threshold: `64,000` to `100,000` keys**

#### Why this is the sweet spot:
1. **Flushes at $\approx 64\text{ MB}$ chunks:** Matches modern NVMe/SSD sequential write speeds perfectly (flushes in $< 50\text{ ms}$).
2. **Fast Crash Recovery:** Replaying a 64MB WAL on startup takes $< 0.3\text{ seconds}$.
3. **Low GC Pressure:** Avoids long Java garbage collection pauses while maintaining multi-million operations-per-second write throughput in RAM.

---

# Phase 4 - Visualizer
### How to run with Production Config:

#### Via Java Code:
```java
// Production config: 100,000 writes in RAM before flushing 64MB SSTable to disk
SidDBEngine db = new SidDBEngine("production_db", 100_000);
```

#### Via Visualizer Studio:
Type **`100000`** in the `⚙️ Flush Threshold` box in the top bar and click **`Apply Settings`**.



# Phase 5 — Raft

**Week 6–9**

Implement:

```text
Follower
Candidate
Leader
```

Leader election:

```text
Node A ─────┐
Node B ─────┼── election
Node C ─────┘
       ↓
    Node B
     Leader
```

Then:

```text
Client
  ↓
Leader
  ↓
Replicate log
  ↓
Follower 1
Follower 2
```

Implement:

* leader election
* heartbeats
* terms
* voting
* log replication
* commit index
* state machine application
* persistence
* crash recovery

---

# Phase 6 — Network failures

**Week 10–11**

This is where I'd deliberately break it.

Simulate:

```text
packet loss
network delay
node crash
node restart
network partition
slow follower
leader failure
```

Example:

```text
        partition
Node A --------X-------- Node B
                         Node C

        ↓

      Node A
      Leader

        ↓ crash

Node B becomes leader
Node C follows
```

Verify consistency.

---

# Phase 7 — Benchmarking

**Week 12–14**

Benchmark:

```text
GET throughput
PUT throughput
P99 latency
compaction overhead
recovery time
Raft replication latency
```

Compare:

```text
1 node
3 nodes
5 nodes
```

---

# Phase 8 — Production Hardening & High-Performance Enhancements

**Week 15–18**

Advance SidDB from an in-process verified prototype into a production-grade distributed storage engine.

```text
             Client Application / Benchmark Driver
                               |
                TCP Socket Transport (NIO)
                               |
              +----------------+----------------+
              |                                 |
        Node 1 (Port 9001)              Node 2 (Port 9002)
  +---------------------------+   +---------------------------+
  | Raft Consensus Engine     |   | Raft Consensus Engine     |
  |  ├─ Leader Lease / ReadIdx|   |  ├─ Leader Lease / ReadIdx|
  |  └─ Sync WAL fsync Policy |   |  └─ Sync WAL fsync Policy |
  |                           |   |                           |
  | LSM Storage Engine        |   | LSM Storage Engine        |
  |  ├─ MemTable (SkipList)   |   |  ├─ MemTable (SkipList)   |
  |  ├─ Async Compactor (Bg)  |   |  ├─ Async Compactor (Bg)  |
  |  └─ WAF / SAF Telemetry   |   |  └─ WAF / SAF Telemetry   |
  +---------------------------+   +---------------------------+
```

---

### 1. High-Performance Linearizable Reads (Leader Leases & ReadIndex)

* **Motivation**: Currently, linearizable reads propose a dummy write through Raft consensus, incurring full disk I/O, log replication, and 27–40 ms quorum latency (~600 ops/sec).
* **Architecture**:
  * **Leader Leases**: A leader elected by majority maintains a time-bounded lease bounded by `electionTimeoutMin - clockDrift`. As long as the lease is active, local reads are guaranteed linearizable without consensus roundtrips.
  * **ReadIndex Protocol**:
    1. Leader records current `commitIndex` as `readIndex`.
    2. Sends a low-overhead heartbeat to quorum (no log writes) to ensure it hasn't been superseded.
    3. Waits until `lastApplied >= readIndex`, then serves read directly from MemTable / SSTable.
* **Target Metric**: Linearizable read throughput: **>100,000 ops/sec** with sub-millisecond P99 latency (<1 ms).

---

### 2. Follower WAL Durability & Dynamic `fsync` Policies

* **Motivation**: Close the theoretical durability loophole where followers acknowledge `AppendEntries` while data remains in OS page cache.
* **Architecture**:
  * Introduce configurable `SyncPolicy`:
    * `SYNC_EVERY_ENTRY`: Follower invokes `FileChannel.force(true)` synchronously before acknowledging `AppendEntriesResult(success = true)`.
    * `SYNC_BATCH`: Fsync periodically or after batched frame thresholds.
    * `ASYNC_FLUSH`: Default high-throughput memory-buffered mode.
  * Durability SLA verification tests under sudden power-loss simulation.
* **Target Metric**: Zero theoretical log loss on simultaneous power loss of minority nodes.

---

### 3. Decoupled Asynchronous Compactor & Amplification Metrics

* **Motivation**: MemTable flushes and Level-0 to Level-1 compactions currently run synchronously on the write path, causing ~270 ms disk merge stalls under high concurrency (e.g. 64 clients).
* **Architecture**:
  * **Asynchronous `CompactionExecutor`**: Dedicated background thread pool decoupling SSTable merge routines from client write mutations.
  * **Backpressure Mechanism**: Dynamic write throttling when uncompacted L0 SSTable count exceeds safe thresholds.
  * **Real-time LSM Telemetry**:
    * **Merge Throughput**: Dynamic MB/s merged.
    * **Write Amplification Factor (WAF)**: $\text{Bytes Written to Disk} / \text{Bytes Written by User}$.
    * **Space Amplification Factor (SAF)**: $\text{Total SSTable Disk Usage} / \text{Live Data Size}$.
* **Target Metric**: Elimination of write-stall latency spikes; P99 write latency kept stable under continuous high client load.

---

### 4. Distributed TCP Socket Transport (`SocketTransport`)

* **Motivation**: Transition SidDB from thread-based in-memory simulated networking to a true distributed network operating across distinct OS processes and physical machines.
* **Architecture**:
  * Java NIO (`ServerSocketChannel`, `SocketChannel`) or Netty-based framing.
  * Efficient binary payload framing (`[Length: 4B][Type: 1B][CorrelationId: 8B][Payload]`).
  * Connection pooling, heartbeat keep-alives, and automatic reconnection on peer failure.
  * CLI node launcher: `java -jar siddb.jar --node-id=node-1 --port=9001 --peers=node-2:9002,node-3:9003`.
* **Target Metric**: Full multi-node cluster deployment running across separate JVM processes or distributed servers.