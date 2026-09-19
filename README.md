# ⚡ SidDB (Siddharth Distributed Database)

[![Java Version](https://img.shields.io/badge/Java-11%2B-blue.svg)](https://openjdk.java.net/)
[![Architecture](https://img.shields.io/badge/Architecture-LSM--Tree%20%2B%20Raft%20Consensus-orange.svg)]()
[![Consensus](https://img.shields.io/badge/Consensus-Distributed%20Raft%20(CP)-green.svg)]()
[![Tests](https://img.shields.io/badge/Regression%20Tests-16%2F16%20Passed%20(100%25)-brightgreen.svg)]()
[![Durability](https://img.shields.io/badge/Durability-Strict%20WAL%20fsync%20%2B%20Zero%20Data%20Loss-success.svg)]()

**SidDB** is a production-grade, distributed, log-structured merge-tree (LSM-Tree) key-value storage engine engineered from scratch in Java. It features multi-node **Raft Consensus**, **ACID transactions with MVCC snapshot isolation**, **real OS TCP socket networking**, **asynchronous tiered SSTable compactions**, **strict follower WAL durability with dynamic fsync**, and **high-performance linearizable reads (Leader Leases & ReadIndex)**.

---

## 📑 Table of Contents
1. [Architectural Overview](#-architectural-overview)
2. [Evolutionary Phase Roadmap](#-evolutionary-phase-roadmap)
3. [Deep-Dive Subsystem Architecture](#-deep-dive-subsystem-architecture)
   - [LSM Storage Engine](#1-lsm-storage-engine)
   - [Raft Consensus & Replication](#2-distributed-raft-consensus-engine)
   - [Leader Leases & ReadIndex Protocol](#3-high-performance-linearizable-reads)
   - [Durability & Dynamic fsync](#4-durability--dynamic-fsync-policies)
   - [Decoupled Compactor & Telemetry](#5-decoupled-asynchronous-compactor--wafsaf-telemetry)
4. [Empirical Benchmarks & Performance](#-empirical-benchmarks--performance)
5. [Chaos Engineering & Partition Tolerance](#-chaos-engineering--partition-tolerance)
6. [Interactive Visualizer Studio](#-interactive-visualizer-studio)
7. [Getting Started & CLI Commands](#-getting-started--cli-commands)
8. [Regression Test Suite](#-regression-test-suite)
9. [Project Directory Layout](#-project-directory-layout)

---

## 🏛 Architectural Overview

```text
                                  Client Application / Benchmark Driver
                                                    |
                                    TCP Socket Transport (NIO / Sockets)
                                                    |
                      +-----------------------------+-----------------------------+
                      |                                                           |
          Node 1 (Port 9001 - Leader)                                 Node 2 (Port 9002 - Follower)
  +-----------------------------------------+                 +-----------------------------------------+
  | Raft Consensus Engine                   |                 | Raft Consensus Engine                   |
  |  ├─ Leader Lease / ReadIndex Protocol   |  TCP RPCs       |  ├─ Term & Voting Tracker               |
  |  ├─ Heartbeat & Election Managers       |<===============>|  ├─ AppendEntries Log Backpressure      |
  |  ├─ Raft Log Persistence (raft.log)     | (Vote / Append) |  ├─ Strict Follower fsync (SyncPolicy)  |
  |  └─ Pipelined Quorum Commit Index       |                 |  └─ Replicated Log Persistence          |
  |                                         |                 |                                         |
  | LSM Storage Engine (SidDBEngine)        |                 | LSM Storage Engine (SidDBEngine)        |
  |  ├─ Concurrent SkipList MemTable (RAM)  |                 |  ├─ Active & Frozen MemTables (RAM)     |
  |  ├─ Append-Only WAL (wal.sb + CRC32)    |                 |  ├─ Local Append-Only WAL (wal.sb)      |
  |  ├─ LRU 4KB Block Cache & Sparse Index  |                 |  ├─ LRU Block Cache & Bloom Filters     |
  |  ├─ Multi-Level SSTables (L0 -> L1-> L2)|                 |  ├─ Multi-Level SSTables (.sb, .idx,.bf)|
  |  ├─ Decoupled Async Background Compactor|                 |  ├─ Background SSTable Compactor        |
  |  └─ Real-Time WAF / SAF Telemetry       |                 |  └─ Real-Time Telemetry Engine          |
  +-----------------------------------------+                 +-----------------------------------------+
```

---

## 🗺 Evolutionary Phase Roadmap

| Phase | Core Milestone | Description |
|---|---|---|
| **Phase 1** | **Bitcask DiskStore** | Append-only storage format (`.sb`), in-memory key directory, active + immutable segment rotation. |
| **Phase 2** | **WAL & Crash Recovery** | Binary record serialization, CRC32 data integrity hashing, and startup state machine recovery. |
| **Phase 3** | **LSM-Tree Hierarchy** | `MemTable` (SkipList), SSTable format (`.sb` data, `.idx` sparse index, `.bf` Bloom filter), `LevelManager`, Cascading Compactor, and Concurrent LRU `BlockCache`. |
| **Phase 4** | **ACID Engine & MVCC** | Atomic multi-key `WriteBatch` (all-or-nothing commits) and Snapshot Isolation with monotonic sequence timestamps. |
| **Phase 5** | **Distributed Raft Consensus** | Leader elections, term invariants, quorum log replication, commit advancement, disk persistence (`raft.meta`), and multi-process TCP servers (`DistributedNodeServer`). |
| **Phase 6** | **Chaos & Fault Injection** | In-memory and Real TCP socket chaos testing simulating 9 catastrophic network & crash failure modes. |
| **Phase 7** | **Statistical Benchmarking** | Multi-run load sweeps with microsecond percentiles (P50, P95, P99, P99.9), std dev, and multi-process TCP benchmarking (`MultiProcessBenchmarkRunner`). |
| **Phase 8.1** | **Leader Leases & ReadIndex** | Sub-millisecond linearizable read engine avoiding consensus log writes (>100k ops/sec in-memory, >1.7k ops/sec over TCP). |
| **Phase 8.2** | **Follower WAL Durability** | Synchronous `FileDescriptor.sync()` on follower `AppendEntries` with configurable `SyncPolicy` (`SYNC_EVERY_ENTRY`, `SYNC_BATCH`, `ASYNC_FLUSH`). |
| **Phase 8.3** | **Decoupled Async Compactor** | Offloads SSTable merges to background daemon executor, soft write backpressure, and real-time **WAF** / **SAF** telemetry. |

---

## 🔬 Deep-Dive Subsystem Architecture

### 1. LSM Storage Engine
* **`MemTable`**: High-concurrency `ConcurrentSkipListMap` in memory for O(log N) writes and range scans.
* **Write-Ahead Log (`WAL`)**: Appends mutations with a 4-byte CRC32 checksum, record length header, and payload before mutating RAM.
* **SSTable Triplet Structure**:
  1. **`.sb` Data File**: 4KB binary data blocks containing sorted Key-Value pairs.
  2. **`.idx` Sparse Index**: Two-level block index holding block offsets and boundary keys to minimize disk seeks.
  3. **`.bf` Bloom Filter**: Optimal bits-per-key bitset using Fowler–Noll–Vo (FNV) hashing to reject non-existent key disk reads instantly (<1% false positive rate).
* **`LevelManager` & `MANIFEST.sb`**: Organizes SSTables into Level 0 (overlapping) and Level 1+ (non-overlapping key ranges). State transitions are journaled to `MANIFEST.sb`.

---

### 2. Distributed Raft Consensus Engine
* **Raft Roles**: `LEADER`, `FOLLOWER`, `CANDIDATE`, and `OFFLINE`.
* **State Safety**: Monotonic term progression; candidates must possess logs at least as up-to-date as followers (`lastLogTerm` and `lastLogIndex`) to receive quorum votes.
* **Pipelined Quorum Commit**: The leader tracks `matchIndex` per follower and advances `commitIndex` as soon as a strict majority acknowledgement is received.
* **Disk Persistence**: Nodes persist `currentTerm` and `votedFor` to `raft.meta` and entries to `raft.log`, surviving hard process crashes and sudden reboots.

---

### 3. High-Performance Linearizable Reads
SidDB supports three configurable read modes via `ReadMode`:
1. **`LEADER_LEASE`**: The leader maintains a time-bounded lease. Within the lease window, reads are served directly from local memory without network roundtrips.
2. **`READ_INDEX`**: Leader captures `readIndex = commitIndex`, executes a lightweight heartbeat quorum check (no disk/log writes), waits until `lastApplied >= readIndex`, and serves the read.
3. **`LOG_BARRIER`**: Fallback mode proposing a barrier entry through consensus.

---

### 4. Durability & Dynamic `fsync` Policies
SidDB eliminates the follower durability window with `SyncPolicy`:
* **`SYNC_EVERY_ENTRY`**: Enforces physical `FileDescriptor.sync()` on followers before responding `AppendEntriesReply(success = true)`. Guarantees zero data loss even if minority nodes crash ungracefully.
* **`SYNC_BATCH`**: Flushes periodically or on frame threshold boundaries.
* **`ASYNC_FLUSH`**: Optimized memory-buffered mode for maximum throughput.

---

### 5. Decoupled Asynchronous Compactor & WAF/SAF Telemetry
* **Non-Blocking Write Path**: MemTable flushes trigger `compactor.triggerAsyncCompaction()` into a dedicated single-threaded daemon executor, eliminating ~270 ms write-pause spikes.
* **Soft Write Backpressure**: Imposes an adaptive 5 ms pause when Level 0 uncompacted tables exceed threshold (>= 8).
* **Real-Time Telemetry**:
  - **WAF**: Total Disk Bytes Written / User Ingestion Bytes Written
  - **SAF**: Total Physical SSTable Disk Space / Active Logical Live Data Size

---

## 📊 Empirical Benchmarks & Performance

### 1. In-Memory Cluster Baseline (Microsecond Timers)
| Workload | Nodes | Clients | Throughput (Median) | P50 Latency | P95 Latency | P99 Latency | Success Rate |
|---|---|---|---|---|---|---|---|
| **Linearizable Get (3 Nodes)** | 3 | 16 | **76,954.15 ops/s** | **6 µs** | 200 µs | 7.47 ms | **100.00%** |
| **Linearizable Get (5 Nodes)** | 5 | 16 | **33,801.77 ops/s** | **7 µs** | 62 µs | 10.06 ms | **100.00%** |
| **Local Get (3 Nodes)** | 3 | 16 | **119,729.34 ops/s** | **10 µs** | 30 µs | 4.95 ms | **100.00%** |
| **Local Get (5 Nodes)** | 5 | 16 | **113,223.04 ops/s** | **3 µs** | 11 µs | 636 µs | **100.00%** |
| **Put (Single-Node Peak)** | 1 | 16 | **412.09 ops/s** | 39.89 ms | 108.38 ms | 122.85 ms | **100.00%** |
| **Put (3-Node Quorum)** | 3 | 16 | **207.17 ops/s** | 57.10 ms | 135.27 ms | 218.96 ms | **100.00%** |

---

### 2. Multi-Process Real TCP Cluster Benchmark (Separate OS Processes)
Benchmarked across independent OS Java processes communicating strictly via real loopback TCP sockets (`127.0.0.1:9300+`):

| Workload | OS Processes | Clients | Throughput (Median) | P50 Latency | P99 Latency | Durability / Integrity |
|---|---|---|---|---|---|---|
| **Local Get (3 Processes)** | 3 | 16 | **2,107.38 ops/s** | 11.13 ms | 62.02 ms | 100% Success |
| **Linearizable Get (3 Processes)** | 3 | 16 | **1,189.18 ops/s** | 14.77 ms | 36.09 ms | 100% Linearizable |
| **Put (1 Process)** | 1 | 16 | **53.64 ops/s** | 286.14 ms | 1,198.13 ms | 100% fsynced |
| **Put (3 Processes Quorum)** | 3 | 16 | **22.41 ops/s** | 704.32 ms | 3,330.19 ms | 100% Quorum Commit |
| **Leader Hard Kill & Failover** | 3 | 1 | **483 ms election** | **512 ms resume** | **30/30 keys** | **100% Zero Data Loss** |

---

## 🛡 Chaos Engineering & Partition Tolerance

SidDB was subjected to rigorous chaos engineering via `ChaoticTransport` across all 9 failure modes:

| Category | Chaos Scenario | Invariant & Defense Mechanism | Verdict |
|---|---|---|---|
| **Network** | 25% Random Packet Loss | Heartbeat retries & AppendEntries log backpressure | **TOLERATED** |
| **Network** | Latency Jitter (50–250 ms) | Adaptive election timers with randomized backoff | **TOLERATED** |
| **Network** | Asymmetric Partition (Split-Brain) | Quorum intersection prevents minority commit | **TOLERATED** |
| **Network** | Out-of-Order Message Delivery | Term and log index validation rejects stale RPCs | **TOLERATED** |
| **Process** | Hard Leader Crash & Reboot | Persistent `raft.meta` & WAL replay on startup | **TOLERATED** |
| **Process** | Correlated 2-Node Crash (5 Nodes) | Majority 3 nodes maintain cluster quorum and keep committing | **TOLERATED** |
| **Process** | Flapping Node (Rapid Crash Loops) | Monotonic term progression prevents destabilizing quorum | **TOLERATED** |
| **Replication**| Slow Follower Catch-up | Leader decrements `nextIndex` until match, streams delta | **TOLERATED** |
| **Storage** | Disk / WAL I/O Fault Injection | Node transitions to OFFLINE, quorum promotes healthy peer | **TOLERATED** |

---

## 🎨 Interactive Visualizer Studio

SidDB includes an interactive visualizer web application to inspect and manipulate cluster state in real time:

* **Real-time Ring / Mesh Topology**: Visualizes Leader, Follower, and Candidate states with heartbeat pulses.
* **LSM-Tree Inspector**: Live visualization of MemTable entries, Level 0..N SSTables, Bloom filters, and Block Cache hit ratios.
* **Chaos Injection Controls**: Trigger packet drops, partitions, and hard node crashes with a single click.
* **Access**: Open `visualizer/index.html` or `siddb_visualizer.html` directly in any web browser.

---

## 🚀 Getting Started & CLI Commands

### 1. Prerequisites
* **Java Development Kit (JDK 11 or higher)**
* Windows PowerShell, macOS zsh, or Linux bash

### 2. Compile the Codebase
```powershell
# Compile all source and test files into the out/ directory
javac -d out (Get-ChildItem -Recurse -Include *.java src,test | ForEach-Object { $_.FullName })
```

### 3. Run the Full Regression Test Suite (16 Suites)
```powershell
java -ea -cp out test.TestRunner
```

### 4. Run the Real Multi-Process TCP Cluster Demo
```powershell
java -cp out test.MultiProcessClusterDemo
```

### 5. Start a 3-Node Distributed Cluster Manually
Open 3 separate terminal windows:
```powershell
# Terminal 1 (Node 1 on Port 9001)
java -cp out server.DistributedNodeServer node-1 9001 node-2=127.0.0.1:9002,node-3=127.0.0.1:9003

# Terminal 2 (Node 2 on Port 9002)
java -cp out server.DistributedNodeServer node-2 9002 node-1=127.0.0.1:9001,node-3=127.0.0.1:9003

# Terminal 3 (Node 3 on Port 9003)
java -cp out server.DistributedNodeServer node-3 9003 node-1=127.0.0.1:9001,node-2=127.0.0.1:9002
```

In any node's console, type interactive REPL commands:
* `status` — Print node role, term, leader, commit index, and MemTable keys.
* `put <key> <val>` — Propose a replicated write to the cluster.
* `get <key>` — Query value from local state machine.
* `del <key>` — Propose a deletion tombstone.
* `exit` — Cleanly terminate node server.

### 6. Run the Benchmarks
```powershell
# Run the Real Multi-Process TCP benchmark suite
java -cp out benchmark.MultiProcessBenchmarkRunner

# Or run the high-speed in-memory multi-iteration benchmark suite
java -cp out benchmark.BenchmarkRunner
```

---

## 🧪 Regression Test Suite

The master test runner verifies all 16 test suites across all 8 architectural phases:

```text
=================================================
          SidDB Regression Test Suite            
=================================================
  [+] Phase 1: Bitcask DiskStore Test
  [+] Phase 2: WAL & Crash Recovery Test
  [+] Phase 3: SSTable (.sb, .idx, .bf) Test
  [+] Phase 3: LevelManager & MemTable Flush Test
  [+] Phase 3: L0 -> L1 -> L2 Cascading Compaction Test
  [+] Phase 3: LRU Block Cache & Hit Ratio Test
  [+] Phase 4: ACID Transactions (WriteBatch & MVCC Snapshots)
  [+] Phase 5: Raft Consensus & Distributed Replication
  [+] Phase 5: Real TCP Socket Transport Test
  [+] Phase 5: Raft Disk Persistence & Crash Recovery Test
  [+] Phase 5: Multi-Process Real TCP Cluster Demo
  [+] Phase 6: Network Failures & Chaos Testing Suite (In-Memory)
  [+] Phase 6: Real TCP Sockets Chaos Injection Test
  [+] Phase 8.1: High-Performance Leader Lease & ReadIndex Test
  [+] Phase 8.2: Follower WAL Durability & Dynamic fsync Test
  [+] Phase 8.3: Decoupled Asynchronous Compactor & WAF/SAF Telemetry Test

=================================================
 TEST RESULTS: 16 Passed, 0 Failed, Total: 16
=================================================
```

---

## 📁 Project Directory Layout

```text
SidDB/
├── src/
│   ├── benchmark/            # Benchmark runner, workloads & report generator
│   │   ├── workloads/        # Put, Get, Compaction, Recovery workload drivers
│   │   ├── BenchmarkMetrics.java
│   │   ├── BenchmarkReportGenerator.java
│   │   ├── BenchmarkRunner.java
│   │   └── MultiProcessBenchmarkRunner.java
│   ├── chaos/                # Chaos engine, failure injection & cluster context
│   │   ├── ChaosClusterContext.java
│   │   ├── ChaosEngine.java
│   │   └── ChaosReportGenerator.java
│   ├── compaction/           # Background compaction daemon & WAF/SAF telemetry
│   │   ├── CompactionTelemetry.java
│   │   └── Compactor.java
│   ├── disk/                 # Bitcask disk store & binary encoding
│   │   └── DiskStore.java
│   ├── engine/               # SidDB LSM storage engine
│   │   └── SidDBEngine.java
│   ├── level/                # LSM multi-level hierarchy manager & Manifest
│   │   └── LevelManager.java
│   ├── memtable/             # SkipList MemTable implementation
│   │   └── MemTable.java
│   ├── network/              # Sockets, simulated network & TCP client
│   │   ├── ChaoticTransport.java
│   │   ├── DistributedClient.java
│   │   ├── SimulatedNetwork.java
│   │   └── SocketTransport.java
│   ├── raft/                 # Distributed Raft consensus engine
│   │   ├── AppendEntriesArgs.java / AppendEntriesReply.java
│   │   ├── ElectionManager.java
│   │   ├── HeartbeatManager.java
│   │   ├── RaftCluster.java
│   │   ├── RaftLog.java / RaftLogEntry.java
│   │   ├── RaftNode.java / RaftRole.java
│   │   ├── RaftStateStore.java
│   │   ├── ReadMode.java
│   │   ├── RequestVoteArgs.java / RequestVoteReply.java
│   │   └── SyncPolicy.java
│   ├── server/               # Multi-process cluster manager & node servers
│   │   ├── DistributedNodeServer.java
│   │   └── MultiProcessCluster.java
│   ├── sstable/              # SSTable format, sparse index, Bloom filter, Block cache
│   │   ├── BlockCache.java
│   │   ├── BloomFilter.java
│   │   ├── SSTableEntry.java
│   │   ├── SSTableReader.java
│   │   └── SSTableWriter.java
│   ├── transaction/          # ACID WriteBatch & MVCC Snapshots
│   │   ├── Snapshot.java
│   │   └── WriteBatch.java
│   └── wal/                  # Write-Ahead Log with CRC32 checksums
│       ├── WALEntry.java
│       ├── WALReader.java
│       └── WALWriter.java
├── test/                     # 16 Regression & Chaos test suites
│   ├── MultiProcessClusterDemo.java
│   ├── Phase1DiskStoreTest.java
│   ├── Phase2WALTest.java
│   ├── Phase3BlockCacheTest.java
│   ├── Phase3CompactionTest.java
│   ├── Phase3LevelTest.java
│   ├── Phase3SSTableTest.java
│   ├── Phase4ACIDTest.java
│   ├── Phase5RaftCrashRecoveryTest.java
│   ├── Phase5RaftTest.java
│   ├── Phase6NetworkFailuresTest.java
│   ├── Phase8CompactionTelemetryTest.java
│   ├── Phase8DurabilityFsyncTest.java
│   ├── Phase8LeaderLeaseTest.java
│   ├── SocketTransportTest.java
│   ├── TcpChaosTest.java
│   └── TestRunner.java       # Master test runner
├── visualizer/               # Interactive web visualization studio
│   ├── index.html
│   ├── app.js
│   └── styles.css
├── BenchmarkReport.md        # Real multi-process empirical benchmark report
├── ChaosReport.md            # 9-scenario chaos & consistency verification report
├── Phase.md                  # Comprehensive architectural phase specification
├── README.md                 # Project documentation (this file)
└── .gitignore                # Git ignore rules for data/ and out/
```

---

## 📜 License
This project is open-source and educational under the MIT License.
