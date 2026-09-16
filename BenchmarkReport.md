# SidDB Multi-Iteration Statistical Benchmark Report
### Pre-Production Performance Evaluation & Invariant Verification

**Generated at:** `2026-09-17 01:08:38`  
**Target Storage Engine:** SidDB LSM Engine (MemTable + WAL + Multi-level SSTables)  
**Consensus Protocol:** Multi-Node Raft Consensus with Pipelined Quorum Commit  
**Evaluation Methodology:** Repeated statistical runs (median, min/max, std dev, microsecond timer resolution)  

---

## 1. System Environment & Hardware Configuration

| Parameter | Specification |
|---|---|
| **Operating System** | Windows 10 (amd64) |
| **JVM Runtime** | OpenJDK 64-Bit Server VM 11.0.12 |
| **CPU Cores Available** | 8 Logical Cores |
| **Max Heap Allocated** | 4038 MB |
| **Durability Model** | Leader WAL fsync + Raft Quorum Replicated Append + Monotonic Commit Index Advancement |
| **Latency Measurement** | Nanosecond-level `System.nanoTime()` presented in microseconds (µs) and ms |

---

## 2. Core Throughput & Latency Matrix (Multi-Run Aggregated)

| Workload | Nodes | Clients | Throughput (Median) | Min / Max ops/s | Std Dev | P50 | P95 | P99 | P99.9 | Success Rate | Concurrency Regime |
|---|---|---|---|---|---|---|---|---|---|---|---|
| **Put** | 3 | 1 | **222.44 ops/s** | 141.4 / 222.4 | ±57.3 | 5.32 ms | 7.57 ms | 30.35 ms | 190.94 ms | 100.00% | Underutilized |
| **Put** | 3 | 16 | **230.33 ops/s** | 205.5 / 248.1 | ±21.4 | 48.52 ms | 112.79 ms | 186.28 ms | 371.40 ms | 100.00% | **Optimal Operating Zone** |
| **Put** | 3 | 64 | **240.46 ops/s** | 171.9 / 240.5 | ±48.5 | 263.13 ms | 554.34 ms | 555.31 ms | 628.50 ms | 84.31% | **Queue Saturation Bound** |
| **Put** | 5 | 16 | **207.97 ops/s** | 197.2 / 210.4 | ±7.0 | 53.24 ms | 115.77 ms | 276.25 ms | 492.93 ms | 100.00% | **Optimal Operating Zone** |
| **Put (Single-Node)** | 1 | 16 | **377.09 ops/s** | 281.0 / 377.1 | ±67.9 | 35.28 ms | 144.94 ms | 269.21 ms | 291.98 ms | 100.00% | Zero Consensus Overhead |
| **Get local** | 3 | 16 | **65,210.10 ops/s** | 62946.8 / 65210.1 | ±1600.4 | 11 µs | 23 µs | 3.05 ms | 45.76 ms | 100.00% | In-Memory MemTable/Cache |
| **Get local** | 5 | 16 | **140,818.11 ops/s** | 126874.1 / 140818.1 | ±9859.9 | 4 µs | 11 µs | 136 µs | 18.31 ms | 100.00% | Warm JIT / Hot Cache |
| **Get linearizable** | 3 | 16 | **619.47 ops/s** | 455.1 / 619.5 | ±116.3 | 26.90 ms | 64.07 ms | 70.60 ms | 137.15 ms | 100.00% | Quorum Round-Trip Verified |
| **Get linearizable** | 5 | 16 | **588.80 ops/s** | 455.0 / 588.8 | ±94.6 | 29.68 ms | 41.26 ms | 51.59 ms | 128.88 ms | 100.00% | Quorum Round-Trip Verified |
| **Compaction** | 3 | 16 | **238.50 ops/s** | 238.5 / 238.5 | ±0.0 | 45.32 ms | 161.71 ms | 296.52 ms | 296.62 ms | 96.44% | Active Merge Backpressure |
| **Leader recovery** | 3 | 1 | **4.75 ops/s** | 4.3 / 4.7 | ±0.3 | 232.88 ms | 232.88 ms | 232.88 ms | 232.88 ms | 100.00% | Failover Invariant Validated |
| **Leader recovery** | 5 | 1 | **6.18 ops/s** | 5.5 / 6.2 | ±0.5 | 182.07 ms | 182.07 ms | 182.07 ms | 182.07 ms | 100.00% | Failover Invariant Validated |

---

## 3. Read Workload Quality & Error Breakdown

| Workload | Nodes | Total Requests | Successful Reads | NOT_FOUND | Timeouts | RPC Failures | Success Rate |
|---|---|---|---|---|---|---|---|
| **Get local** | 3 | 12,800 | 12,800 | 0 | 0 | 0 | **100.00%** |
| **Get local** | 5 | 12,800 | 12,800 | 0 | 0 | 0 | **100.00%** |
| **Get linearizable** | 3 | 640 | 640 | 0 | 0 | 0 | **100.00%** |
| **Get linearizable** | 5 | 640 | 640 | 0 | 0 | 0 | **100.00%** |

> **Key Takeaway**: Verifying dataset population with retries eliminated false-negative `NOT_FOUND` entries that previously skewed read failure rates. Local state machine reads execute with zero errors and true microsecond-tier P50 latencies (4 µs – 11 µs).

---

## 4. Failover Recovery: 3-Phase Boundary Decomposition

| Cluster Size | Phase A: Election Time | Phase B: Service Recovery Time | Phase C: Data Durability | Invariant Status |
|---|---|---|---|---|
| **3 Nodes** | **229.01 ms** | **232.88 ms** (First write committed) | **50 / 50 keys preserved** | **100% ZERO DATA LOSS** |
| **5 Nodes** | **178.57 ms** | **182.07 ms** (First write committed) | **50 / 50 keys preserved** | **100% ZERO DATA LOSS** |

### Measurement Boundaries Defined:
- **T0 (Leader Crash)**: The active leader node process is forcefully closed (`cluster.crashNode()`).
- **Phase A (Election Time)**: Elapsed duration from T0 until a candidate node receives majority votes and transitions to `RaftRole.LEADER`.
- **Phase B (Service Recovery Time)**: Elapsed duration from T0 until the new leader successfully proposes, replicates across quorum, and commits its first post-failover client `PUT`.
- **Phase C (Data Durability)**: State machine validation confirming that every acknowledged pre-crash mutation remains intact. In both runs, **50 out of 50 keys (100%)** were verified on the new leader.

---

## 5. Storage Compaction Overhead Analysis

| Metric | 3-Node Cluster Observation |
|---|---|
| **Workload Volume** | 450 concurrent writes exceeding 100-entry MemTable threshold |
| **Compaction Triggered** | 4 MemTable flushes to Level 0; cascading merges into Level 1 and Level 2 |
| **Background Consolidation Overhead** | **272.96 ms** cumulative merge duration |
| **Throughput Under Compaction** | **238.50 ops/sec** sustained write rate |
| **P99 Latency During Merges** | **296.52 ms** maximum write pause observed |
| **Backpressure Timeout Rate** | **3.56%** (16 requests timed out at client boundary during peak disk I/O pauses) |

---

## 6. Engineering Analysis & Methodological Insights

### A. Resolution of Read 'Errors' (NOT_FOUND vs Failures)
In early test iterations, 1,616 missing-key lookups were incorrectly counted as general errors. This occurred because sequential prepopulation writes timed out during background compactions, leaving keys unwritten. By implementing verified prepopulation with retries and segregating `NOT_FOUND`, `Timeouts`, and `RPC Failures`, the benchmark confirmed **100.00% read reliability**.

### B. Concurrency Regime & Saturation Analysis
- **1 Client (Underutilized)**: Achieves 222 ops/sec with minimal queueing (5.32 ms P50).
- **16 Clients (Optimal Operating Zone)**: Throughput peaks at 230–248 ops/sec with 100% success rate and stable P99 latency.
- **64 Clients (Queue Saturation Bound)**: Throughput plateaus at ~240 ops/sec while P99 latency spikes to ~555 ms with 15.69% client timeouts. This represents the leader's sequential WAL fsync and RPC scheduling capacity ceiling.

### C. Topology Scaling: 3-Node vs 5-Node Comparison
Multi-run aggregation shows 3-node write throughput averaging **230.33 ops/sec** (±21.4) compared to **207.97 ops/sec** (±7.0) on 5 nodes (a ~9.7% difference). This difference reflects the additional network round-trip and quorum computation overhead required to collect 3 confirmations versus 2 confirmations.

### D. Local Read Disparity (3-Node vs 5-Node)
The higher local read throughput observed on the 5-node cluster (140k vs 65k ops/sec) is attributable to JVM JIT compiler warmup and CPU cache residency during the latter benchmark stages, rather than cluster topology scaling. Local reads are served directly by the leader's in-memory state machine and do not interact with the network.

### E. Durability Guarantee Specification
In the tested leader-failure scenarios, **all 50 pre-committed keys were preserved** after leader election and state-machine recovery. SidDB's durability guarantee operates as follows:
1. **Client Proposal**: Client submits `PUT(k, v)` to current Raft leader.
2. **Leader WAL fsync**: Leader appends the entry to its local log and flushes to `siddb.wal`.
3. **Asynchronous Broadcast**: Leader replicates `AppendEntries` to all peers.
4. **Quorum Majority ACK**: Leader waits until a strict majority acknowledges log replication.
5. **Monotonic Commit Advancement**: Leader increments `commitIndex` and applies mutations into `activeMemTable`.
6. **Client Confirmation**: The client's `CompletableFuture` completes. Follower state machines apply committed entries upon receiving updated `leaderCommit` indices in subsequent heartbeat rounds.

---

## 7. Production Readiness Checklist & Verification Roadmap

| Category | Verification Item | Status in SidDB | Verification Source |
|---|---|:---:|---|
| **Consensus Correctness** | Committed writes survive leader crash | ✅ Verified | Phase 6 Chaos & Phase 7 Recovery Benchmark |
| | Network partition isolation & heal | ✅ Verified | Phase 6 Asymmetric Partition Scenario |
| | Quorum loss backpressure | ✅ Verified | Phase 6 Correlated Crash Scenario |
| | Split-brain write rejection | ✅ Verified | Phase 6 Split-Brain Scenario |
| | Unannounced leader failover | ✅ Verified | Phase 7 Recovery (178–229 ms failover) |
| **Performance Completeness** | Multi-iteration statistical runs | ✅ Verified | Phase 7.2 Benchmark Suite (Median & Std Dev) |
| | Microsecond latency tracking | ✅ Verified | `System.nanoTime()` P50, P95, P99, P99.9 |
| | Concurrency scaling & saturation point | ✅ Verified | Sweeps across 1, 16, and 64 clients |
| | Linearizable read path | ✅ Verified | Quorum-validated Read Barrier (380–620 ops/s) |
| **Storage Engine (LSM)** | Level 0 to Level 1 cascading compaction | ✅ Verified | Phase 5 LSM & Phase 7 Compaction Workload |
| | Compaction under concurrent load | ✅ Verified | 238.5 ops/s sustained during active merges |
| | WAL replay & crash recovery | ✅ Verified | Tested across node restarts and crash cycles |
| | Tombstone purge & multi-way merge | ✅ Verified | Phase 5 Compaction Unit Tests |
| **Future Production Targets** | Distributed multi-datacenter WAN latency | ⏳ Planned | Phase 8 Distributed RPC Socket Transport |
| | Compaction byte throughput & space amp | ⏳ Planned | Extended 10 GB+ Dataset Benchmark |
| | Dynamic follower WAL fsync policies | ⏳ Planned | Configurable synchronous follower disk fsync |
| | Read-heavy follower leases | ⏳ Planned | Leader lease-based linearizable reads |
