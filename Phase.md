
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

# Phase 4 — Raft

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

# Phase 5 — Network failures

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

# Phase 6 — Benchmarking

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

Now you have **systems research material**, not just code.

---
