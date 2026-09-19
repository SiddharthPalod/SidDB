package raft;

public enum ReadMode {
    /**
     * Time-bounded leader lease: Reads are served immediately from local state
     * machine/MemTable as long as the leader holds a valid lease confirmed by majority heartbeats.
     * Throughput: >100k ops/sec, Latency: Sub-millisecond.
     */
    LEADER_LEASE,

    /**
     * ReadIndex protocol: Leader captures current commitIndex, exchanges lightweight
     * heartbeats with a quorum to confirm leadership without log writes, waits for lastApplied >= readIndex,
     * and then serves the read.
     */
    READ_INDEX,

    /**
     * Legacy Log Barrier: Proposes a full log entry through Raft consensus to achieve linearizability.
     */
    LOG_BARRIER
}
