package raft;

public enum SyncPolicy {
    /**
     * Strict durability: Every append on the leader and every AppendEntries on followers
     * forces a synchronous FileDescriptor.sync() before acknowledging.
     * Guarantees zero theoretical data loss on immediate power cut.
     */
    SYNC_EVERY_ENTRY,

    /**
     * Batched durability: Flushes to disk periodically or after batch thresholds.
     */
    SYNC_BATCH,

    /**
     * High-throughput asynchronous flush: Writes to OS buffer cache.
     */
    ASYNC_FLUSH
}