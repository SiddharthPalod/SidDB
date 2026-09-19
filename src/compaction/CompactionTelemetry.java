package compaction;

import java.util.concurrent.atomic.AtomicLong;

public class CompactionTelemetry {

    private final AtomicLong userBytesWritten = new AtomicLong(0);
    private final AtomicLong diskBytesWritten = new AtomicLong(0);
    private final AtomicLong bytesMerged = new AtomicLong(0);
    private final AtomicLong totalMergeDurationMs = new AtomicLong(0);
    private final AtomicLong compactionsCount = new AtomicLong(0);
    private final AtomicLong backpressureEvents = new AtomicLong(0);

    public void recordUserWrite(long bytes) {
        userBytesWritten.addAndGet(bytes);
    }

    public void recordDiskWrite(long bytes) {
        diskBytesWritten.addAndGet(bytes);
    }

    public void recordCompaction(long inputBytes, long durationMs) {
        compactionsCount.incrementAndGet();
        bytesMerged.addAndGet(inputBytes);
        totalMergeDurationMs.addAndGet(Math.max(1, durationMs));
    }

    public void recordBackpressureEvent() {
        backpressureEvents.incrementAndGet();
    }

    public long getUserBytesWritten() {
        return userBytesWritten.get();
    }

    public long getDiskBytesWritten() {
        return diskBytesWritten.get();
    }

    public long getBytesMerged() {
        return bytesMerged.get();
    }

    public long getTotalMergeDurationMs() {
        return totalMergeDurationMs.get();
    }

    public long getCompactionsCount() {
        return compactionsCount.get();
    }

    public long getBackpressureEvents() {
        return backpressureEvents.get();
    }

    /**
     * Write Amplification Factor (WAF) = Total Disk Bytes Written / User Logical Bytes Written.
     */
    public double getWAF() {
        long userBytes = userBytesWritten.get();
        if (userBytes <= 0) return 1.0;
        return (double) diskBytesWritten.get() / userBytes;
    }

    /**
     * Space Amplification Factor (SAF) = Total SSTable Disk Usage / Live Active Data Size.
     */
    public double getSAF(long totalDiskBytes, long liveDataBytes) {
        if (liveDataBytes <= 0) return 1.0;
        return (double) totalDiskBytes / liveDataBytes;
    }

    /**
     * Merge Throughput in MB/second.
     */
    public double getMergeThroughputMBs() {
        long durationMs = totalMergeDurationMs.get();
        if (durationMs <= 0) return 0.0;
        double mbMerged = (double) bytesMerged.get() / (1024.0 * 1024.0);
        double seconds = durationMs / 1000.0;
        return mbMerged / Math.max(0.001, seconds);
    }

    @Override
    public String toString() {
        return String.format("CompactionTelemetry[compactions=%d, WAF=%.2f, mergeThroughput=%.2f MB/s, backpressureEvents=%d]",
                getCompactionsCount(), getWAF(), getMergeThroughputMBs(), getBackpressureEvents());
    }
}