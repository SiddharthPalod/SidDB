package sstable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class BlockCache {

    public static final int DEFAULT_CAPACITY_BLOCKS = 1024; // Holds up to ~4MB of hot blocks

    public static class BlockKey {
        private final String sstablePath;
        private final long fileOffset;

        public BlockKey(String sstablePath, long fileOffset) {
            this.sstablePath = sstablePath;
            this.fileOffset = fileOffset;
        }

        public String getSstablePath() {
            return sstablePath;
        }

        public long getFileOffset() {
            return fileOffset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof BlockKey)) return false;
            BlockKey blockKey = (BlockKey) o;
            return fileOffset == blockKey.fileOffset && Objects.equals(sstablePath, blockKey.sstablePath);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sstablePath, fileOffset);
        }

        @Override
        public String toString() {
            return sstablePath + "@" + fileOffset;
        }
    }

    private final int capacity;
    private final Map<BlockKey, List<SSTableEntry>> lruMap;
    private final AtomicLong hitCount;
    private final AtomicLong missCount;

    public BlockCache() {
        this(DEFAULT_CAPACITY_BLOCKS);
    }

    public BlockCache(int capacity) {
        this.capacity = capacity;
        this.hitCount = new AtomicLong(0);
        this.missCount = new AtomicLong(0);

        // LinkedHashMap with accessOrder = true implements true LRU in O(1) time
        this.lruMap = new LinkedHashMap<BlockKey, List<SSTableEntry>>(capacity, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<BlockKey, List<SSTableEntry>> eldest) {
                return size() > BlockCache.this.capacity;
            }
        };
    }

    public synchronized List<SSTableEntry> get(String sstablePath, long fileOffset) {
        BlockKey key = new BlockKey(sstablePath, fileOffset);
        List<SSTableEntry> entries = lruMap.get(key);
        if (entries != null) {
            hitCount.incrementAndGet();
            return entries;
        } else {
            missCount.incrementAndGet();
            return null;
        }
    }

    public synchronized void put(String sstablePath, long fileOffset, List<SSTableEntry> blockEntries) {
        BlockKey key = new BlockKey(sstablePath, fileOffset);
        lruMap.put(key, blockEntries);
    }

    public synchronized void invalidateSSTable(String sstablePath) {
        lruMap.keySet().removeIf(k -> k.getSstablePath().equals(sstablePath));
    }

    public synchronized void clear() {
        lruMap.clear();
        hitCount.set(0);
        missCount.set(0);
    }

    public synchronized int size() {
        return lruMap.size();
    }

    public int getCapacity() {
        return capacity;
    }

    public long getHitCount() {
        return hitCount.get();
    }

    public long getMissCount() {
        return missCount.get();
    }

    public double getHitRatio() {
        long total = hitCount.get() + missCount.get();
        if (total == 0) return 0.0;
        return (double) hitCount.get() / total;
    }

    @Override
    public synchronized String toString() {
        return String.format("BlockCache[Size: %d/%d, Hits: %d, Misses: %d, HitRatio: %.2f%%]",
                size(), capacity, hitCount.get(), missCount.get(), getHitRatio() * 100);
    }
}
