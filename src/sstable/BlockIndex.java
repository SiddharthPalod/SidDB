package sstable;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BlockIndex {

    public static class IndexEntry {
        private final String firstKey;
        private final long fileOffset;

        public IndexEntry(String firstKey, long fileOffset) {
            this.firstKey = firstKey;
            this.fileOffset = fileOffset;
        }

        public String getFirstKey() {
            return firstKey;
        }

        public long getFileOffset() {
            return fileOffset;
        }
    }

    private final List<IndexEntry> entries;

    public BlockIndex() {
        this.entries = new ArrayList<>();
    }

    public BlockIndex(List<IndexEntry> entries) {
        this.entries = entries;
    }

    public void addEntry(String firstKey, long fileOffset) {
        entries.add(new IndexEntry(firstKey, fileOffset));
    }

    public List<IndexEntry> getEntries() {
        return entries;
    }

    public int size() {
        return entries.size();
    }

    /**
     * Binary search to locate the block that could contain the target key.
     * Finds the block with the largest firstKey <= targetKey.
     */
    public IndexEntry findCandidateBlock(String targetKey) {
        if (entries.isEmpty()) {
            return null;
        }

        int low = 0;
        int high = entries.size() - 1;
        int candidateIdx = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            String midKey = entries.get(mid).getFirstKey();
            int cmp = midKey.compareTo(targetKey);

            if (cmp <= 0) {
                candidateIdx = mid; // Candidate found, check if a closer one exists to the right
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        if (candidateIdx != -1) {
            return entries.get(candidateIdx);
        }
        // If targetKey is smaller than the first block's first key, start from block 0
        return entries.get(0);
    }

    public void writeToFile(File file) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
            dos.writeInt(entries.size());
            for (IndexEntry entry : entries) {
                dos.writeUTF(entry.getFirstKey());
                dos.writeLong(entry.getFileOffset());
            }
        }
    }

    public static BlockIndex readFromFile(File file) throws IOException {
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            int count = dis.readInt();
            List<IndexEntry> entries = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                String firstKey = dis.readUTF();
                long fileOffset = dis.readLong();
                entries.add(new IndexEntry(firstKey, fileOffset));
            }
            return new BlockIndex(entries);
        }
    }
}
