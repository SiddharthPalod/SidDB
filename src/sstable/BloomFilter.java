package sstable;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

public class BloomFilter {

    private final BitSet bitSet;
    private final int bitSetSize;
    private final int numHashFunctions;

    public BloomFilter(int expectedElements, double falsePositiveRate) {
        if (expectedElements <= 0) {
            expectedElements = 1;
        }
        if (falsePositiveRate <= 0.0 || falsePositiveRate >= 1.0) {
            falsePositiveRate = 0.01; // 1% default
        }

        // Optimal bit size: m = - (n * ln(p)) / (ln(2)^2)
        this.bitSetSize = Math.max(64, (int) Math.ceil(-1 * expectedElements * Math.log(falsePositiveRate) / (Math.log(2) * Math.log(2))));
        
        // Optimal hash count: k = (m / n) * ln(2)
        this.numHashFunctions = Math.max(1, (int) Math.round(((double) bitSetSize / expectedElements) * Math.log(2)));
        this.bitSet = new BitSet(bitSetSize);
    }

    private BloomFilter(BitSet bitSet, int bitSetSize, int numHashFunctions) {
        this.bitSet = bitSet;
        this.bitSetSize = bitSetSize;
        this.numHashFunctions = numHashFunctions;
    }

    public void add(String key) {
        int[] hashes = getHashes(key);
        for (int hash : hashes) {
            int bitIndex = Math.abs(hash % bitSetSize);
            bitSet.set(bitIndex);
        }
    }

    public boolean mightContain(String key) {
        int[] hashes = getHashes(key);
        for (int hash : hashes) {
            int bitIndex = Math.abs(hash % bitSetSize);
            if (!bitSet.get(bitIndex)) {
                return false; // Definitely does not exist
            }
        }
        return true; // Might exist
    }

    /**
     * Kirsch-Mitzenmacher double-hashing technique:
     * hash_i(key) = hash1(key) + i * hash2(key)
     */
    private int[] getHashes(String key) {
        byte[] data = key.getBytes(StandardCharsets.UTF_8);
        int hash1 = murmur3(data, 0);
        int hash2 = murmur3(data, hash1);

        int[] result = new int[numHashFunctions];
        for (int i = 0; i < numHashFunctions; i++) {
            result[i] = hash1 + (i * hash2);
        }
        return result;
    }

    private int murmur3(byte[] data, int seed) {
        int c1 = 0xcc9e2d51;
        int c2 = 0x1b873593;
        int h1 = seed;

        int roundedEnd = (data.length & 0xfffffffc);
        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);
            k1 *= c1;
            k1 = (k1 << 15) | (k1 >>> 17);
            k1 *= c2;

            h1 ^= k1;
            h1 = (h1 << 13) | (h1 >>> 19);
            h1 = h1 * 5 + 0xe6546b64;
        }

        int k1 = 0;
        switch (data.length & 0x03) {
            case 3:
                k1 ^= (data[roundedEnd + 2] & 0xff) << 16;
            case 2:
                k1 ^= (data[roundedEnd + 1] & 0xff) << 8;
            case 1:
                k1 ^= (data[roundedEnd] & 0xff);
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >>> 17);
                k1 *= c2;
                h1 ^= k1;
        }

        h1 ^= data.length;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);
        return h1;
    }

    public void writeToFile(File file) throws IOException {
        try (DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))) {
            dos.writeInt(bitSetSize);
            dos.writeInt(numHashFunctions);
            byte[] bytes = bitSet.toByteArray();
            dos.writeInt(bytes.length);
            dos.write(bytes);
        }
    }

    public static BloomFilter readFromFile(File file) throws IOException {
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))) {
            int bitSetSize = dis.readInt();
            int numHashFunctions = dis.readInt();
            int length = dis.readInt();
            byte[] bytes = new byte[length];
            dis.readFully(bytes);
            BitSet bitSet = BitSet.valueOf(bytes);
            return new BloomFilter(bitSet, bitSetSize, numHashFunctions);
        }
    }
}
