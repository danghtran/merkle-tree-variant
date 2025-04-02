package com.cloud.merkle;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MerkleTree {
    public static byte[] genRootFromProof(byte[] hash, byte[][] proof, MessageDigest md) {
        byte[] result = hash;
        byte[] merged = new byte[64];
        for (int i = 0; i < proof.length; i++) {
            if (proof[i][32] == 1) {
                System.arraycopy(result, 0, merged, 0, 32);
                System.arraycopy(proof[i], 0, merged, 32, 32);
            } else {
                System.arraycopy(proof[i], 0, merged, 0, 32);
                System.arraycopy(result, 0, merged, 32, 32);
            }
            result = md.digest(merged);
        }
        return result;
    }

    public static byte[] getRootFromTree(byte[][][] tree) {
        return tree[tree.length - 1][0];
    }

    public static byte[][] generateMerkleProof(byte[] hash, byte[][][] tree) {
        byte[][] proof = new byte[tree.length - 1][33];
        int hashIndex = indexOf(hash, tree[0]);
        if (hashIndex != -1) {
            for (int i = 0; i < tree.length - 1; i++) {
                // level i
                int proofIndex = hashIndex + (int) Math.pow(-1, hashIndex % 2);
                System.arraycopy(tree[i][proofIndex], 0, proof[i], 0, 32);
                proof[i][32] = (byte) (proofIndex % 2);
                hashIndex = hashIndex / 2;
            }
        }
        return proof;
    }

    public static byte[][] generateMerkleProof(byte[] hash, byte[][] hashBatch, MessageDigest md) {
        int n = (int) (Math.log(hashBatch.length) / Math.log(2)) + 1;
        byte[][] proof = new byte[n - 1][33];
        int hashIndex = indexOf(hash, hashBatch);
        if (hashIndex != -1) {
            byte[][] hashToProcess = hashBatch;
            for (int i = 0; i < n - 1; i++) {
                // level i
                byte[][] lv = new byte[hashToProcess.length / 2][32];
                for (int j = 0; j < hashToProcess.length; j+=2) {
                    // pair j and j+1
                    byte[] merged = new byte[64];
                    System.arraycopy(hashToProcess[j], 0, merged, 0, 32);
                    System.arraycopy(hashToProcess[j + 1], 0, merged, 32, 32);
                    lv[j/2] = md.digest(merged);

                    if (j == hashIndex || j + 1 == hashIndex) {
                        System.arraycopy(hashToProcess[hashIndex != j ? j: j + 1], 0, proof[i], 0, 32);
                        proof[i][33] = (byte) ((hashIndex % 2) ^ 1);
                        hashIndex = hashIndex / 2;
                    }
                }
                hashToProcess = lv;
            }
        }
        return proof;
    }

    public static int indexOf(byte[] hash, byte[][] hashBatch) {
        for (int i = 0; i < hashBatch.length; i++) {
            if (Arrays.equals(hash, hashBatch[i])) {
                return i;
            }
        }
        return -1;
    }

    public static byte[] genMerkleRootFromRaw(byte[][] data) throws NoSuchAlgorithmException {
        byte[][] hashes = new byte[data.length][32];
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < data.length; i++) {
            hashes[i] = md.digest(data[i]);
        }
        return genMerkleRootFromHash(hashes);
    }

    public static byte[] genMerkleRootFromHash(byte[][] hashes) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        int n = (int) (Math.log(hashes.length) / Math.log(2)) + 1;
        byte[][] hashToProcess = processLevels(hashes, md, n);
        return hashToProcess[0];
    }

    private static byte[][] processLevels(byte[][] hashes, MessageDigest md, int levels) {
        byte[][] hashToProcess = hashes;
        for (int i = 1; i < levels; i++) {
            // level i
            byte[][] lv;
            if ((hashToProcess.length / 2) % 2 == 1) {
                lv = new byte[hashToProcess.length / 2 + 1][32];
            } else {
                lv = new byte[hashToProcess.length / 2][32];
            }
            for (int j = 0; j < hashToProcess.length; j+=2) {
                // pair j and j+1
                byte[] merged = new byte[64];
                System.arraycopy(hashToProcess[j], 0, merged, 0, 32);
                System.arraycopy(hashToProcess[j + 1], 0, merged, 32, 32);
                lv[j/2] = md.digest(merged);
            }
            if ((hashToProcess.length / 2) % 2 == 1) {
                lv[lv.length - 1] = lv[lv.length - 2];
            }
            hashToProcess = lv;
        }
        return hashToProcess;
    }

    public static byte[] generateMerkleRoot(byte[][] hashBatch, int level) throws NoSuchAlgorithmException {
        int n = (int) (Math.log(hashBatch.length) / Math.log(2)) + 1;
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[][] hashToProcess = processLevels(hashBatch, md, n);
        while (n < level) {
            byte[] merged = new byte[64];
            System.arraycopy(hashToProcess[0], 0, merged, 0, 32);
            System.arraycopy(hashToProcess[0], 0, merged, 32, 32);
            hashToProcess[0] = md.digest(merged);
            n++;
        }
        return hashToProcess[0];
    }

    public static byte[][][] generateMerkleTree(byte[][] leaves, MessageDigest md) {
        List<byte[][]> levels = new ArrayList<>();
        levels.add(leaves);

        byte[][] currentLevel = leaves;
        while (currentLevel.length > 1) {
            if (currentLevel.length % 2 == 1) {
                currentLevel = duplicateLastNode(currentLevel);
            }
            byte[][] parentLevel = new byte[currentLevel.length / 2][md.getDigestLength()];
            for (int i = 0; i < currentLevel.length; i += 2) {
                byte[] merged = concat(currentLevel[i], currentLevel[i + 1]);
                parentLevel[i / 2] = md.digest(merged);
            }
            levels.add(parentLevel);
            currentLevel = parentLevel;
        }

        return convertTo3D(levels);
    }

    private static byte[][] duplicateLastNode(byte[][] level) {
        byte[][] extended = new byte[level.length + 1][];
        System.arraycopy(level, 0, extended, 0, level.length);
        extended[level.length] = level[level.length - 1]; // duplicate last
        return extended;
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] merged = new byte[a.length + b.length];
        System.arraycopy(a, 0, merged, 0, a.length);
        System.arraycopy(b, 0, merged, a.length, b.length);
        return merged;
    }

    public static byte[][][] convertTo3D(List<byte[][]> list) {
        byte[][][] array = new byte[list.size()][][];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }
        return array;
    }
}
