package com.cloud.merkle;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

public class ParallelMerkleTree {

    public static class HashTask extends RecursiveTask<ILinkedList<byte[]>> {
        private final int threshold;
        private final byte[][] array;
        private final int start;
        private final int end;

        HashTask(byte[][] array, int start, int end, int threshold) {
            this.array = array;
            this.start = start;
            this.end = end;
            this.threshold = threshold;
        }

        @Override
        protected ILinkedList<byte[]> compute() {
            if (end - start <= threshold) {
                try {
                    MessageDigest md = MessageDigest.getInstance("SHA-256");
                    ILinkedList<byte[]> res = new ILinkedList<>();
                    for (int i = start; i < end; i+=2) {
                        byte[] merged = new byte[64];
                        System.arraycopy(array[i], 0, merged, 0, 32);
                        System.arraycopy(array[i + 1], 0, merged, 32, 32);
                        res.addLast(md.digest(merged));
                    }
                    return res;
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            }
            int pairs = (end - start) / 2;
            HashTask leftTask = new HashTask(array, start, start + (pairs / 2) * 2, threshold);
            HashTask rightTask = new HashTask(array, start + (pairs / 2) * 2, end, threshold);

            leftTask.fork();
            ILinkedList<byte[]> right = rightTask.compute();
            ILinkedList<byte[]> left = leftTask.join();
            left.addAll(right);
            return left;
        }
    }

    public static byte[] genMerkleRootFromRaw(byte[][] data, int threshold) throws NoSuchAlgorithmException {
        byte[][] hashes = new byte[data.length][32];
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < data.length; i++) {
            hashes[i] = md.digest(data[i]);
        }
        return genMerkleRootFromHash(hashes, threshold);
    }

    public static byte[] genMerkleRootFromHash(byte[][] hashes, int threshold) {
        int n = (int) (Math.log(hashes.length) / Math.log(2)) + 1;
        byte[][] hashToProcess = hashes;
        ForkJoinPool pool = ForkJoinPool.commonPool();
        for (int i = 1; i < n; i++) {
            // level i
            HashTask task = new HashTask(hashToProcess, 0, hashToProcess.length, threshold);
            ILinkedList<byte[]> res = pool.invoke(task);

            if ((hashToProcess.length / 2) % 2 == 1) {
                res.addLast(res.tail.value);
            }
            byte[][] lv = new byte[res.size][];
            res.toArray(lv);
            hashToProcess = lv;
        }
        return hashToProcess[0];
    }
}
