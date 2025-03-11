package com.cloud.merkle;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

public class ParallelMerkleTree {

    public static class HashTask extends RecursiveTask<ILinkedList<byte[]>> {
        private static final int THRESHOLD = 1024;
        private final byte[][] array;
        private final int start;
        private final int end;

        HashTask(byte[][] array, int start, int end) {
            this.array = array;
            this.start = start;
            this.end = end;
        }

        @Override
        protected ILinkedList<byte[]> compute() {
            if (end - start <= THRESHOLD) {
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
            HashTask leftTask = new HashTask(array, start, start + (pairs / 2) * 2);
            HashTask rightTask = new HashTask(array, start + (pairs / 2) * 2, end);

            leftTask.fork();
            ILinkedList<byte[]> right = rightTask.compute();
            ILinkedList<byte[]> left = leftTask.join();
            left.addAll(right);
            return left;
        }
    }

    public static byte[][][] generateMerkleTree(byte[][] hashBatch) {
        int n = (int) (Math.log(hashBatch.length) / Math.log(2)) + 1;
        byte[][][] tree = new byte[n][][];
        tree[0] = hashBatch;
        ForkJoinPool pool = ForkJoinPool.commonPool();
        for (int i = 1; i < n; i++) {
            // level i
            byte[][] hashToProcess = tree[i - 1];
            HashTask task = new HashTask(hashToProcess, 0, hashToProcess.length);
            ILinkedList<byte[]> res = pool.invoke(task);

            if ((hashToProcess.length / 2) % 2 == 1) {
                res.addLast(res.tail.value);
            }
            byte[][] lv = new byte[res.size][];
            res.toArray(lv);
            tree[i] = lv;
        }
        return tree;
    }
}
