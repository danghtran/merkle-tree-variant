package com.cloud.merkle;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

public class RecursiveMerkleTree {
    // only store the root node of partial trees
    public static class PartialTreeTask extends RecursiveTask<byte[]> {
        private final int threshold; // 1024 nodes for each partial tree
        private final int level;
        private final byte[][] array;

        PartialTreeTask(byte[][] array, int level, int threshold) {
            this.array = array;
            this.threshold = threshold;
            if (level != 0) {
                this.level =  level;
            } else {
                this.level = (int) (Math.log(array.length) / Math.log(2)) + 1;
            }

        }
        @Override
        protected byte[] compute() {
            if (array.length <= threshold) {
                // calculate directly with Standard Merkle Tree
                try {
                    return MerkleTree.generateMerkleRoot(array, level);
                } catch (NoSuchAlgorithmException e) {

                }
            }
            // 512 pairs for each sub tree
            List<PartialTreeTask> tasks = new ArrayList<>();
            int lv = (int) (Math.log(threshold) / Math.log(2)) + 1;
            int size = array.length;
            int offset = 0;
            while (size > 0) {
                byte[][] sub;
                if (size < threshold) {
                    sub = new byte[size][32];
                } else {
                    sub = new byte[threshold][32];
                }
                System.arraycopy(array, offset, sub, 0, sub.length);
                tasks.add(new PartialTreeTask(sub, lv, threshold));
                size -= threshold;
                offset += threshold;
            }
            tasks.forEach(ForkJoinTask::fork);
            byte[][] res = new byte[tasks.size()][32];
            for (int i = 0; i < tasks.size(); i++) {
                res[i] = tasks.get(i).join();
            }
            PartialTreeTask finalTask = new PartialTreeTask(res, 0, threshold);
            return finalTask.compute();
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
        ForkJoinPool pool = ForkJoinPool.commonPool();
        PartialTreeTask task = new PartialTreeTask(hashes, 0, threshold);
        return pool.invoke(task);
    }
}
