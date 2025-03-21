package com.cloud.merkle;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;

public class Helper implements HttpFunction {
    public static long measureRecursiveMerkleTree(byte[][] data, int threshold) throws NoSuchAlgorithmException {
        Instant start = Instant.now();
        byte[] root = RecursiveMerkleTree.genMerkleRootFromRaw(data, threshold);
        Instant end = Instant.now();
        return Duration.between(start,end).toMillis();
    }

    public static long measureParallelMerkleTree(byte[][] data, int threshold) throws NoSuchAlgorithmException {
        Instant start = Instant.now();
        byte[] root = ParallelMerkleTree.genMerkleRootFromRaw(data, threshold);
        Instant end = Instant.now();
        return Duration.between(start,end).toMillis();
    }

    public static long measureStandardMerkleTree(byte[][] data) throws NoSuchAlgorithmException {
        Instant start = Instant.now();
        byte[] root = MerkleTree.genMerkleRootFromRaw(data);
        Instant end = Instant.now();
        return Duration.between(start,end).toMillis();
    }

    /***
     * Parse data into uniform chunks (ideally each row is a chunk)
     */
    public static byte[][] parseData(byte[] input, int chunkSize) {
        byte[][] dataBytes = new byte[input.length/ chunkSize][chunkSize];
        for (int i = 0; i < dataBytes.length; i++) {
            System.arraycopy(input, i * chunkSize, dataBytes[i], 0, chunkSize);
        }
        return dataBytes;
    }

    public static String encodeHexString(byte[] byteArray) {
        StringBuilder hexStringBuffer = new StringBuilder();
        for (byte b : byteArray) {
            hexStringBuffer.append(byteToHex(b));
        }
        return hexStringBuffer.toString();
    }

    public static String byteToHex(byte num) {
        char[] hexDigits = new char[2];
        hexDigits[0] = Character.forDigit((num >> 4) & 0xF, 16);
        hexDigits[1] = Character.forDigit((num & 0xF), 16);
        return new String(hexDigits);
    }

    @Override
    public void service(HttpRequest httpRequest, HttpResponse httpResponse) throws Exception {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Blob blob = storage.get("run-sources-protean-music-381914-us-central1", "data/standard/test1.txt");
        if (blob == null) {
            httpResponse.getWriter().write("File not found.");
            return;
        }

        try (BufferedReader br = new BufferedReader(new InputStreamReader(Channels.newInputStream(blob.reader())))) {
            String line;
            ILinkedList<byte[]> nodes = new ILinkedList<>();
            while ((line = br.readLine()) != null) {
                nodes.addLast(line.getBytes(StandardCharsets.UTF_8));
            }
            byte[][] data = new byte[nodes.size][];
            nodes.toArray(data);
            int threshold = httpRequest.getFirstQueryParameter("threshold")
                    .map(Integer::parseInt)
                    .orElse(1024);
            httpResponse.getWriter().write(String.format("Standard: %d\n", measureStandardMerkleTree(data)));
            httpResponse.getWriter().write(String.format("Parallel: %d\n", measureParallelMerkleTree(data, threshold)));
            httpResponse.getWriter().write(String.format("Recursive: %d\n", measureRecursiveMerkleTree(data, threshold)));
        } catch (NoSuchAlgorithmException ae) {
            httpResponse.getWriter().write("SHA-256 not supported");
        }
    }
}
