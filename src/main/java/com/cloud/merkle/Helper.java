package com.cloud.merkle;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;

public class Helper implements HttpFunction {
    public static long measureRecursiveMerkleTree(byte[][] data) throws NoSuchAlgorithmException {
        Instant start = Instant.now();
        byte[] root = RecursiveMerkleTree.generateMerkleRoot(data);
        Instant end = Instant.now();
        return Duration.between(start,end).toMillis();
    }

    public static long measureParallelMerkleTree(byte[][] data) throws NoSuchAlgorithmException {
        Instant start = Instant.now();
        byte[] root = ParallelMerkleTree.generateMerkleRoot(data);
        Instant end = Instant.now();
        return Duration.between(start,end).toMillis();
    }

    public static long measureStandardMerkleTree(byte[][] data) throws NoSuchAlgorithmException {
        Instant start = Instant.now();
        byte[] root = MerkleTree.generateMerkleRoot(data);
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
        Blob blob = storage.get("run-sources-protean-music-381914-us-central1", "data/standard/test.txt");
        if (blob == null) {
            httpResponse.getWriter().write("File not found.");
            return;
        }

        byte[] raw = blob.getContent();
        byte[][] data = parseData(raw, 64);

        try {
            httpResponse.getWriter().write(String.format("Standard: %d\n", measureStandardMerkleTree(data)));
            httpResponse.getWriter().write(String.format("Parallel: %d\n", measureParallelMerkleTree(data)));
            httpResponse.getWriter().write(String.format("Recursive: %d\n", measureRecursiveMerkleTree(data)));
        }
        catch (NoSuchAlgorithmException ae) {
            httpResponse.getWriter().write("SHA-256 not supported");
        }
    }
}
