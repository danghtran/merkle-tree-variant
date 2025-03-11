package com.cloud.merkle;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;

public class Helper implements HttpFunction {

    public static void measureParallelMerkleTree(String inputFile, int chunkSize) {
        try {
            byte[][] dataBytes = parseData(inputFile, chunkSize);
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            Instant start = Instant.now();
            byte[][] hashBatch = hashData(dataBytes, md);
            byte[][][] tree = ParallelMerkleTree.generateMerkleTree(hashBatch);
            Instant end = Instant.now();
            System.out.println(Duration.between(start,end).toMillis());
        }
        catch (IOException e) {
            System.out.println("Cannot read input file");
        }
        catch (NoSuchAlgorithmException ae) {
            System.out.println("SHA-256 not supported");
        }
    }

    public static void measureStandardMerkleTree(String inputFile, int chunkSize) {
        try {
            byte[][] dataBytes = parseData(inputFile, chunkSize);
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            Instant start = Instant.now();
            byte[][] hashBatch = hashData(dataBytes, md);
            byte[][][] tree = MerkleTree.generateMerkleTree(hashBatch, md);
            Instant end = Instant.now();
            System.out.println(Duration.between(start,end).toMillis());
        }
        catch (IOException e) {
            System.out.println("Cannot read input file");
        }
        catch (NoSuchAlgorithmException ae) {
            System.out.println("SHA-256 not supported");
        }
    }

    /***
     * Parse data into uniform chunks (ideally each row is a chunk)
     */
    public static byte[][] parseData(String fileName, int chunkSize) throws IOException {
        File file = new File(fileName);
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[][] dataBytes = new byte[(int) (file.length() / chunkSize)][chunkSize];
            for (int i = 0; i < dataBytes.length; i++) {
                fis.read(dataBytes[i], 0, chunkSize);
            }
            fis.close();
            return dataBytes;
        }
    }

    public static byte[][] hashData(byte[][] dataChunks, MessageDigest md) {
        byte[][] hashBatch = new byte[dataChunks.length][32];
        for (int i = 0; i < dataChunks.length; i++) {
            hashBatch[i] = md.digest(dataChunks[i]);
        }
        return hashBatch;
    }

    public static byte[] strToBytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
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

    public static void createTestFile() {
        try (FileOutputStream fos = new FileOutputStream("testp.txt")) {
            for (int i = 0; i < 8; i++) {
                fos.write("Text no\n".getBytes(StandardCharsets.UTF_8));
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    @Override
    public void service(HttpRequest httpRequest, HttpResponse httpResponse) throws Exception {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Blob blob = storage.get("run-sources-protean-music-381914-us-central1", "data/standard/test.txt");
        if (blob == null) {
            httpResponse.getWriter().write("File not found.");
            return;
        }
        byte[] b = new byte[8];
        System.arraycopy(blob.getContent(), 0, b, 0, 8);
        String t = encodeHexString(b);
        httpResponse.getWriter().write(t);
    }
}
