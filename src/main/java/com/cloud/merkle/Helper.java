package com.cloud.merkle;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class Helper implements HttpFunction {

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

    @Override
    public void service(HttpRequest httpRequest, HttpResponse httpResponse) throws Exception {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Blob blob = storage.get("run-sources-protean-music-381914-us-central1", "data/standard/test.txt");
        if (blob == null) {
            httpResponse.getWriter().write("File not found.");
            return;
        }
        String[] params = httpRequest.getReader().readLine().split(",");
        int offset = Integer.parseInt(params[0]);
        int nodes = Integer.parseInt(params[1]);
        int chunkSize = Integer.parseInt(params[2]);

        byte[][] data = new byte[nodes][chunkSize];
        byte[] raw = blob.getContent();

        for (int i = 0; i < nodes; i++) {
            System.arraycopy(raw, offset + i*chunkSize, data[i], 0, chunkSize);
        }

        httpResponse.getWriter().write("Parallel");
        Instant s1 = Instant.now();
        ParallelMerkleTree.generateMerkleTree(data);
        Instant e1 = Instant.now();
        httpResponse.getWriter().write(String.valueOf(Duration.between(s1,e1).toMillis()));

        httpResponse.getWriter().write("\n");

        try {
            httpResponse.getWriter().write("Standard");
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            Instant start = Instant.now();
            MerkleTree.generateMerkleTree(data, md);
            Instant end = Instant.now();
            httpResponse.getWriter().write(String.valueOf(Duration.between(start,end).toMillis()));
        }
        catch (NoSuchAlgorithmException ae) {
            httpResponse.getWriter().write("SHA-256 not supported");
        }
    }
}
