package com.cloud.merkle;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class LambdaWorker implements RequestHandler<APIGatewayV2HTTPEvent, String> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String S3_URL = "https://merkle-tree-inputs.s3.us-east-1.amazonaws.com/";

    @Override
    public String handleRequest(APIGatewayV2HTTPEvent event, Context context) {
        String body = event.getBody();

        EventPayload input = null;
        try {
            input = objectMapper.readValue(body, EventPayload.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        if (context != null) {
            context.getLogger().log("Received input: " + input);
            context.getLogger().log("Received input.fileName: " + input.fileName);
            context.getLogger().log("Received input.chunkSize: " + input.chunkSize);
            context.getLogger().log("Received input.skip: " + input.skip);
            context.getLogger().log("Received input.take: " + input.take);
        }

        try {
            byte[][] dataBytes = parseDataFromS3(S3_URL + input.fileName, input.chunkSize, input.skip, input.take);
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            Instant start = Instant.now();
            byte[][] hashBatch = hashData(dataBytes, md);
            byte[][][] tree = MerkleTree.generateMerkleTree(hashBatch, md);
            Instant end = Instant.now();
            System.out.println(Duration.between(start,end).toMillis());
            System.out.println(tree.length);
            System.out.println(tree[tree.length-1].length);
            return java.util.HexFormat.of().formatHex(tree[tree.length-1][0]); // hex string of root node
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<String> listFilesUsingJavaIO(String dir) {
        return Stream.of(Objects.requireNonNull(new File(dir).listFiles()))
                .filter(file -> !file.isDirectory())
                .map(File::getName)
                .collect(Collectors.toSet());
    }

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

    public static byte[][] parseDataFromS3(String s3Url, int chunkSize, int skip, int take) throws IOException {
        // Prepare local file path
        String localFilePath = "/tmp/" + extractFileName(s3Url);
        File localFile = new File(localFilePath);

        // Download only if file does not exist
        if (!localFile.exists()) {
            System.out.println("Downloading file from S3...");
            downloadFileFromS3(s3Url, localFilePath);
        } else {
            System.out.println("File already exists locally. Skipping download.");
        }

        // Parse the file
        return parseData(localFilePath, chunkSize, skip, take);
    }

    private static void downloadFileFromS3(String s3Url, String localFilePath) throws IOException {
        try (InputStream in = new URL(s3Url).openStream();
             FileOutputStream out = new FileOutputStream(localFilePath)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
    }

    private static String extractFileName(String s3Url) {
        return s3Url.substring(s3Url.lastIndexOf('/') + 1);
    }

    public static byte[][] parseData(String fileName, int chunkSize, int skip, int take) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(fileName, "r")) {
            long position = (long) skip * chunkSize;
            raf.seek(position);

            byte[][] dataBytes = new byte[take][chunkSize];
            for (int i = 0; i < take; i++) {
                int totalRead = 0;
                while (totalRead < chunkSize) {
                    int bytesRead = raf.read(dataBytes[i], totalRead, chunkSize - totalRead);
                    if (bytesRead < 0) {
                        break;
                    }
                    totalRead += bytesRead;
                }
                if (totalRead < chunkSize) {
                    break;
                }
            }
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

    public static void main(String[] args) {
//        var handler = new MyLambdaHandler();
//        var payload = new EventPayload();
//        payload.skip = 0;
//        payload.take = 5;
//        payload.fileName = "test_2m.txt";
//        payload.chunkSize = 5;
//        System.out.println(handler.handleRequest(payload, null));
    }
}
