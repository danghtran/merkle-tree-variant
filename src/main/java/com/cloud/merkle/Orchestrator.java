package example;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.System.out;

public class Orchestrator {

    public static String LAMBDA_URL = "https://h2f4g6imsmua54grka7n7lpldy0goljp.lambda-url.us-east-1.on.aws/";
    private static final int MAX_CONCURRENT_REQUESTS = 300;
    private static final Semaphore semaphore = new Semaphore(MAX_CONCURRENT_REQUESTS);
    private static final HttpClient client = HttpClient.newHttpClient();

    public static void main(String[] args) throws NoSuchAlgorithmException {
//        int[] workers = {32, 64, 128};
//        int[] linesInFileLog2 = {20, 21, 22, 23, 24, 25};
//        int[] workers = {32};
        String[] dataFiles = {"test_1m.txt", "test_1m.txt", "test_5m.txt", "test_20m.txt"};
        int[] numLines = {1_000_000, 1_000_000, 5_000_000, 20_000_000};
//        int[] chunksPerWorker = {1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144};
        int[] chunksPerWorker = {16384};

        for (var c: chunksPerWorker) {
            out.println("** Chunks per worker: " + c + "**");
            for (int i = 0; i < dataFiles.length; i++) {
                var fileName = dataFiles[i];
                var lines = numLines[i];
                out.println("\t File: " + fileName);
//                out.println("\t Chunks per worker: " + lines/numWorker);
                Instant start = Instant.now();
                var rootHash = constructMerkleTree(fileName, c, lines);
                out.println("\t Calculated root hash: " + rootHash);
                Instant end = Instant.now();
                out.println("\t Time taken: " + Duration.between(start,end).toMillis());
                out.println();
            }
            out.println();
            out.println();
        }
    }

    public static String constructMerkleTree(String fileName, int chunksPerWorker, int totalChunks) throws NoSuchAlgorithmException {
//        int totalChunks = 1 << totalLinesLogBase2;  // 2^20 lines = 1,048,576 lines
//        int chunksPerWorker = totalChunks / numWorkers; // Each worker processes 16,384 lines
        int workers = (int) Math.ceil((double) totalChunks / chunksPerWorker);
        out.println("\t Workers: " + workers);

        HttpClient client = HttpClient.newHttpClient();

        // Create a list of asynchronous HTTP requests
        List<CompletableFuture<String>> futures = IntStream.range(0, workers)
                .mapToObj(i -> {
                    int skip = i * chunksPerWorker;
                    // Prepare JSON payload with appropriate "skip" and "take" values
                    String jsonPayload = String.format(
                            "{\"fileName\":\"%s\", \"chunkSize\":8, \"skip\":%d, \"take\":%d}",
                            fileName, skip, chunksPerWorker
                    );

                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(LAMBDA_URL))
                            .header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                            .build();

                    try {
                        semaphore.acquire();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    // Send request asynchronously and extract the response body (the hex hash string)
                    return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                            .thenApply(HttpResponse::body)
                            .whenComplete((response, throwable) -> semaphore.release());
                })
                .toList();

//        var futures = IntStream.range(0, workers)
//                .mapToObj(i -> {
//                    int skip = i * chunksPerWorker;
//                    // Prepare JSON payload with appropriate "skip" and "take" values
//                    String jsonPayload = String.format(
//                            "{\"fileName\":\"%s\", \"chunkSize\":8, \"skip\":%d, \"take\":%d}",
//                            fileName, skip, chunksPerWorker
//                    );
//
//                    HttpRequest request = HttpRequest.newBuilder()
//                            .uri(URI.create(LAMBDA_URL))
//                            .header("Content-Type", "application/json")
//                            .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
//                            .build();
//
//                    // Create a CompletableFuture that:
//                    // 1. Acquires a permit from the semaphore before sending the request.
//                    // 2. Sends the request asynchronously.
//                    // 3. Releases the permit when the request completes.
//                    return CompletableFuture.runAsync(() -> {
//                        try {
//                            semaphore.acquire();
//                            System.out.println(("acquried"));
//                        } catch (InterruptedException e) {
//                            throw new RuntimeException(e);
//                        }
//                    }).thenCompose(ignored ->
//                            client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
//                    ).thenApply(HttpResponse::body).whenComplete((response, throwable) -> {
//                        semaphore.release();
//                        System.out.println("released");
//                    });
//                })
//                .toList();

        // Wait for all asynchronous calls to complete
        CompletableFuture<Void> allDone = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        CompletableFuture<List<String>> allHashesFuture = allDone.thenApply(v ->
                futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toList())
        );

        // Get the list of hash strings
        List<String> hashList = allHashesFuture.join();

        byte[][] subTreeHashes = (byte[][]) hashList.stream().map(h -> java.util.HexFormat.of().parseHex(h)).toArray(byte[][]::new);

        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[][][] tree = MerkleTree.generateMerkleTree(subTreeHashes, md);

        return java.util.HexFormat.of().formatHex(tree[tree.length-1][0]);
    }

}
