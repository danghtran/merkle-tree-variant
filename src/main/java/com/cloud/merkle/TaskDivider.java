package com.cloud.merkle;

import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpResponse;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.tasks.v2.*;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class TaskDivider implements HttpFunction {
    public int[] divideTask() {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Blob blob = storage.get("run-sources-protean-music-381914-us-central1", "data/standard/test.txt");
        if (blob == null) {
            return null;
        }
        // divide 2 mil records into 5 chunks of 400000 lines (800 nodes),
        // each node is 4000 bytes (500 lines)
        long raw = blob.getSize();

        // scan 1st chunk each chunk is 3200000 bytes)
        int[] res = new int[3];
        res[0] = 0; //offset
        res[1] = 800; // nodes
        res[1] = 4000; // chunk size
        return res;
    }

    public Task createHttpTask(int[] body) {
        try (CloudTasksClient client = CloudTasksClient.create()) {
            String queuePath = QueueName.of("protean-music-381914", "us-central1", "merkle-tasks").toString();

            // Create HTTP Request for the task
            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .setUrl("https://merkle-tree-variant-812821988920.us-central1.run.app")
                    .setHttpMethod(HttpMethod.GET)
                    .setBody(ByteString.copyFrom(
                            String.format("%d,%d,%d", body[0], body[1], body[2])
                                    .getBytes(StandardCharsets.UTF_8)))
                    .build();

            // Create the task
            Task task = Task.newBuilder()
                    .setHttpRequest(httpRequest)
                    .build();

            // Send task to Cloud Tasks queue
            return client.createTask(queuePath, task);
        } catch (IOException e) {

        }
        return null;
    }

    @Override
    public void service(com.google.cloud.functions.HttpRequest httpRequest, HttpResponse httpResponse) throws Exception {
        Task task = createHttpTask(divideTask());
        if (task == null) {
            httpResponse.getWriter().write("Failed to create tasks");
        } else {
            httpResponse.getWriter().write("Created tasks");
        }
    }
}
