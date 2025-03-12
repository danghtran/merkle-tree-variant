package com.cloud.merkle;

import com.google.cloud.tasks.v2.*;

import java.io.IOException;

public class TaskDivider {
    public static void main(String[] args) {
        createHttpTask();
    }

    public static void createHttpTask() {
        try (CloudTasksClient client = CloudTasksClient.create()) {
            String queuePath = QueueName.of("protean-music-381914", "us-central1", "merkle-tasks").toString();

            // Create HTTP Request for the task
            HttpRequest httpRequest = HttpRequest.newBuilder()
                    .setUrl("https://merkle-tree-variant-812821988920.us-central1.run.app")
                    .setHttpMethod(HttpMethod.GET)
                    .build();

            // Create the task
            Task task = Task.newBuilder()
                    .setHttpRequest(httpRequest)
                    .build();

            // Send task to Cloud Tasks queue
            Task response = client.createTask(queuePath, task);
            System.out.println("Task created: " + response.getName());
        } catch (IOException e) {

        }
    }
}
