package com.example.logger;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class RandomSentenceLogger {
    private static final String LOG_FILE_PATH = "/opt/project/flink-wordcount/log/fluentd/random_sentences.log";
    private static final Random RANDOM = new Random();

    private static final String[] SENTENCES = {
            "The quick brown fox jumps over the lazy dog",
            "Apache Flink is a powerful stream processing framework",
            "Real time analytics requires low latency processing",
            "Flink supports both streaming and batch processing",
            "Stateful computations are crucial for event processing"
    };

    public static void main(String[] args) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE_PATH, true))) {
            while (true) {
                String sentence = SENTENCES[RANDOM.nextInt(SENTENCES.length)];
                writer.write(sentence);
                writer.newLine();
                writer.flush();
                System.out.println("Logged: " + sentence);
                Thread.sleep(1000);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
