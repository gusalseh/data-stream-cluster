package com.example.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class RandomSentenceSource implements SourceFunction<String> {

    private volatile boolean isRunning = true;
    private final Random random = new Random();
    private static final String LOG_FILE_PATH = "/opt/project/flink-wordcount/log/fluentd/random_sentences.log"

    // 랜덤 문장 배열
    private final String[] sentences = {
            "The quick brown fox jumps over the lazy dog",
            "Apache Flink is a powerful stream processing framework",
            "Real time analytics requires low latency processing",
            "Flink supports both streaming and batch processing",
            "Stateful computations are crucial for event processing"
    };

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            // 랜덤 문장 선택
            String sentence = sentences[random.nextInt(sentences.length)];
            // 문장 수집
            ctx.collect(sentence);
            // 로그 파일에 쓰기
            logToFile(sentence);
            // 1초 간격으로 문장 생성
            Thread.sleep(1000);
        }
    }

    private void logToFile(String message) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE_PATH, true))) {
            writer.write(message);
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
