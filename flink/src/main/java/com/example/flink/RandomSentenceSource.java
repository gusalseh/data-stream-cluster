package com.example.flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomSentenceSource implements SourceFunction<String> {

    private volatile boolean isRunning = true;
    private final Random random = new Random();

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
            // 1초 간격으로 문장 생성
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
