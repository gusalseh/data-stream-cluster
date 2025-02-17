package com.example.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountJob {
    public static void main(String[] args) throws Exception {
        // Flink 실행 환경 생성
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 랜덤 영어 문장 생성
        DataStream<String> sentences = env.addSource(new RandomSentenceSource());

        // 각 문장을 공백 단위로 분할 후 단어 갯수 계산
        DataStream<Tuple2<String, Integer>> wordCounts = sentences
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) {
                        // 공백 기준 단어 분리
                        for (String word : sentence.split("\\s+")) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .keyBy(value -> value.f0)
                .sum(1);

        // 결과를 표준 출력으로 내보냄
        // 병렬 출력이 섞이지 않도록 병렬도 1로 설정
        wordCounts.print().setParallelism(1);

        // Flink 작업 실행
        // 종료를 위해서는 수동으로 취소해야 함
        env.execute("Random Sentence WordCount");
    }
}
