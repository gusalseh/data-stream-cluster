package com.example.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

// Job 설명: Kafka Consumer -> JSON 파싱 -> 예시 조건 분류 -> stout
// 프로덕션에서는 Sink(Opensearch Sink)로 결과 전송 가능
public class KafkaFlinkJob {

    public static void main(String[] args) throws Exception {
        // parse arguments, e.g. --topic=positive_topic --group.id=flink-group
        final ParameterTool params = ParameterTool.fromArgs(args);
        String topic = params.get("topic", "positive_topic");
        String groupId = params.get("group.id", "flink-group");

        // Flink env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka consumer props
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka:9092");
        props.setProperty("group.id", groupId);
        // 추가 props

        // Kafka consumer 생성
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                props
        );
        consumer.setStartFromEarliest();

        // 소스 추가
        env.addSource(consumer)
                // parse JSON
                .map(line -> {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(line);

                    // score >= 0.5 인 값만 분류
                    double score = node.has("score") ? node.get("score").asDouble() : 0.0;
                    if(score >= 0.5) {
                        System.out.println("VALID: " + line);
                    } else {
                        System.out.println("INVALID: " + line);
                    }
                    return line;
                })
                .returns(String.class)
                .uid("map-json")
                .name("Map JSON data")
                .print();

        // Job 실행
        env.execute("Kafka-Flink JSON Processing Job");
    }
}
