package com.example.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Properties;

/**
 * KstJob:
 *  - Kafka 토픽("kafka2flink")에서 JSON 메시지 수신
 *  - JSON 파싱 → Event 객체
 *  - fornetDB.kst 테이블에 INSERT
 */
public class KstJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost");
        props.setProperty("group.id", "flink_consumer_group");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "kafka2flink",
                new SimpleStringSchema(),
                props
        );

        DataStream<String> stream = env.addSource(kafkaConsumer);

        DataStream<Event> eventStream = stream.map(new MapFunction<String, Event>() {
            private final ObjectMapper mapper = new ObjectMapper();

            @Override
            public Event map(String rawJson) throws Exception {
                JsonNode root = mapper.readTree(rawJson);

                Event e = new Event();

                e.set번호(root.has("번호") ? root.get("번호").asInt() : 0);

                if (root.has("발생시간")) {
                    e.set발생시간(Timestamp.valueOf(root.get("발생시간").asText()));
                } else {
                    e.set발생시간(new Timestamp(System.currentTimeMillis()));
                }

                e.set설비(root.has("설비") ? root.get("설비").asText() : null);
                e.set설비_타입(root.has("설비 타입") ? root.get("설비 타입").asText() : null);
                e.set기관(root.has("기관") ? root.get("기관").asText() : null);
                e.set이벤트_생성_설비IP(root.has("이벤트 생성 설비IP") ? root.get("이벤트 생성 설비IP").asText() : null);
                e.set이벤트(root.has("이벤트") ? root.get("이벤트").asText() : null);
                e.set이벤트_분류(root.has("이벤트 분류") ? root.get("이벤트 분류").asText() : null);
                e.set시그니처(root.has("시그니처") ? root.get("시그니처").asText() : null);
                e.set출발지_IP(root.has("출발지 IP") ? root.get("출발지 IP").asText() : null);
                e.set출발지_Port(root.has("출발지 Port") ? root.get("출발지 Port").asInt() : 0);
                e.set출발지_그룹(root.has("출발지 그룹") ? root.get("출발지 그룹").asText() : null);
                e.set목적지_IP(root.has("목적지 IP") ? root.get("목적지 IP").asText() : null);
                e.set목적지_Port(root.has("목적지 Port") ? root.get("목적지 Port").asInt() : 0);
                e.set목적지_그룹(root.has("목적지 그룹") ? root.get("목적지 그룹").asText() : null);
                e.set프로토콜(root.has("프로토콜") ? root.get("프로토콜").asText() : null);
                e.set이벤트_수집서버(root.has("이벤트 수집서버") ? root.get("이벤트 수집서버").asText() : null);
                e.set네트워크(root.has("네트워크") ? root.get("네트워크").asText() : null);
                e.set호스트(root.has("호스트") ? root.get("호스트").asText() : null);
                e.set서비스(root.has("서비스") ? root.get("서비스").asText() : null);
                e.set인터페이스(root.has("인터페이스") ? root.get("인터페이스").asText() : null);
                e.set프로토콜_번호(root.has("프로토콜 번호") ? root.get("프로토콜 번호").asInt() : 0);
                e.set대응내용(root.has("대응내용") ? root.get("대응내용").asText() : null);
                e.set이벤트_발생_횟수(root.has("이벤트 발생 횟수") ? root.get("이벤트 발생 횟수").asInt() : 0);
                e.set사용자(root.has("사용자") ? root.get("사용자").asText() : null);
                e.set규칙(root.has("규칙") ? root.get("규칙").asText() : null);
                e.set규칙_ID(root.has("규칙 ID") ? root.get("규칙 ID").asText() : null);
                e.set기타(root.has("기타") ? root.get("기타").asText() : null);

                return e;
            }
        });

        String insertSql = "INSERT INTO fornetDB.kst (" +
                "`번호`, `발생시간`, `설비`, `설비 타입`, `기관`, `이벤트 생성 설비IP`, `이벤트`, " +
                "`이벤트 분류`, `시그니처`, `출발지 IP`, `출발지 Port`, `출발지 그룹`, " +
                "`목적지 IP`, `목적지 Port`, `목적지 그룹`, `프로토콜`, `이벤트 수집서버`, " +
                "`네트워크`, `호스트`, `서비스`, `인터페이스`, `프로토콜 번호`, " +
                "`대응내용`, `이벤트 발생 횟수`, `사용자`, `규칙`, `규칙 ID`, `기타`" +
                ") VALUES (" +
                "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?" +
                ")";

        var jdbcSink = JdbcSink.sink(
                insertSql,
                (PreparedStatement ps, Event ev) -> {
                    ps.setInt(1, ev.get번호());
                    ps.setTimestamp(2, ev.get발생시간());
                    ps.setString(3, ev.get설비());
                    ps.setString(4, ev.get설비_타입());
                    ps.setString(5, ev.get기관());
                    ps.setString(6, ev.get이벤트_생성_설비IP());
                    ps.setString(7, ev.get이벤트());
                    ps.setString(8, ev.get이벤트_분류());
                    ps.setString(9, ev.get시그니처());
                    ps.setString(10, ev.get출발지_IP());
                    ps.setInt(11, ev.get출발지_Port());
                    ps.setString(12, ev.get출발지_그룹());
                    ps.setString(13, ev.get목적지_IP());
                    ps.setInt(14, ev.get목적지_Port());
                    ps.setString(15, ev.get목적지_그룹());
                    ps.setString(16, ev.get프로토콜());
                    ps.setString(17, ev.get이벤트_수집서버());
                    ps.setString(18, ev.get네트워크());
                    ps.setString(19, ev.get호스트());
                    ps.setString(20, ev.get서비스());
                    ps.setString(21, ev.get인터페이스());
                    ps.setInt(22, ev.get프로토콜_번호());
                    ps.setString(23, ev.get대응내용());
                    ps.setInt(24, ev.get이벤트_발생_횟수());
                    ps.setString(25, ev.get사용자());
                    ps.setString(26, ev.get규칙());
                    ps.setString(27, ev.get규칙_ID());
                    ps.setString(28, ev.get기타());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(1000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mariadb://50.100.100.69:3306/fornetDB")
                        .withDriverName("org.mariadb.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("qwer1234")
                        .build()
        );

        eventStream.addSink(jdbcSink);

        env.execute("KstJob - Kafka -> fornetDB.kst");
    }

    public static class Event {
        private int 번호;
        private Timestamp 발생시간;
        private String 설비;
        private String 설비_타입;
        private String 기관;
        private String 이벤트_생성_설비IP;
        private String 이벤트;
        private String 이벤트_분류;
        private String 시그니처;
        private String 출발지_IP;
        private int 출발지_Port;
        private String 출발지_그룹;
        private String 목적지_IP;
        private int 목적지_Port;
        private String 목적지_그룹;
        private String 프로토콜;
        private String 이벤트_수집서버;
        private String 네트워크;
        private String 호스트;
        private String 서비스;
        private String 인터페이스;
        private int 프로토콜_번호;
        private String 대응내용;
        private int 이벤트_발생_횟수;
        private String 사용자;
        private String 규칙;
        private String 규칙_ID;
        private String 기타;

        public int get번호() { return 번호; }
        public void set번호(int 번호) { this.번호 = 번호; }

        public Timestamp get발생시간() { return 발생시간; }
        public void set발생시간(Timestamp 발생시간) { this.발생시간 = 발생시간; }

        public String get설비() { return 설비; }
        public void set설비(String 설비) { this.설비 = 설비; }

        public String get설비_타입() { return 설비_타입; }
        public void set설비_타입(String 설비_타입) { this.설비_타입 = 설비_타입; }

        public String get기관() { return 기관; }
        public void set기관(String 기관) { this.기관 = 기관; }

        public String get이벤트_생성_설비IP() { return 이벤트_생성_설비IP; }
        public void set이벤트_생성_설비IP(String 이벤트_생성_설비IP) { this.이벤트_생성_설비IP = 이벤트_생성_설비IP; }

        public String get이벤트() { return 이벤트; }
        public void set이벤트(String 이벤트) { this.이벤트 = 이벤트; }

        public String get이벤트_분류() { return 이벤트_분류; }
        public void set이벤트_분류(String 이벤트_분류) { this.이벤트_분류 = 이벤트_분류; }

        public String get시그니처() { return 시그니처; }
        public void set시그니처(String 시그니처) { this.시그니처 = 시그니처; }

        public String get출발지_IP() { return 출발지_IP; }
        public void set출발지_IP(String 출발지_IP) { this.출발지_IP = 출발지_IP; }

        public int get출발지_Port() { return 출발지_Port; }
        public void set출발지_Port(int 출발지_Port) { this.출발지_Port = 출발지_Port; }

        public String get출발지_그룹() { return 출발지_그룹; }
        public void set출발지_그룹(String 출발지_그룹) { this.출발지_그룹 = 출발지_그룹; }

        public String get목적지_IP() { return 목적지_IP; }
        public void set목적지_IP(String 목적지_IP) { this.목적지_IP = 목적지_IP; }

        public int get목적지_Port() { return 목적지_Port; }
        public void set목적지_Port(int 목적지_Port) { this.목적지_Port = 목적지_Port; }

        public String get목적지_그룹() { return 목적지_그룹; }
        public void set목적지_그룹(String 목적지_그룹) { this.목적지_그룹 = 목적지_그룹; }

        public String get프로토콜() { return 프로토콜; }
        public void set프로토콜(String 프로토콜) { this.프로토콜 = 프로토콜; }

        public String get이벤트_수집서버() { return 이벤트_수집서버; }
        public void set이벤트_수집서버(String 이벤트_수집서버) { this.이벤트_수집서버 = 이벤트_수집서버; }

        public String get네트워크() { return 네트워크; }
        public void set네트워크(String 네트워크) { this.네트워크 = 네트워크; }

        public String get호스트() { return 호스트; }
        public void set호스트(String 호스트) { this.호스트 = 호스트; }

        public String get서비스() { return 서비스; }
        public void set서비스(String 서비스) { this.서비스 = 서비스; }

        public String get인터페이스() { return 인터페이스; }
        public void set인터페이스(String 인터페이스) { this.인터페이스 = 인터페이스; }

        public int get프로토콜_번호() { return 프로토콜_번호; }
        public void set프로토콜_번호(int 프로토콜_번호) { this.프로토콜_번호 = 프로토콜_번호; }

        public String get대응내용() { return 대응내용; }
        public void set대응내용(String 대응내용) { this.대응내용 = 대응내용; }

        public int get이벤트_발생_횟수() { return 이벤트_발생_횟수; }
        public void set이벤트_발생_횟수(int 이벤트_발생_횟수) { this.이벤트_발생_횟수 = 이벤트_발생_횟수; }

        public String get사용자() { return 사용자; }
        public void set사용자(String 사용자) { this.사용자 = 사용자; }

        public String get규칙() { return 규칙; }
        public void set규칙(String 규칙) { this.규칙 = 규칙; }

        public String get규칙_ID() { return 규칙_ID; }
        public void set규칙_ID(String 규칙_ID) { this.규칙_ID = 규칙_ID; }

        public String get기타() { return 기타; }
        public void set기타(String 기타) { this.기타 = 기타; }
    }
}
