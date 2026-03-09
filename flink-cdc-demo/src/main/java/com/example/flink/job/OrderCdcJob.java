package com.example.flink.job;

import com.example.flink.model.AuditLog;
import com.example.flink.model.OrderEvent;
import com.example.flink.sink.AuditLogSink;
import com.example.flink.sink.DlqSink;
import com.example.flink.util.JsonUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.Properties;

public class OrderCdcJob {

    // Dead Letter Queue 側輸出 Tag
    static final OutputTag<OrderEvent> DLQ_TAG =
        new OutputTag<OrderEvent>("dlq") {};

    public static void main(String[] args) throws Exception {

        // 1. 環境設定
        final StreamExecutionEnvironment env = buildEnvironment();

        // 2. CDC Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
            .hostname(getEnv("MYSQL_HOST", "mysql.flink-lab.svc.cluster.local"))
            .port(Integer.parseInt(getEnv("MYSQL_PORT", "3306")))
            .databaseList("inventory")
            .tableList("inventory.orders")
            .username(getEnv("MYSQL_USER", "flink"))
            .password(getEnv("MYSQL_PASSWORD", "flinkpass"))
            .startupOptions(StartupOptions.initial())  // 初次執行全量快照
            .deserializer(new JsonDebeziumDeserializationSchema())
            .debeziumProperties(buildDebeziumProperties())
            .build();

        DataStream<String> cdcStream = env
            .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL CDC Source")
            .setParallelism(1);  // CDC Source 必須是 1

        // 3. 解析與豐富化
        SingleOutputStreamOperator<OrderEvent> enrichedStream = cdcStream
            .process(new CdcEventParser())
            .name("CDC Event Parser");

        // 4. 業務處理（含 DLQ 分流）
        SingleOutputStreamOperator<OrderEvent> processedStream = enrichedStream
            .process(new OrderProcessor())
            .name("Order Processor");

        // 5. 主流：寫入 Audit Log
        processedStream
            .map(event -> toAuditLog(event, "PROCESSED"))
            .addSink(new AuditLogSink())
            .name("Audit Log Sink");

        // 6. DLQ：失敗事件寫入 Kafka
        processedStream
            .getSideOutput(DLQ_TAG)
            .map(event -> toAuditLog(event, "FAILED"))
            .addSink(new DlqSink(getEnv("KAFKA_BROKERS", "flink-kafka-kafka-brokers.kafka:9092")))
            .name("DLQ Kafka Sink");

        env.execute("Order CDC Pipeline");
    }

    // --- 環境設定 ---
    private static StreamExecutionEnvironment buildEnvironment() {
        Configuration config = new Configuration();

        // Checkpoint 設定
        config.set(CheckpointingOptions.CHECKPOINTING_INTERVAL,
            java.time.Duration.ofSeconds(30));
        config.set(CheckpointingOptions.CHECKPOINTING_MODE,
            CheckpointingMode.EXACTLY_ONCE);
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE,
            "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
            getEnv("CHECKPOINT_DIR", "s3a://flink-checkpoints/order-cdc"));
        config.set(CheckpointingOptions.MAX_RETAINED_CHECKPOINTS, 5);
        config.set(CheckpointingOptions.TOLERABLE_FAILURE_NUMBER, 3);

        // State Backend
        config.setString("state.backend", "rocksdb");
        config.setBoolean("state.backend.incremental", true);

        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment(config);

        // 重啟策略：指數退避，最多 10 次
        env.setRestartStrategy(
            RestartStrategies.exponentialDelayRestart(
                Time.seconds(1),
                Time.minutes(5),
                2.0,
                Time.minutes(10),
                0.1
            )
        );

        return env;
    }

    // --- CDC 事件解析 ---
    static class CdcEventParser extends ProcessFunction<String, OrderEvent> {
        @Override
        public void processElement(String json, Context ctx, Collector<OrderEvent> out) {
            try {
                OrderEvent event = JsonUtil.parseCdcEvent(json);
                event.setProcessedAt(Instant.now());
                out.collect(event);
            } catch (Exception e) {
                // 解析失敗直接進 DLQ
                OrderEvent failEvent = OrderEvent.builder()
                    .operationType("PARSE_ERROR")
                    .capturedAt(Instant.now())
                    .processedAt(Instant.now())
                    .build();
                ctx.output(DLQ_TAG, failEvent);
            }
        }
    }

    // --- 業務處理器（含 DLQ 分流）---
    static class OrderProcessor extends ProcessFunction<OrderEvent, OrderEvent> {
        @Override
        public void processElement(OrderEvent event, Context ctx, Collector<OrderEvent> out) {
            try {
                // 業務驗證：金額不可為負數
                if (event.getAmount() != null &&
                    event.getAmount().compareTo(java.math.BigDecimal.ZERO) < 0) {
                    throw new IllegalArgumentException(
                        "Negative amount detected: " + event.getAmount());
                }
                // 正常流程
                out.collect(event);
            } catch (Exception e) {
                // 業務錯誤 -> DLQ
                ctx.output(DLQ_TAG, event);
            }
        }
    }

    // --- 工具方法 ---
    private static AuditLog toAuditLog(OrderEvent event, String status) {
        return AuditLog.builder()
            .sourceTable(event.getSourceTable())
            .operation(event.getOperationType() == null ? "?" :
                event.getOperationType().substring(0, 1))
            .binlogFile(event.getBinlogFile())
            .binlogPos(event.getBinlogPosition())
            .gtid(event.getGtid())
            .capturedAt(event.getCapturedAt())
            .processedAt(Instant.now())
            .status(status)
            .payloadAfter(JsonUtil.toJson(event))
            .jobId(System.getenv("JOB_ID"))
            .build();
    }

    private static Properties buildDebeziumProperties() {
        Properties props = new Properties();
        props.setProperty("decimal.handling.mode", "string");
        props.setProperty("snapshot.mode", "initial");
        props.setProperty("database.history.kafka.bootstrap.servers",
            getEnv("KAFKA_BROKERS", "flink-kafka-kafka-brokers.kafka:9092"));
        props.setProperty("database.history.kafka.topic", "schema-changes.inventory");
        return props;
    }

    private static String getEnv(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isEmpty()) ? val : defaultValue;
    }
}
