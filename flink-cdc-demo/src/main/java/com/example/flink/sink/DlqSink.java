package com.example.flink.sink;

import com.example.flink.model.AuditLog;
import com.example.flink.util.JsonUtil;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * Dead Letter Queue Sink 工廠 - 建立寫入 Kafka DLQ Topic 的 KafkaSink
 * 使用 EXACTLY_ONCE 事務保證，搭配 Flink Checkpoint 達到端到端嚴格一次語義
 *
 * 使用方式：
 *   stream.sinkTo(DlqSink.build("broker:9092"));
 */
public class DlqSink {

    public static final String DLQ_TOPIC = "flink.orders.dlq";
    private static final String TXN_PREFIX = "dlq-sink";

    /**
     * 建立 KafkaSink 實例，真正寫入 Kafka DLQ Topic
     * 使用 Flink 新版 Sink API (sinkTo) 而非舊版 addSink
     *
     * EXACTLY_ONCE 需求：
     * - Flink Checkpoint 已啟用（CheckpointManager）
     * - Kafka broker 端 transaction.max.timeout.ms >= 此處 transaction.timeout.ms
     * - 下游消費者需設定 isolation.level=read_committed
     */
    public static KafkaSink<AuditLog> build(String bootstrapServers) {
        Properties kafkaProps = new Properties();
        // 事務逾時須 > checkpoint 間隔(30s) 且 <= broker transaction.max.timeout.ms(預設 900s)
        kafkaProps.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "600000");

        return KafkaSink.<AuditLog>builder()
            .setBootstrapServers(bootstrapServers)
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix(TXN_PREFIX)
            .setKafkaProducerConfig(kafkaProps)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(DLQ_TOPIC)
                    .setValueSerializationSchema(
                        new org.apache.flink.api.common.serialization.SerializationSchema<AuditLog>() {
                            @Override
                            public byte[] serialize(AuditLog value) {
                                return JsonUtil.toJson(value).getBytes();
                            }
                        })
                    .build()
            )
            .build();
    }
}
