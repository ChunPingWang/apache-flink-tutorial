package com.example.flink.sink;

import com.example.flink.model.AuditLog;
import com.example.flink.util.JsonUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Properties;

/**
 * Dead Letter Queue Sink - 將處理失敗的事件寫入 Kafka DLQ Topic
 * 供後續人工審查或自動補償使用
 */
public class DlqSink implements SinkFunction<AuditLog> {

    private final String bootstrapServers;

    public DlqSink(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    /**
     * 建立 KafkaSink 實例
     * 注意：KafkaSink 實作的是新版 Sink 介面，
     * 實際部署時建議直接使用 .sinkTo(buildKafkaSink()) 取代 addSink()
     */
    public KafkaSink<AuditLog> buildKafkaSink() {
        return KafkaSink.<AuditLog>builder()
            .setBootstrapServers(bootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic("flink.orders.dlq")
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

    @Override
    public void invoke(AuditLog value, Context context) throws Exception {
        // 此方法為相容性保留，實際建議使用 buildKafkaSink() + sinkTo()
        // 在教學範例中，可透過 print() 替代觀察 DLQ 輸出
        System.out.println("[DLQ] " + JsonUtil.toJson(value));
    }
}
