package com.example.flink.util;

import com.example.flink.model.OrderEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * JSON 工具類 - 處理 Debezium CDC JSON 事件的解析與序列化
 */
public class JsonUtil {

    private static final ObjectMapper MAPPER = new ObjectMapper()
        .registerModule(new JavaTimeModule());

    /**
     * 解析 Debezium CDC JSON 事件為 OrderEvent
     *
     * Debezium JSON 格式：
     * {
     *   "before": { ... },
     *   "after":  { "id": 1, "customer_id": 1001, ... },
     *   "source": { "file": "mysql-bin.000001", "pos": 154, "gtid": "...", "table": "orders" },
     *   "op": "c"  // c=create, u=update, d=delete, r=read(snapshot)
     * }
     */
    public static OrderEvent parseCdcEvent(String json) throws Exception {
        JsonNode root = MAPPER.readTree(json);

        String op = root.path("op").asText("");
        String operationType = switch (op) {
            case "c", "r" -> "INSERT";
            case "u"      -> "UPDATE";
            case "d"      -> "DELETE";
            default       -> "UNKNOWN";
        };

        JsonNode source = root.path("source");
        JsonNode after  = root.path("after");
        JsonNode before = root.path("before");

        OrderEvent.OrderEventBuilder builder = OrderEvent.builder()
            .operationType(operationType)
            .sourceTable(source.path("table").asText(null))
            .binlogFile(source.path("file").asText(null))
            .binlogPosition(source.path("pos").asLong(0))
            .gtid(source.path("gtid").asText(null))
            .capturedAt(Instant.now());

        // 解析 after 欄位（INSERT / UPDATE）
        if (!after.isMissingNode() && !after.isNull()) {
            builder.id(after.path("id").asLong())
                .customerId(after.path("customer_id").asLong())
                .amount(new BigDecimal(after.path("amount").asText("0")))
                .status(after.path("status").asText(null));

            if (after.has("created_at")) {
                builder.createdAt(parseTimestamp(after.path("created_at")));
            }
            if (after.has("updated_at")) {
                builder.updatedAt(parseTimestamp(after.path("updated_at")));
            }
        }

        // 解析 before 欄位（UPDATE / DELETE）
        if (!before.isMissingNode() && !before.isNull()) {
            builder.beforeStatus(before.path("status").asText(null));
            if (before.has("amount")) {
                builder.beforeAmount(new BigDecimal(before.path("amount").asText("0")));
            }
        }

        return builder.build();
    }

    /**
     * 將物件序列化為 JSON 字串
     */
    public static String toJson(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            return "{}";
        }
    }

    /**
     * 解析 Debezium 時間戳（可能是 epoch millis 或字串）
     */
    private static Instant parseTimestamp(JsonNode node) {
        if (node.isNumber()) {
            return Instant.ofEpochMilli(node.asLong());
        }
        try {
            return Instant.parse(node.asText());
        } catch (Exception e) {
            return Instant.now();
        }
    }
}
