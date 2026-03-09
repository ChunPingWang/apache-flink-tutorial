package com.example.flink.sink;

import com.example.flink.model.AuditLog;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Timestamp;

/**
 * Audit Log Sink - 使用 JDBC 冪等寫入 PostgreSQL
 * 透過 ON CONFLICT 保證冪等性，避免重複寫入
 */
public class AuditLogSink implements SinkFunction<AuditLog> {

    private final SinkFunction<AuditLog> delegate;

    public AuditLogSink() {
        this.delegate = JdbcSink.sink(
            // 使用 ON CONFLICT 保證冪等性
            """
            INSERT INTO cdc_audit_log
              (source_table, operation, binlog_file, binlog_pos, gtid,
               captured_at, processed_at, status, payload_after, job_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
            ON CONFLICT (binlog_file, binlog_pos)
            DO UPDATE SET
              processed_at = EXCLUDED.processed_at,
              status       = EXCLUDED.status
            """,
            (stmt, log) -> {
                stmt.setString(1, log.getSourceTable());
                stmt.setString(2, log.getOperation());
                stmt.setString(3, log.getBinlogFile());
                stmt.setLong  (4, log.getBinlogPos() != null ? log.getBinlogPos() : 0L);
                stmt.setString(5, log.getGtid());
                stmt.setTimestamp(6, log.getCapturedAt() != null ?
                    Timestamp.from(log.getCapturedAt()) : null);
                stmt.setTimestamp(7, Timestamp.from(log.getProcessedAt()));
                stmt.setString(8, log.getStatus());
                stmt.setString(9, log.getPayloadAfter());
                stmt.setString(10, log.getJobId());
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(500)
                .withBatchIntervalMs(1000)
                .withMaxRetries(3)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(System.getenv().getOrDefault(
                    "AUDIT_DB_URL",
                    "jdbc:postgresql://postgres.flink-lab:5432/auditdb"))
                .withDriverName("org.postgresql.Driver")
                .withUsername(System.getenv().getOrDefault("AUDIT_DB_USER", "audit"))
                .withPassword(System.getenv().getOrDefault("AUDIT_DB_PASS", "auditpass"))
                .build()
        );
    }

    @Override
    public void invoke(AuditLog value, Context context) throws Exception {
        delegate.invoke(value, context);
    }
}
