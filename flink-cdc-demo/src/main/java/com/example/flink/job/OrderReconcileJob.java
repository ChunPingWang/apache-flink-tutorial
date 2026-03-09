package com.example.flink.job;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * 定期對帳 Job：比對 Audit Log 處理筆數 vs MySQL 實際變更筆數
 * 建議每小時觸發一次（使用外部排程器 or Flink Cron）
 */
public class OrderReconcileJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        // 對帳時間窗口：最近 1 小時
        Instant windowEnd   = Instant.now().truncatedTo(ChronoUnit.HOURS);
        Instant windowStart = windowEnd.minus(1, ChronoUnit.HOURS);

        DataStream<ReconcileResult> results = env
            .addSource(new ReconcileSource(windowStart, windowEnd))
            .name("Reconcile Source");

        results
            .filter(r -> r.getDiff() != 0)
            .print()  // 有差異時輸出告警（可對接 PagerDuty / Slack）
            .name("Reconcile Alert Sink");

        env.execute("Order Reconcile Job");
    }

    static class ReconcileResult {
        private Instant windowStart;
        private Instant windowEnd;
        private long auditLogCount;
        private long sourceDbCount;
        private long diff;

        public long getDiff() { return diff; }
        public void setDiff(long diff) { this.diff = diff; }

        @Override
        public String toString() {
            return String.format(
                "[RECONCILE] window=%s~%s auditLog=%d source=%d diff=%d %s",
                windowStart, windowEnd, auditLogCount, sourceDbCount, diff,
                diff == 0 ? "OK" : "MISMATCH");
        }
    }

    static class ReconcileSource extends RichSourceFunction<ReconcileResult> {
        private final Instant windowStart;
        private final Instant windowEnd;

        ReconcileSource(Instant windowStart, Instant windowEnd) {
            this.windowStart = windowStart;
            this.windowEnd   = windowEnd;
        }

        @Override
        public void run(SourceContext<ReconcileResult> ctx) throws Exception {
            long auditCount  = queryAuditLogCount();
            long sourceCount = querySourceDbCount();

            ReconcileResult result = new ReconcileResult();
            result.windowStart   = windowStart;
            result.windowEnd     = windowEnd;
            result.auditLogCount = auditCount;
            result.sourceDbCount = sourceCount;
            result.setDiff(sourceCount - auditCount);

            ctx.collect(result);
        }

        private long queryAuditLogCount() throws SQLException {
            String url = System.getenv().getOrDefault(
                "AUDIT_DB_URL", "jdbc:postgresql://postgres.flink-lab:5432/auditdb");
            try (Connection conn = DriverManager.getConnection(url, "audit", "auditpass");
                 PreparedStatement stmt = conn.prepareStatement(
                     "SELECT COUNT(*) FROM cdc_audit_log " +
                     "WHERE source_table = 'orders' " +
                     "AND captured_at BETWEEN ? AND ? " +
                     "AND status = 'PROCESSED'")) {
                stmt.setTimestamp(1, Timestamp.from(windowStart));
                stmt.setTimestamp(2, Timestamp.from(windowEnd));
                ResultSet rs = stmt.executeQuery();
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }

        private long querySourceDbCount() throws SQLException {
            String url = System.getenv().getOrDefault(
                "MYSQL_URL", "jdbc:mysql://mysql.flink-lab:3306/inventory");
            try (Connection conn = DriverManager.getConnection(url, "flink", "flinkpass");
                 PreparedStatement stmt = conn.prepareStatement(
                     "SELECT COUNT(*) FROM orders " +
                     "WHERE updated_at BETWEEN ? AND ?")) {
                stmt.setTimestamp(1, Timestamp.from(windowStart));
                stmt.setTimestamp(2, Timestamp.from(windowEnd));
                ResultSet rs = stmt.executeQuery();
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }

        @Override
        public void cancel() {}
    }
}
