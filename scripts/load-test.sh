#!/bin/bash
# load-test.sh - Flink CDC 效能壓力測試

echo "=== Flink CDC 壓力測試 ==="
ROWS=${1:-10000}
START_TIME=$(date +%s%3N)

echo "插入 $ROWS 筆測試資料..."

# 批次插入
kubectl exec -it -n flink-lab \
  $(kubectl get pod -l app=mysql -n flink-lab -o name) -- \
  mysql -u root -prootpass inventory <<EOF
SET autocommit=0;
$(for i in $(seq 1 $ROWS); do
  echo "INSERT INTO orders (customer_id, amount, status) VALUES ($RANDOM, $RANDOM.00, 'LOAD_TEST');";
done)
COMMIT;
EOF

# 等待處理完成
echo "等待 Flink 處理..."
sleep 30

END_TIME=$(date +%s%3N)
DURATION=$(( ($END_TIME - $START_TIME) / 1000 ))
TPS=$(( $ROWS / $DURATION ))

echo "插入 $ROWS 筆，耗時 ${DURATION}s，TPS: $TPS"

# 查詢 Audit Log 確認處理完成
echo "=== Audit Log 統計 ==="
kubectl exec -n flink-lab \
  $(kubectl get pod -l app=postgres -n flink-lab -o name) -- \
  psql -U audit -c \
  "SELECT status, COUNT(*) FROM cdc_audit_log
   WHERE source_table='orders' GROUP BY 1"
