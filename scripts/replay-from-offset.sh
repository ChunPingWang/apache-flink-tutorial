#!/bin/bash
# replay-from-offset.sh
# 用途：從指定 Binlog 位置重播 CDC 事件

FLINK_JOB_ID=$1
BINLOG_FILE=${2:-"mysql-bin.000001"}
BINLOG_POS=${3:-4}

echo "=== CDC 補償重播 ==="
echo "Job ID:      $FLINK_JOB_ID"
echo "Binlog File: $BINLOG_FILE"
echo "Binlog Pos:  $BINLOG_POS"

# 1. 建立 Savepoint
echo "[1/3] 建立 Savepoint..."
SAVEPOINT_PATH=$(kubectl exec -n flink-lab \
  $(kubectl get pod -n flink-lab -l component=jobmanager -o name) -- \
  flink savepoint $FLINK_JOB_ID s3a://flink-savepoints/manual/)

echo "Savepoint: $SAVEPOINT_PATH"

# 2. 停止現有 Job
echo "[2/3] 停止 Job..."
kubectl patch flinkdeployment order-cdc-job -n flink-lab \
  --type merge -p '{"spec":{"job":{"state":"suspended"}}}'

# 3. 以新起點重啟（需修改 startupOptions）
echo "[3/3] 請修改 flink-cdc-job.yaml 中的 startupOptions 後重新 apply"
echo "  startupOptions: specificOffset($BINLOG_FILE, $BINLOG_POS)"
