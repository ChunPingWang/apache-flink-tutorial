#!/bin/bash
# savepoint-ops.sh - Savepoint 操作腳本

JOB_NAMESPACE="flink-lab"
JOB_NAME="order-cdc-job"
SAVEPOINT_DIR="s3a://flink-savepoints/manual"

# --- 取得 Job ID ---
get_job_id() {
    kubectl exec -n $JOB_NAMESPACE \
        $(kubectl get pod -n $JOB_NAMESPACE -l component=jobmanager -o name | head -1) -- \
        flink list -r | grep "RUNNING" | awk '{print $4}'
}

# --- 建立 Savepoint ---
create_savepoint() {
    JOB_ID=$(get_job_id)
    echo "Creating savepoint for job: $JOB_ID"

    kubectl exec -n $JOB_NAMESPACE \
        $(kubectl get pod -n $JOB_NAMESPACE -l component=jobmanager -o name | head -1) -- \
        flink savepoint $JOB_ID $SAVEPOINT_DIR

    echo "Savepoint created successfully"
}

# --- 從 Savepoint 恢復 ---
restore_from_savepoint() {
    SAVEPOINT_PATH=$1
    echo "Restoring from savepoint: $SAVEPOINT_PATH"

    kubectl patch flinkdeployment $JOB_NAME -n $JOB_NAMESPACE \
        --type merge \
        -p "{\"spec\":{\"job\":{\"initialSavepointPath\":\"$SAVEPOINT_PATH\",\"state\":\"running\"}}}"
}

# --- 列出所有 Savepoint ---
list_savepoints() {
    echo "=== Available Savepoints ==="
    kubectl exec -n minio \
        $(kubectl get pod -n minio -l app=minio -o name) -- \
        mc ls minio/flink-savepoints/manual/
}

# --- 主選單 ---
case "$1" in
    create)  create_savepoint ;;
    restore) restore_from_savepoint "$2" ;;
    list)    list_savepoints ;;
    *)
        echo "Usage: $0 {create|restore <path>|list}"
        exit 1
esac
