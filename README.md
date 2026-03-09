# Apache Flink CDC 完整教學

> 從零開始 &rarr; Kind/K8s 部署 &rarr; 可靠度設計 &rarr; 持久化存儲 &rarr; AWS Outposts PoC

**Flink 版本**：1.19.x | **語言**：Java 17+ | **難度**：初學者 &rarr; 進階
**預計時程**：8 週（每週 4～6 小時）

---

## 目錄

- [課程總覽](#課程總覽)
- [技術棧](#技術棧)
- [專案結構](#專案結構)
- [Module 1：Flink 基礎概念](#module-1flink-基礎概念)
- [Module 2：本地環境建置 (Kind)](#module-2本地環境建置-kind)
- [Module 3：第一個 Flink Job（Java）](#module-3第一個-flink-jobjava)
- [Module 4：CDC 整合與可靠度設計](#module-4cdc-整合與可靠度設計)
- [Module 5：可觀測性（Observability）](#module-5可觀測性observability)
- [Module 6：持久化存儲設計（MinIO/S3）](#module-6持久化存儲設計minios3)
- [Module 7：AWS Outposts PoC](#module-7aws-outposts-poc)
- [Module 8：生產就緒 Checklist](#module-8生產就緒-checklist)
- [測試方法](#測試方法)
- [常用指令參考](#常用指令參考)

---

## 課程總覽

### 學習路徑

```
Week 1-2         Week 3-4              Week 5-6               Week 7-8
────────────     ──────────────────    ────────────────────   ──────────────────
Flink 概念   →   Kind 環境 + 第一個  →  CDC 可靠度 +       →  AWS Outposts PoC
基礎知識          Java Job               可觀測性設計           + 生產 Checklist
                                         持久化存儲
```

---

## 技術棧

| 層次 | 技術 | 用途 |
|------|------|------|
| 容器平台 | Kind &rarr; AWS Outposts EKS | K8s 執行環境 |
| Flink 管理 | Flink Kubernetes Operator | Job 生命週期管理 |
| CDC 來源 | Flink CDC (Debezium) | MySQL/PostgreSQL 變更擷取 |
| 訊息佇列 | Apache Kafka | 事件緩衝與 DLQ |
| 存儲 | MinIO (S3 相容) | Checkpoint / Savepoint |
| 資料庫 | MySQL 8.0 | 來源 DB |
| 監控 | Prometheus + Grafana | 指標可觀測 |
| 語言 | Java 17 + Maven | Job 開發 |

---

## 專案結構

```
apache-flink-tutorial/
├── README.md                           # 本文件
├── kind-flink-cluster.yaml             # Kind 叢集設定
├── .gitignore
│
├── flink-cdc-demo/                     # Maven Java 專案
│   ├── pom.xml                         # Maven 設定 (Flink 1.19 + CDC 3.1)
│   └── src/main/java/com/example/flink/
│       ├── job/
│       │   ├── OrderCdcJob.java        # 主 CDC Job（含 DLQ 分流）
│       │   └── OrderReconcileJob.java  # 對帳 Job
│       ├── model/
│       │   ├── OrderEvent.java         # CDC 事件模型
│       │   └── AuditLog.java           # 稽核日誌模型
│       ├── sink/
│       │   ├── AuditLogSink.java       # JDBC 冪等寫入 Sink
│       │   └── DlqSink.java           # Kafka DLQ Sink
│       └── util/
│           ├── JsonUtil.java           # Debezium JSON 解析
│           └── CheckpointManager.java  # Checkpoint 設定管理
│
├── k8s/                                # Kubernetes 部署檔
│   ├── mysql-deployment.yaml           # MySQL 8.0 (Binlog + GTID)
│   ├── flink-cdc-job.yaml              # FlinkDeployment CR (Kind 環境)
│   ├── flink-cdc-job-outposts.yaml     # FlinkDeployment CR (AWS Outposts)
│   ├── flink-service-monitor.yaml      # Prometheus ServiceMonitor
│   ├── flink-alerts.yaml               # PrometheusRule 告警規則
│   ├── flink-network-policy.yaml       # NetworkPolicy
│   └── minio-production.yaml           # MinIO StatefulSet
│
└── scripts/                            # 操作腳本
    ├── init-mysql.sql                  # MySQL 初始化 SQL
    ├── savepoint-ops.sh                # Savepoint 操作腳本
    ├── replay-from-offset.sh           # CDC 補償重播腳本
    └── load-test.sh                    # 效能壓力測試腳本
```

---

## Module 1：Flink 基礎概念

### 核心架構

```
┌────────────────────────────────────────────────────┐
│               Flink Cluster                         │
│                                                     │
│  ┌──────────────────┐    ┌───────────────────────┐ │
│  │   JobManager     │    │   TaskManager(s)      │ │
│  │                  │    │                       │ │
│  │  - 任務排程       │    │  - 實際執行 Task       │ │
│  │  - Checkpoint 協調│    │  - State 本地存放      │ │
│  │  - 故障恢復       │    │  - 資料處理            │ │
│  └──────────────────┘    └───────────────────────┘ │
└────────────────────────────────────────────────────┘
```

### 關鍵概念

| 概念 | 說明 | 類比 |
|------|------|------|
| **Stream** | 無界資料流 | 永不停止的水流 |
| **Operator** | 資料轉換算子 | 工廠生產線上的一道工序 |
| **State** | 算子的運算狀態 | 工人的工作台（記憶當前進度）|
| **Checkpoint** | 定期狀態快照 | 遊戲存檔 |
| **Savepoint** | 手動狀態快照 | 計劃性備份（升版用）|
| **Watermark** | 時間邊界標記 | 等待遲到同學的截止時間 |
| **Window** | 時間/數量切割 | 把無限水流切成一杯一杯 |
| **Backpressure** | 下游消費不及上游 | 工廠產能瓶頸 |

### 時間語義

```
Event Time    ── 事件實際發生時間（最準確，需處理亂序）
Ingestion Time── 進入 Flink 的時間（折衷方案）
Processing Time── Flink 處理時的系統時間（最簡單，不準確）

銀行/金融場景：一律使用 Event Time + Watermark
```

### Exactly-Once 語義原理

```
Checkpoint N 完成
    ↓
所有 Operator State 快照到 S3/MinIO
    ↓
Source offset 也寫入快照（Binlog position）
    ↓
若 Job 崩潰 → 從 Checkpoint N 的 offset 重播
    ↓
下游 Sink 若支援冪等寫入 → 達到 Exactly-Once
```

---

## Module 2：本地環境建置 (Kind)

### 前置需求

```bash
Docker          >= 24.0
Kind            >= 0.22
kubectl         >= 1.28
Helm            >= 3.14
Java            >= 17
Maven           >= 3.9
```

### Step 1：安裝工具

```bash
# macOS (Homebrew)
brew install kind kubectl helm

# 驗證
kind version && kubectl version --client && helm version
```

### Step 2：建立 Kind 叢集

```bash
mkdir -p /tmp/flink-data
kind create cluster --config kind-flink-cluster.yaml

# 驗證：3 個節點都應為 Ready
kubectl get nodes
```

### Step 3：安裝 Flink Kubernetes Operator

```bash
# 安裝 cert-manager（Operator 依賴）
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.4/cert-manager.yaml
kubectl wait --for=condition=Ready pod -l app=cert-manager -n cert-manager --timeout=120s

# 安裝 Flink Operator
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.8.0/
helm repo update

helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-system \
  --create-namespace \
  --set webhook.create=true

# 驗證
kubectl get pods -n flink-system
```

### Step 4：安裝 MinIO（本地 S3）

```bash
kubectl create namespace minio
helm repo add minio https://charts.min.io/
helm install minio minio/minio \
  --namespace minio \
  --set rootUser=minioadmin \
  --set rootPassword=minioadmin123 \
  --set mode=standalone \
  --set resources.requests.memory=512Mi \
  --set persistence.size=10Gi

kubectl wait --for=condition=Ready pod -l app=minio -n minio --timeout=120s

# Port-forward
kubectl port-forward svc/minio 9000:9000 9001:9001 -n minio &

# 建立 Bucket
brew install minio/stable/mc
mc alias set local http://localhost:9000 minioadmin minioadmin123
mc mb local/flink-checkpoints
mc mb local/flink-savepoints
```

### Step 5：安裝 MySQL

```bash
kubectl create namespace flink-lab
kubectl apply -f k8s/mysql-deployment.yaml
kubectl wait --for=condition=Ready pod -l app=mysql -n flink-lab --timeout=60s

# 初始化資料表
kubectl exec -it $(kubectl get pod -l app=mysql -n flink-lab -o name) -n flink-lab -- \
  mysql -u root -prootpass inventory < scripts/init-mysql.sql
```

### Step 6：安裝 Kafka（Strimzi）

```bash
kubectl create namespace kafka
kubectl apply -f https://strimzi.io/install/latest?namespace=kafka -n kafka

kubectl apply -f - <<EOF
apiVersion: kafka.strimzi.io/v1beta2
kind: Kafka
metadata:
  name: flink-kafka
  namespace: kafka
spec:
  kafka:
    replicas: 1
    listeners:
      - name: plain
        port: 9092
        type: internal
        tls: false
    storage:
      type: ephemeral
  zookeeper:
    replicas: 1
    storage:
      type: ephemeral
  entityOperator:
    topicOperator: {}
EOF

kubectl wait kafka/flink-kafka --for=condition=Ready --timeout=300s -n kafka
```

### Step 7：安裝 Prometheus + Grafana

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace \
  --set grafana.adminPassword=admin123

# Grafana: http://localhost:3000 (admin / admin123)
kubectl port-forward svc/monitoring-grafana 3000:80 -n monitoring &
```

### 環境驗證

```bash
# 所有 Pod 都應為 Running
kubectl get pods -n flink-system
kubectl get pods -n minio
kubectl get pods -n flink-lab
kubectl get pods -n kafka
kubectl get pods -n monitoring
```

---

## Module 3：第一個 Flink Job（Java）

### 建置與部署

```bash
# 建置 JAR
cd flink-cdc-demo
mvn clean package -DskipTests

# 載入 image 到 Kind（如使用自定義 image）
# kind load docker-image flink-cdc-demo:1.0.0 --name flink-lab

# 部署 FlinkDeployment
kubectl apply -f k8s/flink-cdc-job.yaml

# 觀察狀態
kubectl get flinkdeployment -n flink-lab
kubectl describe flinkdeployment order-cdc-job -n flink-lab
```

### 驗證 Job

```bash
# 確認 Job 運行中
kubectl get pods -n flink-lab | grep order-cdc

# 觸發 CDC 事件
kubectl exec -it $(kubectl get pod -l app=mysql -n flink-lab -o name) -n flink-lab -- \
  mysql -u root -prootpass inventory -e \
  "INSERT INTO orders (customer_id, amount, status) VALUES (9999, 88888.88, 'NEW');"

# 查看 TaskManager 日誌
kubectl logs -l component=taskmanager -n flink-lab --tail=50
```

### Java 程式碼說明

| 檔案 | 用途 |
|------|------|
| `OrderCdcJob.java` | 主 Job：CDC Source &rarr; 解析 &rarr; 業務處理 &rarr; Audit Log + DLQ |
| `OrderReconcileJob.java` | 對帳 Job：比對 Audit Log vs MySQL 實際變更筆數 |
| `OrderEvent.java` | CDC 事件資料模型（含 before/after 狀態） |
| `AuditLog.java` | 稽核日誌模型 |
| `AuditLogSink.java` | JDBC 冪等寫入（ON CONFLICT） |
| `DlqSink.java` | Kafka Dead Letter Queue Sink |
| `JsonUtil.java` | Debezium JSON 事件解析器 |
| `CheckpointManager.java` | 生產/開發環境 Checkpoint 設定 |

---

## Module 4：CDC 整合與可靠度設計

### Binlog 設定驗證

```sql
SHOW VARIABLES LIKE 'log_bin%';
SHOW VARIABLES LIKE 'binlog_format';        -- 應為 ROW
SHOW VARIABLES LIKE 'gtid_mode';            -- 應為 ON
SET GLOBAL binlog_expire_logs_seconds = 604800;  -- 保留 7 天
```

### Startup Options 選擇

| 模式 | 用途 |
|------|------|
| `initial()` | 初次部署：全量快照 + 增量 |
| `latest()` | 只追最新變更（不補歷史） |
| `specificOffset(file, pos)` | 從指定 Binlog 位置恢復 |
| `timestamp(ms)` | 從指定時間戳恢復 |

### 補償操作

```bash
# 從指定 Binlog 位置重播
./scripts/replay-from-offset.sh <JOB_ID> mysql-bin.000003 154

# 執行對帳 Job
# （需在 Flink 環境中提交 OrderReconcileJob）
```

---

## Module 5：可觀測性（Observability）

### 部署監控元件

```bash
kubectl apply -f k8s/flink-service-monitor.yaml
kubectl apply -f k8s/flink-alerts.yaml
```

### 關鍵告警規則

| 告警 | 觸發條件 | 嚴重程度 |
|------|----------|----------|
| FlinkCdcLagHigh | CDC Lag > 60 秒（持續 2 分鐘） | Warning |
| FlinkCheckpointFailed | 5 分鐘內 > 3 次 Checkpoint 失敗 | Critical |
| FlinkNoRecordsReceived | 5 分鐘內無任何資料接收 | Critical |
| FlinkDlqHasMessages | DLQ 有未處理訊息 | Warning |
| FlinkBackpressureHigh | 反壓 > 50%（持續 3 分鐘） | Warning |

### Grafana Dashboard

```bash
# 匯入 Flink 官方 Dashboard (ID: 14370)
kubectl port-forward svc/monitoring-grafana 3000:80 -n monitoring &
# http://localhost:3000 → Dashboards → Import → ID: 14370
```

### 重要監控指標

| 指標 | 說明 |
|------|------|
| numRecordsIn | CDC 接收事件速率 (events/sec) |
| currentFetchLag | Binlog 消費落後時間 (ms) |
| checkpointDuration | Checkpoint 耗時 (ms) |
| checkpointSize | Checkpoint 大小 (bytes) |
| numberOfRestarts | Job 重啟次數 |
| isBackPressured | 反壓狀態 (0/1) |

---

## Module 6：持久化存儲設計（MinIO/S3）

### 存儲分層架構

```
┌──────────────┬───────────────────┬───────────────────────┐
│ Layer 1      │ Layer 2           │ Layer 3               │
│ RocksDB PVC  │ MinIO (S3)        │ 備份存儲              │
│ (本地暫存)    │ (Checkpoint主存)   │ (長期保留)            │
│              │                   │                       │
│ • 高速讀寫    │ • 持久化          │ • 跨 AZ 複製          │
│ • 重啟即失效  │ • Checkpoint      │ • 90天以上保留         │
│ • 4Gi per TM │ • Savepoint       │ • 合規稽核用           │
└──────────────┴───────────────────┴───────────────────────┘
```

### Savepoint 操作

```bash
./scripts/savepoint-ops.sh create              # 建立 Savepoint
./scripts/savepoint-ops.sh restore <path>      # 從 Savepoint 恢復
./scripts/savepoint-ops.sh list                # 列出所有 Savepoint
```

### 存儲容量規劃

```
Checkpoint 大小估算：
  State 大小 x 複製因子 x 保留數量
  例：100MB State x 1.5 x 5 個 = 750 MB

建議 MinIO 最小容量：生產環境 >= 100 GB
```

---

## Module 7：AWS Outposts PoC

### Kind vs Outposts 差異

| 面向 | Kind (本地) | AWS Outposts |
|------|------------|--------------|
| K8s | Kind | EKS on Outposts |
| 存儲 | MinIO | S3 on Outposts |
| Kafka | Strimzi | Amazon MSK |
| 監控 | Prometheus | CloudWatch + Managed Grafana |
| 資料庫 | MySQL Pod | RDS on Outposts |
| IAM | 無 | AWS IAM + IRSA |

### PoC 執行計劃（4 週）

- **Week 1**：EKS/S3/MSK/RDS 基礎建設
- **Week 2**：Flink Job 移植 + IRSA 設定
- **Week 3**：故障注入 + 效能壓力測試
- **Week 4**：可觀測性 + PoC 報告

### PoC 驗收標準

- CDC Job 正常啟動，增量 Lag < 5 秒
- Checkpoint 寫入 S3 on Outposts 成功
- Kill Pod 後自動恢復，無資料遺漏
- 吞吐量 >= 10,000 events/sec，P99 < 500ms

---

## Module 8：生產就緒 Checklist

### 安全性

```bash
# 使用 K8s Secret 管理憑證
kubectl create secret generic mysql-credentials \
  --from-literal=username=flink \
  --from-literal=password='secure-password-here' \
  -n flink-lab

# 部署 NetworkPolicy
kubectl apply -f k8s/flink-network-policy.yaml
```

### 資源建議（生產環境）

| 元件 | Memory | CPU |
|------|--------|-----|
| JobManager | 2048m | 1 |
| TaskManager | 4096m (2GB Heap + 2GB Off-Heap) | 2 |

### 升版程序（Zero-Downtime）

```bash
# 1. 建立 Savepoint
./scripts/savepoint-ops.sh create

# 2. 更新 Image / JAR
kubectl patch flinkdeployment order-cdc-job -n flink-lab \
  --type merge \
  -p '{"spec":{"job":{"upgradeMode":"stateful"}}}'

# 3. 確認升版成功
kubectl get flinkdeployment order-cdc-job -n flink-lab
```

---

## 測試方法

| 測試編號 | 測試項目 | 說明 |
|---------|---------|------|
| T-01 | 基本 CDC 功能 | INSERT/UPDATE/DELETE 事件捕捉 |
| T-02 | Checkpoint 恢復 | Kill Pod 後自動恢復，驗證無資料遺漏 |
| T-03 | DLQ 路由 | 負金額訂單正確路由至 Kafka DLQ |
| T-04 | Savepoint 升版 | 零停機升版流程驗證 |
| T-05 | 效能壓力 | 批量插入 10,000 筆，測量 TPS |
| T-06 | 可觀測性 | Prometheus 指標收集驗證 |

### T-01 範例：基本 CDC 測試

```bash
# INSERT
kubectl exec -it -n flink-lab \
  $(kubectl get pod -l app=mysql -n flink-lab -o name) -- \
  mysql -u root -prootpass inventory -e \
  "INSERT INTO orders (customer_id, amount, status) VALUES (1001, 5000, 'NEW');"

# UPDATE
kubectl exec -it -n flink-lab \
  $(kubectl get pod -l app=mysql -n flink-lab -o name) -- \
  mysql -u root -prootpass inventory -e \
  "UPDATE orders SET status='COMPLETED' WHERE id=LAST_INSERT_ID();"

# 驗證：Audit Log 應出現 2 筆記錄 (I/U)
```

### T-05 範例：效能壓力測試

```bash
./scripts/load-test.sh 10000
```

---

## 常用指令參考

```bash
# Flink Job 狀態
kubectl get flinkdeployment -n flink-lab
kubectl describe flinkdeployment order-cdc-job -n flink-lab
kubectl logs -l component=jobmanager -n flink-lab
kubectl logs -l component=taskmanager -n flink-lab

# Savepoint 操作
./scripts/savepoint-ops.sh create
./scripts/savepoint-ops.sh restore <path>
./scripts/savepoint-ops.sh list

# MinIO
mc ls local/flink-checkpoints/
mc du local/flink-checkpoints/

# Kafka DLQ
kubectl exec -n kafka \
  $(kubectl get pod -n kafka -l strimzi.io/name=flink-kafka-kafka -o name | head -1) -- \
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic flink.orders.dlq --from-beginning --max-messages 5

# Port-Forward
kubectl port-forward svc/order-cdc-job-rest 8081:8081 -n flink-lab   # Flink UI
kubectl port-forward svc/monitoring-grafana 3000:80 -n monitoring     # Grafana
kubectl port-forward svc/minio 9001:9001 -n minio                    # MinIO Console
```

---

*版本：1.0.0 | 適用 Flink 版本：1.19.x*
