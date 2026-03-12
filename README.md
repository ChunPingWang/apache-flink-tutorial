# Apache Flink CDC 完整教學

> 從零開始 &rarr; Kind/K8s 部署 &rarr; 可靠度設計 &rarr; 持久化存儲 &rarr; AWS Outposts PoC

**Flink 版本**：1.19.x | **語言**：Java 17+ | **難度**：初學者 &rarr; 進階
**預計時程**：8 週（每週 4～6 小時）

---

## 目錄

- [課程總覽](#課程總覽)
- [技術棧](#技術棧)
- [可靠度設計總覽（Reliability by Design）](#可靠度設計總覽reliability-by-design)
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
| 訊息佇列 | Apache Kafka | Dead Letter Queue (DLQ) |
| 存儲 | MinIO (S3 相容) | Checkpoint / Savepoint |
| 資料庫 | MySQL 8.0 | 來源 DB |
| 監控 | Prometheus + Grafana | 指標可觀測 |
| 語言 | Java 17 + Maven | Job 開發 |

### Kafka 在本 PoC 的角色

在本架構中，Kafka 的角色是 **Dead Letter Queue (DLQ)**，用於暫存處理失敗的事件。

```
MySQL CDC Source → 解析 → 業務驗證 ─┬─ 正常 → AuditLogSink (PostgreSQL)
                                     │
                                     └─ 異常 → DLQ (Kafka topic: flink.orders.dlq)
```

**觸發 DLQ 的情況：**
1. **JSON 解析失敗** — `CdcEventParser` 無法解析 Debezium 事件
2. **業務驗證失敗** — 例如金額為負數

**DLQ 的價值：**
- 失敗事件不會遺失，可供人工審查或自動補償
- 搭配告警規則 `FlinkDlqHasMessages`，DLQ 有積壓時即時通知

**重要區別：** Kafka 在此架構中 **不是** CDC 的傳輸層。CDC 事件由 Flink CDC Connector 直接從 MySQL Binlog 讀取，不經過 Kafka。這與常見的 `Debezium → Kafka → Flink` 架構不同。

**簡化選項：** PoC 初期可先用 `print()` 或寫入檔案替代 Kafka DLQ，待環境穩定後再接上。`DlqSink.java` 中已提供 `System.out.println` 作為 fallback。

---

## 可靠度設計總覽（Reliability by Design）

本專案從架構層面實現端到端資料可靠性，以下為四大核心機制及其優先級：

| 優先級 | 機制 | 目的 | 實作位置 |
|--------|------|------|----------|
| **P0** | 啟用 Checkpointing | 故障恢復、資料不遺失 | `CheckpointManager.java` |
| **P0** | 冪等 Sink (ON CONFLICT) | 防止重複資料 | `AuditLogSink.java` |
| **P1** | DLQ 機制 | 失敗資料可追蹤重處理 | `DlqSink.java` + `OrderCdcJob.java` |
| **P1** | 指數退避重啟策略 | 持續故障時更具韌性 | `OrderCdcJob.java` |

### P0 — 啟用 Checkpointing

Checkpoint 是 Flink 故障恢復的基石。當 Job 異常崩潰時，Flink 會從最近一次成功的 Checkpoint 恢復所有 Operator 狀態與 Source offset（Binlog position），確保資料不遺失。

**實作方式**（`CheckpointManager.configureProduction()`）：

```java
// 每 30 秒觸發一次 Checkpoint，使用 EXACTLY_ONCE 語義
env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);

env.getCheckpointConfig()
    .setCheckpointTimeout(10 * 60 * 1000)          // 逾時 10 分鐘
    .setMinPauseBetweenCheckpoints(5_000)           // 兩次 Checkpoint 最小間隔 5 秒
    .setMaxConcurrentCheckpoints(1)                 // 同時最多 1 個 Checkpoint
    .setTolerableCheckpointFailureNumber(3)         // 允許連續失敗 3 次
    .setExternalizedCheckpointCleanup(              // Job 取消時保留 Checkpoint
        RETAIN_ON_CANCELLATION);

// 搭配 RocksDB 增量 Checkpoint + S3/MinIO 持久化存儲
env.getCheckpointConfig().setCheckpointStorage(s3BasePath + "/checkpoints");
```

**故障恢復流程**：

```
Job 崩潰 → Flink 偵測到 TaskManager 失聯
         → 從最近成功的 Checkpoint 讀取 State 快照
         → 將 CDC Source offset 回溯到 Checkpoint 記錄的 Binlog position
         → 重新部署 Task 並從該 offset 重播事件
         → 搭配冪等 Sink → 重播不會產生重複資料
```

### P0 — 冪等 Sink（ON CONFLICT）

Checkpoint 恢復時會重播部分已處理過的事件。若 Sink 不具備冪等性，將導致資料重複。本專案透過 PostgreSQL 的 `ON CONFLICT` 語法，以 `(binlog_file, binlog_pos)` 為唯一鍵，保證同一筆 CDC 事件無論寫入幾次，結果都相同。

**實作方式**（`AuditLogSink.java`）：

```sql
INSERT INTO cdc_audit_log
  (source_table, operation, binlog_file, binlog_pos, gtid,
   captured_at, processed_at, status, payload_after, job_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
ON CONFLICT (binlog_file, binlog_pos)
DO UPDATE SET
  processed_at = EXCLUDED.processed_at,
  status       = EXCLUDED.status
```

**為什麼用 `(binlog_file, binlog_pos)` 做唯一鍵**：每筆 CDC 事件在 MySQL Binlog 中有唯一的 `(file, position)` 組合，這是天然的冪等鍵。即使 Flink 重播相同事件，SQL 只會更新 `processed_at` 和 `status`，不會產生新的重複行。

### P1 — DLQ（Dead Letter Queue）機制

無法正確處理的事件不應被丟棄或阻塞主流程。本專案透過 Flink Side Output 將失敗事件分流至 Kafka DLQ topic，確保這些事件可被追蹤、稽核、並在修復後重新處理。

**觸發 DLQ 的場景**：

| 場景 | 觸發位置 | 範例 |
|------|----------|------|
| JSON 解析失敗 | `CdcEventParser` | Debezium 事件格式異常 |
| 業務驗證失敗 | `OrderProcessor` | 訂單金額為負數 |

**實作方式**（`OrderCdcJob.java` + `DlqSink.java`）：

```java
// 1. 定義 Side Output Tag
static final OutputTag<OrderEvent> DLQ_TAG = new OutputTag<OrderEvent>("dlq") {};

// 2. 在 ProcessFunction 中將異常事件導向 DLQ
if (amount < 0) {
    ctx.output(DLQ_TAG, event);  // 分流至 DLQ
} else {
    out.collect(event);          // 正常流程
}

// 3. 收集 Side Output 並寫入 Kafka DLQ Topic
processedStream.getSideOutput(DLQ_TAG)
    .map(event -> toAuditLog(event, "FAILED"))
    .sinkTo(DlqSink.build(kafkaBrokers));  // 寫入 Kafka topic: flink.orders.dlq
```

**DLQ Sink 同樣使用 `EXACTLY_ONCE` 事務保證**，搭配 Kafka 事務（`transactional.id.prefix = dlq-sink`），確保 DLQ 事件也不會在 Checkpoint 恢復時重複寫入。

**後續處理**：DLQ 中的事件可透過告警規則 `FlinkDlqHasMessages` 觸發通知，由人工審查或自動補償程式重新處理。

### P1 — 指數退避重啟策略

當 Job 因暫時性故障（如網路抖動、資料庫連線中斷）而失敗時，Flink 會自動重啟。指數退避策略避免了固定間隔重啟在持續故障場景下的「重啟風暴」，讓系統有時間恢復。

**實作方式**（`OrderCdcJob.buildEnvironment()`）：

```java
env.setRestartStrategy(
    RestartStrategies.exponentialDelayRestart(
        Time.seconds(1),     // 初始延遲：1 秒
        Time.minutes(5),     // 最大延遲：5 分鐘
        2.0,                 // 退避倍數：每次加倍
        Time.minutes(10),    // 重置窗口：穩定運行 10 分鐘後重置計數
        0.1                  // 抖動因子：±10% 隨機偏移，避免多 Job 同時重啟
    )
);
```

**重啟行為示意**：

```
第 1 次失敗 → 等待 ~1 秒後重啟
第 2 次失敗 → 等待 ~2 秒後重啟
第 3 次失敗 → 等待 ~4 秒後重啟
第 4 次失敗 → 等待 ~8 秒後重啟
  ...
第 N 次失敗 → 等待最多 5 分鐘後重啟
（穩定運行超過 10 分鐘後，延遲計數重置為 1 秒）
```

**與固定間隔重啟的對比**：

| 策略 | 暫時性故障 | 持續性故障 | 資源消耗 |
|------|-----------|-----------|---------|
| 固定間隔 (`fixedDelayRestart`) | 快速恢復 | 持續消耗資源反覆重啟 | 高 |
| **指數退避** (`exponentialDelayRestart`) | 快速恢復（初始 1s） | 逐步拉長間隔，減少無效重啟 | **低** |

### 四大機制協同運作

```
MySQL Binlog 事件
       │
       ▼
  CDC Source（記錄 offset）
       │
       ▼
  ┌─ Checkpoint ──────────────────────────────────────────────┐
  │  每 30 秒快照：Source offset + 所有 Operator State        │
  │  故障時從快照恢復 → 保證 P0：資料不遺失                     │
  └───────────────────────────────────────────────────────────┘
       │
       ▼
  解析 & 業務處理
       │
  ┌────┴────┐
  │ 正常     │ 異常
  ▼         ▼
Audit Log  DLQ (Kafka)
ON CONFLICT  EXACTLY_ONCE 事務
  │           │
  │           └─→ P1：失敗事件可追蹤重處理
  └─────────────→ P0：冪等寫入，重播不重複

整個 Job 由指數退避重啟策略保護 → P1：持續故障時更具韌性
```

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
