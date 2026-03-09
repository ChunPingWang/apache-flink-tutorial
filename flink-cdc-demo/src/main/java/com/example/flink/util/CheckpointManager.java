package com.example.flink.util;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Checkpoint 設定管理器
 * 根據環境（生產/開發）提供不同的 Checkpoint 配置
 */
public class CheckpointManager {

    /**
     * 生產環境 Checkpoint 設定
     * 適用於金融場景（高可靠性需求）
     */
    public static void configureProduction(StreamExecutionEnvironment env,
                                            String s3BasePath) {
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig()
            // Checkpoint 逾時：10 分鐘
            .setCheckpointTimeout(10 * 60 * 1000)
            // 兩次 Checkpoint 最小間隔：5 秒（避免連續失敗時過度觸發）
            .setMinPauseBetweenCheckpoints(5_000)
            // 最多同時進行 1 個 Checkpoint
            .setMaxConcurrentCheckpoints(1)
            // 允許連續失敗 3 次
            .setTolerableCheckpointFailureNumber(3)
            // Job 取消時保留 Checkpoint（方便恢復）
            .setExternalizedCheckpointCleanup(
                org.apache.flink.streaming.api.environment.CheckpointConfig
                    .ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 存儲目錄
        env.getCheckpointConfig()
            .setCheckpointStorage(s3BasePath + "/checkpoints");
    }

    /**
     * 開發/測試環境設定（較寬鬆）
     */
    public static void configureDevelopment(StreamExecutionEnvironment env) {
        env.enableCheckpointing(60_000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("/tmp/flink-checkpoints");
    }
}
