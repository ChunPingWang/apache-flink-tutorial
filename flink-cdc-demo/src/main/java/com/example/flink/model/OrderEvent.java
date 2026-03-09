package com.example.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderEvent {

    // --- CDC 元資料 ---
    private String  operationType;   // INSERT / UPDATE / DELETE
    private String  sourceTable;
    private String  binlogFile;
    private Long    binlogPosition;
    private String  gtid;
    private Instant capturedAt;

    // --- 業務欄位（after 狀態）---
    private Long       id;
    private Long       customerId;
    private BigDecimal amount;
    private String     status;
    private Instant    createdAt;
    private Instant    updatedAt;

    // --- before 狀態（UPDATE/DELETE 時有值）---
    private String beforeStatus;
    private BigDecimal beforeAmount;

    // --- 處理追蹤 ---
    private String  processingJobId;
    private Instant processedAt;
    private String  checkpointId;
}
