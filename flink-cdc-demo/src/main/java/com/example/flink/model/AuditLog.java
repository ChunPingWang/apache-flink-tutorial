package com.example.flink.model;

import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditLog {

    private Long    id;
    private String  sourceTable;
    private String  operation;        // I / U / D
    private String  binlogFile;
    private Long    binlogPos;
    private String  gtid;
    private Instant capturedAt;
    private Instant processedAt;
    private String  status;           // RECEIVED / PROCESSED / FAILED
    private String  payloadBefore;    // JSON
    private String  payloadAfter;     // JSON
    private String  errorMsg;
    private String  jobId;
}
