package com.mehryar.example.kafkastreamserrorhandling.model;

import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Builder
@Data
public class ErrorRecord {
    private String applicationId;
    private RecordStatus code;
    private String srcTopic;
    private long srcOffset;
    private int srcPartition;
    private Instant timeStamp;
}
