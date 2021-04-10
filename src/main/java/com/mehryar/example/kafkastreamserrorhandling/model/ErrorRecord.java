package com.mehryar.example.kafkastreamserrorhandling.model;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class ErrorRecord {
    private String srcTopic;
    private Long srcOffset;
    private String srcPartition;
}
