package com.mehryar.example.kafkastreamserrorhandling.model;


import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RecordWrapper {
    private RecordStatus status;
    private Object data;
}
