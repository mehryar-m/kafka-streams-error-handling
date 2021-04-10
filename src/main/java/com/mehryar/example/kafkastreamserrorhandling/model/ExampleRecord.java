package com.mehryar.example.kafkastreamserrorhandling.model;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class ExampleRecord {

    private ExampleRecordState state;
    private String data;

}
