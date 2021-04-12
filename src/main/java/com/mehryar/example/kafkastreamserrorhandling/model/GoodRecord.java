package com.mehryar.example.kafkastreamserrorhandling.model;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class GoodRecord {
    private String data;

}
