package com.mehryar.example.kafkastreamserrorhandling.model;

public final class RecordStatus {
    public static final String SUCCESS  = "100";
    public static final String BAD_MAPPING = "101";
    public static final String BAD_JOIN = "102";
    public static final String EXCEPTION = "103";
}
