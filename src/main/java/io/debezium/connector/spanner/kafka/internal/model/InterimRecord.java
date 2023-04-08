package io.debezium.connector.spanner.kafka.internal.model;

public class InterimRecord {
    private boolean isOriginUcsb;
    private String record;

    public InterimRecord(String rec) {
        this.record = rec;
        this.isOriginUcsb = false;
    }

    public InterimRecord(String rec, boolean isUcsb) {
        this.record = rec;
        this.isOriginUcsb = isUcsb;
    }

    public String getRecord() {
        return this.record;
    }

    public boolean isOriginUcsb() {
        return this.isOriginUcsb;
    }
}
