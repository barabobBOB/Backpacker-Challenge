package com.project.entity;

public class ProcessStatus {
    private int id;
    private String partitionKey;
    private String processName;
    private String status;
    private String errorMessage;
    private String timezone;
    private String lastUpdated;
    private String type;

    public ProcessStatus(String partitionKey, String processName, String status, String errorMessage, String timezone, String type) {
        this.partitionKey = partitionKey;
        this.processName = processName;
        this.status = status;
        this.errorMessage = errorMessage;
        this.timezone = timezone;
        this.type = type;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getProcessName() {
        return processName;
    }

    public void setProcessName(String processName) {
        this.processName = processName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public String getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(String lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
