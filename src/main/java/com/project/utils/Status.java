package com.project.utils;

public enum Status {
    SUCCESS("success"),
    FAILURE("failure");

    private final String status;

    Status(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }
}
