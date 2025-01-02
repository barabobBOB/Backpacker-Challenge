package com.project.utils;

public enum Status {
    SUCCESS("success"),
    FAIL("failure");

    private final String status;

    Status(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }
}
