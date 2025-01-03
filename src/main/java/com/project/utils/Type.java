package com.project.utils;

public enum Type {
    FILE("file"),
    HIVE("hive");

    private final String type;

    Type(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
