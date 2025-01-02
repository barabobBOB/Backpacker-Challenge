package com.project.utils;

public enum TimePartition {
    YEAR("yyyy", "partition_year", "year"),
    MONTH("yyyy-MM", "partition_month", "month"),
    DAY("yyyy-MM-dd", "partition_day", "day"),
    HOUR("yyyy-MM-dd-HH", "partition_hour", "hour"),
    MINUTE("yyyy-MM-dd-HH-mm", "partition_minute", "minute"),
    SECOND("yyyy-MM-dd-HH-mm-ss", "partition_second", "second");

    private final String format;
    private final String columnName;
    private final String inputType;

    TimePartition(String format, String columnName, String inputType) {
        this.format = format;
        this.columnName = columnName;
        this.inputType = inputType;
    }

    public String getFormat() {
        return format;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getInputType() {
        return inputType;
    }

    public String getColumnValueExpr(String sourceColumn) {
        return "date_format(" + sourceColumn + ", '" + format + "')";
    }
}
