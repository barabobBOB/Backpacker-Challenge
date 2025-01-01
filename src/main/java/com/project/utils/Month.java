package com.project.utils;

public enum Month {
    JAN("Jan"),
    FEB("Feb"),
    MAR("Mar"),
    APR("Apr"),
    MAY("May"),
    JUN("Jun"),
    JUL("Jul"),
    AUG("Aug"),
    SEP("Sep"),
    OCT("Oct"),
    NOV("Nov"),
    DEC("Dec");

    private final String abbreviation;

    Month(String abbreviation) {
        this.abbreviation = abbreviation;
    }

    public String getAbbreviation() {
        return abbreviation;
    }

    // 다음 월 반환
    public Month next() {
        return values()[(this.ordinal() + 1) % 12];
    }

    // 문자열로부터 Enum 값 반환
    public static Month fromString(String abbreviation) {
        for (Month month : values()) {
            if (month.abbreviation.equalsIgnoreCase(abbreviation)) {
                return month;
            }
        }
        throw new IllegalArgumentException("Invalid month abbreviation: " + abbreviation);
    }
}

