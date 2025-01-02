package com.project.utils;

import java.util.ArrayList;
import java.util.List;

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

    public Month next() {
        return values()[(this.ordinal() + 1) % 12];
    }

    public static Month fromString(String abbreviation) {
        for (Month month : values()) {
            if (month.abbreviation.equalsIgnoreCase(abbreviation)) {
                return month;
            }
        }
        throw new IllegalArgumentException("Invalid month abbreviation: " + abbreviation);
    }

    public static List<String> getMonthsInRange(String start, String end) {
        String[] startParts = start.split("-");
        String[] endParts = end.split("-");

        int startYear = Integer.parseInt(startParts[0]);
        String startMonth = startParts[1];

        int endYear = Integer.parseInt(endParts[0]);
        String endMonth = endParts[1];

        List<String> result = new ArrayList<>();
        int currentYear = startYear;
        Month currentMonth = Month.fromString(startMonth);

        while (!(currentYear == endYear && currentMonth == Month.fromString(endMonth).next())) {
            result.add(currentYear + "-" + currentMonth.getAbbreviation());

            currentMonth = currentMonth.next();
            if (currentMonth == Month.JAN) {
                currentYear++;
            }
        }

        return result;
    }
}

