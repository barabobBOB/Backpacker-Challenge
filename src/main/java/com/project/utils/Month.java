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

    /**
     * 주어진 월의 축약형 문자열(abbreviation)을 기반으로 해당 월(Month) 객체를 반환합니다.
     *
     * @param abbreviation 월의 축약형 문자열 (예: "Jan", "Feb", "Mar" 등)
     * @return 주어진 축약형에 해당하는 Month 객체
     * @throws IllegalArgumentException 유효하지 않은 축약형 문자열이 주어졌을 경우 예외 발생
     */
    public static Month fromString(String abbreviation) {
        for (Month month : values()) {
            if (month.abbreviation.equalsIgnoreCase(abbreviation)) {
                return month;
            }
        }
        throw new IllegalArgumentException("Invalid month abbreviation: " + abbreviation);
    }

    /**
     * 주어진 시작 월과 종료 월 범위 내의 모든 월을 리스트로 반환합니다.
     *
     * @param start 시작 월 (형식: "yyyy-MM", 예: "2019-01")
     * @param end 종료 월 (형식: "yyyy-MM", 예: "2019-12")
     * @return 시작 월과 종료 월 사이의 월을 포함하는 리스트
     */
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

