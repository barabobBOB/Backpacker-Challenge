package com.project;

import com.project.utils.Month;
import com.project.utils.TimePartition;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

public class Main {
    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {

        String timezone = "Asia/Seoul";
        String startMonth = "2019-Oct";
        String endMonth = "2019-Nov";
        TimePartition partition = TimePartition.MONTH;

        String inputPath = "./data/input/";
        String outputPath = "./data/partitions/";

        TimeZone.setDefault(TimeZone.getTimeZone(timezone));

        SparkSession spark = SparkSession.builder()
                .appName("ECommerceActivityLog")
                .master("local[*]")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.sql.parquet.compression.codec", "snappy")
                .enableHiveSupport()
                .getOrCreate();

        List<String> date = getMonthsInRange(startMonth, endMonth);
        String safeTimezone = timezone.replace("/", "_");

        // 파일 리스트 만들기
        for (String month : date) {
            Dataset<Row> monthData = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(inputPath + month + ".csv");
            Dataset<Row> processedData = monthData
                    .withColumn("event_time_" + safeTimezone, functions.from_utc_timestamp(functions.col("event_time"), timezone))
                    .withColumn("partition_date", functions.date_format(functions.col("event_time_" + safeTimezone), partition.getFormat()));

            processedData.write()
                    .mode(SaveMode.Overwrite)
                    .partitionBy("partition_date")
                    .parquet(outputPath + timezone + "/" + partition);

            monthData.unpersist();
            processedData.unpersist();
        }

        spark.sql("" +
                "CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_activity_log (" +
                "user_id STRING, " +
                "item_id STRING, " +
                "category_id STRING, " +
                "behavior STRING, " +
                "event_time STRING, " +
                "event_time_" +
                safeTimezone +
                " TIMESTAMP, " +
                "partition_date STRING" +
                ") STORED AS PARQUET " +
                "LOCATION '" + outputPath + "/" + timezone + "/" + partition + "'");

        spark.sql("SET hive.exec.dynamic.partition = true");
        spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict");
//        spark.sql("MSCK REPAIR TABLE ecommerce_activity_log"); // 파티션 정보 갱신
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
